/* Base class for process which is reading from redo log files
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "../common/AlignedAllocCompat.h"
#include "../common/RedoLogRecord.h"
#include "../common/exception/RuntimeException.h"
#include "../common/metrics/Metrics.h"
#include "../common/types/Seq.h"
#include "Reader.h"

namespace OpenLogReplicator {
    const char* Reader::REDO_MSG[]{
        "OK",
        "OVERWRITTEN",
        "FINISHED",
        "STOPPED",
        "SHUTDOWN",
        "EMPTY",
        "READ ERROR",
        "WRITE ERROR",
        "SEQUENCE ERROR",
        "CRC ERROR",
        "BLOCK ERROR",
        "BAD DATA ERROR",
        "OTHER ERROR"
    };

    Reader::Reader(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum):
        Thread(newCtx, std::move(newAlias)),
        database(std::move(newDatabase)),
        configuredBlockSum(newConfiguredBlockSum),
        group(newGroup) {}

    void Reader::initialize() {
        if (redoBufferList == nullptr) {
            redoBufferList = new uint8_t*[ctx->memoryChunksReadBufferMax];
            memset(redoBufferList, 0, ctx->memoryChunksReadBufferMax * sizeof(uint8_t*));
        }

        if (headerBuffer == nullptr) {
            headerBuffer = static_cast<uint8_t*>(aligned_alloc(Ctx::MEMORY_ALIGNMENT, PAGE_SIZE_MAX * 2));
            if (unlikely(headerBuffer == nullptr))
                throw RuntimeException(10016, "couldn't allocate " + std::to_string(PAGE_SIZE_MAX * 2) +
                                       " bytes memory for: read header");
        }

        if (!ctx->redoCopyPath.empty()) {
            if (opendir(ctx->redoCopyPath.c_str()) == nullptr)
                throw RuntimeException(10012, "directory: " + ctx->redoCopyPath + " - can't read");
        }
    }

    void Reader::wakeUp() {
        contextSet(CONTEXT::MUTEX, REASON::READER_WAKE_UP);
        {
            std::unique_lock const lck(mtx);
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();
            condParserSleeping.notify_all();
        }
        contextSet(CONTEXT::CPU);
    }

    Reader::~Reader() {
        for (uint num = 0; num < ctx->memoryChunksReadBufferMax; ++num)
            bufferFree(this, num);

        delete[] redoBufferList;
        redoBufferList = nullptr;

        if (headerBuffer != nullptr) {
            free(headerBuffer);
            headerBuffer = nullptr;
        }

        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }
    }

    Reader::REDO_CODE Reader::checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint) {
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_CODE::EMPTY;

        if ((blockSize == 512 && buffer[1] != 0x22) ||
            (blockSize == 1024 && buffer[1] != 0x22) ||
            (blockSize == 4096 && buffer[1] != 0x82)) {
            ctx->error(40001, "file: " + fileName + " - block: " + std::to_string(blockNumber) + " - invalid block size: " +
                       std::to_string(blockSize) + ", header[1]: " + std::to_string(static_cast<uint>(buffer[1])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        const typeBlk blockNumberHeader = ctx->read32(buffer + 4);
        const auto sequenceHeader = Seq(ctx->read32(buffer + 8));

        if (sequence == Seq::zero() || status == STATUS::UPDATE) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                if (sequence != sequenceHeader) {
                    ctx->warning(60024, "file: " + fileName + " - invalid header sequence, found: " + sequenceHeader.toString() +
                                 ", expected: " + sequence.toString());
                    return REDO_CODE::ERROR_SEQUENCE;
                }
            } else {
                if (sequence > sequenceHeader)
                    return REDO_CODE::EMPTY;
                if (sequence < sequenceHeader)
                    return REDO_CODE::OVERWRITTEN;
            }
        }

        if (blockNumberHeader != blockNumber) {
            ctx->error(40002, "file: " + fileName + " - invalid header block number: " + std::to_string(blockNumberHeader) +
                       ", expected: " + std::to_string(blockNumber));
            return REDO_CODE::ERROR_BLOCK;
        }

        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
            const typeSum chSum = ctx->read16(buffer + 14);
            const typeSum chSumCalculated = calcChSum(buffer, blockSize);
            if (chSum != chSumCalculated) {
                if (showHint) {
                    ctx->warning(60025, "file: " + fileName + " - block: " + std::to_string(blockNumber) +
                                 " - invalid header checksum, expected: " + std::to_string(chSum) + ", calculated: " +
                                 std::to_string(chSumCalculated));
                    if (!hintDisplayed) {
                        if (!configuredBlockSum) {
                            ctx->hint("set DB_BLOCK_CHECKSUM = TYPICAL on the database or turn off consistency checking in OpenLogReplicator"
                                    " setting parameter disable-checks: " + std::to_string(static_cast<int>(Ctx::DISABLE_CHECKS::BLOCK_SUM)) +
                                    " for the reader");
                        }
                        hintDisplayed = true;
                    }
                }
                return REDO_CODE::ERROR_CRC;
            }
        }

        return REDO_CODE::OK;
    }

    void Reader::refreshFileSize() {
        // Subclass specific: nothing to refresh by default
    }

    uint Reader::readSize(uint prevRead) {
        if (prevRead < blockSize)
            return blockSize;

        prevRead *= 2;
        prevRead = std::min<uint64_t>(prevRead, Ctx::MEMORY_CHUNK_SIZE);

        return prevRead;
    }

    Reader::REDO_CODE Reader::reloadHeaderRead() {
        if (ctx->softShutdown)
            return REDO_CODE::ERROR;

        int actualRead = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2 : PAGE_SIZE_MAX * 2);
        if (actualRead < Ctx::MIN_BLOCK_SIZE) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            return REDO_CODE::ERROR_READ;
        }
        if (ctx->metrics != nullptr)
            ctx->metrics->emitBytesRead(actualRead);

        // Check file header
        if (headerBuffer[0] != 0) {
            ctx->error(40003, "file: " + fileName + " - invalid header[0]: " + std::to_string(static_cast<uint>(headerBuffer[0])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        if (headerBuffer[28] == 0x7A && headerBuffer[29] == 0x7B && headerBuffer[30] == 0x7C && headerBuffer[31] == 0x7D) {
            if (!ctx->isBigEndian())
                ctx->setBigEndian();
        } else if (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A || ctx->isBigEndian()) {
            ctx->error(40004, "file: " + fileName + " - invalid header[28-31]: " + std::to_string(static_cast<uint>(headerBuffer[28])) +
                       ", " + std::to_string(static_cast<uint>(headerBuffer[29])) + ", " + std::to_string(static_cast<uint>(headerBuffer[30])) +
                       ", " + std::to_string(static_cast<uint>(headerBuffer[31])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        bool blockSizeOK = false;
        blockSize = ctx->read32(headerBuffer + 20);
        if ((blockSize == 512 && headerBuffer[1] == 0x22) || (blockSize == 1024 && headerBuffer[1] == 0x22) || (blockSize == 4096 && headerBuffer[1] == 0x82))
            blockSizeOK = true;

        if (!blockSizeOK) {
            ctx->error(40005, "file: " + fileName + " - invalid block size: " + std::to_string(blockSize) + ", header[1]: " +
                       std::to_string(static_cast<uint>(headerBuffer[1])));
            blockSize = 0;
            return REDO_CODE::ERROR_BAD_DATA;
        }

        if (actualRead < static_cast<int>(blockSize * 2)) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            return REDO_CODE::ERROR_READ;
        }

        if (!ctx->redoCopyPath.empty()) {
            if (static_cast<uint>(actualRead) > blockSize * 2)
                actualRead = static_cast<int>(blockSize * 2);

            const auto sequenceHeader = Seq(ctx->read32(headerBuffer + blockSize + 8));
            if (fileCopySequence != sequenceHeader) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }
            }

            if (fileCopyDes == -1) {
                fileNameWrite = ctx->redoCopyPath + "/" + database + "_" + sequenceHeader.toString() + ".arc";
                fileCopyDes = open(fileNameWrite.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
                if (unlikely(fileCopyDes == -1))
                    throw RuntimeException(10006, "file: " + fileNameWrite + " - open for writing returned: " + strerror(errno));
                ctx->info(0, "writing redo log copy to: " + fileNameWrite);
                fileCopySequence = sequenceHeader;
            }

            const int bytesWritten = pwrite(fileCopyDes, headerBuffer, actualRead, 0);
            if (bytesWritten != actualRead) {
                ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                           std::to_string(actualRead) + ", code returned: " + strerror(errno));
                return REDO_CODE::ERROR_WRITE;
            }
        }

        return REDO_CODE::OK;
    }

    Reader::REDO_CODE Reader::reloadHeader() {
        REDO_CODE retReload = reloadHeaderRead();
        if (retReload != REDO_CODE::OK)
            return retReload;

        uint32_t version;
        compatVsn = ctx->read32(headerBuffer + blockSize + 20);
        if (compatVsn == 0)
            return REDO_CODE::EMPTY;

        if ((compatVsn >= 0x0B200000 && compatVsn <= 0x0B200400)     // 11.2.0.0 - 11.2.0.4
            || (compatVsn >= 0x0C100000 && compatVsn <= 0x0C100200)  // 12.1.0.0 - 12.1.0.2
            || (compatVsn >= 0x0C200000 && compatVsn <= 0x0C200100)  // 12.2.0.0 - 12.2.0.1
            || (compatVsn >= 0x12000000 && compatVsn <= 0x120E0000)  // 18.0.0.0 - 18.14.0.0
            || (compatVsn >= 0x13000000 && compatVsn <= 0x131C0000)  // 19.0.0.0 - 19.28.0.0
            || (compatVsn >= 0x15000000 && compatVsn <= 0x15080000)  // 21.0.0.0 - 21.8.0.0
            || (compatVsn >= 0x17000000 && compatVsn <= 0x171A2000)) // 23.0.0.0 - 23.26.2.0 (23free, 23ai, 26ai)
            version = compatVsn;
        else {
            ctx->error(40006, "file: " + fileName + " - invalid database version: " + std::to_string(compatVsn));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        activation = ctx->read32(headerBuffer + blockSize + 52);
        numBlocksHeader = ctx->read32(headerBuffer + blockSize + 156);
        resetlogs = ctx->read32(headerBuffer + blockSize + 160);
        thread = ctx->read16(headerBuffer + blockSize + 176);
        firstScnHeader = ctx->readScn(headerBuffer + blockSize + 180);
        firstTimeHeader = ctx->read32(headerBuffer + blockSize + 188);
        nextScnHeader = ctx->readScn(headerBuffer + blockSize + 192);
        nextTime = ctx->read32(headerBuffer + blockSize + 200);

        if (numBlocksHeader != Ctx::ZERO_BLK && group == 0) {
            // The block count from the redo header is authoritative when it says *more* than the size from
            // stat: the size from stat can be too small or zero (a filesystem bridge such as asmfs reports 0
            // for a file that is not fully visible yet) and the reader would then poll forever on a position
            // at or above the file size - without notifying the parser, which waits for the end of the archive
            // in checkFinished, and without ever reading the rest of the archive.
            const uint64_t fileSizeHeader = static_cast<uint64_t>(numBlocksHeader) * blockSize;
            if (fileSizeHeader > fileSize) {
                fileSize = fileSizeHeader;
                ctx->info(0, "raising redo log size to: " + std::to_string(fileSize) + " for: " + fileName);
            } else if (fileSizeHeader < fileSize) {
                // A smaller header must not shorten the archive: the header block count of an archived log is
                // not always the size of the file - measured 131072 for a 242688 byte archive, and lowering
                // the size there stopped the reader in the middle of the archive and skipped a whole committed
                // transaction in silence. The padding case the old correction was for ends the same way: the
                // read loop stops at the end of the file and the blocks behind the last redo are empty (see
                // archiveEndConfirmed).
                ctx->info(0, "keeping redo log size: " + std::to_string(fileSize) + " (header block count suggests: " +
                          std::to_string(fileSizeHeader) + ") for: " + fileName);
            }
        }

        if (ctx->version == 0) {
            char SID[9];
            memcpy(SID, headerBuffer + blockSize + 28, 8);
            SID[8] = 0;
            ctx->version = version;
            if (compatVsn >= RedoLogRecord::REDO_VERSION_23_0)
                ctx->columnLimit = Ctx::COLUMN_LIMIT_23_0;
            const auto sequenceHeader = Seq(ctx->read32(headerBuffer + blockSize + 8));

            if (compatVsn < RedoLogRecord::REDO_VERSION_18_0) {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 20) & 0xF) + "." +
                        std::to_string((compatVsn >> 16) & 0xF) + "." + std::to_string((compatVsn >> 8) & 0xFF);
            } else {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 16) & 0xFF) + "." +
                        std::to_string((compatVsn >> 8) & 0xFF);
            }
            ctx->info(0, "found redo log version: " + ctx->versionStr + ", activation: " + std::to_string(activation) + ", resetlogs: " +
                      std::to_string(resetlogs) + ", page: " + std::to_string(blockSize) + ", sequence: " + sequenceHeader.toString() +
                      ", SID: " + SID + ", endian: " + (ctx->isBigEndian() ? "BIG" : "LITTLE"));
        }

        if (version != ctx->version) {
            ctx->error(40007, "file: " + fileName + " - invalid database version: " + std::to_string(compatVsn) + ", expected: " +
                       std::to_string(ctx->version));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        uint badBlockCrcCount = 0;
        retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "block: 1 check: " + std::to_string(static_cast<uint>(retReload)));

        while (retReload == REDO_CODE::ERROR_CRC) {
            ++badBlockCrcCount;
            if (badBlockCrcCount == BAD_CDC_MAX_CNT)
                return REDO_CODE::ERROR_BAD_DATA;

            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(ctx->redoReadSleepUs);
            contextSet(CONTEXT::CPU);
            retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "block: 1 check: " + std::to_string(static_cast<uint>(retReload)));
        }

        if (retReload != REDO_CODE::OK)
            return retReload;

        if (firstScn == Scn::none() || status == STATUS::UPDATE) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            if (firstScnHeader != firstScn) {
                ctx->error(40008, "file: " + fileName + " - invalid first scn value: " + firstScnHeader.toString() + ", expected: " + firstScn.toString());
                return REDO_CODE::ERROR_BAD_DATA;
            }
        }

        // Updating nextScn if changed
        if (nextScn == Scn::none() && nextScnHeader != Scn::none()) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "updating next scn to: " + nextScnHeader.toString());
            nextScn = nextScnHeader;
        } else if (nextScn != Scn::none() && nextScnHeader != Scn::none() && nextScn != nextScnHeader) {
            ctx->error(40009, "file: " + fileName + " - invalid next scn value: " + nextScnHeader.toString() + ", expected: " +
                       nextScn.toString());
            return REDO_CODE::ERROR_BAD_DATA;
        }

        return retReload;
    }

    bool Reader::read1() {
        uint toRead = readSize(lastRead);

        if (bufferScan + toRead > fileSize)
            toRead = fileSize - bufferScan;

        const uint64_t redoBufferPos = bufferScan % Ctx::MEMORY_CHUNK_SIZE;
        const uint64_t redoBufferNum = (bufferScan / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;
        if (redoBufferPos + toRead > Ctx::MEMORY_CHUNK_SIZE)
            toRead = Ctx::MEMORY_CHUNK_SIZE - redoBufferPos;

        if (toRead == 0) {
            ctx->error(40010, "file: " + fileName + " - zero to read, start: " + std::to_string(bufferStart) + ", end: " +
                       std::to_string(bufferEnd) + ", scan: " + std::to_string(bufferScan));
            ret = REDO_CODE::ERROR;
            return false;
        }

        bufferAllocate(redoBufferNum);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "reading#1 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                          std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") bytes: " + std::to_string(toRead));
        const int actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferScan, toRead);

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "reading#1 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                          std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") got: " + std::to_string(actualRead));
        if (actualRead < 0) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            ret = REDO_CODE::ERROR_READ;
            return false;
        }
        if (ctx->metrics != nullptr)
            ctx->metrics->emitBytesRead(actualRead);

        if (actualRead > 0 && fileCopyDes != -1 && (ctx->redoVerifyDelayUs == 0 || group == 0)) {
            const int bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead, bufferEnd);
            if (bytesWritten != actualRead) {
                ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                           std::to_string(actualRead) + ", code returned: " + strerror(errno));
                ret = REDO_CODE::ERROR_WRITE;
                return false;
            }
        }

        const typeBlk maxNumBlock = actualRead / blockSize;
        const typeBlk bufferScanBlock = bufferScan / blockSize;
        uint goodBlocks = 0;
        auto currentRet = REDO_CODE::OK;

        // Check which blocks are good
        for (typeBlk numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize), bufferScanBlock + numBlock,
                                          ctx->redoVerifyDelayUs == 0 || group == 0);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "block: " + std::to_string(bufferScanBlock + numBlock) + " check: " +
                              std::to_string(static_cast<uint>(currentRet)));

            if (currentRet != REDO_CODE::OK)
                break;
            ++goodBlocks;
        }

        // Partial online redo log file
        if (goodBlocks == 0 && group == 0) {
            // Not a single good block was found although the archive may still hold data at this position: a
            // filesystem bridge (asmfs) can report a size the file does not have yet, answer the read with
            // zero bytes or answer a shorter size - and it can return zero filled blocks over data it has not
            // made visible yet. Reporting the archive as finished here would silently skip the rest of it and
            // move the checkpoint past redo that was never read, so verify the end before accepting it
            // (see archiveEndConfirmed) - if it is not the end, stop instead: the position is saved and the
            // archive is read again on the next scheduler pass.
            if (!archiveEndConfirmed(this)) {
                ret = REDO_CODE::STOPPED;
                return false;
            }

            if (nextScnHeader != Scn::none()) {
                ret = REDO_CODE::FINISHED;
                nextScn = nextScnHeader;
            } else {
                ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) + " - unexpected end of file");
                ret = REDO_CODE::STOPPED;
            }
            return false;
        }

        // Treat bad blocks as empty
        if (currentRet == REDO_CODE::ERROR_CRC && ctx->redoVerifyDelayUs > 0 && group != 0)
            currentRet = REDO_CODE::EMPTY;

        if (goodBlocks == 0 && currentRet != REDO_CODE::OK && currentRet != REDO_CODE::EMPTY) {
            ret = currentRet;
            return false;
        }

        // Check for log switch
        if (goodBlocks == 0 && currentRet == REDO_CODE::EMPTY) {
            currentRet = reloadHeader();
            if (currentRet != REDO_CODE::OK) {
                ret = currentRet;
                return false;
            }
            reachedZero = true;
        } else {
            readBlocks = true;
            reachedZero = false;
        }

        lastRead = goodBlocks * blockSize;
        lastReadTime = ctx->clock->getTimeUt();
        if (goodBlocks > 0) {
            if (ctx->redoVerifyDelayUs > 0 && group != 0) {
                bufferScan += goodBlocks * blockSize;

                for (uint numBlock = 0; numBlock < goodBlocks; ++numBlock) {
                    auto* readTimeP = reinterpret_cast<time_t*>(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize));
                    *readTimeP = lastReadTime;
                }
            } else {
                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_READ1);
                    std::unique_lock const lck(mtx);
                    bufferEnd += goodBlocks * blockSize;
                    bufferScan = bufferEnd;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
            }
        }

        // Batch mode with a partial online redo log file.
        // A block carrying another sequence is not "data that could not be read": it is a block of a
        // different redo log, and that is precisely how this file ends here. The end verification used for
        // the size driven decisions (archiveEndConfirmed) must therefore not be applied: it reads a
        // non-empty block as data that was skipped, which by construction is always the case at this point.
        if (currentRet == REDO_CODE::ERROR_SEQUENCE && group == 0) {
            if (nextScnHeader != Scn::none()) {
                ret = REDO_CODE::FINISHED;
                nextScn = nextScnHeader;
            } else {
                ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) + " - unexpected end of file");
                ret = REDO_CODE::STOPPED;
            }
            return false;
        }

        return true;
    }

    bool Reader::read2() {
        uint maxNumBlock = (bufferScan - bufferEnd) / blockSize;
        uint goodBlocks = 0;
        maxNumBlock = std::min<uint64_t>(maxNumBlock, Ctx::MEMORY_CHUNK_SIZE / blockSize);

        for (uint numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            const uint64_t redoBufferPos = (bufferEnd + numBlock * blockSize) % Ctx::MEMORY_CHUNK_SIZE;
            const uint64_t redoBufferNum = ((bufferEnd + numBlock * blockSize) / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;

            const auto* const readTimeP = reinterpret_cast<const time_ut*>(redoBufferList[redoBufferNum] + redoBufferPos);
            if (*readTimeP + static_cast<time_ut>(ctx->redoVerifyDelayUs) < loopTime) {
                ++goodBlocks;
            } else {
                readTime = *readTimeP + static_cast<time_t>(ctx->redoVerifyDelayUs);
                break;
            }
        }

        if (goodBlocks > 0) {
            uint toRead = readSize(goodBlocks * blockSize);
            toRead = std::min(toRead, goodBlocks * blockSize);

            const uint64_t redoBufferPos = bufferEnd % Ctx::MEMORY_CHUNK_SIZE;
            const uint64_t redoBufferNum = (bufferEnd / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;

            if (redoBufferPos + toRead > Ctx::MEMORY_CHUNK_SIZE)
                toRead = Ctx::MEMORY_CHUNK_SIZE - redoBufferPos;

            if (toRead == 0) {
                ctx->error(40011, "zero to read (start: " + std::to_string(bufferStart) + ", end: " + std::to_string(bufferEnd) +
                           ", scan: " + std::to_string(bufferScan) + "): " + fileName);
                ret = REDO_CODE::ERROR;
                return false;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "reading#2 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                              std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") bytes: " + std::to_string(toRead));
            const int actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferEnd, toRead);

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "reading#2 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                              std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") got: " + std::to_string(actualRead));

            if (actualRead < 0) {
                ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
                ret = REDO_CODE::ERROR_READ;
                return false;
            }
            if (ctx->metrics != nullptr)
                ctx->metrics->emitBytesRead(actualRead);

            if (actualRead > 0 && fileCopyDes != -1) {
                const int bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead, bufferEnd);
                if (bytesWritten != actualRead) {
                    ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) +
                               " bytes written instead of " + std::to_string(actualRead) + ", code returned: " + strerror(errno));
                    ret = REDO_CODE::ERROR_WRITE;
                    return false;
                }
            }

            readBlocks = true;
            auto currentRet = REDO_CODE::OK;
            maxNumBlock = actualRead / blockSize;
            const typeBlk bufferEndBlock = bufferEnd / blockSize;

            // Check which blocks are good
            for (uint numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize),
                                              bufferEndBlock + numBlock, true);
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                    ctx->logTrace(Ctx::TRACE::DISK, "block: " + std::to_string(bufferEndBlock + numBlock) + " check: " +
                                  std::to_string(static_cast<uint>(currentRet)));

                if (currentRet != REDO_CODE::OK)
                    break;
                ++goodBlocks;
            }

            // Verify header for online redo logs after every successful read
            if (currentRet == REDO_CODE::OK && group > 0)
                currentRet = reloadHeader();

            if (currentRet != REDO_CODE::OK) {
                ret = currentRet;
                return false;
            }

            {
                contextSet(CONTEXT::MUTEX, REASON::READER_READ2);
                std::unique_lock const lck(mtx);
                bufferEnd += actualRead;
                condParserSleeping.notify_all();
            }
            contextSet(CONTEXT::CPU);
        }

        return true;
    }

    void Reader::mainLoop() {
        while (!ctx->softShutdown) {
            {
                contextSet(CONTEXT::MUTEX, REASON::READER_MAIN1);
                std::unique_lock lck(mtx);
                condParserSleeping.notify_all();

                if (status == STATUS::SLEEPING && !ctx->softShutdown) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                        ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:mainLoop:sleep");
                    contextSet(CONTEXT::WAIT, REASON::READER_NO_WORK);
                    // An unbounded wait here can deadlock with the scheduler: when the parser ends up in
                    // Reader::updateRedoLog (which waits for this reader to re-check the archive) while this
                    // reader waits for the parser to wake it up, both wait forever and the replication stops
                    // in silence - no warning, no watchdog. Waking up on a timer changes nothing in the data
                    // flow (a sleeping reader with no status change just goes back to sleep) and keeps the
                    // handshake observable.
                    condReaderSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                    contextSet(CONTEXT::MUTEX, REASON::READER_MAIN2);
                } else if (status == STATUS::READ && !ctx->softShutdown && (bufferEnd % Ctx::MEMORY_CHUNK_SIZE) == 0) {
                    ctx->warning(0, "buffer full?");
                }
            }
            contextSet(CONTEXT::CPU);

            if (ctx->softShutdown)
                break;

            if (status == STATUS::CHECK) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                    ctx->logTrace(Ctx::TRACE::FILE, "trying to open: " + fileName);
                redoClose();
                const REDO_CODE currentRet = redoOpen();
                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_STATUS);
                    std::unique_lock const lck(mtx);
                    ret = currentRet;
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
                continue;
            }

            if (status == STATUS::UPDATE) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }

                sumRead = 0;
                sumTime = 0;
                const REDO_CODE currentRet = reloadHeader();
                if (currentRet == REDO_CODE::OK) {
                    bufferStart = blockSize * 2;
                    bufferEnd = blockSize * 2;
                }

                for (uint num = 0; num < ctx->memoryChunksReadBufferMax; ++num)
                    bufferFree(this, num);

                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_SLEEP1);
                    std::unique_lock const lck(mtx);
                    ret = currentRet;
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
            } else if (status == STATUS::READ) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                    ctx->logTrace(Ctx::TRACE::DISK, "reading " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                  std::to_string(bufferEnd) + ") at size: " + std::to_string(fileSize));
                lastRead = blockSize;
                lastReadTime = 0;
                readTime = 0;
                bufferScan = bufferEnd;
                reachedZero = false;
                // Progress watchdog: consecutive loop iterations that did not read anything and did not
                // move the end of the buffer.
                uint64_t stallIterations = 0;
                uint64_t lastStallBufferEnd = 0;

                while (!ctx->softShutdown && status == STATUS::READ) {
                    loopTime = ctx->clock->getTimeUt();
                    readBlocks = false;
                    readTime = 0;

                    // ">=" and not "==": the size can be revised (reloadHeader, refreshFileSize) and the
                    // read loop must never be able to sit behind a position it has already passed - it
                    // would then neither read nor notify the parser and the whole run would stall without
                    // a single message. Both sizes are compared: fileSize is the one that may have been
                    // raised from the redo header, fileSizeStat the one the filesystem reported - a stale
                    // small answer for either of them must not end the archive (see archiveEndConfirmed).
                    const uint64_t realEnd = fileSizeStat > fileSize ? fileSizeStat : fileSize;
                    if (bufferEnd >= realEnd) {
                        // The end of the redo log reached - but the size known here can still be too small: the
                        // redo header block count is written while the archiver copies the log, and a
                        // filesystem bridge can bound the reads by the size the file had when the handle was
                        // opened (asmfs answers stat with the current size but clamps reads to the size
                        // snapshotted at open). Accepting such a size as the end of the archive skips its tail
                        // silently: the parser finishes the archive, the checkpoint moves past redo that was
                        // never read and the low-water-mark stops advancing (the transactions held in the
                        // commit sorter are never released again). Re-read the size before finishing: if the
                        // file turns out to be bigger, keep reading - and if the tail is still not readable,
                        // the read below stops the archive (60044) and it is picked up again with a fresh
                        // handle on the next pass.
                        const uint64_t fileSizeBefore = fileSize;
                        refreshFileSize();
                        if (group == 0 && numBlocksHeader != Ctx::ZERO_BLK) {
                            const uint64_t fileSizeHeader = static_cast<uint64_t>(numBlocksHeader) * blockSize;
                            if (fileSizeHeader > fileSize)
                                fileSize = fileSizeHeader;
                        }
                        if (fileSize > fileSizeBefore) {
                            ctx->info(0, "updating redo log size to: " + std::to_string(fileSize) + " for: " + fileName);
                            continue;
                        }

                        // Same rule as the other end-of-archive decision in this loop: the parser stopped
                        // before the size we trust in, so look at the file - real redo at the position means
                        // the end is bogus (a stale size, a zero filled block), stop and let the next pass
                        // with a fresh handle read the tail instead of skipping it in silence.
                        if (!archiveEndConfirmed(this)) {
                            ret = REDO_CODE::STOPPED;
                            break;
                        }

                        if (nextScnHeader != Scn::none()) {
                            ret = REDO_CODE::FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                                         " - unexpected end of file");
                            ret = REDO_CODE::STOPPED;
                        }
                        break;
                    }

                    // Buffer full?
                    if (bufferStart + ctx->bufferSizeMax == bufferEnd) {
                        contextSet(CONTEXT::MUTEX, REASON::READER_FULL);
                        std::unique_lock lck(mtx);
                        uint64_t fullIterations = 0;
                        while (!ctx->softShutdown && bufferStart + ctx->bufferSizeMax == bufferEnd) {
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                                ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:mainLoop:bufferFull");
                            contextSet(CONTEXT::WAIT, REASON::READER_BUFFER_FULL);
                            condBufferFull.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                            contextSet(CONTEXT::MUTEX, REASON::READER_FULL);
                            ++fullIterations;
                            // Never wait here in silence: if the parser does not release the read buffer
                            // (for example when it waits in Reader::checkFinished for this very reader),
                            // the whole replication stops without a single message in the log.
                            if (fullIterations == 200 || (fullIterations % 1200) == 0)
                                ctx->warning(60046, "file: " + fileName + " - the reader is waiting for free read buffer space for: " +
                                             std::to_string(fullIterations * ctx->redoReadSleepUs / 1000000) + " s (buffer: " +
                                             std::to_string(bufferStart) + "-" + std::to_string(bufferEnd) + ", limit: " +
                                             std::to_string(ctx->bufferSizeMax) + ", size: " + std::to_string(fileSize) +
                                             ", status: " + std::to_string(static_cast<int>(status.load())) + ", ret: " +
                                             REDO_MSG[static_cast<uint>(ret.load())] + ")");
                        }
                        contextSet(CONTEXT::CPU);
                        if (ctx->softShutdown)
                            break;
                        continue;
                    }

                    if (bufferEnd < bufferScan)
                        if (!read2())
                            break;

                    // The size taken at open time can be stale: refresh it before giving up on this position,
                    // otherwise the reader polls here without notifying the parser (waiting in checkFinished)
                    if (bufferScan >= fileSize)
                        refreshFileSize();

                    // #1 read
                    if (bufferScan < fileSize && (bufferIsFree() || (bufferScan % Ctx::MEMORY_CHUNK_SIZE) > 0)
                        && (!reachedZero || lastReadTime + static_cast<time_t>(ctx->redoReadSleepUs) < loopTime))
                        if (!read1())
                            break;

                    if (numBlocksHeader != Ctx::ZERO_BLK && bufferEnd >= static_cast<uint64_t>(numBlocksHeader) * blockSize) {
                        if (nextScnHeader != Scn::none()) {
                            ret = REDO_CODE::FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                                         " - unexpected end of file");
                            ret = REDO_CODE::STOPPED;
                        }
                        break;
                    }

                    // The reader can not make progress here: nothing was read and the position did not
                    // move (the file is smaller than the size the reader knows about, or no read buffer
                    // is free). Report it periodically instead of spinning in silence - this used to look
                    // like a hung process without a single message.
                    if (!readBlocks && bufferEnd == lastStallBufferEnd) {
                        ++stallIterations;
                        if (stallIterations == 200 || (stallIterations % 1200) == 0)
                            ctx->warning(60044, "file: " + fileName + " - no progress at position: " + std::to_string(bufferScan) +
                                         ", size: " + std::to_string(fileSize) + " for: " +
                                         std::to_string(stallIterations * ctx->redoReadSleepUs / 1000000) + " s");
                    } else {
                        stallIterations = 0;
                        lastStallBufferEnd = bufferEnd;
                    }

                    // Sleep some time
                    if (!readBlocks) {
                        if (readTime == 0) {
                            contextSet(CONTEXT::SLEEP);
                            ctx->usleepInt(ctx->redoReadSleepUs);
                            contextSet(CONTEXT::CPU);
                        } else {
                            const time_ut nowTime = ctx->clock->getTimeUt();
                            if (readTime > nowTime) {
                                if (static_cast<time_ut>(ctx->redoReadSleepUs) < readTime - nowTime) {
                                    contextSet(CONTEXT::SLEEP);
                                    ctx->usleepInt(ctx->redoReadSleepUs);
                                    contextSet(CONTEXT::CPU);
                                } else {
                                    contextSet(CONTEXT::SLEEP);
                                    ctx->usleepInt(readTime - nowTime);
                                    contextSet(CONTEXT::CPU);
                                }
                            }
                        }
                    }
                }

                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_SLEEP2);
                    std::unique_lock const lck(mtx);
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
            }
        }
    }

    typeSum Reader::calcChSum(uint8_t* buffer, uint size) const {
        const typeSum oldChSum = ctx->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint i = 0; i < size / 8; ++i, buffer += sizeof(uint64_t))
            sum ^= *reinterpret_cast<const uint64_t*>(buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void Reader::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "reader (" + ss.str() + ") start");
        }

        try {
            mainLoop();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        redoClose();
        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "reader (" + ss.str() + ") stop");
        }
    }

    void Reader::bufferAllocate(uint num) {
        {
            contextSet(CONTEXT::MUTEX, REASON::READER_ALLOCATE1);
            std::unique_lock const lck(mtx);
            if (redoBufferList[num] != nullptr) {
                contextSet(CONTEXT::CPU);
                return;
            }
        }
        contextSet(CONTEXT::CPU);

        uint8_t* buffer = ctx->getMemoryChunk(this, Ctx::MEMORY::READER);

        {
            contextSet(CONTEXT::MUTEX, REASON::READER_ALLOCATE2);
            std::unique_lock const lck(mtx);
            redoBufferList[num] = buffer;
            --ctx->bufferSizeFree;
        }
        contextSet(CONTEXT::CPU);
    }

    void Reader::bufferFree(Thread* t, uint num) {
        uint8_t* buffer;
        {
            t->contextSet(CONTEXT::MUTEX, REASON::READER_FREE);
            std::unique_lock const lck(mtx);
            if (redoBufferList[num] == nullptr) {
                t->contextSet(CONTEXT::CPU);
                return;
            }
            buffer = redoBufferList[num];
            redoBufferList[num] = nullptr;
            ++ctx->bufferSizeFree;
        }
        t->contextSet(CONTEXT::CPU);

        ctx->freeMemoryChunk(this, Ctx::MEMORY::READER, buffer);
    }

    bool Reader::bufferIsFree() {
        bool isFree;
        {
            contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_FREE);
            std::unique_lock const lck(mtx);
            isFree = (ctx->bufferSizeFree > 0);
        }
        contextSet(CONTEXT::CPU);
        return isFree;
    }

    void Reader::printHeaderInfo(std::ostringstream& ss, const std::string& path) const {
        char SID[9];
        memcpy(SID, headerBuffer + blockSize + 28, 8);
        SID[8] = 0;

        ss << "DUMP OF REDO FROM FILE '" << path << "'\n";
        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2)
            ss << " Container ID: 0\n Container UID: 0\n";
        ss << " Opcodes *.*\n";
        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2)
            ss << " Container ID: 0\n Container UID: 0\n";
        ss << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff\n";
        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
            ss << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff\n";
        else
            ss << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff\n";
        ss << " Times: creation thru eternity\n";

        const uint32_t dbid = ctx->read32(headerBuffer + blockSize + 24);
        const uint32_t controlSeq = ctx->read32(headerBuffer + blockSize + 36);
        const uint32_t fileSizeHeader = ctx->read32(headerBuffer + blockSize + 40);
        const uint16_t fileNumber = ctx->read16(headerBuffer + blockSize + 48);

        ss << " FILE HEADER:\n" <<
                "\tCompatibility Vsn = " << std::dec << compatVsn << "=0x" << std::hex << compatVsn << '\n' <<
                "\tDb ID=" << std::dec << dbid << "=0x" << std::hex << dbid << ", Db Name='" << SID << "'\n" <<
                "\tActivation ID=" << std::dec << activation << "=0x" << std::hex << activation << '\n' <<
                "\tControl Seq=" << std::dec << controlSeq << "=0x" << std::hex << controlSeq << ", File size=" << std::dec << fileSizeHeader << "=0x" <<
                std::hex << fileSizeHeader << '\n' <<
                "\tFile Number=" << std::dec << fileNumber << ", Blksiz=" << std::dec << blockSize << ", File Type=2 LOG\n";

        const auto seq = Seq(ctx->read32(headerBuffer + blockSize + 8));
        uint8_t descrip[65];
        memcpy(descrip, headerBuffer + blockSize + 92, 64);
        descrip[64] = 0;
        const uint32_t hws = ctx->read32(headerBuffer + blockSize + 172);
        const uint8_t eot = headerBuffer[blockSize + 204];
        const uint8_t dis = headerBuffer[blockSize + 205];

        ss << R"( descrip:")" << descrip << R"(")" << '\n' <<
                " thread: " << std::dec << thread <<
                " nab: 0x" << std::hex << numBlocksHeader <<
                " seq: " << seq.toStringHex(8) <<
                " hws: 0x" << std::hex << hws <<
                " eot: " << std::dec << static_cast<uint>(eot) <<
                " dis: " << std::dec << static_cast<uint>(dis) << '\n';

        const Scn resetlogsScn = ctx->readScn(headerBuffer + blockSize + 164);
        const typeResetlogs prevResetlogsCnt = ctx->read32(headerBuffer + blockSize + 292);
        const Scn prevResetlogsScn = ctx->readScn(headerBuffer + blockSize + 284);
        const Scn enabledScn = ctx->readScn(headerBuffer + blockSize + 208);
        const Time enabledTime(ctx->read32(headerBuffer + blockSize + 216));
        const Scn threadClosedScn = ctx->readScn(headerBuffer + blockSize + 220);
        const Time threadClosedTime(ctx->read32(headerBuffer + blockSize + 228));
        const Scn termialRecScn = ctx->readScn(headerBuffer + blockSize + 240);
        const Time termialRecTime(ctx->read32(headerBuffer + blockSize + 248));
        const Scn mostRecentScn = ctx->readScn(headerBuffer + blockSize + 260);
        const typeSum chSum = ctx->read16(headerBuffer + blockSize + 14);
        const typeSum chSum2 = calcChSum(headerBuffer + blockSize, blockSize);

        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << resetlogsScn.to48() <<
                    " (" << resetlogsScn.toString() << ")\n" <<
                    " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << prevResetlogsScn.to48() <<
                    " (" << prevResetlogsScn.toString() << ")\n" <<
                    " Low  scn: " << firstScnHeader.to48() << " (" << firstScnHeader.toString() << ")" << " " << firstTimeHeader << '\n' <<
                    " Next scn: " << nextScnHeader.to48() << " (" << nextScn.toString() << ")" << " " << nextTime << '\n' <<
                    " Enabled scn: " << enabledScn.to48() << " (" << enabledScn.toString() << ")" << " " << enabledTime << '\n' <<
                    " Thread closed scn: " << threadClosedScn.to48() << " (" << threadClosedScn.toString() << ")" <<
                    " " << threadClosedTime << '\n' <<
                    " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << '\n' <<
                    " Terminal recovery stop scn: " << termialRecScn.to48() << '\n' <<
                    " Terminal recovery  " << termialRecTime << '\n' <<
                    " Most recent redo scn: " << mostRecentScn.to48() << '\n';
        } else {
            const Scn realNextScn = ctx->readScn(headerBuffer + blockSize + 272);

            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << resetlogsScn.to64() << '\n' <<
                    " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << prevResetlogsScn.to64() << '\n' <<
                    " Low  scn: " << firstScnHeader.to64() << " " << firstTimeHeader << '\n' <<
                    " Next scn: " << nextScnHeader.to64() << " " << nextTime << '\n' <<
                    " Enabled scn: " << enabledScn.to64() << " " << enabledTime << '\n' <<
                    " Thread closed scn: " << threadClosedScn.to64() << " " << threadClosedTime << '\n' <<
                    " Real next scn: " << realNextScn.to64() << '\n' <<
                    " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << '\n' <<
                    " Terminal recovery stop scn: " << termialRecScn.to64() << '\n' <<
                    " Terminal recovery  " << termialRecTime << '\n' <<
                    " Most recent redo scn: " << mostRecentScn.to64() << '\n';
        }

        const uint32_t largestLwn = ctx->read32(headerBuffer + blockSize + 268);
        ss << " Largest LWN: " << std::dec << largestLwn << " blocks\n";

        const uint32_t miscFlags = ctx->read32(headerBuffer + blockSize + 236);
        const char* endOfRedo;
        if ((miscFlags & FLAGS_END) != 0)
            endOfRedo = "Yes";
        else
            endOfRedo = "No";
        if ((miscFlags & FLAGS_CLOSEDTHREAD) != 0)
            ss << " FailOver End-of-redo stream : " << endOfRedo << '\n';
        else
            ss << " End-of-redo stream : " << endOfRedo << '\n';

        if ((miscFlags & FLAGS_ASYNC) != 0)
            ss << " Archivelog created using asynchronous network transmittal" << '\n';

        if ((miscFlags & FLAGS_NODATALOSS) != 0)
            ss << " No ctx-loss mode\n";

        if ((miscFlags & FLAGS_RESYNC) != 0)
            ss << " Resynchronization mode\n";
        else
            ss << " Unprotected mode\n";

        if ((miscFlags & FLAGS_CLOSEDTHREAD) != 0)
            ss << " Closed thread archival\n";

        if ((miscFlags & FLAGS_MAXPERFORMANCE) != 0)
            ss << " Maximize performance mode\n";

        ss << " Miscellaneous flags: 0x" << std::hex << miscFlags << '\n';

        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2) {
            const uint32_t miscFlags2 = ctx->read32(headerBuffer + blockSize + 296);
            ss << " Miscellaneous second flags: 0x" << std::hex << miscFlags2 << '\n';
        }

        const auto thr = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 432));
        const auto seq2 = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 436));
        const Scn scn2 = ctx->readScn(headerBuffer + blockSize + 440);
        const uint8_t zeroBlocks = headerBuffer[blockSize + 206];
        const uint8_t formatId = headerBuffer[blockSize + 207];
        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
                    " seq: " << std::dec << seq2 <<
                    " scn: " << scn2.to48() << '\n' <<
                    " Zero blocks: " << std::dec << static_cast<uint>(zeroBlocks) << '\n' <<
                    " Format ID is " << std::dec << static_cast<uint>(formatId) << '\n';
        else
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
                    " seq: " << std::dec << seq2 <<
                    " scn: " << scn2.to64() << '\n' <<
                    " Zero blocks: " << std::dec << static_cast<uint>(zeroBlocks) << '\n' <<
                    " Format ID is " << std::dec << static_cast<uint>(formatId) << '\n';

        const uint32_t standbyApplyDelay = ctx->read32(headerBuffer + blockSize + 280);
        if (standbyApplyDelay > 0)
            ss << " Standby Apply Delay: " << std::dec << standbyApplyDelay << " minute(s) \n";

        const Time standbyLogCloseTime(ctx->read32(headerBuffer + blockSize + 304));
        if (standbyLogCloseTime.getVal() > 0)
            ss << " Standby Log Close Time:  " << standbyLogCloseTime << '\n';

        ss << " redo log key is ";
        for (uint i = 448; i < 448 + 16; ++i)
            ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(headerBuffer[blockSize + i]);
        ss << '\n';

        const uint16_t redoKeyFlag = ctx->read16(headerBuffer + blockSize + 480);
        ss << " redo log key flag is " << std::dec << redoKeyFlag << '\n';
        constexpr uint16_t enabledRedoThreads = 1; // TODO: find field position/size
        ss << " Enabled redo threads: " << std::dec << enabledRedoThreads << " \n";
    }

    uint Reader::getBlockSize() const {
        return blockSize;
    }

    FileOffset Reader::getBufferStart() const {
        return FileOffset(bufferStart);
    }

    FileOffset Reader::getBufferEnd() const {
        return FileOffset(bufferEnd);
    }

    Reader::REDO_CODE Reader::getRet() const {
        return ret;
    }

    Scn Reader::getFirstScn() const {
        return firstScn;
    }

    Scn Reader::getFirstScnHeader() const {
        return firstScnHeader;
    }

    Scn Reader::getNextScn() const {
        return nextScn;
    }

    Time Reader::getNextTime() const {
        return nextTime;
    }

    typeBlk Reader::getNumBlocks() const {
        return numBlocksHeader;
    }

    int Reader::getGroup() const {
        return group;
    }

    Seq Reader::getSequence() const {
        return sequence;
    }

    typeResetlogs Reader::getResetlogs() const {
        return resetlogs;
    }

    typeActivation Reader::getActivation() const {
        return activation;
    }

    uint64_t Reader::getSumRead() const {
        return sumRead;
    }

    uint64_t Reader::getSumTime() const {
        return sumTime;
    }

    uint16_t Reader::getThread() const {
        return thread;
    }

    void Reader::setRet(REDO_CODE newRet) {
        ret = newRet;
    }

    void Reader::setBufferStartEnd(FileOffset newBufferStart, FileOffset newBufferEnd) {
        bufferStart = newBufferStart.getData();
        bufferEnd = newBufferEnd.getData();
    }

    bool Reader::checkRedoLog() {
        contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_REDO);
        std::unique_lock lck(mtx);
        status = STATUS::CHECK;
        sequence = Seq::zero();
        firstScn = Scn::none();
        nextScn = Scn::none();
        condBufferFull.notify_all();
        condReaderSleeping.notify_all();

        while (status == STATUS::CHECK) {
            if (ctx->softShutdown)
                break;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:checkRedoLog");
            contextSet(CONTEXT::WAIT, REASON::READER_CHECK);
            // Bounded: see the mainLoop sleep - this wait and the reader's sleep must never deadlock.
            condParserSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
        }
        contextSet(CONTEXT::CPU);
        return (ret == REDO_CODE::OK);
    }

    bool Reader::updateRedoLog() {
        for (;;) {
            contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO1);
            std::unique_lock lck(mtx);
            status = STATUS::UPDATE;
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();

            while (status == STATUS::UPDATE) {
                if (ctx->softShutdown)
                    break;
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:updateRedoLog");
                contextSet(CONTEXT::WAIT);
                // Bounded for the same reason as the mainLoop sleep below: this wait and the reader's sleep
                // must never deadlock each other. The timeout only matters when the reader thread went to
                // sleep without a notification - a regular update still wakes it up immediately.
                condParserSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO2);
            }

            if (ret == REDO_CODE::EMPTY) {
                contextSet(CONTEXT::WAIT, REASON::READER_EMPTY);
                condParserSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO3);
                continue;
            }

            contextSet(CONTEXT::CPU);
            return (ret == REDO_CODE::OK);
        }
    }

    void Reader::setStatusRead() {
        {
            contextSet(CONTEXT::MUTEX, REASON::READER_SET_READ);
            std::unique_lock const lck(mtx);
            status = STATUS::READ;
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();
        }
        contextSet(CONTEXT::CPU);
    }

    void Reader::confirmReadData(FileOffset confirmedBufferStart) {
        contextSet(CONTEXT::MUTEX, REASON::READER_CONFIRM);
        {
            std::unique_lock const lck(mtx);
            bufferStart = confirmedBufferStart.getData();
            if (status == STATUS::READ) {
                condBufferFull.notify_all();
            }
        }
        contextSet(CONTEXT::CPU);
    }

    int Reader::redoReadProbe(uint8_t* buf, uint64_t offset, uint size) {
        return redoRead(buf, offset, size);
    }

    bool Reader::archiveEndConfirmed(Thread* t) {
        // Neither size this reader knows can settle this on its own: the one from stat can be stale (measured
        // 131072 for a 242688 byte archive, and it is only ever raised, so the wrong value sticks) and the
        // block count in the redo header can be too small as well (measured the same 131072 - and the read
        // loop used to end the archive right there). Both shapes of the anomaly - a stale size, or an empty
        // block returned over data - skipped whole committed transactions in silence ("FINISHED, position:
        // 131072 / 242688"). What is left to consult is the file itself: read the position the parser stopped
        // at. Real redo there means the end is bogus (stop; the position is saved and the archive is read
        // again with a fresh handle on the next pass), zeroes or a zero length mean padding or the real end.
        uint64_t end = fileSizeStat;
        if (fileSize > end)
            end = fileSize;
        // The sizes are trusted to decide how often to probe, not whether: when they say the file ends here,
        // one read is still made - that is the case a stale size hides. Three when data is expected behind
        // the position, because a stale zero answer of the bridge is transient.
        const bool dataExpected = bufferScan < end;
        const uint attempts = dataExpected ? END_CHECK_ATTEMPTS : 1;

        if (blockSize == 0 || blockSize > PAGE_SIZE_MAX)
            return true;

        // Aligned: the file is opened with O_DIRECT, a buffer from the heap would fail with EINVAL.
        uint8_t* probe = nullptr;
        if (posix_memalign(reinterpret_cast<void**>(&probe), PAGE_SIZE_MAX, PAGE_SIZE_MAX) != 0)
            return true;
        memset(probe, 0, PAGE_SIZE_MAX);

        bool confirmed = true;
        for (uint attempt = 0; attempt < attempts; ++attempt) {
            t->contextSet(CONTEXT::OS, REASON::OS);
            const int bytes = redoReadProbe(probe, bufferScan, blockSize);
            t->contextSet(CONTEXT::CPU);
            if (bytes >= static_cast<int>(blockSize) && (probe[0] != 0 || probe[1] != 0)) {
                ctx->warning(60044, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                             " - the file holds redo data at this position (size from stat: " + std::to_string(fileSizeStat) +
                             ", from header: " + std::to_string(static_cast<uint64_t>(numBlocksHeader) * blockSize) +
                             "), the file is not fully readable yet");
                confirmed = false;
                break;
            }
            if (ctx->archReadSleepUs > 0 && attempt + 1 < attempts) {
                t->contextSet(CONTEXT::SLEEP);
                ctx->usleepInt(ctx->archReadSleepUs);
                t->contextSet(CONTEXT::CPU);
            }
        }
        free(probe);

        if (!confirmed)
            return false;

        if (dataExpected) {
            // The position stays empty although data was expected behind it: it is the padding of a log that
            // was not filled up. The warning keeps visible that the tail was accepted without being read.
            ctx->warning(60044, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                         " - the rest of the file (size: " + std::to_string(fileSize) +
                         ") stays empty on re-read, treating it as padding of an unfilled redo log");
        }
        return true;
    }

    bool Reader::checkFinished(Thread* t, FileOffset confirmedBufferStart) {
        t->contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_FINISHED);
        {
            std::unique_lock lck(mtx);
            if (bufferStart < confirmedBufferStart.getData()) {
                bufferStart = confirmedBufferStart.getData();
                // The parser has just confirmed the last chunk of the read buffer. The reader can be
                // waiting for free space in the read buffer right now ("buffer full") - without this
                // notification it would keep sleeping while the parser waits below for a reader that
                // can not make progress any more: a deadlock without a single message in the log.
                if (status == STATUS::READ)
                    condBufferFull.notify_all();
            }

            // All work done
            if (confirmedBufferStart.getData() == bufferEnd) {
                if (ret == REDO_CODE::STOPPED || ret == REDO_CODE::OVERWRITTEN || ret == REDO_CODE::FINISHED || status == STATUS::SLEEPING) {
                    t->contextSet(CONTEXT::CPU);
                    return true;
                }
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:checkFinished");
                // Wait for the reader to finish this redo log or to append more data. The wait is repeated
                // instead of waiting once unconditionally: the timeout changes nothing in the data flow
                // (every pass through the loop is a no-op while nothing new is available), it only makes
                // the handshake observable - the reader can also be blocked on the read buffer (see
                // Reader::mainLoop) and then nothing at all would be written to the log.
                uint64_t stallIterations = 0;
                while (!ctx->softShutdown && confirmedBufferStart.getData() == bufferEnd &&
                       ret != REDO_CODE::STOPPED && ret != REDO_CODE::OVERWRITTEN && ret != REDO_CODE::FINISHED &&
                       status != STATUS::SLEEPING) {
                    t->contextSet(CONTEXT::WAIT, REASON::READER_FINISHED);
                    condParserSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                    t->contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_FINISHED);
                    ++stallIterations;
                    if (stallIterations == 200 || (stallIterations % 1200) == 0)
                        ctx->warning(60046, "file: " + fileName + " - the parser is waiting for the reader for: " +
                                     std::to_string(stallIterations * ctx->redoReadSleepUs / 1000000) + " s (position: " +
                                     std::to_string(bufferScan) + ", buffer: " + std::to_string(bufferStart) + "-" +
                                     std::to_string(bufferEnd) + ", size: " + std::to_string(fileSize) + ", status: " +
                                     std::to_string(static_cast<int>(status.load())) + ", ret: " +
                                     REDO_MSG[static_cast<uint>(ret.load())] + ")");
                }
                if (ctx->softShutdown) {
                    t->contextSet(CONTEXT::CPU);
                    return false;
                }
                if (ret == REDO_CODE::STOPPED || ret == REDO_CODE::OVERWRITTEN || ret == REDO_CODE::FINISHED || status == STATUS::SLEEPING) {
                    t->contextSet(CONTEXT::CPU);
                    return true;
                }
            }
        }
        t->contextSet(CONTEXT::CPU);
        return false;
    }
}
