/* Context of program
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

#define GLOBALS 1

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <csignal>
#include <execinfo.h>
#include <iostream>
#include <set>
#include <string>
#include <unistd.h>

#include "AlignedAllocCompat.h"
#include "ClockHW.h"
#include "Ctx.h"
#include "Thread.h"
#include "exception/DataException.h"
#include "exception/RuntimeException.h"
#include "metrics/Metrics.h"
#include "types/Data.h"
#include "types/IntX.h"

auto OLR_LOCALES = OpenLogReplicator::Ctx::LOCALES::TIMESTAMP;

namespace OpenLogReplicator {
    // The order must match the MEMORY enum: BUILDER, MISC, PARSER, READER, TRANSACTIONS, WRITER.
    // "misc" used to be missing, which shifted all the following names and made the error messages
    // point at the wrong module (and the writer one fall back to an empty string).
    const std::string Ctx::memoryModules[MEMORY_COUNT] {
        "builder",
        "misc",
        "parser",
        "reader",
        "transactions",
        "writer"
    };

    IntX IntX::BASE10[DIGITS][10];

    Ctx::Ctx():
            mainThread(pthread_self()),
            dumpStream(std::make_unique<std::ofstream>()) {
        clock = new ClockHW();
        tzset();
        logTimezone = -timezone;
        hostTimezone = -timezone;
    }

    Ctx::~Ctx() {
        lobIdToXidMap.clear();

        while (memoryChunksAllocated > 0) {
            --memoryChunksAllocated;
            free(memoryChunks[memoryChunksAllocated]);
            memoryChunks[memoryChunksAllocated] = nullptr;
        }

        if (memoryChunks != nullptr) {
            delete[] memoryChunks;
            memoryChunks = nullptr;
        }

        if (metrics != nullptr) {
            metrics->shutdown();
            delete metrics;
            metrics = nullptr;
        }

        if (clock != nullptr) {
            delete clock;
            clock = nullptr;
        }
    }

    void Ctx::checkJsonFields(const std::string& fileName, const rapidjson::Value& value, const std::vector<std::string>& names) {
        for (const auto& child: value.GetObject()) {
            bool found = false;
            for (const auto& name: names) {
                if (name == child.name.GetString()) {
                    found = true;
                    break;
                }
            }

            if (unlikely(!found && memcmp(child.name.GetString(), "xdb-xnm", 7) != 0 &&
                    memcmp(child.name.GetString(), "xdb-xpt", 7) != 0 &&
                    memcmp(child.name.GetString(), "xdb-xqn", 7) != 0))
                throw DataException(20003, "file: " + fileName + " - parse error, attribute " + child.name.GetString() + " not expected");
        }
    }

    const rapidjson::Value& Ctx::getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsArray()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        const uint64_t val = ret.GetUint64();
        if (unlikely(val > 0xFFFF))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        const int64_t val = ret.GetInt64();
        if (unlikely((val > static_cast<int64_t>(0x7FFF)) || (val < -static_cast<int64_t>(0x8000))))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<int16_t>(val);
    }

    uint32_t Ctx::getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        const uint64_t val = ret.GetUint64();
        if (unlikely(val > 0xFFFFFFFF))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<uint32_t>(val);
    }

    int32_t Ctx::getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        const int64_t val = ret.GetInt64();
        if (unlikely((val > static_cast<int64_t>(0x7FFFFFFF)) || (val < -static_cast<int64_t>(0x80000000))))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too big (" + std::to_string(val) + ")");
        return static_cast<int32_t>(val);
    }

    uint64_t Ctx::getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed 64-bit number");
        return ret.GetInt64();
    }

    uint Ctx::getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsUint()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an unsigned number");
        return ret.GetUint();
    }

    int Ctx::getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsInt()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a signed number");
        return ret.GetInt();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsObject()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not an object");
        return ret;
    }

    std::string Ctx::getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field) {
        if (unlikely(!value.HasMember(field)))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " not found");
        const rapidjson::Value& ret = value[field];
        if (unlikely(!ret.IsString()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is not a string");
        if (unlikely(ret.GetStringLength() > maxLength))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + " is too long (" +
                                std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return {ret.GetString()};
    }

    const rapidjson::Value& Ctx::getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsArray()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an array");
        return ret;
    }

    uint16_t Ctx::getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an unsigned 64-bit number");
        const uint64_t val = ret.GetUint64();
        if (unlikely(val > 0xFFFF))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is too big (" + std::to_string(val) + ")");
        return val;
    }

    int16_t Ctx::getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not a signed 64-bit number");
        const int64_t val = ret.GetInt64();
        if (unlikely((val > static_cast<int64_t>(0x7FFF)) || (val < -static_cast<int64_t>(0x8000))))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is too big (" + std::to_string(val) + ")");
        return static_cast<int16_t>(val);
    }

    uint32_t Ctx::getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an unsigned 64-bit number");
        const uint64_t val = ret.GetUint64();
        if (unlikely(val > 0xFFFFFFFF))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is too big (" + std::to_string(val) + ")");
        return static_cast<uint32_t>(val);
    }

    int32_t Ctx::getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not a signed 64-bit number");
        const int64_t val = ret.GetInt64();
        if (unlikely((val > static_cast<int64_t>(0x7FFFFFFF)) || (val < -static_cast<int64_t>(0x80000000))))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is too big (" + std::to_string(val) + ")");
        return static_cast<int32_t>(val);
    }

    uint64_t Ctx::getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsUint64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an unsigned 64-bit number");
        return ret.GetUint64();
    }

    int64_t Ctx::getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsInt64()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not a signed 64-bit number");
        return ret.GetInt64();
    }

    uint Ctx::getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsUint()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an unsigned number");
        return ret.GetUint();
    }

    int Ctx::getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsInt()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not a signed number");
        return ret.GetInt();
    }

    const rapidjson::Value& Ctx::getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsObject()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not an object");
        return ret;
    }

    std::string Ctx::getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field, uint num) {
        const rapidjson::Value& ret = value[num];
        if (unlikely(!ret.IsString()))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is not a string");
        if (unlikely(ret.GetStringLength() > maxLength))
            throw DataException(20003, "file: " + fileName + " - parse error, field " + field + "[" + std::to_string(num) +
                                "] is too long (" + std::to_string(ret.GetStringLength()) + ", max: " + std::to_string(maxLength) + ")");
        return {ret.GetString()};
    }

    void Ctx::initialize(uint64_t memoryChunkKb, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t memoryReadBufferMaxMb, uint64_t memoryReadBufferMinMb,
                         uint64_t memorySwapMb, uint64_t memoryUnswapBufferMinMb, uint64_t memoryWriteBufferMaxMb, uint64_t memoryWriteBufferMinMb) {
        {
            std::unique_lock const lck(memoryMtx);
            // The pool block is also the transaction data block: every accounting below is in these blocks,
            // and the MB based options are converted with mbsToChunks().
            memoryChunkSize = memoryChunkKb * 1024;
            memoryChunksPerMb = (MEMORY_CHUNK_SIZE_MB * 1024 * 1024) / memoryChunkSize;

            memoryChunksMin = mbsToChunks(memoryMinMb);
            memoryChunksMax = mbsToChunks(memoryMaxMb);
            memoryChunksSwap = mbsToChunks(memorySwapMb);

            memoryChunksReadBufferMax = mbsToChunks(memoryReadBufferMaxMb);
            memoryChunksReadBufferMin = mbsToChunks(memoryReadBufferMinMb);
            memoryChunksUnswapBufferMin = mbsToChunks(memoryUnswapBufferMinMb);
            memoryChunksWriteBufferMax = mbsToChunks(memoryWriteBufferMaxMb);
            memoryChunksWriteBufferMin = mbsToChunks(memoryWriteBufferMinMb);
            bufferSizeMax = memoryReadBufferMaxMb * 1024 * 1024;
            bufferSizeFree = mbsToChunks(memoryReadBufferMaxMb);

            memoryChunks = new uint8_t*[memoryChunksMax];
            for (uint64_t i = 0; i < memoryChunksMin; ++i) {
                memoryChunks[i] = static_cast<uint8_t*>(aligned_alloc(MEMORY_ALIGNMENT, memoryChunkSize));
                if (unlikely(memoryChunks[i] == nullptr))
                    throw RuntimeException(10016, "couldn't allocate " + std::to_string(memoryChunkSize) +
                                           " bytes memory for: memory chunks#2");
                ++memoryChunksAllocated;
                ++memoryChunksFree;
            }
            memoryChunksHWM = memoryChunksMin;
        }

        if (metrics != nullptr) {
            metrics->emitMemoryAllocatedMb(chunksToMb(memoryChunksAllocated));
            metrics->emitMemoryUsedTotalMb(0);
        }
    }

    void Ctx::wakeAllOutOfMemory() {
        std::unique_lock const lck(memoryMtx);
        condOutOfMemory.notify_all();
    }

    bool Ctx::nothingToSwap(Thread* t) const {
        bool ret;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_NOTHING_TO_SWAP);
            std::unique_lock const lck(memoryMtx);
            ret = memoryChunksSwap == 0 || (memoryChunksAllocated - memoryChunksFree < memoryChunksSwap);
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return ret;
    }

    uint64_t Ctx::getMemoryHWM() const {
        std::unique_lock const lck(memoryMtx);
        return chunksToMb(memoryChunksHWM);
    }

    uint64_t Ctx::getFreeMemory(Thread* t) const {
        uint64_t ret;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_FREE_MEMORY);
            std::unique_lock const lck(memoryMtx);
            ret = chunksToMb(memoryChunksFree);
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return ret;
    }

    uint64_t Ctx::getAllocatedMemory() const {
        std::unique_lock const lck(memoryMtx);
        return chunksToMb(memoryChunksAllocated);
    }

    uint64_t Ctx::getMaxMemory() const {
        std::unique_lock const lck(memoryMtx);
        return chunksToMb(memoryChunksMax);
    }

    uint64_t Ctx::getBlockedThreads() const {
        std::unique_lock const lck(memoryMtx);
        return memoryBlockedThreads;
    }

    uint64_t Ctx::getReservedFreeChunks() const {
        std::unique_lock const lck(memoryMtx);
        // The same reserved zone that getMemoryChunk() keeps for the reader, the writer and the unswap
        // buffer: chunks below it are not handed out to a thread that can not swap memory itself
        uint64_t ret = memoryChunksUnswapBufferMin;
        if (memoryModulesAllocated[static_cast<uint>(MEMORY::READER)] < memoryChunksReadBufferMin)
            ret += memoryChunksReadBufferMin - memoryModulesAllocated[static_cast<uint>(MEMORY::READER)];
        if (memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] < memoryChunksWriteBufferMin)
            ret += memoryChunksWriteBufferMin - memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)];
        return ret;
    }

    std::string Ctx::getMemoryModulesText() const {
        std::unique_lock const lck(memoryMtx);
        std::string ret;
        for (uint i = 0; i < MEMORY_COUNT; ++i) {
            if (i > 0)
                ret += ", ";
            ret += memoryModules[i] + ": " + std::to_string(chunksToMb(memoryModulesAllocated[i])) + "MB";
        }
        return ret;
    }

    uint64_t Ctx::getSwapMemory(Thread* t) const {
        uint64_t ret;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_GET_SWAP);
            std::unique_lock const lck(memoryMtx);
            ret = chunksToMb(memoryChunksSwap);
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return ret;
    }

    uint8_t* Ctx::getMemoryChunk(Thread* t, MEMORY module, bool swap) {
        uint64_t allocatedModule = 0;
        uint64_t usedTotal = 0;
        uint64_t allocatedTotal = 0;
        uint8_t* chunk = nullptr;

        t->contextSet(Thread::CONTEXT::MEM, Thread::REASON::MEM);
        {
            std::unique_lock lck(memoryMtx);
            while (true) {
                if (module == MEMORY::READER) {
                    if (memoryModulesAllocated[static_cast<uint>(MEMORY::READER)] < memoryChunksReadBufferMin)
                        break;
                } else if (module == MEMORY::BUILDER) {
                    if (memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] < memoryChunksWriteBufferMin)
                        break;
                }

                uint64_t reservedChunks = 0;
                if (memoryModulesAllocated[static_cast<uint>(MEMORY::READER)] < memoryChunksReadBufferMin)
                    reservedChunks += memoryChunksReadBufferMin - memoryModulesAllocated[static_cast<uint>(MEMORY::READER)];
                if (memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] < memoryChunksWriteBufferMin)
                    reservedChunks += memoryChunksWriteBufferMin - memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)];
                if (!swap)
                    reservedChunks += memoryChunksUnswapBufferMin;

                if (module != MEMORY::BUILDER || memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] < memoryChunksWriteBufferMax) {
                    if (memoryChunksFree > reservedChunks)
                        break;

                    if (memoryChunksAllocated < memoryChunksMax) {
                        t->contextSet(Thread::CONTEXT::OS, Thread::REASON::OS);
                        memoryChunks[memoryChunksFree] = static_cast<uint8_t*>(aligned_alloc(MEMORY_ALIGNMENT, memoryChunkSize));
                        t->contextSet(Thread::CONTEXT::MEM, Thread::REASON::MEM);
                        if (unlikely(memoryChunks[memoryChunksFree] == nullptr))
                            throw RuntimeException(10016, "couldn't allocate " + std::to_string(memoryChunkSize) +
                                                   " bytes memory for: " + memoryModules[static_cast<uint>(module)]);
                        ++memoryChunksFree;
                        allocatedTotal = ++memoryChunksAllocated;

                        memoryChunksHWM = std::max(memoryChunksAllocated, memoryChunksHWM);
                        break;
                    }
                }

                // A blocked parser thread means a deadlock, not a delay: the commit sorter is flushed only
                // by the parser itself (Parser::processRedoLog), so no other thread can release the
                // transaction chunks it is waiting for. Marking only the PARSER module used to miss the
                // case of a transaction chunk grow (MEMORY::TRANSACTIONS) - the pool was exhausted, the
                // parser was blocked on memory and the whole run stalled without a single message.
                if (module == MEMORY::PARSER || t == parserThread)
                    outOfMemoryParser = true;

                if (hardShutdown)
                    return nullptr;

                if (unlikely(isTraceSet(TRACE::SLEEP)))
                    logTrace(TRACE::SLEEP, "Ctx:getMemoryChunk");
                t->contextSet(Thread::CONTEXT::WAIT, Thread::REASON::MEMORY_EXHAUSTED);
                ++memoryBlockedThreads;
                condOutOfMemory.wait(lck);
                --memoryBlockedThreads;
                t->contextSet(Thread::CONTEXT::MEM, Thread::REASON::MEM);
            }

            if (module == MEMORY::PARSER)
                outOfMemoryParser = false;

            --memoryChunksFree;
            usedTotal = memoryChunksAllocated - memoryChunksFree;
            allocatedModule = ++memoryModulesAllocated[static_cast<uint>(module)];
            memoryModulesHWM[static_cast<uint>(module)] = std::max(memoryModulesAllocated[static_cast<uint>(module)],
                                                                   memoryModulesHWM[static_cast<uint>(module)]);
            chunk = memoryChunks[memoryChunksFree];
        }
        t->contextSet(Thread::CONTEXT::CPU);

        if (unlikely(hardShutdown))
            throw RuntimeException(10018, "shutdown during memory allocation");

        if (metrics != nullptr) {
            if (allocatedTotal > 0)
                metrics->emitMemoryAllocatedMb(chunksToMb(allocatedTotal));

            metrics->emitMemoryUsedTotalMb(chunksToMb(usedTotal));

            switch (module) {
                case MEMORY::BUILDER:
                    metrics->emitMemoryUsedMbBuilder(chunksToMb(allocatedModule));
                    break;

                case MEMORY::MISC:
                    metrics->emitMemoryUsedMbMisc(chunksToMb(allocatedModule));
                    break;

                case MEMORY::PARSER:
                    metrics->emitMemoryUsedMbParser(chunksToMb(allocatedModule));
                    break;

                case MEMORY::READER:
                    metrics->emitMemoryUsedMbReader(chunksToMb(allocatedModule));
                    break;

                case MEMORY::TRANSACTIONS:
                    metrics->emitMemoryUsedMbTransactions(chunksToMb(allocatedModule));
                    break;

                case MEMORY::WRITER:
                    metrics->emitMemoryUsedMbWriter(chunksToMb(allocatedModule));
            }
        }

        return chunk;
    }

    void Ctx::freeMemoryChunk(Thread* t, MEMORY module, uint8_t* chunk) {
        uint64_t allocatedModule = 0;
        uint64_t usedTotal = 0;
        uint64_t allocatedTotal = 0;
        t->contextSet(Thread::CONTEXT::MEM, Thread::REASON::MEM);
        {
            std::unique_lock const lck(memoryMtx);

            if (unlikely(memoryChunksFree == memoryChunksAllocated))
                throw RuntimeException(50001, "trying to free unknown memory block for: " + memoryModules[static_cast<uint>(module)]);

            // Keep memoryChunksMin reserved
            if (memoryChunksFree >= memoryChunksMin)
                allocatedTotal = --memoryChunksAllocated;
            else {
                memoryChunks[memoryChunksFree++] = chunk;
                chunk = nullptr;
            }

            usedTotal = memoryChunksAllocated - memoryChunksFree;
            allocatedModule = --memoryModulesAllocated[static_cast<uint>(module)];

            condOutOfMemory.notify_all();
        }

        if (chunk != nullptr) {
            t->contextSet(Thread::CONTEXT::OS, Thread::REASON::OS);
            free(chunk);
        }

        t->contextSet(Thread::CONTEXT::CPU);
        if (metrics != nullptr) {
            if (allocatedTotal > 0)
                metrics->emitMemoryAllocatedMb(chunksToMb(allocatedTotal));

            metrics->emitMemoryUsedTotalMb(chunksToMb(usedTotal));

            switch (module) {
                case MEMORY::BUILDER:
                    metrics->emitMemoryUsedMbBuilder(chunksToMb(allocatedModule));
                    break;

                case MEMORY::MISC:
                    metrics->emitMemoryUsedMbMisc(chunksToMb(allocatedModule));
                    break;

                case MEMORY::PARSER:
                    metrics->emitMemoryUsedMbParser(chunksToMb(allocatedModule));
                    break;

                case MEMORY::READER:
                    metrics->emitMemoryUsedMbReader(chunksToMb(allocatedModule));
                    break;

                case MEMORY::TRANSACTIONS:
                    metrics->emitMemoryUsedMbTransactions(chunksToMb(allocatedModule));
                    break;

                case MEMORY::WRITER:
                    metrics->emitMemoryUsedMbWriter(chunksToMb(allocatedModule));
            }
        }
    }

    void Ctx::swappedMemoryInit(Thread* t, const SwapKey& swapKey) {
        bool slept = false;
        auto* sc = new SwapChunk();
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_MEMORY_INIT);
            std::unique_lock lck(swapMtx);

            // Only the same transaction (same thread, container and Xid) can hold this key: with the redo
            // threads in the key this is a transaction that is parsed twice, not a transaction of another
            // instance that happens to use the same undo slot.
            // The wait stays unbounded: the holder is the same transaction and the only thing that can release
            // the key is its delivery, which this very thread has to do - releasing it early or giving up would
            // corrupt the transaction. It only reports itself, because an unbounded silent wait looks exactly
            // like an idle run: that is how the RAC Xid collision (61cd93c) presented itself - the run simply
            // stopped, with no message at all.
            uint64_t waitSince = 0;
            uint64_t lastReport = 0;
            while (!hardShutdown) {
                if (swapChunks.find(swapKey) == swapChunks.end())
                    break;

                slept = true;
                const uint64_t now = clock->getTimeUt();
                if (waitSince == 0)
                    waitSince = now;
                if (now >= waitSince + SWAP_WAIT_REPORT_US && (lastReport == 0 || now >= lastReport + SWAP_WAIT_REPORT_US)) {
                    warning(60050, "swap memory of transaction: " + swapKey.toString() + " is still held after " +
                            std::to_string((now - waitSince) / 1000000) + " s of waiting for its delivery (swapped "
                            "transactions: " + std::to_string(swapChunks.size()) + ", committed and waiting for "
                            "delivery: " + std::to_string(commitedXids.size()) + ") - the same transaction is parsed "
                            "twice (slice boundary / instance recovery), or its delivery is stalled (see 60045/60047)");
                    lastReport = now;
                }

                t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::MEMORY_BLOCKED);
                // The parser is now out of the game: it holds no buffer lock (swapMtx is released by wait_for)
                // and touches neither the sorter nor the builder, so the writer may drive a sorter tick and
                // deliver the transaction this wait is waiting for (defect C).
                parserInSwapWait.store(true, std::memory_order_relaxed);
                reusedTransactions.wait_for(lck, std::chrono::microseconds(SWAP_WAIT_REPORT_US));
                parserInSwapWait.store(false, std::memory_order_relaxed);
                t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_MEMORY_INIT);
            }

            if (slept && metrics != nullptr) {
                // A re-parse ran into the swap key of a transaction that is already committed and not delivered
                // yet, so this thread had to wait for that delivery: the count and the time it cost. A climbing
                // counter here means re-parsed redo (see bytes_parsed_repeated) is running into undelivered
                // transactions - the wait is what the skip-on-re-parse change is meant to remove.
                metrics->emitDuplicateParseWaits(1);
                if (waitSince != 0 && clock != nullptr)
                    metrics->emitDuplicateParseWaitUs(clock->getTimeUt() - waitSince);
            }

            swapChunks.insert_or_assign(swapKey, sc);
        }
        t->contextSet(Thread::CONTEXT::CPU);
        if (slept && unlikely(isTraceSet(TRACE::TRANSACTION)))
            logTrace(TRACE::TRANSACTION, "swap memory stalled transaction: " + swapKey.toString());
    }

    uint64_t Ctx::swappedMemorySize(Thread* t, const SwapKey& swapKey) const {
        uint64_t ret;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_SIZE);
            std::unique_lock const lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during memory size");
            const SwapChunk* sc = it->second;
            ret = sc->chunks.size();
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return ret;
    }

    uint8_t* Ctx::swappedMemoryGet(Thread* t, const SwapKey& swapKey, int64_t index) {
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_GET);
            std::unique_lock lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during memory get");
            const SwapChunk* sc = it->second;

            while (!hardShutdown) {
                if (index < sc->swappedMin || index > sc->swappedMax) {
                    t->contextSet(Thread::CONTEXT::CPU);
                    return sc->chunks.at(index);
                }

                chunksMemoryManager.notify_all();
                chunksTransaction.wait(lck);
            }
        }

        t->contextSet(Thread::CONTEXT::CPU);
        return nullptr;
    }

    void Ctx::swappedMemoryRelease(Thread* t, const SwapKey& swapKey, int64_t index) {
        uint8_t* tc;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_RELEASE);
            std::unique_lock const lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during memory release");
            SwapChunk* sc = it->second;
            tc = sc->chunks.at(index);
            sc->chunks[index] = nullptr;
        }
        t->contextSet(Thread::CONTEXT::CPU);

        freeMemoryChunk(t, MEMORY::TRANSACTIONS, tc);
    }

    [[nodiscard]] uint8_t* Ctx::swappedMemoryGrow(Thread* t, const SwapKey& swapKey) {
        SwapChunk* sc;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_GROW1);
            std::unique_lock const lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during memory grow");
            sc = it->second;
        }
        t->contextSet(Thread::CONTEXT::CPU);

        uint8_t* tc = getMemoryChunk(t, MEMORY::TRANSACTIONS);
        memset(tc, 0, sizeof(uint64_t) + sizeof(uint32_t));

        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_GROW2);
            std::unique_lock const lck(swapMtx);
            sc->chunks.push_back(tc);
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return tc;
    }

    uint8_t* Ctx::swappedMemoryShrink(Thread* t, const SwapKey& swapKey) {
        SwapChunk* sc;
        uint8_t* tc;
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_SHRINK1);
            std::unique_lock const lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during memory shrink");
            sc = it->second;
            tc = sc->chunks.back();
            sc->chunks.pop_back();
        }

        freeMemoryChunk(t, MEMORY::TRANSACTIONS, tc);

        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_SHRINK2);
            std::unique_lock lck(swapMtx);
            if (sc->chunks.empty()) {
                t->contextSet(Thread::CONTEXT::CPU);
                return nullptr;
            }
            const int64_t index = sc->chunks.size() - 1;

            swappedShrinkXid = swapKey;
            while (!hardShutdown) {
                if (index < sc->swappedMin || index > sc->swappedMax)
                    break;

                chunksMemoryManager.notify_all();
                chunksTransaction.wait(lck);
            }
            swappedShrinkXid = SwapKey{};
            tc = sc->chunks.back();
        }
        t->contextSet(Thread::CONTEXT::CPU);
        return tc;
    }

    void Ctx::swappedMemoryFlush(Thread* t, const SwapKey& swapKey) {
        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_FLUSH1);
            std::unique_lock const lck(swapMtx);
            swappedFlushXid = swapKey;
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Ctx::swappedMemoryRemove(Thread* t, const SwapKey& swapKey) {
        SwapChunk* sc = nullptr;
        bool alreadyRemoved = false;
        {
            t->contextSet(Thread::CONTEXT::CPU);
            std::unique_lock const lck(swapMtx);
            const auto& it = swapChunks.find(swapKey);
            if (unlikely(it == swapChunks.end())) {
                // The chunk can be legitimately gone already: MemoryManager::cleanOldTransactions() erases an
                // entry as soon as the transaction has been released, so releasing the same transaction twice is a
                // race and not an error. Throwing 50070 here used to abort OpenLogReplicator while it was
                // recovering from a burst of undo slot take-overs (see warning 60041): the recovery path releases
                // the unfinished transaction, the memory manager may clean it up, and a later release of the same
                // transaction then failed the whole run.
                if (swappedFlushXid == swapKey)
                    swappedFlushXid = SwapKey{};
                alreadyRemoved = true;
            } else {
                sc = it->second;
                sc->release = true;
                swappedFlushXid = SwapKey{};
            }
        }
        t->contextSet(Thread::CONTEXT::CPU);

        if (unlikely(alreadyRemoved)) {
            if (unlikely(isTraceSet(TRACE::TRANSACTION)))
                logTrace(TRACE::TRANSACTION, "swap memory already released for transaction: " + swapKey.toString());
            return;
        }

        for (auto* tc: sc->chunks)
            if (tc != nullptr)
                freeMemoryChunk(t, MEMORY::TRANSACTIONS, tc);

        {
            t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_FLUSH2);
            std::unique_lock const lck(swapMtx);
            sc->chunks.clear();
            commitedXids.push_back(swapKey);
            chunksMemoryManager.notify_all();
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Ctx::releaseDeferredSwaps() {
        // Hand the collected keys over first: the releases below re-enter swappedMemoryKeyRelease(), which must
        // find the flag cleared (and may collect again if it were not).
        std::vector<SwapKey> pending;
        pending.swap(deferredSwapReleases);
        for (const SwapKey& swapKey: pending)
            swappedMemoryKeyRelease(swapKey);
    }

    void Ctx::swappedMemoryKeyRelease(const SwapKey& swapKey) {
        // Stage 2: a take-over delivery must finish before the parser it releases can run again (see
        // Ctx::deferSwapReleases). Collected here, released by the delivering thread when it is done.
        if (unlikely(deferSwapReleases)) {
            deferredSwapReleases.push_back(swapKey);
            return;
        }

        SwapChunk* sc = nullptr;
        {
            std::unique_lock const lck(swapMtx);
            const auto it = swapChunks.find(swapKey);
            if (it == swapChunks.end())
                return;
            sc = it->second;
            // Still holding swapped out data: that entry belongs to the purge()/MemoryManager path, which knows
            // how to unlink the swap file - dropping it here would lose that data and the file.
            if (!sc->chunks.empty())
                return;
            if (swappedFlushXid == swapKey)
                swappedFlushXid = SwapKey{};
            swapChunks.erase(it);
            // The memory manager still has to unlink the swap file, so the key is handed over to it.
            commitedXids.push_back(swapKey);
            reusedTransactions.notify_all();
        }
        delete sc;
    }

    void Ctx::wontSwap(Thread* t, uint64_t openTransactions, uint64_t swappableTransactions) const {
        t->contextSet(Thread::CONTEXT::MUTEX, Thread::REASON::CTX_SWAPPED_WONT);
        std::unique_lock const lck(memoryMtx);

        if (!outOfMemoryParser) {
            t->contextSet(Thread::CONTEXT::CPU);
            return;
        }

        if (memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] > memoryChunksWriteBufferMin) {
            t->contextSet(Thread::CONTEXT::CPU);
            return;
        }

        hint("try to restart with higher value of 'memory-max-mb' parameter or if big transaction - add to 'skip-xid' list; "
                "transaction would be skipped");
        if (memoryModulesAllocated[static_cast<uint>(MEMORY::READER)] > 5)
            hint("amount of disk buffer is too high, try to decrease 'memory-read-buffer-max-mb' parameter, current utilization: " +
                    std::to_string(chunksToMb(memoryModulesAllocated[static_cast<uint>(MEMORY::READER)])) + "MB");
        if (swappableTransactions == 0 && openTransactions > 0) {
            // The pool usage is reported from the blocks actually handed out: the number of open transactions is
            // not a byte figure (most of them are shells without a single change), so it can not be read as MB.
            const uint64_t usedChunks = memoryChunksAllocated - memoryChunksFree;
            hint("the memory is held by " + std::to_string(openTransactions) + " open transaction(s) and none of them can be swapped "
                    "out: a transaction that holds data owns at least " + std::to_string(memoryChunkSize / 1024) + " KB "
                    "('transaction-block-kb') and only transactions owning more than one block are swappable, so \"max-mb\" has to "
                    "cover the peak number of concurrently open transactions plus the read/write buffers (pool: " +
                    std::to_string(chunksToMb(usedChunks)) + " MB used by " + std::to_string(openTransactions) + " open transaction(s), "
                    "in " + std::to_string(usedChunks) + " block(s) of " + std::to_string(memoryChunkSize / 1024) + " KB)");
        }
        throw RuntimeException(10017, "out of memory");
    }

    void Ctx::stopHard() {
        logTrace(TRACE::THREADS, "stop hard");

        {
            std::unique_lock const lck(mtx);

            if (hardShutdown)
                return;
            hardShutdown = true;
            softShutdown = true;

            condMainLoop.notify_all();
        }
        {
            std::unique_lock const lck(memoryMtx);
            condOutOfMemory.notify_all();
        }
        if (metrics != nullptr) {
            metrics->emitServiceStateInitializing(0);
            metrics->emitServiceStateStarting(0);
            metrics->emitServiceStateReady(0);
            metrics->emitServiceStateReplicating(0);
            metrics->emitServiceStateFinishing(0);
            metrics->emitServiceStateAborting(1);
        }
    }

    void Ctx::stopSoft() {
        logTrace(TRACE::THREADS, "stop soft");

        std::unique_lock const lck(mtx);
        if (softShutdown)
            return;

        softShutdown = true;
        condMainLoop.notify_all();
        if (metrics != nullptr) {
            metrics->emitServiceStateInitializing(0);
            metrics->emitServiceStateStarting(0);
            metrics->emitServiceStateReady(0);
            metrics->emitServiceStateReplicating(0);
            metrics->emitServiceStateFinishing(1);
        }
    }

    void Ctx::mainFinish() {
        logTrace(TRACE::THREADS, "main finish start");

        while (wakeThreads()) {
            usleepInt(10000);
            wakeAllOutOfMemory();
        }

        while (!threads.empty()) {
            Thread* thread;
            {
                std::unique_lock const lck(mtx);
                thread = *(threads.cbegin());
            }
            finishThread(thread);
        }

        logTrace(TRACE::THREADS, "main finish end");
    }

    void Ctx::mainLoop() {
        logTrace(TRACE::THREADS, "main loop start");

        {
            std::unique_lock lck(mtx);
            if (!hardShutdown) {
                if (unlikely(isTraceSet(TRACE::SLEEP)))
                    logTrace(TRACE::SLEEP, "Ctx:mainLoop");
                condMainLoop.wait(lck);
            }
        }

        logTrace(TRACE::THREADS, "main loop end");
    }

    void Ctx::printStacktrace() {
        void* array[128];
        int size;
        std::stringstream result;
        result << "stacktrace for thread: " + std::to_string(reinterpret_cast<uint64_t>(pthread_self())) + "\n";
        {
            std::unique_lock const lck(mtx);
            size = backtrace(array, 128);
        }
        char** ptr = backtrace_symbols(array, size);

        if (ptr == nullptr) {
            result << "empty";
            error(10014, result.str());
            return;
        }

        for (int i = 0; i < size; ++i)
            result << ptr[i] << "\n";

        free(ptr);

        error(10014, result.str());
    }

    void Ctx::signalHandler(int s) {
        if (!hardShutdown) {
            error(10015, "caught signal: " + std::to_string(s));
            stopHard();
        }
    }

    bool Ctx::wakeThreads() {
        logTrace(TRACE::THREADS, "wake threads");

        bool wakingUp = false;
        {
            std::unique_lock const lck(mtx);
            for (Thread* thread: threads) {
                if (!thread->finished) {
                    logTrace(TRACE::THREADS, "waking up thread: " + thread->alias);
                    thread->wakeUp();
                    wakingUp = true;
                }
            }
        }
        wakeAllOutOfMemory();

        return wakingUp;
    }

    void Ctx::spawnThread(Thread* t) {
        logTrace(TRACE::THREADS, "spawn: " + t->alias);

        if (unlikely(pthread_create(&t->pthread, nullptr, &Thread::runStatic, t)))
            throw RuntimeException(10013, "spawning thread: " + t->alias);
        {
            std::unique_lock const lck(mtx);
            threads.insert(t);
        }
    }

    void Ctx::finishThread(Thread* t) {
        logTrace(TRACE::THREADS, "finish: " + t->alias);

        std::unique_lock const lck(mtx);
        if (threads.find(t) == threads.end())
            return;
        threads.erase(t);
        pthread_join(t->pthread, nullptr);
    }

    void Ctx::signalDump() const {
        if (mainThread != pthread_self())
            return;

        std::unique_lock const lck(mtx);
        printMemoryUsageCurrent();
        for (Thread* thread: threads) {
            error(10014, "Dump: " + thread->getName() + " " + std::to_string(reinterpret_cast<uint64_t>(thread->pthread)) +
                  " context: " + std::to_string(static_cast<uint>(thread->curContext)) +
                  " reason: " + std::to_string(static_cast<uint>(thread->curReason)) +
                  " switches: " + std::to_string(thread->contextSwitches));
            pthread_kill(thread->pthread, SIGUSR1);
        }
    }

    void Ctx::usleepInt(uint64_t usec) const {
        if (usec == 0)
            return;

        while (usec > 0 && !softShutdown) {
            uint64_t utm = (usec > 10000) ? 10000 : usec;
            usleep(utm);
            usec -= utm;
        }
    }

    void Ctx::welcome(const std::string& message) const {
        constexpr int code = 0;
        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::hint(const std::string& message) const {
        if (logLevel < LOG::ERROR)
            return;

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " HINT  " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "HINT  " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::error(int code, const std::string& message) const {
        if (logLevel < LOG::ERROR)
            return;

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " ERROR " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "ERROR " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::warning(int code, const std::string& message) const {
        if (logLevel < LOG::WARNING)
            return;

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " WARN  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "WARN  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::info(int code, const std::string& message) const {
        if (logLevel < LOG::INFO)
            return;

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "INFO  " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::debug(int code, const std::string& message) const {
        if (logLevel < LOG::DEBUG)
            return;

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " DEBUG " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "DEBUG " << std::setw(5) << std::setfill('0') << std::dec << code << " " << message << std::endl;
            std::cerr << s.str();
        }
    }

    void Ctx::logTraceInt(TRACE mask, const std::string& message) const {
        std::string code;
        switch (mask) {
            case TRACE::DML:
                code = "DML  ";
                break;

            case TRACE::DUMP:
                code = "DUMP ";
                break;

            case TRACE::LOB:
                code = "LOB  ";
                break;

            case TRACE::LWN:
                code = "LWN  ";
                break;

            case TRACE::THREADS:
                code = "THRD ";
                break;

            case TRACE::SQL:
                code = "SQL  ";
                break;

            case TRACE::FILE:
                code = "FILE ";
                break;

            case TRACE::DISK:
                code = "DISK ";
                break;

            case TRACE::PERFORMANCE:
                code = "PERFM";
                break;

            case TRACE::TRANSACTION:
                code = "TRANX";
                break;

            case TRACE::REDO:
                code = "REDO ";
                break;

            case TRACE::ARCHIVE_LIST:
                code = "ARCHL";
                break;

            case TRACE::SCHEMA_LIST:
                code = "SCHEM";
                break;

            case TRACE::WRITER:
                code = "WRITR";
                break;

            case TRACE::CHECKPOINT:
                code = "CHKPT";
                break;

            case TRACE::SYSTEM:
                code = "SYSTM";
                break;

            case TRACE::LOB_DATA:
                code = "LOBDT";
                break;

            case TRACE::SLEEP:
                code = "SLEEP";
                break;

            case TRACE::CONDITION:
                code = "CONDT";
                break;

            case TRACE::STREAM:
                code = "STRM ";
                break;
        }

        if (OLR_LOCALES == LOCALES::TIMESTAMP) {
            std::ostringstream s;
            char timestamp[30];
            Data::epochToIso8601(clock->getTimeT() + logTimezone, timestamp, false, false);
            s << timestamp << " TRACE " << code << " " << message << '\n';
            std::cerr << s.str();
        } else {
            std::ostringstream s;
            s << "TRACE " << code << " " << message << '\n';
            std::cerr << s.str();
        }
    }

    void Ctx::printMemoryUsageHWM() const {
        info(0, "Memory HWM: " + std::to_string(getMemoryHWM()) + "MB, builder HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::BUILDER)] * MEMORY_CHUNK_SIZE_MB) + "MB, misc HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::MISC)] * MEMORY_CHUNK_SIZE_MB) + "MB, parser HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::PARSER)] * MEMORY_CHUNK_SIZE_MB) + "MB, disk read buffer HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::READER)] * MEMORY_CHUNK_SIZE_MB) + "MB, transaction HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::TRANSACTIONS)] * MEMORY_CHUNK_SIZE_MB) + "MB, swapped: " +
             std::to_string(swappedMB) + "MB, disk write buffer HWM: " +
             std::to_string(memoryModulesHWM[static_cast<uint>(MEMORY::WRITER)] * MEMORY_CHUNK_SIZE_MB) + "MB");
    }

    void Ctx::printMemoryUsageCurrent() const {
        info(0, "Memory current swap: " + std::to_string(memoryChunksSwap * MEMORY_CHUNK_SIZE_MB) + "MB, allocated: " +
             std::to_string(memoryChunksAllocated * MEMORY_CHUNK_SIZE_MB) + "MB, free: " +
             std::to_string(memoryChunksFree * MEMORY_CHUNK_SIZE_MB) + "MB, memory builder: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::BUILDER)] * MEMORY_CHUNK_SIZE_MB) + "MB, misc: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::MISC)] * MEMORY_CHUNK_SIZE_MB) + "MB, parser: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::PARSER)] * MEMORY_CHUNK_SIZE_MB) + "MB, disk read buffer: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::READER)] * MEMORY_CHUNK_SIZE_MB) + "MB, transaction: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::TRANSACTIONS)] * MEMORY_CHUNK_SIZE_MB) + "MB, swapped: " +
             std::to_string(swappedMB) + "MB, disk write buffer: " +
             std::to_string(memoryModulesAllocated[static_cast<uint>(MEMORY::WRITER)] * MEMORY_CHUNK_SIZE_MB) + "MB");
    }
}
