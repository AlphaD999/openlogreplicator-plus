/* Thread reading database redo Logs using offline mode
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

#include <cerrno>
#include <cstddef>
#include <dirent.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "../common/DbIncarnation.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RedoLogException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/types/Seq.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/DeliveryExecutor.h"
#include "../parser/Parser.h"
#include "../parser/Transaction.h"
#include "../reader/ReaderFilesystem.h"
#include "Replicator.h"

namespace OpenLogReplicator {
    Replicator::Replicator(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                           TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase):
            Thread(newCtx, std::move(newAlias)),
            archGetLog(newArchGetLog),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            database(std::move(newDatabase)),
            deliveryExecutor(newCtx, newBuilder, newMetadata, newTransactionBuffer, newMetadata->commitSorter) {
        ctx->parserThread = this;
    }

    Replicator::~Replicator() {
        readerDropAll();

        for (const Parser* parser: archiveRedoQueue)
            delete parser;
        archiveRedoQueue.clear();

        for (const Parser* parser: gapArchives)
            delete parser;
        gapArchives.clear();

        for (const Parser* parser: onlineRedoSet)
            delete parser;
        onlineRedoSet.clear();

        pathMapping.clear();
        redoLogsBatch.clear();
    }

    void Replicator::initialize() {}

    void Replicator::cleanArchList() {
        for (const Parser* parser: archiveRedoQueue)
            delete parser;
        archiveRedoQueue.clear();

        for (const Parser* parser: gapArchives)
            delete parser;
        gapArchives.clear();
    }

    bool Replicator::archiveQueued(uint16_t thread, Seq sequence) const {
        // Diagnosis (TRACE::CHECKPOINT): one line per skip, i.e. per archive the list handed out again while a
        // Parser for it was still waiting here - exactly the case in which a second Parser would have been built
        // and would have read the file from its beginning (the saved resume position is a single slot, see the
        // comment on the declaration in Replicator.h). Counting these lines is what says whether this guard still
        // has work to do in a given replication; the trace sits in this predicate and not at the call sites so
        // that every caller is covered and no counter member is needed.
        for (const Parser* parser: archiveRedoQueue)
            if (parser->thread == thread && parser->sequence == sequence) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                    ctx->logTrace(Ctx::TRACE::CHECKPOINT, "archiveQueued: skip thread " + std::to_string(thread) + " seq: " +
                                  sequence.toString() + " - still queued: " + std::to_string(archiveRedoQueue.size()) +
                                  ", parked: " + std::to_string(gapArchives.size()));
                return true;
            }

        for (const Parser* parser: gapArchives)
            if (parser->thread == thread && parser->sequence == sequence) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                    ctx->logTrace(Ctx::TRACE::CHECKPOINT, "archiveQueued: skip thread " + std::to_string(thread) + " seq: " +
                                  sequence.toString() + " - still parked: " + std::to_string(gapArchives.size()) +
                                  ", queued: " + std::to_string(archiveRedoQueue.size()));
                return true;
            }

        return false;
    }

    size_t Replicator::bestArchiveIndex() const {
        size_t best = 0;

        for (size_t i = 1; i < archiveRedoQueue.size(); ++i) {
            const Parser* p1 = archiveRedoQueue[i];
            const Parser* p2 = archiveRedoQueue[best];
            const Scn scn1 = p1->orderScn();
            const Scn scn2 = p2->orderScn();

            bool better;
            if (scn1 == scn2) {
                if (p1->thread != p2->thread)
                    better = p1->thread < p2->thread;
                else
                    better = p1->sequence < p2->sequence;
            } else if (scn1 == Scn::none())
                better = false;
            else if (scn2 == Scn::none())
                better = true;
            else
                better = scn1 < scn2;

            if (better)
                best = i;
        }

        return best;
    }

    void Replicator::updateOnlineLogs() const {
        for (Parser* onlineRedo: onlineRedoSet) {
            if (!onlineRedo->reader->updateRedoLog())
                throw RuntimeException(10039, "updating of online redo logs failed for " + onlineRedo->path);
            onlineRedo->sequence = onlineRedo->reader->getSequence();
            onlineRedo->firstScn = onlineRedo->reader->getFirstScn();
            onlineRedo->nextScn = onlineRedo->reader->getNextScn();
        }
    }

    void Replicator::readerDropAll() {
        for (;;) {
            bool wakingUp = false;
            for (Reader* reader: readers) {
                if (!reader->finished) {
                    reader->wakeUp();
                    wakingUp = true;
                }
            }
            if (!wakingUp)
                break;
            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(1000);
            contextSet(CONTEXT::CPU);
        }

        while (!readers.empty()) {
            Reader* reader = *(readers.begin());
            ctx->finishThread(reader);
            readers.erase(reader);
            delete reader;
        }

        archReader = nullptr;
        readers.clear();
    }

    void Replicator::loadDatabaseMetadata() {
        archReader = readerCreate(0);
    }

    void Replicator::positionReader() {
        if (metadata->startSequence != Seq::none())
            metadata->setSeqFileOffset(metadata->startSequence, FileOffset::zero());
        else
            metadata->setSeqFileOffset(Seq(Seq::zero()), FileOffset::zero());
    }

    void Replicator::verifySchema(Scn currentScn __attribute__((unused))) {
        // Nothing for offline mode
    }

    void Replicator::createSchema() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
            metadata->allowCheckpoints();
            return;
        }

        throw RuntimeException(10040, "schema file missing");
    }

    void Replicator::updateOnlineRedoLogData() {
        int64_t lastGroup = -1;
        Reader* onlineReader = nullptr;

        for (const auto* redoLog: metadata->redoLogs) {
            if (redoLog->group != lastGroup || onlineReader == nullptr) {
                onlineReader = readerCreate(redoLog->group);
                onlineReader->paths.clear();
                lastGroup = redoLog->group;
            }
            onlineReader->paths.push_back(redoLog->path);
        }

        checkOnlineRedoLogs();
    }

    void Replicator::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") start");
        }

        // Stage 4: the delivery runs on a thread of its own, so this thread never delivers - it parses, publishes
        // its water-mark and enqueues (see Parser::parse). The thread is spawned here and not when it is first
        // needed: the moment it is needed (defect C: the parser blocked in Ctx::swappedMemoryInit) is exactly the
        // moment the memory pool is at its limit and the replicator is the blocked thread.
        if (metadata->commitSorter != nullptr && !deliveryStarted) {
            deliveryStarted = true;
            ctx->spawnThread(&deliveryExecutor);
        }

        try {
            metadata->waitForWriter(ctx->parserThread);

            loadDatabaseMetadata();
            metadata->readCheckpoints();
            if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                updateOnlineRedoLogData();
            else if (metadata->resetlogs == 0)
                // Archives-only mode (configured explicitly, or a RAC database detected automatically): online
                // redo log data is not read, but the resetlogs of the current incarnation is still needed to
                // locate the starting sequence. Read it from V$DATABASE_INCARNATION - a view query, no online
                // redo log file is touched. A resetlogs already known from a checkpoint file wins.
                loadIncarnations();
            ctx->info(0, "timezone: " + Data::timezoneToString(-timezone) + ", db-timezone: " + Data::timezoneToString(metadata->dbTimezone) +
                      ", log-timezone: " + Data::timezoneToString(ctx->logTimezone) + ", host-timezone: " + Data::timezoneToString(ctx->hostTimezone));

            do {
                if (ctx->softShutdown)
                    break;
                metadata->waitForWriter(ctx->parserThread);

                if (metadata->status == Metadata::STATUS::READY)
                    continue;

                if (ctx->softShutdown)
                    break;
                try {
                    printStartMsg();
                    if (metadata->resetlogs != 0)
                        ctx->info(0, "current resetlogs is: " + std::to_string(metadata->resetlogs));
                    if (metadata->firstDataScn != Scn::none())
                        ctx->info(0, "first data SCN: " + metadata->firstDataScn.toString());
                    if (metadata->firstSchemaScn != Scn::none())
                        ctx->info(0, "first schema SCN: " + metadata->firstSchemaScn.toString());

                    if (metadata->firstDataScn == Scn::none() || metadata->sequence == Seq::none())
                        positionReader();

                    // No schema available?
                    if (metadata->schema->scn == Scn::none())
                        createSchema();
                    else
                        metadata->allowCheckpoints();
                    metadata->schema->updateXmlCtx();

                    if (metadata->sequence == Seq::none())
                        throw BootException(10028, "starting sequence is unknown");

                    if (metadata->firstDataScn == Scn::none())
                        ctx->info(0, "last confirmed scn: <none>, starting sequence: " + metadata->sequence.toString() + ", offset: " +
                                  metadata->fileOffset.toString());
                    else
                        ctx->info(0, "last confirmed scn: " + metadata->firstDataScn.toString() + ", starting sequence: " +
                                  metadata->sequence.toString() + ", offset: " + metadata->fileOffset.toString());

                    if ((metadata->dbBlockChecksum == "OFF" || metadata->dbBlockChecksum == "FALSE") &&
                            !ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
                        ctx->hint("set DB_BLOCK_CHECKSUM = TYPICAL on the database or turn off consistency checking in OpenLogReplicator "
                                "setting parameter disable-checks: " + std::to_string(static_cast<uint>(Ctx::DISABLE_CHECKS::BLOCK_SUM)) +
                                " for the reader");
                    }
                } catch (BootException& ex) {
                    if (!metadata->bootFailsafe)
                        throw RuntimeException(ex.code, ex.msg);

                    ctx->error(ex.code, ex.msg);
                    ctx->info(0, "replication startup failed, waiting for further commands");
                    metadata->setStatusReady(this);
                    continue;
                }

                // Boot succeeded
                ctx->info(0, "resume writer");
                metadata->setStatusReplicating(this);
            } while (metadata->status != Metadata::STATUS::REPLICATING);

            while (!ctx->softShutdown) {
                bool logsProcessed = false;

                logsProcessed |= processArchivedRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!continueWithOnline())
                    break;
                if (ctx->softShutdown)
                    break;

                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                    logsProcessed |= processOnlineRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!logsProcessed) {
                    ctx->info(0, "no redo logs to process, waiting for new redo logs");
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->refreshIntervalUs);
                    contextSet(CONTEXT::CPU);
                }
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RedoLogException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        ctx->info(0, "Replicator for: " + database + " is shutting down");
        transactionBuffer->purge();

        ctx->replicatorFinished = true;
        ctx->printMemoryUsageHWM();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") stop");
        }
    }

    Reader* Replicator::readerCreate(int group) {
        for (Reader* reader: readers)
            if (reader->getGroup() == group)
                return reader;

        auto* readerFS = new ReaderFilesystem(ctx, alias + "-reader-" + std::to_string(group), database, group,
                                              metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
        readers.insert(readerFS);
        readerFS->initialize();

        ctx->spawnThread(readerFS);
        return readerFS;
    }

    void Replicator::checkOnlineRedoLogs() {
        for (const Parser* onlineRedo: onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (Reader* reader: readers) {
            if (reader->getGroup() == 0)
                continue;

            bool foundPath = false;
            for (const std::string& path: reader->paths) {
                reader->fileName = path;
                applyMapping(reader->fileName);
                if (reader->checkRedoLog()) {
                    foundPath = true;
                    auto* parser = new Parser(ctx, builder, metadata, transactionBuffer,
                                              reader->getGroup(), reader->fileName);

                    parser->reader = reader;
                    ctx->info(0, "online redo log: " + reader->fileName);
                    onlineRedoSet.insert(parser);
                    break;
                }
            }

            if (!foundPath) {
                const int64_t badGroup = reader->getGroup();
                for (const std::string& path: reader->paths) {
                    std::string pathMapped(path);
                    applyMapping(pathMapped);
                    reader->showHint(this, path, pathMapped);
                }
                throw RuntimeException(10027, "can't read any member of group " + std::to_string(badGroup));
            }
        }
    }

    // Format uses wildcards:
    // %s - sequence number
    // %S - sequence number zero filled
    // %t - thread id
    // %T - thread id zero filled
    // %r - resetlogs id
    // %a - activation id
    // %d - database id
    // %h - some hash
    Seq Replicator::getSequenceFromFileName(const Replicator* replicator, const std::string& file, uint16_t& thread) {
        Seq sequence{0};
        thread = 1;
        size_t i{};
        size_t j{};

        while (i < replicator->metadata->logArchiveFormat.length() && j < file.length()) {
            if (replicator->metadata->logArchiveFormat[i] == '%') {
                if (i + 1 >= replicator->metadata->logArchiveFormat.length()) {
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                             replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                             std::to_string(i) + ", found end after %");
                    return Seq::zero();
                }
                uint digits = 0;
                if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 't' || replicator->metadata->logArchiveFormat[i + 1] == 'T' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'r' || replicator->metadata->logArchiveFormat[i + 1] == 'a' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'd') {
                    // Some [0-9]*
                    uint32_t number{};
                    while (j < file.length() && file[j] >= '0' && file[j] <= '9') {
                        number = number * 10 + (file[j] - '0');
                        ++j;
                        ++digits;
                    }

                    if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S')
                        sequence = Seq(number);
                    else if (replicator->metadata->logArchiveFormat[i + 1] == 't' || replicator->metadata->logArchiveFormat[i + 1] == 'T')
                        thread = static_cast<uint16_t>(number);
                    i += 2;
                } else if (replicator->metadata->logArchiveFormat[i + 1] == 'h') {
                    // Some [0-9a-z]*
                    while (j < file.length() && ((file[j] >= '0' && file[j] <= '9') || (file[j] >= 'a' && file[j] <= 'z'))) {
                        ++j;
                        ++digits;
                    }
                    i += 2;
                }

                if (digits == 0) {
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                             replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                             std::to_string(i) + ", found no number/hash");
                    return Seq::zero();
                }
            } else if (file[j] == replicator->metadata->logArchiveFormat[i]) {
                ++i;
                ++j;
            } else {
                replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                         replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                         std::to_string(i) + ", found different values");
                return Seq::zero();
            }
        }

        if (i == replicator->metadata->logArchiveFormat.length() && j == file.length())
            return sequence;

        replicator->ctx->warning(60028, "error getting sequence from file: " + file + " log_archive_format: " +
                                 replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                 std::to_string(i) + ", found no sequence");
        return Seq::zero();
    }

    void Replicator::addPathMapping(std::string source, std::string target) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "added mapping [" + source + "] -> [" + target + "]");
        pathMapping.push_back(std::move(source));
        pathMapping.push_back(std::move(target));
    }

    void Replicator::addRedoLogsBatch(std::string path) {
        redoLogsBatch.emplace_back(std::move(path));
    }

    void Replicator::applyMapping(std::string& path) const {
        const size_t newPathLength = path.length();
        std::array<char, Ctx::MAX_PATH_LENGTH> pathBuffer{};

        for (size_t i = 0; i < pathMapping.size() / 2; ++i) {
            const uint64_t sourceLength = pathMapping[i * 2].length();
            const uint64_t targetLength = pathMapping[(i * 2) + 1].length();

            if (sourceLength <= newPathLength &&
                    newPathLength - sourceLength + targetLength < Ctx::MAX_PATH_LENGTH - 1 &&
                    memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {
                memcpy(pathBuffer.data(), pathMapping[i * 2 + 1].c_str(), targetLength);
                memcpy(pathBuffer.data() + targetLength, path.c_str() + sourceLength, newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                path.assign(pathBuffer.data());
                break;
            }
        }
    }

    bool Replicator::checkConnection() {
        return true;
    }

    void Replicator::goStandby() {}

    bool Replicator::continueWithOnline() {
        return true;
    }

    std::string Replicator::getModeName() const {
        return {"offline"};
    }

    void Replicator::archGetLogPath(Replicator* replicator) {
        if (replicator->metadata->logArchiveFormat.empty())
            throw RuntimeException(10044, "missing location of archived redo logs for offline mode");

        std::string mappedPath(replicator->metadata->dbRecoveryFileDest + "/" + replicator->metadata->context + "/archivelog");
        replicator->applyMapping(mappedPath);
        if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
            replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

        DIR* dir = opendir(mappedPath.c_str());
        if (dir == nullptr)
            throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

        std::string newLastCheckedDay;
        const dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            const std::string dName(ent->d_name);
            if (dName == "." || dName == "..")
                continue;

            struct stat fileStat{};
            const std::string mappedSubPath(mappedPath + "/" + ent->d_name);
            if (stat(mappedSubPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedSubPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            // Skip earlier days
            if (replicator->lastCheckedDay.empty() && replicator->lastCheckedDay == ent->d_name)
                continue;

            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath + "/" + ent->d_name);

            const std::string mappedPathWithFile(mappedPath + "/" + ent->d_name);
            DIR* dir2 = opendir(mappedPathWithFile.c_str());
            if (dir2 == nullptr) {
                closedir(dir);
                throw RuntimeException(10012, "directory: " + mappedPathWithFile + " - can't read");
            }

            const dirent* ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                const std::string dName2(ent2->d_name);
                if (dName2 == "." || dName2 == "..")
                    continue;

                const std::string fileName(mappedPath + "/" + ent->d_name + "/" + ent2->d_name);
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                uint16_t fileThread = 1;
                const Seq sequence = getSequenceFromFileName(replicator, ent2->d_name, fileThread);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString() + " thread: " +
                                              std::to_string(fileThread));

                if (sequence == Seq::zero() || sequence < replicator->metadata->threadSequence(fileThread))
                    continue;

                // Same archive still waiting in the queue (a sliced or parked one): do not build a second
                // Parser for it, it would re-read the file from its beginning (see Replicator::archiveQueued)
                if (replicator->archiveQueued(fileThread, sequence))
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, fileName);

                parser->firstScn = Scn::none();
                parser->nextScn = Scn::none();
                parser->sequence = sequence;
                parser->thread = fileThread;
                replicator->archiveRedoQueue.emplace_back(parser);
            }
            closedir(dir2);

            if (newLastCheckedDay.empty() || (newLastCheckedDay != ent->d_name))
                newLastCheckedDay = ent->d_name;
        }
        closedir(dir);

        if (!newLastCheckedDay.empty() || (!replicator->lastCheckedDay.empty() && replicator->lastCheckedDay.compare(newLastCheckedDay) < 0)) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "updating last checked day to: " + newLastCheckedDay);
            replicator->lastCheckedDay = newLastCheckedDay;
        }
    }

    void Replicator::archGetLogList(Replicator* replicator) {
        Seq sequenceStart = Seq::none();
        for (const std::string& mappedPath: replicator->redoLogsBatch) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

            struct stat fileStat{};
            if (stat(mappedPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            // Single file
            if (!S_ISDIR(fileStat.st_mode)) {
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

                // Getting file name from the path
                const char* fileName = mappedPath.c_str();
                size_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                uint16_t fileThread = 1;
                const Seq sequence = getSequenceFromFileName(replicator, fileName + j, fileThread);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString() + " thread: " +
                                              std::to_string(fileThread));

                if (sequence == Seq::zero() || sequence < replicator->metadata->threadSequence(fileThread))
                    continue;

                // The same archive can still be waiting in the queue or in the parked list (a sliced one for its
                // resume, one waiting for a missing predecessor): a second Parser for it would be parsed as well,
                // from the beginning of the file - the saved resume position is a single slot and one of the two
                // consumes it (see Replicator::archiveQueued).
                if (replicator->archiveQueued(fileThread, sequence))
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = Scn::none();
                parser->nextScn = Scn::none();
                parser->sequence = sequence;
                parser->thread = fileThread;
                replicator->archiveRedoQueue.emplace_back(parser);
                if (sequenceStart == Seq::none() || sequenceStart > sequence)
                    sequenceStart = sequence;

            } else {
                // Dir, check all files
                DIR* dir = opendir(mappedPath.c_str());
                if (dir == nullptr)
                    throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

                const dirent* ent;
                while ((ent = readdir(dir)) != nullptr) {
                    const std::string dName(ent->d_name);
                    if (dName == "." || dName == "..")
                        continue;

                    const std::string fileName(mappedPath + "/" + ent->d_name);
                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                    uint16_t fileThread = 1;
                    const Seq sequence = getSequenceFromFileName(replicator, ent->d_name, fileThread);

                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString() + " thread: " +
                                                  std::to_string(fileThread));

                    if (sequence == Seq::zero() || sequence < replicator->metadata->threadSequence(fileThread))
                        continue;

                    // Same as in the single file branch above: do not build a second Parser for an archive that is
                    // still waiting in the queue or in the parked list (see Replicator::archiveQueued).
                    if (replicator->archiveQueued(fileThread, sequence))
                        continue;

                    auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                              replicator->transactionBuffer, 0, fileName);
                    parser->firstScn = Scn::none();
                    parser->nextScn = Scn::none();
                    parser->sequence = sequence;
                    parser->thread = fileThread;
                    replicator->archiveRedoQueue.emplace_back(parser);
                }
                closedir(dir);
            }
        }

        if (sequenceStart != Seq::none() && replicator->metadata->sequence == Seq::zero())
            replicator->metadata->setSeqFileOffset(sequenceStart, FileOffset::zero());
        replicator->redoLogsBatch.clear();
    }

    void Replicator::loadIncarnations() {
        // No database connection in this mode (offline / batch): a checkpoint file has to provide everything
    }

    void Replicator::updateResetlogs() {
        contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_UPDATE);
        std::unique_lock const lck(metadata->mtxCheckpoint);

        for (DbIncarnation* oi: metadata->dbIncarnations) {
            if (oi->resetlogs == metadata->resetlogs) {
                metadata->dbIncarnationCurrent = oi;
                break;
            }
        }

        // Resetlogs is changed. Guard against a null current incarnation (the list can be non-empty while
        // nothing matched) - that case is reported as error 10045 below instead of dereferencing a null pointer.
        if (metadata->dbIncarnationCurrent != nullptr) {
            for (const DbIncarnation* oi: metadata->dbIncarnations) {
                if (oi->resetlogsScn == metadata->nextScn &&
                    metadata->dbIncarnationCurrent->resetlogs == metadata->resetlogs &&
                    oi->priorIncarnation == metadata->dbIncarnationCurrent->incarnation) {
                    ctx->info(0, "new resetlogs detected: " + std::to_string(oi->resetlogs));
                    metadata->setResetlogs(oi->resetlogs);
                    metadata->sequence = Seq::zero();
                    metadata->fileOffset = FileOffset::zero();
                    metadata->threadPos.clear();
                    contextSet(CONTEXT::CPU);
                    return;
                }
            }
        }

        if (metadata->dbIncarnations.empty()) {
            contextSet(CONTEXT::CPU);
            return;
        }

        if (metadata->dbIncarnationCurrent == nullptr) {
            contextSet(CONTEXT::CPU);
            throw RuntimeException(10045, "resetlogs (" + std::to_string(metadata->resetlogs) + ") not found in incarnation list");
        }
        contextSet(CONTEXT::CPU);
    }

    void Replicator::wakeUp() {
        metadata->wakeUp(this);
    }

    void Replicator::printStartMsg() const {
        std::string flagsStr;
        if (ctx->flags != 0)
            flagsStr = " (flags: " + std::to_string(ctx->flags) + ")";

        std::string starting;
        if (!metadata->startTime.empty())
            starting = "time: " + metadata->startTime;
        else if (metadata->startTimeRel > 0)
            starting = "time-rel: " + std::to_string(metadata->startTimeRel);
        else if (metadata->startScn != Scn::none())
            starting = "scn: " + metadata->startScn.toString();
        else
            starting = "NOW";

        std::string startingSeq;
        if (metadata->startSequence != Seq::none())
            startingSeq = ", seq: " + metadata->startSequence.toString();

        ctx->info(0, "Replicator for " + database + " in " + getModeName() + " mode is starting" + flagsStr + " from " + starting +
                  startingSeq);
    }

    void Replicator::observeArchiveSequence(uint16_t thread, Seq sequence, uint64_t currentTime) {
        const auto it = threadMaxSequence.find(thread);
        if (it == threadMaxSequence.end()) {
            threadMaxSequence.try_emplace(thread, sequence);
            threadMaxSequenceTime.try_emplace(thread, currentTime);
            return;
        }

        // Only a higher sequence counts as an advance: the same archive is returned by every pass of the list,
        // so a repeated row must not keep the thread looking alive.
        if (sequence > it->second) {
            it->second = sequence;
            threadMaxSequenceTime[thread] = currentTime;
        }
    }

    uint64_t Replicator::threadInactiveSec(uint16_t thread, uint64_t currentTime) const {
        const auto itTime = threadMaxSequenceTime.find(thread);
        if (itTime == threadMaxSequenceTime.end() || threadMaxSequence.find(thread) == threadMaxSequence.end() ||
            currentTime <= itTime->second)
            return 0;
        return (currentTime - itTime->second) / 1000000;
    }

    Seq Replicator::getThreadMaxSequence(uint16_t thread) const {
        const auto it = threadMaxSequence.find(thread);
        if (it == threadMaxSequence.end())
            return Seq::none();
        return it->second;
    }

    std::string Replicator::archiveWaitState() const {
        if (threadMaxSequence.empty())
            return "the archive list source does not know any redo thread yet";

        std::ostringstream ss;
        ss << "the archive list source reports";
        for (const auto& entry: threadMaxSequence)
            ss << " thread " << entry.first << ": expected seq " << metadata->threadSequence(entry.first).toString() << ", highest known seq "
               << entry.second.toString() << ";";
        return ss.str();
    }

    bool Replicator::processArchivedRedoLogs() {
        Reader::REDO_CODE ret;
        Parser* parser;
        bool logsProcessed = false;

        while (!ctx->softShutdown) {
            // Whether this pass of the loop managed to read anything. Everything left in the queue can be
            // dropped without reading a byte (already parsed archives, archives parked because a predecessor
            // is missing), and the pass has to be told apart from one that did real work.
            bool processedThisPass = false;
            uint64_t skippedThisPass = 0;

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "checking archived redo logs, seq: " + metadata->sequence.toString());
            updateResetlogs();
            archGetLog(this);

            if (archiveRedoQueue.empty()) {
                if (ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY)) {
                    const uint64_t nowUs = ctx->clock->getTimeUt();
                    if (archWaitSince == 0)
                        archWaitSince = nowUs;

                    // An empty list is the one silent case: no row of the list mentions the archives of any
                    // thread, so 60027/60038 (which need a later archive of the same thread to be present) can
                    // not fire and the source looks idle. Report it - with the expected sequence of every thread
                    // against the highest sequence the source reported - and back off, so a source that is down
                    // for hours does not fill the log while a source that is minutes late is still visible.
                    if (ctx->archWaitWarnUs > 0 && nowUs - archWaitSince >= ctx->archWaitWarnUs) {
                        uint64_t interval = ctx->archWaitWarnUs;
                        for (uint64_t n = 0; n < archWaitReportCount && interval < ctx->archWaitWarnUs * 8; ++n)
                            interval *= 2;
                        if (archWaitReportTime == 0 || nowUs - archWaitReportTime >= interval) {
                            archWaitReportTime = nowUs;
                            ++archWaitReportCount;
                            ctx->warning(60056, "no archived redo log to read for: " + std::to_string((nowUs - archWaitSince) / 1000000) +
                                         " s - " + archiveWaitState() + " (\"arch-wait-warn-us\")");
                        }
                    }

                    // Waiting for an archive is an expected wait, not a stalled parser: 60056 reports it, the
                    // progress watchdog (60055) must stay quiet.
                    ctx->watchdogNoteIdle();

                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "archived redo log missing for seq: " + metadata->sequence.toString() +
                                      ", sleeping");
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->archReadSleepUs);
                    contextSet(CONTEXT::CPU);
                } else {
                    break;
                }
            } else if (archWaitSince != 0) {
                ctx->info(0, "archived redo log available after: " + std::to_string((ctx->clock->getTimeUt() - archWaitSince) / 1000000) +
                          " s of waiting for the archive list source");
                archWaitSince = 0;
                archWaitReportTime = 0;
                archWaitReportCount = 0;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
                std::ostringstream ss;
                ss << std::this_thread::get_id();
                ctx->logTrace(Ctx::TRACE::REDO, "searching archived redo log for seq: " + metadata->sequence.toString());
            }
            while (!archiveRedoQueue.empty() && !ctx->softShutdown) {
                const size_t bestIndex = bestArchiveIndex();
                parser = archiveRedoQueue[bestIndex];
                archiveRedoQueue.erase(archiveRedoQueue.begin() + static_cast<std::ptrdiff_t>(bestIndex));
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, parser->path + " is seq: " + parser->sequence.toString() + " thread: " +
                                  std::to_string(parser->thread) + ", scn: " + parser->firstScn.toString());

                // When no metadata exists for the thread, start processing from the first file
                Seq expectedSequence = metadata->threadSequence(parser->thread);
                if (expectedSequence == Seq::zero() || expectedSequence == Seq::none()) {
                    contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_ARCH);
                    metadata->setSeqFileOffset(parser->sequence, FileOffset::zero(), parser->thread);
                    contextSet(CONTEXT::CPU);
                    expectedSequence = parser->sequence;
                }

                // Skip older archived redo logs
                if (parser->sequence < expectedSequence) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "skipping already parsed archive: thread: " +
                                      std::to_string(parser->thread) + ", seq: " + parser->sequence.toString() +
                                      ", expected: " + expectedSequence.toString());
                    ++skippedThisPass;
                    delete parser;
                    continue;
                }

                if (parser->sequence > expectedSequence) {
                    if (metadata->rac) {
                        const uint64_t now = ctx->clock->getTimeUt();
                        const uint64_t inactiveSec = threadInactiveSec(parser->thread, now);
                        const Seq threadMaxSeq = getThreadMaxSequence(parser->thread);
                        const std::string threadMaxSeqStr = threadMaxSeq == Seq::none() ? std::string("none") : threadMaxSeq.toString();
                        // The thread is released only when its own archive stream stopped growing: a thread that
                        // keeps producing archives moves its highest sequence on every arrival, so this is the
                        // "the instance behind this thread is gone" signal - taken from the archived redo log
                        // list itself. SYS.V_$THREAD can not be used for it: on a physical standby it reports
                        // whether the standby instance itself has the thread attached, so the threads of the
                        // other instances look CLOSED while they keep archiving (see Replicator.h).
                        if (ctx->threadInactiveUs > 0 && inactiveSec > 0 && inactiveSec * 1000000 >= ctx->threadInactiveUs) {
                            // Inactive thread stops holding back the global low-water-mark
                            if (metadata->commitSorter != nullptr)
                                metadata->commitSorter->threadClosed(parser->thread);
                            ctx->warning(60038, "couldn't find archive log for thread: " + std::to_string(parser->thread) +
                                         " seq: " + expectedSequence.toString() + ", found: " + parser->sequence.toString() +
                                         ", highest known sequence of the thread: " + threadMaxSeqStr +
                                         ", no new archived redo log for this thread for: " + std::to_string(inactiveSec) +
                                         " s (\"thread-inactive-us\") - thread treated as inactive, it stops holding back the "
                                         "low-water-mark, its transactions can be delivered after transactions with a higher SCN, "
                                         "sleeping " + std::to_string(ctx->archReadSleepUs) + " us");
                        } else
                            ctx->warning(60027, "couldn't find archive log for thread: " + std::to_string(parser->thread) +
                                         " seq: " + expectedSequence.toString() + ", found: " + parser->sequence.toString() +
                                         ", sleeping " + std::to_string(ctx->archReadSleepUs) + " us");
                    } else {
                        ctx->warning(60027, "couldn't find archive log for seq: " + expectedSequence.toString() + ", found: " +
                                     parser->sequence.toString() + ", sleeping " + std::to_string(ctx->archReadSleepUs) + " us");
                    }
                    // Park the archive instead of blocking here: another redo thread may still be parseable and
                    // must not be held back by a missing archive of this one. When nothing else is left, the
                    // parked archives are dropped together with the rest of the list and it is read again from
                    // the database - a late archive is picked up as soon as it appears.
                    gapArchives.emplace_back(parser);
                    if (archiveRedoQueue.empty()) {
                        contextSet(CONTEXT::SLEEP);
                        ctx->usleepInt(ctx->archReadSleepUs);
                        contextSet(CONTEXT::CPU);
                        cleanArchList();
                        archGetLog(this);
                    }
                    continue;
                }

                logsProcessed = true;
                processedThisPass = true;
                // An archive is about to be parsed: the wait for one is over, so the progress watchdog (60055) is
                // armed again (it is suppressed while the archive list has nothing to hand out).
                ctx->parserWaitingForArchive.store(false, std::memory_order_relaxed);
                if (metadata->commitSorter != nullptr)
                    metadata->commitSorter->threadReopened(parser->thread);
                parser->reader = archReader;

                // The archive's sequence is known from the list this parser came from: hand it to the reader
                // before it opens the file, so that every block of it is checked against the archive it claims to
                // belong to (Reader::checkBlockHeader) instead of being taken over from a block header - a block
                // of another archive passes every other check (same block number, a real redo CRC, a SCN inside
                // the range) and made a pass parse foreign bytes as its own records.
                archReader->setArchiveSequence(parser->sequence);

                archReader->fileName = parser->path;
                uint retry = ctx->archReadTries;
                // Set when the archive is dropped instead of read (see 10074): the retry loop below breaks
                // either on a successful open or on this flag, and the parser must not be parsed in the
                // second case.
                bool archiveSkipped = false;

                while (true) {
                    if (ctx->softShutdown)
                        break;
                    if (archReader->checkRedoLog() && archReader->updateRedoLog()) {
                        // Opening the file re-reads the archive list (checkRedoLog/updateRedoLog): hand the
                        // archive identity over once more, so the first block the parser reads is checked against
                        // it and not adopted from a block header (see checkBlockHeader).
                        archReader->setArchiveSequence(parser->sequence);
                        break;
                    }

                    if (retry == 0) {
                        archReader->showHint(this, "", parser->path);
                        if (!ctx->skipUnreadableArchive)
                            throw RuntimeException(10009, "file: " + parser->path + " - failed to open after " +
                                                   std::to_string(ctx->archReadTries) + " tries");
                        // Explicitly skip this one archive of this thread instead of stopping the process. Only
                        // threadPos of this thread is advanced (setNextSequence defaults to thread 1, so the
                        // thread has to be passed) and its saved offset is cleared - a leftover offset would
                        // make the next archive of the thread start in the middle. Every other thread and every
                        // other archive of this thread keeps being read.
                        ctx->error(10009, "file: " + parser->path + " - failed to open after " +
                                   std::to_string(ctx->archReadTries) + " tries");
                        ctx->error(10074, "file: " + parser->path + " - thread: " + std::to_string(parser->thread) +
                                   ", seq: " + parser->sequence.toString() + " - archive skipped (\"skip-unreadable-archive\"), "
                                   "its redo is NOT delivered; reading continues with the next archived redo log of this thread");
                        metadata->setNextSequence(parser->thread);
                        metadata->saveFileOffset(parser->thread, parser->sequence, FileOffset::zero());
                        delete parser;
                        archiveSkipped = true;
                        break;
                    }

                    ctx->info(0, "archived redo log " + parser->path + " is not ready for read, sleeping " +
                              std::to_string(ctx->archReadSleepUs) + " us");
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->archReadSleepUs);
                    contextSet(CONTEXT::CPU);
                    --retry;
                }

                if (archiveSkipped)
                    continue;

                if (ctx->softShutdown)
                    break;

                ret = parser->parse();
                metadata->setFirstNextScn(parser->firstScn, parser->nextScn, parser->thread);

                // Report the outcome of every archive. The parser logs when it starts reading an archive,
                // nothing was logged when it ended - a finished / stopped / dropped archive left no trace at
                // all, which made a run that stopped delivering indistinguishable from an idle one.
                ctx->info(0, "archived redo log " + parser->path + " - thread: " + std::to_string(parser->thread) +
                          ", seq: " + parser->sequence.toString() + " - result: " +
                          std::string(Reader::REDO_MSG[static_cast<uint>(ret)]) + (parser->slicePending ? " (sliced)" : "") +
                          ", position: " + archReader->getBufferStart().toString() + " / " + archReader->getBufferEnd().toString() +
                          ", expected sequence: " + metadata->threadSequence(parser->thread).toString());

                if (ctx->softShutdown) {
                    delete parser;
                    break;
                }

                // Archive sliced: only a part of it was parsed and the position was saved in the metadata. Keep
                // the parser and let the redo thread that is behind go first - this keeps the low-water-mark gap
                // (and with it the transaction memory held by the commit sorter) small.
                if (parser->slicePending) {
                    parser->slicePending = false;
                    // No archiveQueued() check here, and none is wanted: this is the very Parser that was just taken
                    // out of the queue (it was sliced and its position was saved), put back for its resume. The
                    // check exists for an archive a *new* list read hands out again while it is still waiting -
                    // skipping this one would drop the saved resume and lose whatever the slice left unparsed.
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                        ctx->logTrace(Ctx::TRACE::CHECKPOINT, "archive sliced, seq: " + parser->sequence.toString() + " thread: " +
                                      std::to_string(parser->thread) + ", scn: " + parser->sliceScn.toString());
                    archiveRedoQueue.emplace_back(parser);
                    continue;
                }

                if (ret != Reader::REDO_CODE::FINISHED) {
                    if (ret == Reader::REDO_CODE::STOPPED) {
                        delete parser;
                        break;
                    }
                    // A read or decode error inside the archive is fatal for the process as well (10047), which
                    // makes a damaged archive a permanent outage. With "skip-unreadable-archive" the archive is
                    // dropped and the thread moves on to its next archive, reporting what was not delivered.
                    if (ctx->skipUnreadableArchive && (ret == Reader::REDO_CODE::ERROR_BAD_DATA || ret == Reader::REDO_CODE::ERROR_CRC ||
                                                       ret == Reader::REDO_CODE::ERROR_READ || ret == Reader::REDO_CODE::ERROR_BLOCK)) {
                        ctx->error(10074, "file: " + parser->path + " - thread: " + std::to_string(parser->thread) +
                                   ", seq: " + parser->sequence.toString() + " - archive skipped after a read error: " +
                                   std::string(Reader::REDO_MSG[static_cast<uint>(ret)]) + " (\"skip-unreadable-archive\"), "
                                   "its redo is NOT delivered; reading continues with the next archived redo log of this thread");
                        metadata->setNextSequence(parser->thread);
                        metadata->saveFileOffset(parser->thread, parser->sequence, FileOffset::zero());
                        delete parser;
                        continue;
                    }
                    throw RuntimeException(10047, "archive log processing returned: " + std::string(Reader::REDO_MSG[static_cast<uint>(ret)]) + ", code: " +
                                           std::to_string(static_cast<uint>(ret)));
                }

                // verifySchema(metadata->nextScn);

                metadata->setNextSequence(parser->thread);
                delete parser;

                if (ctx->stopLogSwitches > 0) {
                    --ctx->stopLogSwitches;
                    if (ctx->stopLogSwitches == 0) {
                        ctx->info(0, "shutdown started - exhausted number of log switches");
                        ctx->stopSoft();
                    }
                }
            }

            if (processedThisPass) {
                // Something was parsed in this pass, so the parser is not waiting for an archive: the progress
                // watchdog (60055) is armed again.
                ctx->parserWaitingForArchive.store(false, std::memory_order_relaxed);
                idleSince = 0;
                lastIdleReport = 0;
                continue;
            }

            // Nothing was read in this pass: everything the archive list returned was dropped again (already
            // parsed archives, or archives parked because a predecessor is missing while nothing else is
            // left). Wait before polling the list once more - without this the loop would query the database,
            // and log on again whenever the connection is not kept, as fast as it can for as long as no new
            // archive shows up (measured: several full logons per second, forever).
            if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                break;

            // A buffered event that is at or below the low-water-mark is one the delivery thread would release
            // right away - so a run that holds one while nothing is read is a stall and not the normal wait for
            // the next archive of another redo thread. (Until the delivery got its own thread this was the
            // signature of a parser that stops reading: only the parser called the flush, so nothing could move
            // once it stopped. The delivery is independent of that now, which is exactly why the event has to be
            // reported: nothing else moves the water-mark while no redo is parsed.) At most once per 30 s.
            const uint64_t idleNow = ctx->clock->getTimeUt();
            if (idleSince == 0)
                idleSince = idleNow;
            if (metadata->commitSorter != nullptr && metadata->commitSorter->getPendingEvents() > 0 &&
                metadata->commitSorter->getWatermark() != Scn::none() &&
                metadata->commitSorter->getLowestPendingScn() != Scn::none() &&
                metadata->commitSorter->getLowestPendingScn() <= metadata->commitSorter->getWatermark() &&
                (lastIdleReport == 0 || idleNow > lastIdleReport + 30000000)) {
                ctx->warning(60047, "no archived redo log was read for: " + std::to_string((idleNow - idleSince) / 1000000) +
                             " s, but " + std::to_string(metadata->commitSorter->getPendingEvents()) +
                             " committed transaction(s) still wait for the low-water-mark while the first of them (scn: " +
                             metadata->commitSorter->getLowestPendingScn().toString() + ") is already below it (scn: " +
                             metadata->commitSorter->getWatermark().toString() + ", thread watermarks: " +
                             metadata->commitSorter->getThreadWatermarksText() + ", archives in the list: " +
                             std::to_string(archiveRedoQueue.size()) + ", skipped: " + std::to_string(skippedThisPass) +
                             ", parked: " + std::to_string(gapArchives.size()) + ")");
                lastIdleReport = idleNow;
            }

            // Nothing was parsed in this pass, so nothing published a water-mark: a redo thread that produces no
            // archive would keep holding back the low-water-mark and every committed transaction above it stays
            // in memory - the run then looks exactly like an idle one (warning 60047 above only covers the case
            // where the sorter would release something, not this one). The release of such a thread
            // (CommitSorter::refreshIdle) is time based, so it has to be driven from somewhere even when nothing
            // is parsed; the delivery thread does the same thing in its own loop, this is the second driver that
            // is awake exactly when the delivery thread has nothing to do.
            if (metadata->commitSorter != nullptr && metadata->commitSorter->getPendingEvents() > 0) {
                metadata->commitSorter->refreshIdle(idleNow, ctx->idleThreadReleaseUs);
                metadata->commitSorter->notifyDelivery();
            }

            // From here the parser waits for the archive list to hand out a new archive: no progress is the
            // expected state and warning 60056 is what reports it, so the parser progress watchdog (60055) stays
            // quiet instead of reporting a stall for every quiet instance of the cluster.
            ctx->parserWaitingForArchive.store(true, std::memory_order_relaxed);
            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(ctx->archReadSleepUs);
            contextSet(CONTEXT::CPU);
        }

        return logsProcessed;
    }

    bool Replicator::processOnlineRedoLogs() {
        Parser* parser;
        bool logsProcessed = false;

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "checking online redo logs, seq: " + metadata->sequence.toString());
        updateResetlogs();
        updateOnlineLogs();

        while (!ctx->softShutdown) {
            parser = nullptr;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "searching online redo log for seq: " + metadata->sequence.toString());

            // Keep reading online redo logs while it is possible
            bool higher = false;
            const time_ut beginTime = ctx->clock->getTimeUt();

            while (!ctx->softShutdown) {
                for (Parser* onlineRedo: onlineRedoSet) {
                    if (onlineRedo->reader->getSequence() > metadata->sequence)
                        higher = true;

                    if (onlineRedo->reader->getSequence() == metadata->sequence &&
                            (onlineRedo->reader->getNumBlocks() == Ctx::ZERO_BLK || metadata->fileOffset <
                            FileOffset(onlineRedo->reader->getNumBlocks(), onlineRedo->reader->getBlockSize()))) {
                        parser = onlineRedo;
                    }

                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO) && ctx->logLevel >= Ctx::LOG::DEBUG))
                        ctx->logTrace(Ctx::TRACE::REDO, onlineRedo->path + " is seq: " + onlineRedo->sequence.toString() +
                                      ", scn: " + onlineRedo->firstScn.toString() + ", blocks: " +
                                      std::to_string(onlineRedo->reader->getNumBlocks()));
                }

                // All so far read, waiting for switch
                if (parser == nullptr && !higher) {
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->redoReadSleepUs);
                    contextSet(CONTEXT::CPU);
                } else
                    break;

                if (ctx->softShutdown)
                    break;

                const time_ut endTime = ctx->clock->getTimeUt();
                if (beginTime + static_cast<time_ut>(ctx->refreshIntervalUs) < endTime) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                        ctx->logTrace(Ctx::TRACE::REDO, "refresh interval reached, checking online redo logs again");

                    updateOnlineRedoLogData();
                    updateOnlineLogs();
                    goStandby();
                    break;
                }

                updateOnlineLogs();
            }

            if (parser == nullptr)
                break;

            // If online redo log is overwritten - then switch to reading archive logs
            if (ctx->softShutdown)
                break;
            logsProcessed = true;

            const Reader::REDO_CODE ret = parser->parse();
            metadata->setFirstNextScn(parser->firstScn, parser->nextScn);

            if (ctx->softShutdown)
                break;

            if (ret == Reader::REDO_CODE::FINISHED) {
                // verifySchema(metadata->nextScn);
                metadata->setNextSequence();
            } else if (ret == Reader::REDO_CODE::STOPPED || ret == Reader::REDO_CODE::OK) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, "updating redo log files, return code: " + std::to_string(static_cast<uint>(ret)) + ", sequence: " +
                                  metadata->sequence.toString() + ", first scn: " + metadata->firstScn.toString() + ", next scn: " +
                                  metadata->nextScn.toString());

                updateOnlineRedoLogData();
                updateOnlineLogs();
            } else if (ret == Reader::REDO_CODE::OVERWRITTEN) {
                ctx->info(0, "online redo log has been overwritten by new ctx, continuing reading from archived redo log");
                break;
            } else {
                if (parser->group == 0) {
                    throw RuntimeException(10048, "read archived redo log, code: " + std::to_string(static_cast<uint>(ret)));
                }
                throw RuntimeException(10049, "read online redo log, code: " + std::to_string(static_cast<uint>(ret)));
            }

            if (ctx->stopLogSwitches > 0) {
                --ctx->stopLogSwitches;
                if (ctx->stopLogSwitches == 0) {
                    ctx->info(0, "shutdown initiated by number of log switches");
                    ctx->stopSoft();
                }
            }
        }
        return logsProcessed;
    }
}
