/* Base class for process to swapping memory to disk when low
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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Ctx.h"
#include "MemoryManager.h"
#include "exception/RuntimeException.h"
#include "metrics/Metrics.h"

namespace OpenLogReplicator {
    MemoryManager::MemoryManager(Ctx* newCtx, std::string newAlias, std::string newSwapPath):
            Thread(newCtx, std::move(newAlias)),
            swapPath(std::move(newSwapPath)) {}

    MemoryManager::~MemoryManager() {
        cleanup(true);
    }

    void MemoryManager::wakeUp() {
        std::unique_lock const lck(ctx->swapMtx);
        ctx->chunksMemoryManager.notify_all();
    }

    void MemoryManager::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "memory manager (" + ss.str() + ") start");
        }

        try {
            while (!ctx->hardShutdown) {
                const uint64_t discard = cleanOldTransactions();
                if (discard > 0 && ctx->metrics != nullptr)
                    ctx->metrics->emitSwapOperationsMbDiscard(discard);

                if (ctx->softShutdown && ctx->replicatorFinished) {
                    if (!ctx->swapChunks.empty())
                        cleanOldTransactions();
                    break;
                }

                SwapKey swapKey;
                int64_t swapIndex = -1;
                SwapKey unswapKey;
                int64_t unswapIndex = -1;
                bool nothingToDo = false;
                uint64_t openTransactions = 0;
                uint64_t swappableTransactions = 0;

                // Read outside swapMtx: the getters take memoryMtx, and memoryMtx must never be taken while
                // holding swapMtx (the reverse order does not occur anywhere else)
                const uint64_t freeMemoryMb = ctx->getFreeMemory(this);
                const uint64_t allocatedMemoryMb = ctx->getAllocatedMemory();
                const uint64_t maxMemoryMb = ctx->getMaxMemory();
                const uint64_t blockedThreads = ctx->getBlockedThreads();
                const uint64_t reservedMemoryMb = ctx->chunksToMb(ctx->getReservedFreeChunks());
                const std::string memoryModulesMb = ctx->getMemoryModulesText();

                {
                    contextSet(CONTEXT::MUTEX, REASON::MEMORY_RUN1);
                    std::unique_lock lck(ctx->swapMtx);
                    getChunkToUnswap(unswapKey, unswapIndex);
                    getChunkToSwap(swapKey, swapIndex);

                    if (unswapIndex == -1 && swapIndex == -1) {
                        nothingToDo = true;

                        // Nothing to do while the pool is exhausted: if this persists, the parser and the
                        // reader are deadlocked on memory (the commit sorter holds unflushed transactions
                        // that only a further read could release). Warn instead of stalling silently - this
                        // used to look exactly like a hung process with no error at all, and the warning was
                        // additionally suppressed whenever a few reserved chunks happened to be left.
                        openTransactions = ctx->swapChunks.size();
                        for (const auto& entry: ctx->swapChunks) {
                            const SwapChunk* sc = entry.second;
                            if (!sc->release && ctx->swappedFlushXid != entry.first && sc->chunks.size() >= 2)
                                ++swappableTransactions;
                        }

                        if (blockedThreads > 0 && (freeMemoryMb == 0 || freeMemoryMb <= reservedMemoryMb)) {
                            ++exhaustedCycles;
                            if (exhaustedCycles == 6 || (exhaustedCycles % 60) == 0)
                                ctx->warning(60043, "memory pool exhausted with nothing left to swap for " +
                                             std::to_string(exhaustedCycles * 10) + " s (allocated: " +
                                             std::to_string(allocatedMemoryMb) + " MB of " + std::to_string(maxMemoryMb) +
                                             " MB, free: " + std::to_string(freeMemoryMb) + " MB, blocked: " +
                                             std::to_string(blockedThreads) + " thread(s), open transactions: " +
                                             std::to_string(openTransactions) + ", swappable: " +
                                             std::to_string(swappableTransactions) + ") - modules: " + memoryModulesMb +
                                             " - replication is likely stalled; increase \"max-mb\", verify \"swap-path\" is "
                                             "writable, or skip the big transaction via \"transaction-max-mb\"");
                        } else
                            exhaustedCycles = 0;
                    }
                }
                contextSet(CONTEXT::CPU);

                if (nothingToDo) {
                    // Escalation takes memoryMtx only (see wontSwap) and must not run while swapMtx is held
                    if (blockedThreads > 0)
                        ctx->wontSwap(this, openTransactions, swappableTransactions);

                    contextSet(CONTEXT::MUTEX, REASON::MEMORY_RUN1);
                    std::unique_lock lck(ctx->swapMtx);
                    contextSet(CONTEXT::WAIT, REASON::MEMORY_NO_WORK);
                    ctx->chunksMemoryManager.wait_for(lck, std::chrono::milliseconds(10000));
                    contextSet(CONTEXT::CPU);
                    continue;
                }

                if (unswapIndex != -1) {
                    if (unswap(unswapKey, unswapIndex) && ctx->metrics != nullptr)
                        ctx->metrics->emitSwapOperationsMbRead(1);
                    {
                        contextSet(CONTEXT::MUTEX, REASON::MEMORY_RUN2);
                        std::unique_lock const lck(ctx->swapMtx);
                        ctx->chunksTransaction.notify_all();
                    }
                    contextSet(CONTEXT::CPU);
                }
                if (swapIndex != -1 && swap(swapKey, swapIndex) && ctx->metrics != nullptr)
                    ctx->metrics->emitSwapOperationsMbWrite(1);
            }
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "memory manager (" + ss.str() + ") stop");
        }
    }

    void MemoryManager::initialize() {
        cleanup();
    }

    uint64_t MemoryManager::cleanOldTransactions() {
        uint64_t discard = 0;
        while (true) {
            SwapKey swapKey;
            SwapChunk* sc = nullptr;
            bool takenOver = false;
            {
                contextSet(CONTEXT::MUTEX, REASON::MEMORY_CLEAN);
                std::unique_lock const lck(ctx->swapMtx);
                if (ctx->commitedXids.empty()) {
                    contextSet(CONTEXT::CPU);
                    return discard;
                }

                swapKey = ctx->commitedXids.back().first;
                SwapChunk* queued = ctx->commitedXids.back().second;
                ctx->commitedXids.pop_back();
                const auto& it = ctx->swapChunks.find(swapKey);
                if (it != ctx->swapChunks.end() && it->second == queued) {
                    sc = it->second;
                    if (sc->swappedMax >= 0)
                        discard += sc->swappedMax - sc->swappedMin;
                    ctx->swapChunks.erase(it);
                    ctx->reusedTransactions.notify_all();
                } else {
                    if (it != ctx->swapChunks.end())
                        // The key was taken over while this cleanup was pending: what is registered there now is a
                        // new entry of a duplicate parse of the same Xid (Ctx::swappedMemoryInit), not the one
                        // this key was queued for. Removing it used to kill the run with 50070 ("swap chunk not
                        // found ... during memory grow") as soon as that parse appended its first record, and its
                        // swapped data lives in the very file that is unlinked below - so neither is touched.
                        takenOver = true;
                    else
                        // The entry is already gone (Ctx::swappedMemoryKeyRelease() drops it as soon as the
                        // transaction is delivered), but its data on disk is not - the file is still unlinked
                        // below. It is only skipped when the key has a live owner again (see above).
                        takenOver = false;
                    sc = nullptr;
                }
            }
            contextSet(CONTEXT::CPU);
            delete sc;

            if (unlikely(takenOver))
                continue;

            struct stat fileStat{};
            const std::string fileName(swapPath + "/" + swapKey.fileNameSuffix() + ".swap");
            if (stat(fileName.c_str(), &fileStat) == 0) {
                if (unlink(fileName.c_str()) != 0)
                    ctx->error(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
            }
        }
    }

    void MemoryManager::cleanup(bool silent) {
        if (ctx->getSwapMemory(this) == 0)
            return;

        DIR* dir = opendir(swapPath.c_str());
        if (dir == nullptr) {
            if (silent)
                return;
            throw RuntimeException(10012, "directory: " + swapPath + " - can't read");
        }

        dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            const std::string dName(ent->d_name);
            if (dName == "." || dName == "..")
                continue;

            struct stat fileStat{};
            const std::string fileName(ent->d_name);

            const std::string fullName(swapPath + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat) != 0) {
                ctx->warning(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            //ctx->warning(0, "checking file name: " + fileName);
            const std::string suffix(".swap");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            if (!silent)
                ctx->warning(10067, "deleting old swap file from previous execution: " + fullName);

            if (unlink(fullName.c_str()) != 0) {
                if (silent)
                    return;
                throw RuntimeException(10010, "file: " + fullName + " - delete returned: " + strerror(errno));
            }
        }
        closedir(dir);
    }

    void MemoryManager::getChunkToUnswap(SwapKey& swapKey, int64_t& index) const {
        if (ctx->swappedFlushXid.xid.toUint() != 0) {
            const auto& it = ctx->swapChunks.find(ctx->swappedFlushXid);
            if (unlikely(it == ctx->swapChunks.end())) {
                // The transaction was released (delivered, dropped or cleaned up) while its flush was still
                // pending: there is nothing left to unswap and the data is gone with it. Throwing here used to
                // abort the whole run (error 50070) over a key that simply no longer exists.
                ctx->warning(60051, "swap memory of transaction: " + ctx->swappedFlushXid.toString() + " is gone while "
                            "its unswap was pending - nothing to unswap");
                ctx->swappedFlushXid = SwapKey{};
            } else {
                const SwapChunk* sc = it->second;
                if (sc->swappedMin > -1) {
                    index = sc->swappedMin;
                    swapKey = ctx->swappedFlushXid;
                    return;
                }
            }
        }

        if (ctx->swappedShrinkXid.xid.toUint() == 0)
            return;

        const auto& it = ctx->swapChunks.find(ctx->swappedShrinkXid);
        if (unlikely(it == ctx->swapChunks.end())) {
            ctx->warning(60051, "swap memory of transaction: " + ctx->swappedShrinkXid.toString() + " is gone while "
                        "its shrink was pending - nothing to shrink");
            ctx->swappedShrinkXid = SwapKey{};
            return;
        }
        const SwapChunk* sc = it->second;
        if (sc->swappedMax == -1)
            return;

        index = sc->swappedMax;
        swapKey = ctx->swappedShrinkXid;
    }

    void MemoryManager::getChunkToSwap(SwapKey& swapKey, int64_t& index) {
        if (ctx->nothingToSwap(this))
            return;

        for (const auto& [candidateKey, sc]: ctx->swapChunks) {
            if (ctx->swappedFlushXid == candidateKey || sc->release || sc->chunks.size() <= 1)
                continue;

            if (sc->swappedMax < static_cast<int64_t>(sc->chunks.size() - 2)) {
                index = sc->swappedMax + 1;
                swapKey = candidateKey;
                return;
            }
        }
    }

    bool MemoryManager::unswap(const SwapKey& swapKey, int64_t index) {
        uint8_t* tc = ctx->getMemoryChunk(this, Ctx::MEMORY::TRANSACTIONS, true);
        if (tc == nullptr)
            return false;

        const std::string fileName = swapPath + "/" + swapKey.fileNameSuffix() + ".swap";
        struct stat fileStat{};

        if (stat(fileName.c_str(), &fileStat) != 0)
            throw RuntimeException(50072, "swap file: " + fileName + " - get metadata returned: " + strerror(errno));

        int flags = O_RDONLY;
        const uint64_t fileSize = fileStat.st_size;
        if ((fileSize & (ctx->memoryChunkSize - 1)) != 0)
            throw RuntimeException(50072, "swap file: " + fileName + " - wrong file size: " + std::to_string(fileSize));

        if (fileSize < (index + 1) * ctx->memoryChunkSize)
            throw RuntimeException(50072, "swap file: " + fileName + " - too small file size: " + std::to_string(fileSize) + " to read chunk: " +
                                   std::to_string(index));

#if __linux__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        const int fileDes = open(fileName.c_str(), flags);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for read returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        const uint64_t bytes = pread(fileDes, tc, ctx->memoryChunkSize, index * ctx->memoryChunkSize);
        close(fileDes);

        if (bytes != ctx->memoryChunkSize)
            throw RuntimeException(50072, "swap file: " + fileName + " - read returned: " + strerror(errno));

        {
            contextSet(CONTEXT::MUTEX, REASON::MEMORY_UNSWAP);
            std::unique_lock const lck(ctx->swapMtx);
            const auto& it = ctx->swapChunks.find(swapKey);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during unswap read");
            SwapChunk* sc = it->second;
            if (sc->swappedMin == index) {
                sc->chunks[sc->swappedMin] = tc;
                if (sc->swappedMin == sc->swappedMax)
                    sc->swappedMin = sc->swappedMax = -1;
                else
                    ++sc->swappedMin;
                contextSet(CONTEXT::CPU);
                return true;
            }

            if (sc->swappedMax == index) {
                sc->chunks[sc->swappedMax] = tc;
                if (sc->swappedMin == sc->swappedMax) {
                    sc->swappedMin = sc->swappedMax = -1;
                    if (unlink(fileName.c_str()) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - delete returned: " + strerror(errno));
                } else {
                    --sc->swappedMax;
                    if (truncate(fileName.c_str(), (sc->swappedMax + 1) * ctx->memoryChunkSize) != 0)
                        throw RuntimeException(50072, "swap file: " + fileName + " - truncate returned: " + strerror(errno));
                }

                contextSet(CONTEXT::CPU);
                return true;
            }

            throw RuntimeException(50072, "swap file: " + fileName + " - unswapping: " + std::to_string(index) + " not in range " +
                                   std::to_string(sc->swappedMin) + "-" + std::to_string(sc->swappedMax));
        }
    }

    bool MemoryManager::swap(const SwapKey& swapKey, int64_t index) {
        uint8_t* tc;
        SwapChunk* sc;
        {
            contextSet(CONTEXT::MUTEX, REASON::MEMORY_SWAP1);
            std::unique_lock const lck(ctx->swapMtx);
            const auto& it = ctx->swapChunks.find(swapKey);
            if (unlikely(it == ctx->swapChunks.end()))
                throw RuntimeException(50070, "swap chunk not found for transaction: " + swapKey.toString() + " during swap write");
            sc = it->second;

            if (sc->chunks.size() <= 1 || index >= static_cast<int64_t>(sc->chunks.size() - 1) || sc->swappedMax != index - 1) {
                contextSet(CONTEXT::CPU);
                return false;
            }

            tc = sc->chunks[index];

            sc->swappedMax = index;
            if (sc->swappedMin == -1)
                sc->swappedMin = sc->swappedMax;

            sc->chunks[index] = nullptr;
        }
        contextSet(CONTEXT::CPU);

        const std::string fileName = swapPath + "/" + swapKey.fileNameSuffix() + ".swap";

        int flags = O_WRONLY | O_CREAT;
#if __linux__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE))
            flags |= O_DIRECT;
#endif

        constexpr int mode = S_IWUSR | S_IRUSR;
        const int fileDes = open(fileName.c_str(), flags, mode);
        if (fileDes == -1)
            throw RuntimeException(50072, "swap file: " + fileName + " - open for writing returned: " + strerror(errno));

#if __APPLE__
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::DIRECT_DISABLE)) {
            if (fcntl(fileDes, F_GLOBAL_NOCACHE, 1) < 0)
                ctx->error(10008, "file: " + fileName + " - set no cache for file returned: " + strerror(errno));
        }
#endif

        const uint64_t bytes = pwrite(fileDes, tc, ctx->memoryChunkSize, index * ctx->memoryChunkSize);
        if (bytes != ctx->memoryChunkSize) {
            close(fileDes);
            throw RuntimeException(50072, "swap file: " + fileName + " - write returned: " + strerror(errno));
        }
        close(fileDes);
        ++ctx->swappedMB;
        bool remove = false;
        uint64_t truncateSize(0);

        {
            contextSet(CONTEXT::MUTEX, REASON::MEMORY_SWAP2);
            std::unique_lock const lck(ctx->swapMtx);

            if (ctx->swappedShrinkXid == swapKey) {
                sc->chunks[index] = tc;

                if (sc->swappedMax == 0) {
                    sc->swappedMin = sc->swappedMax = -1;
                    remove = true;
                } else {
                    --sc->swappedMax;
                    truncateSize = (sc->swappedMax + 1) * ctx->memoryChunkSize;
                }
                ctx->chunksTransaction.notify_all();
            }
        }
        contextSet(CONTEXT::CPU);

        // discard writes
        if (remove) {
            if (unlink(fileName.c_str()) != 0)
                throw RuntimeException(50072, "swap file: " + fileName + " - delete returned: " + strerror(errno));
            return false;
        }
        if (truncateSize > 0) {
            if (truncate(fileName.c_str(), truncateSize) != 0)
                throw RuntimeException(50072, "swap file: " + fileName + " - truncate returned: " + strerror(errno));
            return false;
        }

        ctx->freeMemoryChunk(this, Ctx::MEMORY::TRANSACTIONS, tc);
        return true;
    }
}
