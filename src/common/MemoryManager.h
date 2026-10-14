/* Header for MemoryManager class
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

#ifndef MEMORY_MANAGER_H_
#define MEMORY_MANAGER_H_

#include <unordered_set>

#include "../common/Thread.h"

namespace OpenLogReplicator {

    class MemoryManager final : public Thread {
    protected:
        std::string swapPath;
        // Keys of the transactions that really have a swap file on disk. A swap file is written only for a
        // transaction that does not fit into the memory pool, which is the exception. Before this set existed
        // cleanOldTransactions() asked the filesystem about every delivered transaction, and each lookup of a
        // file that does not exist leaves a negative dentry in the kernel which is only reclaimed under memory
        // pressure - production collected ~60M of them per day (~18 GB of dentry cache on the local filesystem
        // of the swap path) without any data behind them. Only the memory manager thread touches the set:
        // swap(), unswap() and cleanOldTransactions() are all called from MemoryManager::run().
        std::unordered_set<SwapKey, SwapKeyHash> filesOnDisk;
        // Consecutive "no work to do while the memory pool is exhausted" cycles (one cycle = one
        // 10 s wait). Non-zero for more than a minute means the parser and the reader are deadlocked
        // on memory and replication is stalled - see warning 60043.
        uint64_t exhaustedCycles{0};

    public:
        MemoryManager(Ctx* newCtx, std::string newAlias, std::string newSwapPath);
        ~MemoryManager() override;

        void wakeUp() override;
        void run() override;
        void initialize();

    private:
        uint64_t cleanOldTransactions();
        void cleanup(bool silent = false);
        void getChunkToUnswap(SwapKey& swapKey, int64_t& index) const;
        void getChunkToSwap(SwapKey& swapKey, int64_t& index);
        bool unswap(const SwapKey& swapKey, int64_t index);
        bool swap(const SwapKey& swapKey, int64_t index);

        std::string getName() const override {
            return {"MemoryManager"};
        }
    };
}

#endif
