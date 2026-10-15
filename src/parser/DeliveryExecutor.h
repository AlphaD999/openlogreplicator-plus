/* Delivery thread: runs the commit sorter outside the parser
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#ifndef DELIVERY_EXECUTOR_H_
#define DELIVERY_EXECUTOR_H_

#include <string>

#include "../common/Ctx.h"
#include "../common/Thread.h"
#include "CommitSorter.h"

namespace OpenLogReplicator {
    class Builder;
    class Metadata;
    class Transaction;
    class TransactionBuffer;

    // The delivery has a thread of its own (stage 4). Why it exists: every delivery used to run on the thread
    // that happened to call the sorter - the parser at its LWN boundaries and the writer in its idle loop - and a
    // delivery is the slowest step of the pipeline (a transaction that was swapped out is read back chunk by
    // chunk, see Ctx::swappedMemoryGet and MemoryManager). The parser therefore stood still for as long as a
    // delivery took: measured in production on 2026-10-15, 6 stalls in 7 minutes, up to 70 s each (warning 60055,
    // reader buffer full - warning 60046, delivery throttled to ~150 transactions/s).
    //
    // Now the parser only publishes its water-mark and enqueues the checkpoint event (see Parser::parse), this
    // thread delivers, and the two callbacks are the same code as the parser's single threaded path: the bodies
    // are shared through executeCommitCommon()/executeCheckpointCommon(), so an installation without a commit
    // sorter behaves exactly as before.
    class DeliveryExecutor final : public Thread, public CommitSorter::Executor {
    protected:
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        CommitSorter* sorter;

        // Events delivered by the last tick and whether the defect C reason was already reported: both are only
        // touched by this thread (the executor of the out-of-parser deliveries is this object alone).
        uint64_t deliveredInTick{0};
        bool swapWaitReported{false};

        void run() override;

    public:
        // Wait of the delivery loop. It is a timeout on the notification of the sorter, not a poll loop: only
        // the two decisions nobody notifies about need it - the time based idle thread release
        // ("idle-thread-release-us") and the shutdown.
        static constexpr uint64_t DELIVERY_WAIT_US{100000};

        DeliveryExecutor(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer,
                         CommitSorter* newSorter);
        ~DeliveryExecutor() override = default;

        void executeCommit(Transaction* transaction) override;
        void executeCheckpoint(const CheckpointEvent& event) override;

        // The shared bodies: the parser runs them on its single threaded path (no commit sorter), this thread
        // through the two overrides above. Neither touches parser state - that is what makes the delivery
        // independent of the parser, and it is the reason the bodies were taken out of Parser in the first place.
        static void executeCommitCommon(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer,
                                        Transaction* transaction);
        static void executeCheckpointCommon(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata,
                                            TransactionBuffer* newTransactionBuffer, const CheckpointEvent& event);

        // Woken by Ctx::wakeThreads() (shutdown) - a notification of the sorter makes the wait return at once.
        void wakeUp() override;
        [[nodiscard]] std::string getName() const override;
    };
}

#endif
