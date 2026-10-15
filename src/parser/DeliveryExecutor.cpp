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

#include "../builder/Builder.h"
#include "../common/Ctx.h"
#include "../common/metrics/Metrics.h"
#include "../metadata/Metadata.h"
#include "DeliveryExecutor.h"
#include "Transaction.h"
#include "TransactionBuffer.h"

namespace OpenLogReplicator {
    namespace {
        // Body of Parser::executeCommit, taken out of Parser so that a delivery does not depend on parser state.
        // Unlike the parser path it names the SCN of the event itself: the current LWN SCN of the parser is not
        // available here - and it was never meaningful for a delivery that does not run on the parser, because the
        // parser instance that ran it never parsed anything, so its lwnScn stayed at Scn::none() and the checks
        // below compared against "nothing". The commit SCN of the transaction *is* the SCN of the event.
        void commitCommon(Ctx* ctx, Builder* builder, Metadata* metadata, TransactionBuffer* transactionBuffer,
                          Transaction* transaction) {
            if ((transaction->commitScn > metadata->firstDataScn && !transaction->system) ||
                (transaction->commitScn > metadata->firstSchemaScn && transaction->system)) {
                if (transaction->begin) {
                    transaction->flush(metadata, builder);
                    ctx->thread()->contextSet(Thread::CONTEXT::CPU);
                    if (ctx->metrics != nullptr) {
                        if (transaction->rollback)
                            ctx->metrics->emitTransactionsRollbackOut(1);
                        else
                            ctx->metrics->emitTransactionsCommitOut(1);
                    }

                    if (ctx->stopTransactions > 0 && metadata->isNewData(transaction->commitScn, builder->lwnIdx)) {
                        --ctx->stopTransactions;
                        if (ctx->stopTransactions == 0) {
                            ctx->info(0, "shutdown started - exhausted number of transactions");
                            ctx->stopSoft();
                        }
                    }

                    if (transaction->shutdown && metadata->isNewData(transaction->commitScn, builder->lwnIdx)) {
                        ctx->info(0, "shutdown started - initiated by debug transaction " + transaction->xid.toString() +
                                  " at scn " + transaction->commitScn.toString());
                        ctx->stopSoft();
                    }
                } else {
                    if (ctx->metrics != nullptr) {
                        if (transaction->rollback)
                            ctx->metrics->emitTransactionsRollbackPartial(1);
                        else
                            ctx->metrics->emitTransactionsCommitPartial(1);
                    }
                    ctx->warning(60011, "skipping transaction with no beginning: " + transaction->toString(ctx));
                }
            } else {
                if (ctx->metrics != nullptr) {
                    if (transaction->rollback)
                        ctx->metrics->emitTransactionsRollbackSkip(1);
                    else
                        ctx->metrics->emitTransactionsCommitSkip(1);
                }
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::TRANSACTION)))
                    ctx->logTrace(Ctx::TRACE::TRANSACTION, "skipping transaction already committed: " + transaction->toString(ctx));
            }

            transactionBuffer->dropTransaction(transaction->xid, transaction->conId, transaction->thread, transaction);
            transaction->purge(ctx);
            delete transaction;
        }

        // Body of Parser::executeCheckpoint, taken out of Parser (it already used the SCN of the event itself).
        void checkpointCommon(Ctx* ctx, Builder* builder, Metadata* metadata, TransactionBuffer* transactionBuffer,
                              const CheckpointEvent& event) {
            builder->processCheckpoint(event.sequence, event.scn, event.timestamp, event.fileOffset, event.switchRedo);

            Seq minSequence = Seq::none();
            FileOffset minFileOffset;
            Xid minXid;
            transactionBuffer->checkpoint(minSequence, minFileOffset, minXid);
            metadata->checkpoint(ctx->thread(), event.thread, event.scn, event.timestamp, event.sequence, event.fileOffset,
                                 event.bytes, minSequence, minFileOffset, minXid);

            if (ctx->stopCheckpoints > 0 && metadata->isNewData(event.scn, builder->lwnIdx)) {
                --ctx->stopCheckpoints;
                if (ctx->stopCheckpoints == 0) {
                    ctx->info(0, "shutdown started - exhausted number of checkpoints");
                    ctx->stopSoft();
                }
            }
            if (ctx->metrics != nullptr)
                ctx->metrics->emitCheckpointsOut(1);
        }
    }

    DeliveryExecutor::DeliveryExecutor(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer,
                                       CommitSorter* newSorter):
            Thread(newCtx, "delivery"),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            sorter(newSorter) {}

    std::string DeliveryExecutor::getName() const {
        return "delivery";
    }

    void DeliveryExecutor::wakeUp() {
        Thread::wakeUp();
        // The shutdown is driven by Ctx::wakeThreads(): make the wait of the delivery loop return at once instead
        // of letting it run out its timeout first.
        if (sorter != nullptr)
            sorter->notifyDelivery();
    }

    void DeliveryExecutor::run() {
        // Everything this thread does is booked to itself: Ctx::accountThread is thread local (stage 1), and the
        // parser keeps working while a delivery runs - the memory a delivery releases and the swap keys it releases
        // belong to the delivering thread, not to the parser.
        Ctx::accountThread = this;

        while (!ctx->hardShutdown) {
            if (ctx->softShutdown) {
                // The run is over: nothing may stay buffered behind the low-water-mark any more, so everything that
                // is left is flushed (flushAll() ignores the water-mark on purpose, the order of the events
                // themselves is unchanged - see CommitSorter::flushAll). The writer is stopped after this thread and
                // drains what this produces, so leaving right afterwards loses nothing.
                // In a loop: a system transaction enqueues its own checkpoint while it is being delivered, and that
                // one is only in the sorter after the flush that delivered its commit (this replaces the single
                // flushAll the parser used to run at its last LWN boundary, which had the next pass as a fallback).
                while (!ctx->hardShutdown && sorter->getPendingEvents() > 0)
                    sorter->flushAll(this, this);
                // Wake the writer for whatever the last flush built, otherwise it would sit on it until its
                // fallback timeout runs out.
                if (deliveredInTick > 0)
                    builder->flush();
                break;
            }

            // Wait for an enqueue, a water-mark that moved or a shutdown. The timeout only covers the idle thread
            // release (a time based decision) and the shutdown, neither of which notifies.
            sorter->waitForDelivery(DELIVERY_WAIT_US);

            const bool parserBlocked = ctx->parserInSwapWait.load(std::memory_order_relaxed);
            if (parserBlocked) {
                if (!swapWaitReported) {
                    swapWaitReported = true;
                    ctx->info(0, "the parser is blocked waiting for a swapped transaction to be delivered - the "
                                 "delivery thread releases it (defect C)");
                }
            } else if (swapWaitReported) {
                swapWaitReported = false;
                ctx->info(0, "the parser is not waiting for a swapped transaction any more - the delivery thread is "
                             "back to the ordinary water-mark");
            }

            deliveredInTick = 0;
            // The defect C emergency is part of the tick: while the parser is blocked in Ctx::swappedMemoryInit()
            // the low-water-mark can not move past the transaction it waits for (the parser is the only thread that
            // publishes one), so the lowest buffered commit is delivered without waiting for it.
            // The tick is marked as "in flight" for the parser progress watchdog (60055): since the delivery has a
            // thread of its own, a held deliveryMtx no longer means that the parser is blocked by a lock (the gate
            // is held for a whole delivery), and a report has to tell the two apart.
            ctx->deliveryActiveSinceUs.store(ctx->clock->getTimeUt(), std::memory_order_relaxed);
            sorter->tick(this, ctx->clock->getTimeUt(), ctx->idleThreadReleaseUs, this, parserBlocked);
            ctx->deliveryActiveSinceUs.store(0, std::memory_order_relaxed);

            if (deliveredInTick > 0)
                // This thread is the builder's single writer, so waking the writer is its job as well: the parser
                // used to call Builder::flush() at the end of every pass, and without it a writer that waits for its
                // fallback (5 s, and only while its output queue is empty) would leave the messages sitting there.
                builder->flush();
        }
    }

    void DeliveryExecutor::executeCommit(Transaction* transaction) {
        commitCommon(ctx, builder, metadata, transactionBuffer, transaction);
        ++deliveredInTick;
    }

    void DeliveryExecutor::executeCheckpoint(const CheckpointEvent& event) {
        checkpointCommon(ctx, builder, metadata, transactionBuffer, event);
        ++deliveredInTick;
    }

    void DeliveryExecutor::executeCommitCommon(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata,
                                               TransactionBuffer* newTransactionBuffer, Transaction* transaction) {
        commitCommon(newCtx, newBuilder, newMetadata, newTransactionBuffer, transaction);
    }

    void DeliveryExecutor::executeCheckpointCommon(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata,
                                                   TransactionBuffer* newTransactionBuffer, const CheckpointEvent& event) {
        checkpointCommon(newCtx, newBuilder, newMetadata, newTransactionBuffer, event);
    }
}
