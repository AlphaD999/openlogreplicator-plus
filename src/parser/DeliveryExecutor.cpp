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
#include "../common/exception/RuntimeException.h"
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

            // The min-tran positions are not collected here any more: they are only read when a checkpoint file is
            // written and are gathered there (Metadata::writeCheckpoint), so a LWN event no longer walks the
            // transaction buffer (see Metadata::checkpoint).
            metadata->checkpoint(ctx->thread(), event.thread, event.scn, event.timestamp, event.sequence, event.fileOffset,
                                 event.bytes);

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

        // The whole loop is guarded below (the body keeps its indentation, so the change stays readable as a diff);
        // the handler is what turns a fatal exception here into an error line instead of a SIGABRT.
        try {
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

            // Two ways the parser can be blocked on a delivery that only it could enable: waiting for a swapped
            // transaction (defect C, Ctx::parserInSwapWait) and being stopped by the per-thread backpressure limit
            // with nothing flushable below the low-water-mark (Ctx::parserInBacklogWait, 2026-10-16 field stall).
            // Both arm the same emergency: the lowest buffered event is delivered without the water-mark.
            const bool swapWait = ctx->parserInSwapWait.load(std::memory_order_relaxed);
            const bool backlogWait = !swapWait && ctx->parserInBacklogWait.load(std::memory_order_relaxed);
            const bool parserBlocked = swapWait || backlogWait;
            if (parserBlocked) {
                if (!swapWaitReported) {
                    swapWaitReported = true;
                    ctx->info(0, swapWait ?
                                 "the parser is blocked waiting for a swapped transaction to be delivered - the "
                                 "delivery thread releases it (defect C)" :
                                 "the parser is blocked on the backpressure limit and nothing is flushable below the "
                                 "low-water-mark - the delivery thread delivers what is buffered to release it");
                }
            } else if (swapWaitReported) {
                swapWaitReported = false;
                ctx->info(0, "the parser is not waiting for a swapped transaction or the backpressure limit any more - "
                             "the delivery thread is back to the ordinary water-mark");
            }

            deliveredInTick = 0;
            // The defect C emergency is part of the tick: while the parser is blocked in Ctx::swappedMemoryInit()
            // the low-water-mark can not move past the transaction it waits for (the parser is the only thread that
            // publishes one), so what is buffered is delivered without waiting for it.
            // The tick is marked as "in flight" for the parser progress watchdog (60055): since the delivery has a
            // thread of its own, a held deliveryMtx no longer means that the parser is blocked by a lock (the gate
            // is held for a whole delivery), and a report has to tell the two apart.
            ctx->deliveryActiveSinceUs.store(ctx->clock->getTimeUt(), std::memory_order_relaxed);
            sorter->tick(this, ctx->clock->getTimeUt(), ctx->idleThreadReleaseUs, this, parserBlocked);
            ctx->deliveryActiveSinceUs.store(0, std::memory_order_relaxed);

            // Layered backlog report every 30 s: without it the backlog only becomes visible in 60055, i.e. after
            // the parser has already stalled, and then only as one number that can not be attributed to a layer.
            static thread_local uint64_t layersReportUs = 0;
            const uint64_t layersNowUs = ctx->clock->getTimeUt();
            if (layersReportUs == 0 || layersNowUs - layersReportUs >= 30000000) {
                layersReportUs = layersNowUs;
                ctx->info(0, "backlog layers:" + ctx->layersLine());
            }

            if (deliveredInTick > 0)
                // This thread is the builder's single writer, so waking the writer is its job as well: the parser
                // used to call Builder::flush() at the end of every pass, and without it a writer that waits for its
                // fallback (5 s, and only while its output queue is empty) would leave the messages sitting there.
                builder->flush();
            }
        } catch (RuntimeException& ex) {
            // One handler for the whole loop: everything this thread does - building the messages, reading a
            // transaction back from the swap, writing a checkpoint - can throw, and until now the throw left the
            // thread (std::terminate, SIGABRT) and the process died with nothing but "what(): std::exception" in the
            // log (production 2026-10-22 05:48, no error code anywhere). A failure here can not be recovered from:
            // the buffered state is not trustworthy any more, so it is reported - code, message, stacktrace - and the
            // run is stopped hard, exactly like the other threads do (Parser, Checkpoint, MemoryManager).
            ctx->error(ex.code, ex.msg + " - the delivery thread can not continue, the run is stopped");
            ctx->printStacktrace();
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed in the delivery thread: " + std::string(ex.what()) +
                      " - the run is stopped");
            ctx->printStacktrace();
            ctx->stopHard();
        } catch (std::exception& ex) {
            ctx->error(10014, "unexpected exception in the delivery thread: " + std::string(ex.what()) +
                      " - the run is stopped");
            ctx->printStacktrace();
            ctx->stopHard();
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
