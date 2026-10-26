/* Sorter for commit and checkpoint events across redo threads (RAC support)
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

You should have received a copy of the GNU Affero General Public License
along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "CommitSorter.h"
#include "Transaction.h"

namespace OpenLogReplicator {
    CommitSorter::~CommitSorter() {
        for (const auto& [_, eventList]: events) {
            for (Event* event: eventList)
                delete event;
        }
        events.clear();
        // Nothing is handed back here: the pooled events are owned by the pool.
        for (Event* event: eventPool)
            delete event;
        eventPool.clear();
    }

    CommitSorter::Event* CommitSorter::allocEvent() {
        {
            std::lock_guard const lck(eventPoolMtx);
            if (!eventPool.empty()) {
                Event* const event = eventPool.back();
                eventPool.pop_back();
                return event;
            }
        }
        return new Event();
    }

    void CommitSorter::releaseEvent(Event* event) noexcept {
        if (event == nullptr)
            return;
        event->scn = Scn::none();
        event->commit = false;
        event->transaction = nullptr;
        event->checkpoint = CheckpointEvent{};
        event->thread = 0;
        event->chunks = 0;
        {
            std::lock_guard const lck(eventPoolMtx);
            if (eventPool.size() < EVENT_POOL_MAX) {
                eventPool.push_back(event);
                return;
            }
        }
        delete event;
    }

    void CommitSorter::adaptCredit() {
        // Only in the automatic mode: an operator who configured a value keeps it.
        if (ctx == nullptr || ctx->pendingCommitsPerThread != 0)
            return;

        // The congestion signal: how long the parsers were stopped by the credit in the report interval that just
        // ended. The counters are handed over (exchanged to 0) so every interval stands on its own; a few
        // microseconds lost to the race with a parser that is just adding to them does not matter.
        uint64_t blockedUs = 0;
        for (auto& counter: ctx->statCreditBlockedUs) {
            const uint64_t value = counter.exchange(0, std::memory_order_relaxed);
            if (value > blockedUs)
                blockedUs = value;
        }

        uint64_t credit = ctx->creditCurrent.load(std::memory_order_relaxed);
        if (credit == 0)
            credit = CREDIT_BASE;

        if (blockedUs >= CREDIT_SHRINK_US) {
            const uint64_t lowered = credit / 2 < CREDIT_MIN ? CREDIT_MIN : credit / 2;
            if (lowered < credit) {
                ctx->creditCurrent.store(lowered, std::memory_order_relaxed);
                ctx->warning(60066, "the commit credit is lowered: " + std::to_string(credit) + " -> " +
                             std::to_string(lowered) + " commit(s) per redo thread - a parser was stopped by it for: " +
                             std::to_string(blockedUs / 1000) + " ms in the last report interval, delivery: " +
                             std::to_string(ctx->statDeliveredPerS.load(std::memory_order_relaxed)) + "/s "
                             "(\"pending-commits-per-thread\": 0 = automatic)");
            }
            return;
        }

        // Nothing was stopped by the credit: grow back towards the base while the queue stays shallow. The
        // threshold is a fraction of what the credit allows in total, so a run that merely uses its credit does not
        // keep pushing it up and down.
        if (blockedUs != 0 || credit >= CREDIT_BASE)
            return;
        const uint64_t threads = threadWatermark.empty() ? 1 : threadWatermark.size();
        const uint64_t shallow = credit * threads / 4;
        if (ctx->statSorterEvents.load(std::memory_order_relaxed) >= shallow)
            return;
        uint64_t raised = credit + credit / 4;
        if (raised <= credit)
            raised = credit + 1;
        if (raised > CREDIT_BASE)
            raised = CREDIT_BASE;
        ctx->creditCurrent.store(raised, std::memory_order_relaxed);
        ctx->warning(60066, "the commit credit is raised: " + std::to_string(credit) + " -> " +
                     std::to_string(raised) + " commit(s) per redo thread - nothing was stopped by it and the "
                     "buffered commits are below " + std::to_string(shallow) +
                     " (\"pending-commits-per-thread\": 0 = automatic)");
    }

    void CommitSorter::enqueueCommit(Transaction* transaction, Scn commitScn) {
        auto* event = allocEvent();
        event->scn = commitScn;
        event->commit = true;
        event->transaction = transaction;
        event->thread = transaction->thread;
        // How much of the transaction pool this buffered commit keeps, in chunks: the amount the backpressure is
        // measured in (allowanceLocked). Taken here, once, while the transaction is still untouched by a delivery;
        // at least one chunk, because a transaction that is swapped out reports none (the swap file holds it) and
        // would otherwise be invisible to the bound.
        event->chunks = 1;
        if (ctx != nullptr) {
            Thread* const parserThread = ctx->thread();
            if (parserThread != nullptr) {
                const uint64_t transactionChunks = ctx->swappedMemorySize(parserThread, transaction->swapKey());
                if (transactionChunks > 0)
                    event->chunks = transactionChunks;
            }
        }
        std::unique_lock lck(sorterMtx);
        events[commitScn].push_back(event);
        ++pendingEvents;
        ++pendingByThread[event->thread];
        chunksByThread[event->thread] += event->chunks;
        pendingChunks += event->chunks;
        syncPendingEvents();
        notifyDeliveryLocked();
        // No wait here: the backpressure is applied at the boundary of a record (waitForBacklog, called from
        // Parser::parse) and not per commit - see the comment there.
    }

    void CommitSorter::waitForBacklog(uint16_t thread) {
        // Backpressure (stage 2): with the delivery on its own thread (stage 4) nothing else paces the parser -
        // it can walk away from the delivery until the transaction pool is full, and a buffered commit that owns
        // a single chunk can not be swapped out, so the pool can not be relieved any more (measured: 4,500
        // buffered commits, 4 GB pool, 1 MB blocks -> "ERROR 10017 out of memory").
        //
        // Called at the *head* of a record, right after the parser published its water-mark there, and not per
        // commit inside a record. Two reasons, both measured in the reproduction rig on 2026-10-16 with slicing
        // on:
        //  - the wait must be per redo thread (allowanceLocked): with a global bound a thread that runs ahead
        //    holds back the parser of a thread that is behind, and that thread is exactly the one whose
        //    water-mark has to move for anything to become deliverable (measured: 8,193 events of thread 2 above
        //    thread 1's water-mark, the delivery reporting "deliverable now: 0" - a deadlock);
        //  - the wait must not happen *inside* a record: the water-mark only moves at the next record head, so a
        //    record carrying more commits than the limit (measured: 4,097 commits in one record against a limit
        //    of 4,096) would have the parser wait for a water-mark only it could publish. At the boundary the
        //    water-mark of this thread is already published, so everything of the *previous* record is
        //    deliverable - the delivery drains and the wait ends. The price is that the overshoot is bounded by
        //    one record and not by the limit exactly, which the free half of the pool covers.
        std::unique_lock lck(sorterMtx);
        if (ctx == nullptr)
            return;
        if (allowanceLocked(thread) == 0)
            return; // the pool is not initialized (yet): nothing to bound against

        bool reported = false;
        // Emergency: a wait longer than BACKPRESSURE_EMERGENCY_US means this parser can not go on while the delivery
        // has nothing it may flush below the low-water-mark - the same circle the swapped-memory wait has (see
        // Ctx::parserInSwapWait): the parser is the only thread that publishes a position, and it is the blocked one.
        // The flag makes the delivery thread deliver the whole buffer without waiting for the water-mark; it is
        // cleared on every way out of this function, so the delivery is back to the ordinary water-mark afterwards.
        bool emergency = false;
        const uint64_t waitingSince = ctx->clock->getTimeUt();
        // g4: when this thread was first held by the credit (or by the swap bound) in this call. The accumulated
        // time is what the AIMD controller reads as its congestion signal (Ctx::statCreditBlockedUs): a chunk wait
        // is the pool being full, a credit wait is the design working as intended, and only the second one is a
        // reason to lower the credit.
        uint64_t creditBlockedSince = 0;
        while (!ctx->hardShutdown && !ctx->softShutdown) {
            // Both numbers are re-read on every round: the allowance is what is left of the pool once the other
            // threads have their guaranteed share, so a delivery of another thread (or a thread that stopped
            // parsing) changes it while this one waits.
            const uint64_t allowance = allowanceLocked(thread);
            if (allowance == 0)
                break;
            const auto usedIt = chunksByThread.find(thread);
            const uint64_t used = usedIt == chunksByThread.end() ? 0 : usedIt->second;
            const auto bufferedIt = pendingByThread.find(thread);
            const uint64_t buffered = bufferedIt == pendingByThread.end() ? 0 : bufferedIt->second;
            // g4: the two bounds that are counted in commits instead of chunks. The chunk bound can be satisfied
            // while tens of thousands of commits wait for the low-water-mark, because the swap pump hands the
            // chunks of a written-out transaction back to the pool (production 2026-10-23: 59,048 buffered,
            // 18,245 swapped entries, warning 60062 never fired and the parse fell to 1.6 MB/s). The credit does
            // not care whether the data sits in the pool or on the disk - it bounds how far this thread may run
            // ahead of the delivery, which is what actually keeps the pool and the parse out of the feedback loop.
            const uint64_t credit = ctx->creditToEnforce();
            const bool hitChunks = used > allowance;
            const bool hitCredit = credit > 0 && buffered >= credit;
            const bool hitSwapCap = ctx->swapCapReached();
            if (!hitChunks && !hitCredit && !hitSwapCap)
                break;
            if (hitCredit || hitSwapCap) {
                if (creditBlockedSince == 0)
                    creditBlockedSince = ctx->clock->getTimeUt();
            }

            backpressureCv.wait_for(lck, std::chrono::milliseconds(BACKPRESSURE_WAIT_MS));
            const uint64_t now = ctx->clock->getTimeUt();
            if (!emergency && now > waitingSince && now - waitingSince >= BACKPRESSURE_EMERGENCY_US) {
                emergency = true;
                ctx->parserInBacklogWait.store(true, std::memory_order_relaxed);
                // Wake the delivery thread: it is the one that acts on the flag (DeliveryExecutor::run).
                notifyDeliveryLocked();
            }
            // g4: the credit has its own report. 60062 talks about the pool (chunks); a thread stopped by the
            // credit is not waiting for memory, it is waiting for the delivery to catch up - mixing the two in one
            // message made the analysis read a memory problem where there was an ordering one.
            // Reported per wait and not per report interval, but with a threshold below the emergency release
            // (CREDIT_REPORT_US < BACKPRESSURE_EMERGENCY_US): a wait longer than the emergency one is broken by the
            // delivery flushing without the water-mark, so measuring "how long this call waited" against 30 s could
            // never fire. At most one line per thread per minute.
            const uint64_t lastCreditWarn = creditWarnUs[thread < 16 ? thread : 0];
            if (creditBlockedSince != 0 && now > creditBlockedSince &&
                now - creditBlockedSince >= CREDIT_REPORT_US &&
                (lastCreditWarn == 0 || (now > lastCreditWarn && now - lastCreditWarn >= CREDIT_REPORT_GAP_US))) {
                creditWarnUs[thread < 16 ? thread : 0] = now;
                const Scn creditDeliveryLimit = deliveryLimitLocked();
                uint64_t creditDeliverable = 0;
                for (const auto& [scn, list]: events) {
                    if (creditDeliveryLimit == Scn::none() || !(scn > creditDeliveryLimit))
                        creditDeliverable += list.size();
                }
                ctx->warning(60064, "the parser is stopped by its commit credit for: " +
                             std::to_string((now - creditBlockedSince) / 1000000) + " s - redo thread: " +
                             std::to_string(thread) + " holds " + std::to_string(buffered) +
                             " buffered commit(s) against a credit of " + std::to_string(credit) +
                             (hitSwapCap ? ", the swap bound is reached as well (\"swap_chunks\" of \"swap_chunks_max\")" :
                                           ", and the swap bound is not the reason") +
                             "; deliverable now: " + std::to_string(creditDeliverable) + " of " +
                             std::to_string(pendingEvents) + " buffered event(s), of which " +
                             std::to_string(inDelivery) + " are in the delivery (" +
                             (inDelivery >= static_cast<uint64_t>(pendingEvents) ? "the writer is the one that does not "
                                                                                  "take them, not the water-mark"
                                                                                : "the rest waits for the water-mark") +
                             "), builder ring: " +
                             std::to_string(ctx->statBuilderOut.load(std::memory_order_relaxed)) + " of " +
                             std::to_string(ctx->builderOutLimit.load(std::memory_order_relaxed)) + " chunk(s)" +
                             (ctx->builderOutReached(ctx->statBuilderOut.load(std::memory_order_relaxed)) ?
                              " - full, so the delivery is waiting for the client to confirm" : "") +
                             ", thread watermarks: " + getThreadWatermarksText() + " - this is not a memory "
                             "problem: the commits wait for the delivery, so either the thread that holds the "
                             "low-water-mark (or its archive), the writer or the client is the one to look at");
            }
            if (reported || now <= waitingSince || now - waitingSince < BACKPRESSURE_REPORT_US)
                continue;
            reported = true;
            // How much of the backlog the delivery may take right now and how much of it waits for a higher
            // water-mark: a wait in which nothing is deliverable is not the delivery being slow, it is the
            // water-mark being held back - and that has to be visible in the report.
            // The limit is asked from the same place the flush asks it (deliveryLimitLocked): reporting "deliverable"
            // against a different water-mark than the one flushReady() delivers by is what made the field read
            // "deliverable now: 24580" while the delivery was idle (2026-10-16, the 890 s stall).
            const Scn deliveryLimit = deliveryLimitLocked();
            uint64_t deliverable = 0;
            uint64_t aboveWatermark = 0;
            for (const auto& [scn, list]: events) {
                if (deliveryLimit == Scn::none() || !(scn > deliveryLimit))
                    deliverable += list.size();
                else
                    aboveWatermark += list.size();
            }
            const uint64_t globalCap = ctx->getMemoryChunksMax() / 3;
            ctx->warning(60062, "the parser is waiting for the delivery for: " + std::to_string((now - waitingSince) / 1000000) +
                         " s - the commit sorter holds " + std::to_string(used) + " chunk(s) of this thread against an "
                         "allowance of " + std::to_string(allowance) + " (of the " + std::to_string(pendingChunks) +
                         " chunk(s) / " + std::to_string(pendingEvents) + " event(s) buffered in total, global cap: " +
                         std::to_string(globalCap) + " chunk(s) of " + std::to_string(ctx->getMemoryChunksMax()) +
                         ", events of this thread: " + std::to_string(buffered) +
                         ", credit: " + std::to_string(credit) + (hitSwapCap ? " (reached)" : "") +
                         ", min share: " + std::to_string(minShareLocked(globalCap)) + ", deliverable now: " +
                         std::to_string(deliverable) + ", above the low-water-mark: " + std::to_string(aboveWatermark) +
                         ", thread watermarks: " + getThreadWatermarksText() + ")" +
                         (deliveryLimit == Scn::none() ?
                          "; no thread holds the low-water-mark, so the delivery takes what is buffered" : "") +
                         "; the parse of this thread is stopped on purpose: letting it go on "
                         "would fill the pool with commits that are waiting for the low-water-mark (\"memory-max-mb\" / "
                         "\"transaction-block-kb\")");
        }
        if (creditBlockedSince != 0) {
            const uint64_t now = ctx->clock->getTimeUt();
            if (now > creditBlockedSince)
                ctx->statCreditBlockedUs[thread].fetch_add(now - creditBlockedSince, std::memory_order_relaxed);
        }
        if (emergency)
            ctx->parserInBacklogWait.store(false, std::memory_order_relaxed);
    }

    uint64_t CommitSorter::minShareLocked(uint64_t globalCap) const {
        // One quarter of the even split of the cap: with four redo threads that is 1/16 of the cap each. It is the
        // share a thread is guaranteed against a thread that would otherwise take the whole backlog - enough to
        // parse an archive of a quiet instance without waiting for a delivery (a thread that can not enqueue can
        // not publish a water-mark either, which would freeze the very delivery its allowance waits for).
        const uint64_t threads = threadWatermark.empty() ? 1 : threadWatermark.size();
        uint64_t share = globalCap / (4 * threads);
        if (share < MIN_THREAD_CHUNKS)
            share = MIN_THREAD_CHUNKS;
        const uint64_t shareMax = globalCap / threads;
        if (shareMax > 0 && share > shareMax)
            share = shareMax;
        return share;
    }

    uint64_t CommitSorter::allowanceLocked(uint16_t thread) const {
        if (ctx == nullptr)
            return 0;
        const uint64_t chunks = ctx->getMemoryChunksMax();
        if (chunks == 0)
            return 0;
        // A third of the pool for the buffered commits - the rest takes the open transactions, the reader buffers
        // and the builder. g4 lowered this from a half: on 2026-10-23 the backlog sat at half a pool (6 GB of 12)
        // while the parser crawled, and the credit next to this bound (waitForBacklog) is what really limits the
        // run-ahead, so the chunk bound only has to keep the pool from being eaten by commits alone.
        const uint64_t globalCap = chunks / 3;
        const uint64_t threads = threadWatermark.empty() ? 1 : threadWatermark.size();
        const uint64_t minShare = minShareLocked(globalCap);
        // What the *other* threads are guaranteed is held back from this one, so that the sum over all threads
        // stays below the cap and every thread can always make progress - the pool is not first-come-first-served,
        // but it is also never taken away from a thread that has data (that is what the constant this replaces did:
        // it handed 1/(2N) to every thread and left the rest of the pool unused when a thread had nothing to parse).
        uint64_t usedByOthers = 0;
        for (const auto& [otherThread, used]: chunksByThread) {
            if (otherThread != thread && used > 0)
                usedByOthers += used;
        }
        const uint64_t reservedForOthers = (threads > 0 ? threads - 1 : 0) * minShare;
        if (usedByOthers + reservedForOthers >= globalCap)
            return minShare;
        return globalCap - usedByOthers - reservedForOthers;
    }

    void CommitSorter::enqueueCheckpoint(const CheckpointEvent& checkpoint) {
        auto* event = allocEvent();
        event->scn = checkpoint.scn;
        event->commit = false;
        event->checkpoint = checkpoint;
        event->thread = checkpoint.thread;
        std::lock_guard const lck(sorterMtx);
        events[checkpoint.scn].push_back(event);
        ++pendingEvents;
        ++pendingByThread[event->thread];
        syncPendingEvents();
        notifyDeliveryLocked();
    }

    void CommitSorter::setThreadWatermark(uint16_t thread, Scn parsedScn, uint64_t currentTime, uint64_t idleReleaseUs) {
        std::lock_guard const lck(sorterMtx);
        if (parsedScn == Scn::none())
            return;
        // What a thread vouches for is the highest *commit SCN* it can rule out: a commit SCN is the SCN that
        // follows its redo record, so the record at parsedScn may be the commit record of a complete transaction
        // with commitScn == parsedScn + 1 - that transaction is safe to deliver right away, and it has to be:
        // if parsing stops exactly there (the parser blocks on a swap key, defect C), waiting for the next
        // water-mark means waiting forever (see flushLowestPending). A record that is not parsed yet sits at
        // >= parsedScn + 1 and carries a commit SCN >= parsedScn + 2, so nothing incomplete is ever delivered -
        // the low-water-mark is shifted by one for every thread, so the global ordering guarantee is unchanged.
        const Scn parsedCeiling(parsedScn.getData() + 1);
        auto it = threadWatermark.find(thread);
        if (it == threadWatermark.end()) {
            threadWatermark.try_emplace(thread, parsedCeiling);
            threadWatermarkTime.try_emplace(thread, currentTime);
        } else if (parsedCeiling > it->second) {
            it->second = parsedCeiling;
            // Only progress restarts the idle timer: being called again with an unchanged SCN is not
            // "the thread is alive", it is exactly the state a release has to recognize.
            threadWatermarkTime[thread] = currentTime;
            if (idleThreads.erase(thread) > 0 && watermark != Scn::none() && parsedCeiling < watermark) {
                // Back from a release while still behind the water-mark: it does not take part in the minimum
                // until it has caught up. Its events up to the water-mark are still delivered right away - the
                // order of that window was given up when the thread was released.
                unalignedThreads.insert(thread);
                if (ctx != nullptr)
                    ctx->info(0, "redo thread: " + std::to_string(thread) + " is behind the low-water-mark (scn: " +
                              parsedCeiling.toString() + " < " + watermark.toString() + ") - it does not hold it back until "
                              "it has caught up");
            }
            if (watermark != Scn::none() && parsedCeiling >= watermark)
                unalignedThreads.erase(thread);
        }

        refreshIdleLocked(currentTime, idleReleaseUs);
        // A water-mark that moved (or a redo thread that was released here) is what makes buffered events
        // deliverable - the delivery thread has to be woken for every one of them.
        notifyDeliveryLocked();
    }

    void CommitSorter::refreshIdle(uint64_t currentTime, uint64_t idleReleaseUs) {
        std::lock_guard const lck(sorterMtx);
        refreshIdleLocked(currentTime, idleReleaseUs);
        notifyDeliveryLocked();
    }

    void CommitSorter::notifyDeliveryLocked() {
        ++deliverySeq;
        // Notified with the lock held on purpose: the waiter re-reads the sequence under the same lock, so the
        // wake up can not be lost between the state change and the notification.
        deliveryCv.notify_all();
    }

    void CommitSorter::notifyDelivery() {
        {
            std::lock_guard const lck(sorterMtx);
            notifyDeliveryLocked();
        }
    }

    bool CommitSorter::waitForDelivery(uint64_t timeoutUs) {
        std::unique_lock lck(sorterMtx);
        const uint64_t seq = deliverySeq;
        return deliveryCv.wait_for(lck, std::chrono::microseconds(timeoutUs), [this, seq] {
            return deliverySeq != seq || (ctx != nullptr && ctx->hardShutdown);
        });
    }

    void CommitSorter::refreshIdleLocked(uint64_t currentTime, uint64_t idleReleaseUs) {
        if (idleReleaseUs == 0)
            return;

        // An idle redo thread (its instance has not archived anything for a long time) would otherwise
        // hold back the low-water-mark forever: every transaction the other threads commit stays in
        // memory (at least one chunk each) until the instance produces redo again. After the timeout such
        // a thread is left out of the low-water-mark, which bounds the memory at the price of the global
        // SCN order: transactions of the idle thread can be delivered after transactions with a higher SCN.
        for (const auto& [idleThread, lastAdvance]: threadWatermarkTime) {
            if (idleThreads.find(idleThread) != idleThreads.end())
                continue;
            // Already released because the archive list says it has nothing left to read: the timer would only
            // repeat the same statement with a different reason, and reading "did not advance for: 30 s" for a
            // thread that was released on positive evidence sends the analysis in the wrong direction (measured in
            // the rig on 2026-10-20: reason: no-work at 06:58:34, reason: timeout for the same thread at 06:59:04).
            if (noWorkThreads.find(idleThread) != noWorkThreads.end())
                continue;
            if (currentTime > lastAdvance && currentTime - lastAdvance > idleReleaseUs) {
                idleThreads.insert(idleThread);
                if (ctx != nullptr)
                    ctx->warning(60045, "redo thread: " + std::to_string(idleThread) + " did not advance for: " +
                                 std::to_string((currentTime - lastAdvance) / 1000000) + " s - it stops holding back the "
                                 "low-water-mark (reason: timeout, \"idle-thread-release-us\"); its transactions can now "
                                 "be delivered after transactions with a higher SCN");
            }
        }
    }

    void CommitSorter::threadClosed(uint16_t thread) {
        std::lock_guard const lck(sorterMtx);
        closedThreads.insert(thread);
        // A closed thread stops holding back the low-water-mark: its buffered events may be deliverable now.
        notifyDeliveryLocked();
    }

    void CommitSorter::threadReopened(uint16_t thread) {
        std::lock_guard const lck(sorterMtx);
        closedThreads.erase(thread);
        // Same rule as coming back from a release: a thread that is behind the water-mark takes part in the
        // minimum again only once it has caught up (after instance recovery its tail is still to be parsed).
        const auto it = threadWatermark.find(thread);
        if (it != threadWatermark.end() && watermark != Scn::none() && it->second < watermark) {
            unalignedThreads.insert(thread);
            if (ctx != nullptr)
                ctx->info(0, "redo thread: " + std::to_string(thread) + " reopened behind the low-water-mark (scn: " +
                          it->second.toString() + " < " + watermark.toString() + ") - it does not hold it back until it "
                          "has caught up");
        }
        notifyDeliveryLocked();
    }

    void CommitSorter::setThreadWork(uint16_t thread, bool hasWork, Seq expectedSeq, Seq highestSeq) {
        std::lock_guard const lck(sorterMtx);

        if (!hasWork) {
            // A thread that has never published a position does not take part in the minimum anyway: releasing it
            // would only put a name into the set that nothing reads.
            if (threadWatermark.find(thread) == threadWatermark.end())
                return;
            // Reported once per release and not once per archive list pass (the replicator publishes the same
            // state every pass, several times a minute).
            if (!noWorkThreads.insert(thread).second)
                return;
            if (ctx != nullptr)
                ctx->warning(60045, "redo thread: " + std::to_string(thread) + " has no archived redo log left to read "
                             "(expected seq: " + expectedSeq.toString() + ", highest known seq: " + highestSeq.toString() +
                             ") - it stops holding back the low-water-mark (reason: no-work); its transactions can now be "
                             "delivered after transactions with a higher SCN");
        } else {
            if (noWorkThreads.erase(thread) == 0)
                return;
            // The idle timer is the fallback for "the position did not move and nothing tells us why". A thread that
            // just got work back has the answer: it was waiting for an archive. Counting from the last advance would
            // fire the release on the very next tick, because that gap is one archive interval (4-5 minutes for a
            // quiet instance) against a threshold of 180 s - measured in production on 2026-10-20: 33
            // "reason: timeout" warnings in 56 minutes, i.e. one per archive of every quiet thread, each of them
            // 8-10 s *before* that thread finished the archive it was already parsing. Restart the timer here and
            // the fallback only speaks when a thread has work and does not advance for the whole threshold.
            if (ctx != nullptr)
                threadWatermarkTime[thread] = ctx->clock->getTimeUt();
            // The same rule a thread released by the timeout (or by a closed instance) follows: one that is behind
            // the water-mark takes part in the minimum again only once it has caught up. Without it the first
            // archive read after the release - which starts exactly where the thread stopped, i.e. below the
            // water-mark the other threads have already reached - would push the water-mark back and stop every
            // other thread until that archive is parsed, which is the stall the release exists to avoid.
            const auto it = threadWatermark.find(thread);
            if (it != threadWatermark.end() && watermark != Scn::none() && it->second < watermark) {
                unalignedThreads.insert(thread);
                if (ctx != nullptr)
                    ctx->info(0, "redo thread: " + std::to_string(thread) + " has work again and is behind the low-water-mark "
                              "(scn: " + it->second.toString() + " < " + watermark.toString() + ") - it does not hold it back "
                              "until it has caught up");
            } else {
                unalignedThreads.erase(thread);
            }
        }
        // Both directions change what may be delivered: the delivery has to re-read the limit now and not at its
        // next timeout.
        notifyDeliveryLocked();
    }

    namespace {
        // Stage 1 + 2 of the accounting decoupling, for a delivery that does not run on the parser: the memory
        // and the state the delivery books have to be the delivering thread's (Ctx::thread() reads this), and
        // the release that wakes a parser blocked in Ctx::swappedMemoryInit() has to wait for the end of the
        // delivery - a parser that resumed into a running delivery would use the builder and book memory while
        // the delivery is doing both. The destructor runs on every path out, including an exception from the
        // delivery: a release that is collected and never run would leave the parser waiting forever.
        struct DeliveryScope {
            Ctx* ctx;
            bool active;
            // The thread that had the delivery booked to it before this scope (Ctx::accountThread is thread local
            // since stage 4, so this scope only ever changes the value of the thread it runs on: the delivery
            // thread keeps itself booked for its whole life and this restores exactly that).
            Thread* previousAccountThread{nullptr};

            // The parameter is not named "ctx" on purpose: that would shadow the member of the same name
            // (-Wshadow, and a new warning in a file that has to stay clean).
            DeliveryScope(Ctx* newCtx, Thread* deliveryThread) : ctx(newCtx), active(deliveryThread != nullptr) {
                if (!active)
                    return;
                ctx->deferSwapReleases = true;
                previousAccountThread = Ctx::accountThread;
                Ctx::accountThread = deliveryThread;
            }

            ~DeliveryScope() {
                if (!active)
                    return;
                ctx->deferSwapReleases = false;
                Ctx::accountThread = previousAccountThread;
                ctx->releaseDeferredSwaps();
            }
        };
    }

    void CommitSorter::syncPendingEvents() const {
        ctx->statPendingDeliveries.store(pendingEvents, std::memory_order_relaxed);
        // g5: the part of the backlog that is already past the sorter (its commit is being sent right now). Kept
        // separate so that a blocked writer reads as "sorter_pending=4096, in_delivery=4096" and not as an idle
        // delivery.
        ctx->statInDelivery.store(inDelivery, std::memory_order_relaxed);
        // Layered mirror for Ctx::layersLine(): how many events are buffered, in how many SCN groups, and how they
        // sit per redo thread. Relaxed stores only, in the critical section that already holds sorterMtx.
        uint64_t totalEvents = 0;
        for (const auto& [scn, group]: events)
            totalEvents += group.size();
        ctx->statSorterEvents.store(totalEvents, std::memory_order_relaxed);
        ctx->statSorterScns.store(events.size(), std::memory_order_relaxed);
        for (uint16_t thread = 0; thread < 16; ++thread) {
            const auto it = pendingByThread.find(thread);
            const uint64_t buffered = it == pendingByThread.end() ? 0 : it->second;
            ctx->statPendingByThread[thread].store(buffered, std::memory_order_relaxed);
            // g4: the peak since the start, per thread. The credit should hold every thread at or below its limit
            // plus one record; anything above that means a path still runs away from it, and the peak is the number
            // that shows it in the layers report without having to catch the moment.
            if (buffered > ctx->statPendingMaxByThread[thread].load(std::memory_order_relaxed))
                ctx->statPendingMaxByThread[thread].store(buffered, std::memory_order_relaxed);
            // Same mirror for the chunk side: the number the backpressure is really about (warning 60062 and
            // Ctx::layersLine read it without taking the sorter lock).
            const auto itChunks = chunksByThread.find(thread);
            ctx->statChunksByThread[thread].store(itChunks == chunksByThread.end() ? 0 : itChunks->second,
                                                  std::memory_order_relaxed);
        }
        ctx->statSorterChunks.store(pendingChunks, std::memory_order_relaxed);
    }

    uint64_t CommitSorter::getPendingEvents() const {
        std::lock_guard const lck(sorterMtx);
        // Mirror for warning 60050 (Ctx::statPendingDeliveries): that message is written by the parser while it
        // is blocked in Ctx::swappedMemoryInit, and it must not take this lock - a delivery may be in flight and
        // call back into the sorter. Every change keeps the mirror in step (syncPendingEvents), so the
        // pending_commits metric read by the checkpoint thread is just as fresh as this getter.
        syncPendingEvents();
        return pendingEvents;
    }

    void CommitSorter::flushReady(Executor* executor, Thread* deliveryThread) {
        // One delivery at a time over every path (stage 3: the writer drives the sorter as well as the parser):
        // the callbacks of flushUpTo() below run outside the sorter lock and write into the builder, so two
        // deliveries must never overlap. The gate covers the whole delivery, not only the part under sorterMtx.
        const std::unique_lock<std::mutex> deliveryLck(ctx->deliveryMtx);
        const DeliveryScope delivery(ctx, deliveryThread);
        Scn limit;
        {
            std::lock_guard const lck(sorterMtx);
            limit = deliveryLimitLocked();
        }
        flushUpTo(executor, limit);
    }

    Scn CommitSorter::lowWaterMarkLocked() const {
        // Minimum parsed SCN over the threads that take part in the low-water-mark: a closed thread's position is
        // final and holds nothing back, a thread with no archived redo log left to read is in the same position
        // (it can not advance, see setThreadWork), an idle-released one is explicitly released ("its transactions
        // can now be delivered after transactions with a higher SCN", see setThreadWatermark) and an unaligned one
        // came back from a release while still behind, so it must not hold the others back until it has caught up.
        Scn lowWatermark = Scn::none();
        for (const auto& [thread, parsedScn]: threadWatermark) {
            if (closedThreads.find(thread) != closedThreads.end())
                continue;
            if (noWorkThreads.find(thread) != noWorkThreads.end())
                continue;
            if (idleThreads.find(thread) != idleThreads.end())
                continue;
            if (unalignedThreads.find(thread) != unalignedThreads.end())
                continue;
            if (lowWatermark == Scn::none() || parsedScn < lowWatermark)
                lowWatermark = parsedScn;
        }
        // TEST HOOK: pins the minimum at a fixed SCN (see Ctx::debugHoldScn). Everything above it waits in the
        // sorter, which is exactly the state the credit exists for.
        if (ctx != nullptr && ctx->debugHoldScn != 0) {
            const Scn hold(ctx->debugHoldScn);
            if (lowWatermark == Scn::none() || hold < lowWatermark)
                lowWatermark = hold;
        }
        return lowWatermark;
    }

    Scn CommitSorter::deliveryLimitLocked() {
        const Scn lowWatermark = lowWaterMarkLocked();
        if (lowWatermark != Scn::none()) {
            // The water-mark is a lower bound that was already acknowledged as safe, so it never moves back: a thread
            // that comes back below it (see unalignedThreads) must not stall every other thread until it has caught up.
            if (watermark == Scn::none() || lowWatermark > watermark)
                watermark = lowWatermark;
            // What a flush may deliver is the water-mark (raised above, or the one of the last flush)
            return watermark;
        }

        // No redo thread takes part in the low-water-mark: every one of them is closed, has no work left, is
        // idle-released or unaligned (or none has published a position yet, which is the startup case). The release
        // is explicit about what it means - a released thread no longer holds the delivery back - so no thread
        // holding the water-mark means no thread holds anything back: deliver what is buffered, the way flushAll()
        // does at shutdown.
        //
        // Returning instead is what stalled the field for 890 s (measured 2026-10-16, production log): the events
        // stayed buffered, the *old* water-mark kept reporting them as deliverable (warning 60062: "deliverable now:
        // 24580" while the delivery was idle), the buffer only grew until the parser hit the per-thread backpressure
        // limit and blocked in waitForBacklog(), and the parser that could have published a higher position was the
        // blocked one - a circle nothing could break. Reproduced in the rig without any load: 74 flushes with events
        // buffered while every redo thread was idle-released.
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "delivery limit: no thread holds the low-water-mark (threads: " +
                          std::to_string(threadWatermark.size()) + ", closed: " + std::to_string(closedThreads.size()) +
                          ", no work: " + std::to_string(noWorkThreads.size()) + ", idle: " +
                          std::to_string(idleThreads.size()) + ", unaligned: " +
                          std::to_string(unalignedThreads.size()) + ") - watermark: " + watermark.toString() +
                          ", buffered events: " + std::to_string(pendingEvents) + " - flushing them anyway");
        // Nothing to wait for: Scn::none() makes flushUpTo() take everything buffered.
        return Scn::none();
    }

    void CommitSorter::flushAll(Executor* executor, Thread* deliveryThread) {
        const std::unique_lock<std::mutex> deliveryLck(ctx->deliveryMtx);
        const DeliveryScope delivery(ctx, deliveryThread);
        flushUpTo(executor, Scn::none());
    }

    void CommitSorter::tick(Executor* executor, uint64_t currentTime, uint64_t idleReleaseUs, Thread* deliveryThread,
                            bool flushLowest) {
        // The drive of the writer (stage 3): refreshIdle takes the sorter lock on its own and does not deliver,
        // so it needs no gate - the delivery is the flush below, which takes the gate and books the delivery to
        // this thread.
        refreshIdle(currentTime, idleReleaseUs);
        flushReady(executor, deliveryThread);
        if (flushLowest)
            // The defect C emergency: everything buffered is delivered even though the water-mark has not reached
            // it. The parser can not be delivering right now (it is the one blocked in the wait), so nothing can
            // slip in between the flush above and this one. It is load bearing, not a formality: it was measured
            // with the duplicate-Xid hook that it still has to deliver the blocking commit even with the
            // water-mark published at every commit - the minimum over the redo threads is another thread's
            // position there.
            flushLowestPending(executor, deliveryThread);
    }

    void CommitSorter::flushLowestPending(Executor* executor, Thread* deliveryThread) {
        const std::unique_lock<std::mutex> deliveryLck(ctx->deliveryMtx);
        const DeliveryScope delivery(ctx, deliveryThread);
        {
            std::lock_guard const lck(sorterMtx);
            if (events.empty())
                return;
        }
        // Everything that is buffered, not one SCN group per tick. The callers arm this only while a parser is
        // blocked (in the swap wait or on the backpressure) and the water-mark can not be waited for, i.e. the
        // cross thread order is already given up - and the delivery thread only goes through this path once per
        // wait/notify, 100 ms apart at best, so "one SCN group" delivered about 10 groups per second and turned
        // the escape into the stalling mechanism (measured 2026-10-19: the parser blocked for 29 s while 30,139
        // commits sat in the sorter, the delivery idle except for the 5 s emergency rounds). The order of the
        // buffered events themselves is unchanged - they are still delivered by ascending SCN, exactly what
        // flushAll() does at shutdown.
        flushUpTo(executor, Scn::none());
    }

    void CommitSorter::flushUpTo(Executor* executor, Scn limit) {
        // The events are taken under the lock, the callbacks run without it: the executor can enqueue new
        // events (a system transaction enqueues its checkpoint while it is being flushed) and it also writes
        // into the builder, so flushing under the sorter lock would either deadlock on the re-entrant enqueue
        // or serialize the parser against the flush for no reason.
        std::vector<Event*> ready;
        {
            std::lock_guard const lck(sorterMtx);
            while (!events.empty()) {
                const auto it = events.begin();
                if (limit != Scn::none() && it->first > limit)
                    break;

                for (Event* event: it->second)
                    ready.push_back(event);
                events.erase(it);
            }
            // g5: the events are *not* taken out of the accounting here any more. They are handed to the executor
            // below, outside the lock, and only when executeCommit() returns has the backlog really shrunk. Before
            // g5 the counters dropped at collection time, so a writer that could not send (QUEUE_FULL, a black-holed
            // broker, a stalled Kafka) hid the whole backlog in this vector: the credit saw an empty queue and let
            // the parser run on, sorter_events read 0, and only the memory showed it. Reproduced in the rig: 50,000
            // commits buffered, warn 60031 347,162 times, sorter_events=0, no 60064.
            inDelivery += ready.size();
            syncPendingEvents();
        }

        // The accounting of the events that have just been delivered: applied every "ACCOUNTING_BATCH" events and
        // once at the end, so that a long delivery does not take the sorter lock 50,000 times, while a blocked one
        // keeps the whole backlog counted (which is exactly what the credit has to see).
        struct Delivered {
            uint16_t thread;
            uint64_t chunks;
        };
        std::vector<Delivered> delivered;
        delivered.reserve(ready.size());
        const auto applyDelivered = [this, &delivered]() {
            if (delivered.empty())
                return;
            std::lock_guard const lck(sorterMtx);
            pendingEvents = pendingEvents >= delivered.size() ? pendingEvents - delivered.size() : 0;
            inDelivery = inDelivery >= delivered.size() ? inDelivery - delivered.size() : 0;
            for (const Delivered& one: delivered) {
                const auto it2 = pendingByThread.find(one.thread);
                if (it2 != pendingByThread.end() && it2->second > 0)
                    --it2->second;
                // The memory of the delivered transaction is handed back to the allowance of its thread here and
                // not in the executor: this is the point the parser waits on, so the chunk counters and the events
                // have to move together (both under sorterMtx).
                const auto it3 = chunksByThread.find(one.thread);
                if (it3 != chunksByThread.end() && it3->second > 0) {
                    const uint64_t freed = it3->second < one.chunks ? it3->second : one.chunks;
                    it3->second -= freed;
                    pendingChunks = pendingChunks >= freed ? pendingChunks - freed : 0;
                }
            }
            delivered.clear();
            syncPendingEvents();
            // Backpressure: a parser of one of the redo threads may be waiting for exactly this drop - each waits on
            // the count of its own thread, so any drop can bring one under its limit. Notified with the lock held
            // like the delivery wake up (the waiter re-reads the count under the same lock).
            backpressureCv.notify_all();
        };

        try {
            for (Event* event: ready) {
                if (event->commit)
                    executor->executeCommit(event->transaction);
                else
                    executor->executeCheckpoint(event->checkpoint);
                // Read before the event goes back to the pool: releaseEvent() resets both fields.
                delivered.push_back(Delivered{event->thread, event->chunks});
                releaseEvent(event);
                if (delivered.size() >= ACCOUNTING_BATCH)
                    applyDelivered();
            }
        } catch (...) {
            // The counters must not be left behind on an exception out of the delivery: a parser that waits for a
            // drop that never comes would block until shutdown.
            applyDelivered();
            throw;
        }
        applyDelivered();
    }

    std::string CommitSorter::getThreadWatermarksText() const {
        std::string ret;
        for (const auto& [thread, parsedScn]: threadWatermark) {
            if (!ret.empty())
                ret += ", ";
            ret += std::to_string(thread) + ": " + parsedScn.toString();
            if (closedThreads.find(thread) != closedThreads.end())
                ret += " (closed)";
            else if (noWorkThreads.find(thread) != noWorkThreads.end())
                ret += " (no work)";
            else if (idleThreads.find(thread) != idleThreads.end())
                ret += " (released)";
            else if (unalignedThreads.find(thread) != unalignedThreads.end())
                ret += " (unaligned)";
        }
        if (ret.empty())
            ret = "none";
        return ret;
    }
}
