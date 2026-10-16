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
            for (const Event* event: eventList)
                delete event;
        }
        events.clear();
    }

    void CommitSorter::enqueueCommit(Transaction* transaction, Scn commitScn) {
        auto* event = new Event();
        event->scn = commitScn;
        event->commit = true;
        event->transaction = transaction;
        event->thread = transaction->thread;
        std::unique_lock lck(sorterMtx);
        events[commitScn].push_back(event);
        ++pendingEvents;
        ++pendingByThread[event->thread];
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
        //  - the wait must be per redo thread (pendingLimitLocked): with a global bound a thread that runs ahead
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
        const uint64_t limit = pendingLimitLocked();
        if (limit == 0 || pendingByThread[thread] <= limit)
            return;

        bool reported = false;
        const uint64_t waitingSince = ctx->clock->getTimeUt();
        while (pendingByThread[thread] > limit && !ctx->hardShutdown && !ctx->softShutdown) {
            backpressureCv.wait_for(lck, std::chrono::milliseconds(BACKPRESSURE_WAIT_MS));
            const uint64_t now = ctx->clock->getTimeUt();
            if (reported || now <= waitingSince || now - waitingSince < BACKPRESSURE_REPORT_US)
                continue;
            reported = true;
            // How much of the backlog the delivery may take right now and how much of it waits for a higher
            // water-mark: a wait in which nothing is deliverable is not the delivery being slow, it is the
            // water-mark being held back - and that has to be visible in the report.
            uint64_t deliverable = 0;
            uint64_t aboveWatermark = 0;
            for (const auto& [scn, list]: events) {
                if (watermark != Scn::none() && !(scn > watermark))
                    deliverable += list.size();
                else
                    aboveWatermark += list.size();
            }
            ctx->warning(60062, "the parser is waiting for the delivery for: " + std::to_string((now - waitingSince) / 1000000) +
                         " s - the commit sorter holds " + std::to_string(pendingByThread[thread]) + " buffered commit(s) of this "
                         "thread and " + std::to_string(pendingEvents) + " in total (per thread limit: " +
                         std::to_string(limit) + ", deliverable now: " + std::to_string(deliverable) +
                         ", above the low-water-mark: " + std::to_string(aboveWatermark) + ", transaction pool: " +
                         std::to_string(ctx->getMemoryChunksMax()) + " chunk(s), thread watermarks: " +
                         getThreadWatermarksText() + "); the parse of this thread is stopped on purpose: letting it go on "
                         "would fill the pool with commits that are waiting for the low-water-mark (\"memory-max-mb\" / "
                         "\"transaction-block-kb\")");
        }
    }

    uint64_t CommitSorter::pendingLimitLocked() const {
        if (ctx == nullptr)
            return 0;
        const uint64_t chunks = ctx->getMemoryChunksMax();
        if (chunks == 0)
            return 0;
        // Half of the pool for the buffered commits (the other half takes the open transactions, the builder
        // buffers and the reader), divided by the number of redo threads the sorter has seen: every buffered
        // commit owns at least one chunk and a transaction owning exactly one chunk can not be swapped out, so
        // with this bound the backlog of all threads together can not exhaust the pool on its own.
        const uint64_t threads = threadWatermark.empty() ? 1 : threadWatermark.size();
        return chunks / 2 / threads;
    }

    void CommitSorter::enqueueCheckpoint(const CheckpointEvent& checkpoint) {
        auto* event = new Event();
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
            if (currentTime > lastAdvance && currentTime - lastAdvance > idleReleaseUs) {
                idleThreads.insert(idleThread);
                if (ctx != nullptr)
                    ctx->warning(60045, "redo thread: " + std::to_string(idleThread) + " did not advance for: " +
                                 std::to_string((currentTime - lastAdvance) / 1000000) + " s - it stops holding back the "
                                 "low-water-mark (\"idle-thread-release-us\"); its transactions can now be delivered "
                                 "after transactions with a higher SCN");
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
            // Low-water-mark: minimum parsed SCN over all active (non-closed) threads
            Scn lowWatermark = Scn::none();
            for (const auto& [thread, parsedScn]: threadWatermark) {
                if (closedThreads.find(thread) != closedThreads.end())
                    continue;
                // Idle thread released by the timeout: it does not hold back the low-water-mark any more
                if (idleThreads.find(thread) != idleThreads.end())
                    continue;
                // Thread that came back from a release while still behind the water-mark: it must not hold back
                // the other threads until it has caught up (see setThreadWatermark)
                if (unalignedThreads.find(thread) != unalignedThreads.end())
                    continue;
                if (lowWatermark == Scn::none() || parsedScn < lowWatermark)
                    lowWatermark = parsedScn;
            }
            if (lowWatermark == Scn::none())
                return;

            // The water-mark is a lower bound that was already acknowledged as safe, so it never moves back: a
            // thread that comes back below it (see unalignedThreads) must not stall every other thread until it
            // has caught up.
            if (watermark == Scn::none() || lowWatermark > watermark)
                watermark = lowWatermark;
            // What this flush may deliver is the water-mark (raised above, or the one of the last flush)
            limit = watermark;
        }
        flushUpTo(executor, limit);
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
            // The defect C emergency: the lowest buffered event, delivered even though the water-mark has not
            // reached it. The parser can not be delivering right now (it is the one blocked in the wait), so
            // nothing can slip in between the flush above and this one. It is load bearing, not a formality: it
            // was measured with the duplicate-Xid hook that it still has to deliver the blocking commit even
            // with the water-mark published at every commit - the minimum over the redo threads is another
            // thread's position there.
            flushLowestPending(executor, deliveryThread);
    }

    void CommitSorter::flushLowestPending(Executor* executor, Thread* deliveryThread) {
        const std::unique_lock<std::mutex> deliveryLck(ctx->deliveryMtx);
        const DeliveryScope delivery(ctx, deliveryThread);
        Scn lowest;
        {
            std::lock_guard const lck(sorterMtx);
            if (events.empty())
                return;
            lowest = events.begin()->first;
        }
        // flushUpTo() is inclusive, so this takes exactly the lowest buffered SCN group
        flushUpTo(executor, lowest);
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
            pendingEvents -= ready.size();
            for (const Event* event: ready) {
                const auto it2 = pendingByThread.find(event->thread);
                if (it2 != pendingByThread.end() && it2->second > 0)
                    --it2->second;
            }
            syncPendingEvents();
            // Backpressure: a parser of one of the redo threads may be waiting in enqueueCommit() for exactly
            // this drop - each waits on the count of its own thread, so any drop can bring one under its limit.
            // Notified with the lock held like the delivery wake up (the waiter re-reads the count under the
            // same lock).
            backpressureCv.notify_all();
        }

        for (Event* event: ready) {
            if (event->commit)
                executor->executeCommit(event->transaction);
            else
                executor->executeCheckpoint(event->checkpoint);
            delete event;
        }
    }

    std::string CommitSorter::getThreadWatermarksText() const {
        std::string ret;
        for (const auto& [thread, parsedScn]: threadWatermark) {
            if (!ret.empty())
                ret += ", ";
            ret += std::to_string(thread) + ": " + parsedScn.toString();
            if (closedThreads.find(thread) != closedThreads.end())
                ret += " (closed)";
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
