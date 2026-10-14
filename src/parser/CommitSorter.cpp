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
        std::lock_guard const lck(sorterMtx);
        events[commitScn].push_back(event);
        ++pendingEvents;
        syncPendingEvents();
    }

    void CommitSorter::enqueueCheckpoint(const CheckpointEvent& checkpoint) {
        auto* event = new Event();
        event->scn = checkpoint.scn;
        event->commit = false;
        event->checkpoint = checkpoint;
        std::lock_guard const lck(sorterMtx);
        events[checkpoint.scn].push_back(event);
        ++pendingEvents;
        syncPendingEvents();
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
    }

    void CommitSorter::refreshIdle(uint64_t currentTime, uint64_t idleReleaseUs) {
        std::lock_guard const lck(sorterMtx);
        refreshIdleLocked(currentTime, idleReleaseUs);
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

            DeliveryScope(Ctx* ctx, Thread* deliveryThread) : ctx(ctx), active(deliveryThread != nullptr) {
                if (!active)
                    return;
                ctx->deferSwapReleases = true;
                ctx->accountThread.store(deliveryThread, std::memory_order_relaxed);
            }

            ~DeliveryScope() {
                if (!active)
                    return;
                ctx->deferSwapReleases = false;
                ctx->accountThread.store(nullptr, std::memory_order_relaxed);
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
            // Thread that came back from a release while still behind the water-mark: it must not hold back the
            // other threads until it has caught up (see setThreadWatermark)
            if (unalignedThreads.find(thread) != unalignedThreads.end())
                continue;
            if (lowWatermark == Scn::none() || parsedScn < lowWatermark)
                lowWatermark = parsedScn;
        }
        if (lowWatermark == Scn::none())
            return;

        // The water-mark is a lower bound that was already acknowledged as safe, so it never moves back: a
        // thread that comes back below it (see unalignedThreads) must not stall every other thread until it has
        // caught up.
        if (watermark == Scn::none() || lowWatermark > watermark)
            watermark = lowWatermark;
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
            syncPendingEvents();
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
