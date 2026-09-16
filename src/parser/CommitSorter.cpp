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
        events[commitScn].push_back(event);
        ++pendingEvents;
    }

    void CommitSorter::enqueueCheckpoint(const CheckpointEvent& checkpoint) {
        auto* event = new Event();
        event->scn = checkpoint.scn;
        event->commit = false;
        event->checkpoint = checkpoint;
        events[checkpoint.scn].push_back(event);
        ++pendingEvents;
    }

    void CommitSorter::setThreadWatermark(uint16_t thread, Scn parsedScn, uint64_t currentTime, uint64_t idleReleaseUs) {
        auto it = threadWatermark.find(thread);
        if (it == threadWatermark.end()) {
            threadWatermark.try_emplace(thread, parsedScn);
            threadWatermarkTime.try_emplace(thread, currentTime);
        } else if (parsedScn > it->second) {
            it->second = parsedScn;
            // Only progress restarts the idle timer: being called again with an unchanged SCN is not
            // "the thread is alive", it is exactly the state a release has to recognize.
            threadWatermarkTime[thread] = currentTime;
            if (idleThreads.erase(thread) > 0 && watermark != Scn::none() && parsedScn < watermark) {
                // Back from a release while still behind the water-mark: it does not take part in the minimum
                // until it has caught up. Its events up to the water-mark are still delivered right away - the
                // order of that window was given up when the thread was released.
                unalignedThreads.insert(thread);
                if (ctx != nullptr)
                    ctx->info(0, "redo thread: " + std::to_string(thread) + " is behind the low-water-mark (scn: " +
                              parsedScn.toString() + " < " + watermark.toString() + ") - it does not hold it back until "
                              "it has caught up");
            }
            if (watermark != Scn::none() && parsedScn >= watermark)
                unalignedThreads.erase(thread);
        }

        refreshIdle(currentTime, idleReleaseUs);
    }

    void CommitSorter::refreshIdle(uint64_t currentTime, uint64_t idleReleaseUs) {
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
        closedThreads.insert(thread);
    }

    void CommitSorter::threadReopened(uint16_t thread) {
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

    void CommitSorter::flushReady(Executor* executor) {
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
        flushUpTo(executor, watermark);
    }

    void CommitSorter::flushAll(Executor* executor) {
        flushUpTo(executor, Scn::none());
    }

    void CommitSorter::flushUpTo(Executor* executor, Scn limit) {
        while (!events.empty()) {
            const auto it = events.begin();
            if (limit != Scn::none() && it->first > limit)
                break;

            for (const Event* event: it->second) {
                if (event->commit)
                    executor->executeCommit(event->transaction);
                else
                    executor->executeCheckpoint(event->checkpoint);
                delete event;
                --pendingEvents;
            }
            events.erase(it);
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
