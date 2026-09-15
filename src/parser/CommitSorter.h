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

#ifndef COMMIT_SORTER_H_
#define COMMIT_SORTER_H_

#include <map>
#include <set>
#include <vector>

#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class Ctx;
    class Transaction;

    struct CheckpointEvent {
        Seq sequence;
        Scn scn;
        Time timestamp;
        FileOffset fileOffset;
        uint64_t bytes{0};
        bool switchRedo{false};
        // Redo thread the event was produced by: only thread 1 may update the legacy scalar
        // checkpoint fields (the thread 1 view), other threads update threads[] only
        uint16_t thread{1};
    };

    /* Sorts commit and checkpoint events by SCN across redo threads and flushes them in a
       globally consistent order once the low-water-mark (the minimum parsed SCN over all
       active threads) passes the event SCN. Transactions are buffered whole (swapped
       memory) until commit, so only the delivery order of finished events has to be
       sorted - the parsing itself stays single-threaded and per-thread sequential.
       Single-instance deployments run the sorter with a single thread: the watermark
       equals the current parsed SCN and events are flushed immediately, preserving the
       original output order. */
    class CommitSorter final {
    public:
        class Executor {
        public:
            virtual ~Executor() = default;
            virtual void executeCommit(Transaction* transaction) = 0;
            virtual void executeCheckpoint(const CheckpointEvent& event) = 0;
        };

        CommitSorter() = default;
        ~CommitSorter();

        // Only used to report the idle thread release (warning 60045).
        void setContext(Ctx* newCtx) {
            ctx = newCtx;
        }

        // Takes ownership of the flush decision; the transaction itself stays in the
        // transaction buffer until the executor drops it
        void enqueueCommit(Transaction* transaction, Scn commitScn);
        void enqueueCheckpoint(const CheckpointEvent& event);
        // All records with scn <= parsedScn have been analyzed for the thread. "currentTime" is the
        // microsecond clock of the parser and is only used to measure how long a thread has been idle:
        // "idleReleaseUs" (0 = disabled) is the timeout after which a thread that did not advance stops
        // holding back the global low-water-mark.
        void setThreadWatermark(uint16_t thread, Scn parsedScn, uint64_t currentTime, uint64_t idleReleaseUs);
        // Closed thread (instance down): its final watermark stops holding back the global low-water-mark
        void threadClosed(uint16_t thread);
        void threadReopened(uint16_t thread);
        // Flush all events with scn <= low-water-mark
        void flushReady(Executor* executor);
        // Flush everything ignoring the watermark (soft shutdown)
        void flushAll(Executor* executor);

        [[nodiscard]] Scn getWatermark() const {
            return watermark;
        }

        [[nodiscard]] uint64_t getPendingEvents() const {
            return pendingEvents;
        }

        // Diagnostics only (warning 60047): the per thread watermarks, with the threads that do not hold back
        // the low-water-mark any more marked accordingly
        [[nodiscard]] std::string getThreadWatermarksText() const;

        // SCN of the buffered event that would be flushed first (none when nothing is buffered): an event at or
        // below the low-water-mark could be flushed by flushReady, so one that is still buffered while the
        // watermark is above it means that nothing called the sorter at all
        [[nodiscard]] Scn getLowestPendingScn() const {
            return events.empty() ? Scn::none() : events.begin()->first;
        }

    protected:
        struct Event {
            Scn scn;
            bool commit;
            Transaction* transaction{nullptr};
            CheckpointEvent checkpoint;
        };

        Scn watermark{Scn::none()};
        uint64_t pendingEvents{0};
        std::map<Scn, std::vector<Event*>> events; // insertion order preserved within the same SCN
        std::map<uint16_t, Scn> threadWatermark;
        std::set<uint16_t> closedThreads;
        Ctx* ctx{nullptr};
        // Time of the last advance of the parsed SCN per thread, and the threads that stopped holding
        // back the low-water-mark because of it.
        std::map<uint16_t, uint64_t> threadWatermarkTime;
        std::set<uint16_t> idleThreads;

        void flushUpTo(Executor* executor, Scn limit);
    };
}

#endif
