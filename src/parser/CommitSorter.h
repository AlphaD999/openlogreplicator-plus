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
#include <mutex>
#include <set>
#include <vector>

#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {

// Forward declaration: the delivery entry points take the thread the delivery is booked to (Ctx::accountThread,
// stage 1) and whose swap key releases are held back until it is over - the definition is only needed by the
// implementation, so the header does not have to pull in Ctx/Thread.
class Thread;
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
        // Flush all events with scn <= low-water-mark. Every path that delivers takes Ctx::deliveryMtx for the
        // whole delivery, the callbacks run outside the sorter lock. "deliveryThread" is the thread the delivery
        // is booked to (Ctx::accountThread, stage 1) and the thread whose swap key releases are held back until
        // the delivery is over (Ctx::deferSwapReleases, stage 2). The parser passes nothing: its deliveries are
        // booked to the parser (Ctx::thread() falls back to it) and there is nothing to hold back.
        void flushReady(Executor* executor, Thread* deliveryThread = nullptr);
        // Release the redo threads whose parsed SCN did not advance for "idleReleaseUs" (0 = disabled). Split
        // out of setThreadWatermark so that it can also run while nothing is being parsed.
        void refreshIdle(uint64_t currentTime, uint64_t idleReleaseUs);
        // Flush everything ignoring the watermark (soft shutdown)
        void flushAll(Executor* executor, Thread* deliveryThread = nullptr);
        // Stage 3, the independent delivery driver: drive the sorter off the parser thread - release the idle
        // threads and flush what the low-water-mark allows. The parser keeps its own flush points, this is what
        // makes the delivery independent of them: it no longer waits for the parser to reach one, it runs while
        // the writer has nothing to write. With "flushLowest" the delivery also bypasses the low-water-mark,
        // which is the defect C emergency: the parser is blocked in Ctx::swappedMemoryInit() waiting for the
        // delivery it can not run itself, and while it is blocked nothing publishes a higher water-mark, so
        // waiting for the water-mark would be waiting forever.
        void tick(Executor* executor, uint64_t currentTime, uint64_t idleReleaseUs, Thread* deliveryThread = nullptr,
                  bool flushLowest = false);

        // Deliver the lowest buffered event(s) even though the low-water-mark has not reached them. Only called
        // by tick() with flushLowest (the defect C emergency). It is safe there: the event is a *commit* (the
        // transaction is complete), it is the lowest buffered one (nothing can be delivered out of order) and
        // the thread that would have to catch up is the blocked parser itself.
        void flushLowestPending(Executor* executor, Thread* deliveryThread = nullptr);

        // The low-water-mark: still the minimum over the active threads, but each thread publishes "the highest
        // commit SCN I can vouch for", which is one past its parsed position (see setThreadWatermark). Compared
        // with a *parsed position* use isAheadOfWatermark() instead of the plain operators.
        [[nodiscard]] Scn getWatermark() const {
            std::lock_guard const lck(sorterMtx);
            return watermark;
        }

        // Is the position "parsedScn" of a thread ahead of the low-water-mark? The water-mark is one SCN past
        // the position each thread vouches for, so the plain position compares with >= against it.
        [[nodiscard]] bool isAheadOfWatermark(Scn parsedScn) const {
            std::lock_guard const lck(sorterMtx);
            return watermark != Scn::none() && parsedScn >= watermark;
        }

        [[nodiscard]] uint64_t getPendingEvents() const {
            std::lock_guard const lck(sorterMtx);
            return pendingEvents;
        }

        // Diagnostics only (warning 60047): the per thread watermarks, with the threads that do not hold back
        // the low-water-mark any more marked accordingly
        [[nodiscard]] std::string getThreadWatermarksText() const;

        // SCN of the buffered event that would be flushed first (none when nothing is buffered): an event at or
        // below the low-water-mark could be flushed by flushReady, so one that is still buffered while the
        // watermark is above it means that nothing called the sorter at all
        [[nodiscard]] Scn getLowestPendingScn() const {
            std::lock_guard const lck(sorterMtx);
            return events.empty() ? Scn::none() : events.begin()->first;
        }

    protected:
        // Guards every member below and every public method: since the tick can also be driven by the writer
        // thread (see tick) the sorter is no longer driven by the parser thread alone.
        mutable std::mutex sorterMtx;

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
        // Threads that came back from a release (or from a closed instance) while their parsed SCN is still
        // behind the water-mark: they take part in the minimum again only once they have caught up with redo
        // that was already delivered.
        std::set<uint16_t> unalignedThreads;

        void flushUpTo(Executor* executor, Scn limit);
        // Body of refreshIdle() without the lock: setThreadWatermark() already holds it when it releases the
        // idle threads at the end of its own work.
        void refreshIdleLocked(uint64_t currentTime, uint64_t idleReleaseUs);
    };
}

#endif
