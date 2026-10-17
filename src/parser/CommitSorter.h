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

#include <condition_variable>
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
        // transaction buffer until the executor drops it.
        // Buffers a finished transaction. Does not block: the backpressure is applied at the boundary of a record
        // (waitForBacklog), see the comment there for why it is not applied here.
        void enqueueCommit(Transaction* transaction, Scn commitScn);

        // Backpressure (stage 2): waits while the sorter holds more buffered events of "thread" than its share of
        // the transaction pool allows (pendingLimitLocked). Called by the parser at the *head* of a record, right
        // after it published its water-mark there - the wait is per redo thread and at a record boundary, both for
        // reasons that were measured (see the implementation). The lock is released while waiting.
        void waitForBacklog(uint16_t thread);
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
        // the delivery is over (Ctx::deferSwapReleases, stage 2). The delivery thread passes itself (it is the one
        // that delivers); the single threaded path without a sorter passes nothing, its delivery is booked to the
        // parser (Ctx::thread() falls back to it) and there is nothing to hold back.
        void flushReady(Executor* executor, Thread* deliveryThread = nullptr);
        // Release the redo threads whose parsed SCN did not advance for "idleReleaseUs" (0 = disabled). Split
        // out of setThreadWatermark so that it can also run while nothing is being parsed.
        void refreshIdle(uint64_t currentTime, uint64_t idleReleaseUs);
        // Flush everything ignoring the watermark (soft shutdown)
        void flushAll(Executor* executor, Thread* deliveryThread = nullptr);
        // Body of the delivery loop of the delivery thread (DeliveryExecutor): release the idle threads and flush
        // what the low-water-mark allows, in one call. With "flushLowest" the delivery also bypasses the
        // low-water-mark, which is the defect C emergency: the parser is blocked in Ctx::swappedMemoryInit()
        // waiting for the delivery it can not run itself, and while it is blocked nothing publishes a higher
        // water-mark, so waiting for the water-mark would be waiting forever.
        void tick(Executor* executor, uint64_t currentTime, uint64_t idleReleaseUs, Thread* deliveryThread = nullptr,
                  bool flushLowest = false);

        // Stage 4, the delivery driver: the delivery has a thread of its own (DeliveryExecutor) and is woken here
        // instead of being run by whoever happens to call the sorter. Woken by every change that can make an event
        // deliverable - an event was enqueued, a water-mark moved, a redo thread was closed or reopened - so the
        // delivery no longer waits for the parser to reach one of its flush points and the parser no longer runs a
        // delivery at all (see Parser::parse: it only publishes the water-mark and enqueues).
        void notifyDelivery();
        // Wait of the delivery thread. Returns false when the timeout expired without a notification: the timeout
        // is not a poll loop, it only covers the two decisions nobody notifies about - the time based idle thread
        // release (refreshIdle) and the shutdown.
        bool waitForDelivery(uint64_t timeoutUs);

        // Backpressure (stage 2): enqueueCommit() waits while the sorter holds more than the limit of the
        // *enqueuing thread* (pendingLimitLocked - per redo thread, and not global: see the comment there). The
        // wait is a bounded wait per round (the delivery notifies when a count drops), and a wait longer than
        // BACKPRESSURE_REPORT_US is reported once (warning 60062): a bounded wait that nobody can see is the same
        // blind spot the stalls of 2026-10-15 hid in.
        static constexpr uint64_t BACKPRESSURE_WAIT_MS{100};
        static constexpr uint64_t BACKPRESSURE_REPORT_US{30000000};
        // Once a parser has been stopped by the limit for this long, the delivery thread is told to take the lowest
        // buffered event without waiting for the water-mark (Ctx::parserInBacklogWait) - the escape of the same kind
        // as the swapped-memory one. Five seconds: long enough not to cut in front of a delivery that is merely busy,
        // short enough to break a real circle before the read buffer runs dry (60046).
        static constexpr uint64_t BACKPRESSURE_EMERGENCY_US{5000000};

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

        // Defined in CommitSorter.cpp: it mirrors the value into Ctx for warning 60050, and Ctx is an incomplete
        // type in this header (Ctx.h includes it).
        [[nodiscard]] uint64_t getPendingEvents() const;

        // Mirror of pendingEvents into Ctx::statPendingDeliveries, called with sorterMtx held whenever the value
        // changes (also defined in CommitSorter.cpp: Ctx is incomplete here). Warning 60050 and the pending_commits
        // metric read that mirror without taking the sorter lock - a delivery may be in flight and call back in.
        void syncPendingEvents() const;

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

        // Delivery wake up (stage 4): the sequence number counts every change that can make an event deliverable
        // and the delivery thread waits for it to differ from the value it saw. A condition variable and not a
        // flag, because the changes and the wait are on two threads.
        uint64_t deliverySeq{0};
        std::condition_variable deliveryCv;
        // Like the other state changes: only called with sorterMtx held (the wait is race free then, because the
        // waiter re-reads the sequence under the same lock).
        void notifyDeliveryLocked();

        // Backpressure, the second half of stage 2: how many buffered events the sorter may hold before the
        // parser is stopped in enqueueCommit(). 0 = unlimited. Derived once from the transaction pool
        // (initPendingLimitLocked): a buffered commit owns at least one chunk of it, and a transaction that owns
        // a single chunk can not be swapped out, so a backlog beyond the pool can not be relieved any more - the
        // parser then blocks in Ctx::getMemoryChunk and walks into "out of memory" (10017). Until the delivery
        // got a thread of its own (stage 4) the parser delivered inline and the backlog was bounded by one LWN
        // window; with the delivery decoupled nothing paces the parser, so it is paced here. Measured in the
        // reproduction rig before this existed: 4,500 buffered commits with a 4 GB pool and 1 MB blocks
        // (= 4.5 GB) ended the run with 10017.
        std::condition_variable backpressureCv;
        // The backpressure bound of one redo thread, with sorterMtx held: half of the pool, divided by the number
        // of redo threads the sorter has seen (a thread that holds more than one chunk can be swapped out, and a
        // transaction that holds exactly one can not - so the pool has to cover the backlog of every thread plus
        // the open transactions). 0 = no bound (the pool is not initialized yet).
        [[nodiscard]] uint64_t pendingLimitLocked() const;
        // The SCN a flush may deliver up to, with sorterMtx held: Scn::none() means "everything buffered" (no redo
        // thread holds the low-water-mark any more). Both the flush (flushReady) and the backpressure report (60062)
        // ask this - reporting "deliverable" against a different number than the flush delivers by made the field
        // read "deliverable now: 24580" from a delivery that was idle (2026-10-16).
        Scn deliveryLimitLocked();
        // The minimum parsed SCN over the threads that take part in the low-water-mark (not closed, not
        // idle-released, not unaligned); Scn::none() when no thread takes part.
        [[nodiscard]] Scn lowWaterMarkLocked() const;
        struct Event {
            Scn scn;
            bool commit;
            Transaction* transaction{nullptr};
            CheckpointEvent checkpoint;
            // The redo thread this event belongs to: the backlog is accounted per thread (see pendingByThread)
            // so that one thread can not stop the parser of another one.
            uint16_t thread{0};
        };

        Scn watermark{Scn::none()};
        uint64_t pendingEvents{0};
        // Buffered events per redo thread. The backpressure of enqueueCommit() uses the count of *its own*
        // thread: a global count lets a thread that runs ahead hold back the parser of a thread that is behind -
        // and that behind thread is the one whose water-mark has to move for anything to become deliverable.
        // Measured in the reproduction rig on 2026-10-16 (slicing on): 8,193 events of thread 2 above thread 1's
        // water-mark (23,227,363 against 23,242,448), the parser of thread 1 stopped by the global limit and the
        // delivery with nothing it was allowed to take (60062, "deliverable now: 0") - a deadlock that no wait
        // could resolve.
        std::map<uint16_t, uint64_t> pendingByThread;
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
