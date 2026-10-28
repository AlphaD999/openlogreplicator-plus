/* Header for Replicator class
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

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#ifndef REPLICATOR_H_
#define REPLICATOR_H_

#include <fstream>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <deque>
#include <vector>

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/exception/RedoLogException.h"
#include "../parser/DeliveryExecutor.h"

namespace OpenLogReplicator {
    class Parser;
    class Builder;
    class Metadata;
    class Reader;
    class RedoLogRecord;
    class State;
    class Transaction;
    class TransactionBuffer;

    class Replicator : public Thread {
    protected:
        void (*archGetLog)(Replicator* replicator);
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        std::string database;
        // Stage 4: the thread that runs every delivery (and with it the commit sorter tick, including the defect C
        // flushLowestPending). Owned here and not by Ctx: Ctx.h can not include DeliveryExecutor.h (it derives
        // from Thread, Thread.h includes Ctx.h) and a std::unique_ptr of an incomplete type as a member of Ctx
        // does not compile in the translation units that only see Ctx.h. The thread is spawned in run() and is
        // joined by Ctx::mainFinish() - which ~OpenLogReplicator calls before it deletes the replicators - so the
        // life times agree.
        DeliveryExecutor deliveryExecutor;
        bool deliveryStarted{false};
        std::string redoCopyPath;
        // Redo log files
        // One reader per redo thread, created on first use. A reader carries the identity of the archive it has
        // open (fileName, fileSize, header buffer, sequence) and a single object shared by every thread let one
        // pass see - and print - the state of another one: measured 2026-10-28, thread 2 seq 15207 reported
        // 52,630,528, the size of thread 1 seq 19775, and the 60024 gate then judged the middle of a file as
        // its end. Indexed by redo thread (1..threads); slot 0 is unused.
        std::vector<Reader*> archReaders;
        [[nodiscard]] Reader* archReaderFor(uint16_t thread);
        std::string lastCheckedDay;
        // Archived redo logs waiting to be parsed. The next one is selected on demand by the SCN still to be
        // parsed (see bestArchiveIndex) instead of by (thread, sequence): with several redo threads the
        // archives have to be interleaved, otherwise parsing one whole archive keeps every committed
        // transaction of that archive in memory until the other threads catch up (the low-water-mark - the
        // minimum parsed SCN over the threads - only moves then). One chunk (1 MB) is held per open
        // transaction regardless of its size, so this is the difference between a few MB and tens of GB.
        std::vector<Parser*> archiveRedoQueue;
        // Archives parked because the predecessor is missing: the other redo threads may still be parseable
        std::deque<Parser*> gapArchives;
        std::set<Parser*> onlineRedoSet;
        std::set<Reader*> readers;
        std::vector<std::string> pathMapping;
        std::vector<std::string> redoLogsBatch;
        // RAC thread activity, taken from the archived redo log list itself: the highest sequence seen for the
        // thread and the moment it moved last. A thread whose highest sequence does not move for
        // "thread-inactive-us" counts as an instance that stopped producing redo, so a missing archive of it no
        // longer holds back the low-water-mark (see processArchivedRedoLogs). SYS.V_$THREAD is deliberately not
        // consulted: on a physical standby it reports whether the standby instance itself has the thread
        // attached, which makes the threads of all other instances look CLOSED while they keep archiving.
        std::map<uint16_t, Seq> threadMaxSequence;
        std::map<uint16_t, uint64_t> threadMaxSequenceTime;
        // Start of the current "nothing was read in this pass" period and the timestamp of the last report of
        // it (warning 60047, at most one message per 30 s). Both are reset as soon as a pass does real work.
        uint64_t idleSince{0};
        uint64_t lastIdleReport{0};
        // Start of the current "the archive list returned nothing at all" period, when it was reported last and
        // how many times - warning 60056. Reset as soon as an archive is available again, which is reported
        // with an INFO line so that the end of a stall is visible in the log as well.
        uint64_t archWaitSince{0};
        uint64_t archWaitReportTime{0};
        uint64_t archWaitReportCount{0};
        // "expected sequence of every known thread against the highest sequence the source reported", for 60056
        [[nodiscard]] std::string archiveWaitState() const;

        void cleanArchList();
        // True when this (thread, sequence) already has a Parser waiting in the queue: a sliced archive that
        // waits for its resume, or one parked because its predecessor is missing. The archive list is read on
        // every pass of the scheduler and appends what it returns, so without this check the same archive gets
        // a second Parser object. Both are then parsed: the saved position is a single slot that is cleared
        // when it is taken, so one of them resumes and the other starts at the beginning of the file. It
        // re-reads the whole archive (measured on a sliced run: 190,091 records of already committed
        // transactions dropped in one pass) and the two interleaved parsers take undo slots over from each
        // other (measured: 60041, 614 rows of a transaction never delivered).
        //
        // Two things this relies on, and one that is deliberately not guarded:
        //  - a skipped row has to come back later, and it does: the queue and the parked list are only ever
        //    discarded by cleanArchList(), whose single caller (Replicator::processArchivedRedoLogs) follows it
        //    with archGetLog() in the same thread - so after a clean the list is read again and the archive is
        //    queued then. A parked archive has no other way back than that re-read (there is no un-park path).
        //    If a future change discards the queues without re-reading the list, skipping here would starve the
        //    archive.
        //  - the re-queue of a sliced Parser in processArchivedRedoLogs is *not* checked against this predicate:
        //    it puts back the Parser that was just taken out, not a newly listed archive - skipping it would drop
        //    the saved resume.
        //
        // Honest status (2026-10-16, reproduction rig): with the sequence attribution of Reader::checkBlockHeader
        // in place (commit 4d635e8) this predicate did not fire once in a full sliced run over the archives of the
        // rig (skip count 0, and committed_reread_drops = 0 with the guard as well as without it), i.e. the
        // re-reads it was written for were that adoption bug and not duplicate Parsers. It is kept as a cheap
        // consistency guard for the paths that can hand an archive out again (the batch/directory list, which the
        // rig does not exercise) - not as a measured optimisation.
        [[nodiscard]] bool archiveQueued(uint16_t thread, Seq sequence) const;
        // Publishes, per redo thread, whether it still has an archived redo log to read (CommitSorter::setThreadWork).
        // The answer is taken from the state the pass just built and nothing else: the archives waiting in
        // archiveRedoQueue, the ones parked in gapArchives (their predecessor is missing - they are work, not a
        // reason to release) and the highest sequence the archive list reported for the thread. A thread whose
        // sequence still to be read is beyond that highest one has nothing left to read, so its parsed position
        // can not move by waiting - it stops holding back the low-water-mark instead of being waited for. The
        // release is only done on positive evidence (a pass that returned rows and no error); the safe direction
        // - a thread that has work - is always taken, so a failing source can keep a thread in the water-mark but
        // can never release one.
        void updateThreadWork();
        // Index of the archive with the lowest SCN still to be parsed in archiveRedoQueue
        [[nodiscard]] size_t bestArchiveIndex() const;
        void updateOnlineLogs() const;
        void readerDropAll();
        static Seq getSequenceFromFileName(const Replicator* replicator, const std::string& file, uint16_t& thread);
        virtual std::string getModeName() const;
        virtual bool checkConnection();
        virtual bool continueWithOnline();
        virtual void verifySchema(Scn currentScn);
        virtual void createSchema();
        virtual void updateOnlineRedoLogData();
        // Loads the incarnation list from the database. Needed on the archives-only / RAC path as well: online
        // redo log data is not read there, but the resetlogs of the current incarnation is still required to
        // locate the starting redo sequence, otherwise no checkpoint file has to be prepared by hand.
        virtual void loadIncarnations();

    public:
        Replicator(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                   TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase);
        ~Replicator() override;

        // Every archived redo log returned by the archive list of the source connection is reported here: keeps
        // the highest sequence per thread and the moment it moved last (the input of the activity check below)
        void observeArchiveSequence(uint16_t thread, Seq sequence, uint64_t currentTime);
        // Seconds since the highest archived sequence of the thread moved last, 0 when the thread is unknown or
        // did not move since the last observation
        [[nodiscard]] uint64_t threadInactiveSec(uint16_t thread, uint64_t currentTime) const;
        // Highest archived sequence seen for the thread, Seq::none() while the thread is unknown
        [[nodiscard]] Seq getThreadMaxSequence(uint16_t thread) const;

        // What the last archive-list pass did, for diagnostics (60056). threadMaxSequence is only updated from
        // rows the list returned, so a pass that returns nothing leaves it frozen while the source has newer
        // archives listed - the log then says "no archived redo log to read" without telling why. These five
        // numbers are what tells the three cases apart: the bind values were rejected (rows=0, err set), the
        // resetlogs id does not match (rows=0, err empty), or the query itself failed (rows=0, err set).
        Seq archListStart;
        uint64_t archListResetlogs{0};
        uint64_t archListRows{0};
        Seq archListMinSeq;
        Seq archListMaxSeq;
        std::string archListError;

        virtual void initialize();
        virtual void positionReader();
        virtual void loadDatabaseMetadata();
        void run() override;
        virtual Reader* readerCreate(int group);
        void checkOnlineRedoLogs();
        virtual void goStandby();
        void addPathMapping(std::string source, std::string target);
        void addRedoLogsBatch(std::string path);
        static void archGetLogPath(Replicator* replicator);
        static void archGetLogList(Replicator* replicator);
        void applyMapping(std::string& path) const;
        void updateResetlogs();
        void wakeUp() override;
        void printStartMsg() const;
        bool processArchivedRedoLogs();
        bool processOnlineRedoLogs();

        friend class OpenLogReplicator;
        friend class ReplicatorOnline;

        std::string getName() const override {
            return {"Replicator: " + alias};
        }
    };
}

#endif
