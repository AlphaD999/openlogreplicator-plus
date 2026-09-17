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
        std::string redoCopyPath;
        // Redo log files
        Reader* archReader{nullptr};
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
        // Parser used to execute the events that the commit sorter releases outside of parsing (see the idle
        // tick in Replicator.cpp). It never parses anything: the transaction buffer and the builder are
        // shared by all parsers, so it produces the same output as the parser that parsed the archive.
        Parser* flushParser{nullptr};
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

        void cleanArchList();
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
