/* Header for TransactionBuffer class
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

#ifndef TRANSACTION_BUFFER_H_
#define TRANSACTION_BUFFER_H_

#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "../common/Ctx.h"
#include "../common/LobKey.h"
#include "../common/RedoLogRecord.h"
#include "../common/types/FileOffset.h"
#include "../common/types/Types.h"
#include "../common/types/Seq.h"
#include "../common/types/Xid.h"

namespace OpenLogReplicator {
    class Transaction;
    class XmlCtx;
    // Per-thread position (Metadata.h). Only forward declared here: this header is included by Metadata.h's users and
    // must not pull the metadata layer in; the definition is used in TransactionBuffer.cpp.
    struct ThreadPos;

    struct TransactionChunk {
        // The chunk is a whole pool block (Ctx::memoryChunkSize - see memory."transaction-block-kb"), the
        // header below is carved out of its beginning. The usable size is runtime, use
        // TransactionBuffer::dataBufferSize() instead of a constant.
        static constexpr uint32_t HEADER_BUFFER_SIZE = sizeof(uint64_t) + sizeof(uint32_t);

        uint64_t elements;
        uint32_t size;
        uint8_t buffer[1];
    };

    class TransactionBuffer {
    public:
        static constexpr uint32_t ROW_HEADER_OP = 0;
        static constexpr uint32_t ROW_HEADER_DATA0 = sizeof(typeOp2);
        static constexpr uint32_t ROW_HEADER_DATA1 = sizeof(typeOp2) + sizeof(RedoLogRecord);
        static constexpr uint32_t ROW_HEADER_DATA2 = sizeof(typeOp2) + sizeof(RedoLogRecord) + sizeof(RedoLogRecord);
        static constexpr uint32_t ROW_HEADER_TOTAL = sizeof(typeOp2) + sizeof(RedoLogRecord) + sizeof(RedoLogRecord) + sizeof(typeChunkSize);

    protected:
        Ctx* ctx;

        // Usable bytes of one transaction block: the whole pool block minus the chunk header. A single redo
        // log record (plus its row header) has to fit into it, otherwise 50040 is thrown - so the block size
        // ("transaction-block-kb") must stay above the longest redo log record of the database.
        [[nodiscard]] uint32_t dataBufferSize() const {
            return static_cast<uint32_t>(ctx->memoryChunkSize) - TransactionChunk::HEADER_BUFFER_SIZE;
        }

        std::mutex mtx;
        std::unordered_map<XidMap, Transaction*> xidTransactionMap;

        // Tombstones of the transactions that have committed in this run. The same redo can be read a second
        // time in one run (a slice resume, a pass that ended early, an instance recovery), and the records of a
        // transaction that has committed belong to the commit sorter - delivered already or queued - so they
        // have to be dropped here instead of waiting for their delivery (Ctx::swappedMemoryInit).
        //
        // What has to be dropped is not only the newest transaction of an undo slot: a re-parse spans thousands
        // of transactions, so by the time the redo of an older Xid is read again its undo slot has been reused
        // (and the newer transactions have committed too). Keeping one entry per slot dropped that knowledge,
        // the re-read built a second Transaction for an Xid that was already delivered and its rows were
        // delivered twice. Measured in the reproduction rig with slicing on: 179,275 rows delivered for 100,070
        // distinct row ids - 79,205 rows twice, all of thread 2 - while slicing off delivered exactly 101,000
        // rows with no duplicate. The set below is bounded by count, so it covers the re-parse window (a few
        // archives) and not the whole run.
        struct CommittedKey {
            // The same Xid value can exist on two RAC threads: their undo spaces are independent, so the thread
            // and the container are part of the identity (XidMap holds both), plus the sequence number that the
            // XidMap drops.
            XidMap xidMap;
            typeSqn sqn;

            bool operator==(const CommittedKey& other) const {
                return xidMap == other.xidMap && sqn == other.sqn;
            }
        };

        struct CommittedKeyHash {
            size_t operator()(const CommittedKey& key) const noexcept {
                return std::hash<uint64_t>()(key.xidMap ^ (static_cast<uint64_t>(key.sqn) << 17));
            }
        };

        std::unordered_set<CommittedKey, CommittedKeyHash> committedXids;
        std::deque<CommittedKey> committedXidOrder;

        // How many committed transactions are remembered. About 32 bytes per entry holds one entry per commit
        // of the last few minutes at the transaction rates this engine is built for - the redo that is read a
        // second time is a few archives, so the window has to be minutes, not hours.
        static constexpr uint64_t COMMITTED_XIDS_KEEP{2000000};

        // Transactions whose undo slot was taken over while they were still open (a slice boundary cut them, or
        // their commit simply has not been read yet - measured on a sliced run: deleting them lost 1,440 rows that
        // the same archives delivered without slicing). They cannot stay in xidTransactionMap - the slot belongs
        // to the takeover now - so they wait here under their FULL Xid (xidMap + sqn, the same identity the
        // committed tombstones use): their following records continue them and their commit delivers them. The
        // commit sorter removes the entry when the delivery is done (TransactionBuffer::dropTransaction). Bounded
        // by count: the oldest is purged when the cap is exceeded (warning 60060 - those rows are lost if the
        // commit arrives later; the cap is far above the number of concurrently open transactions of a healthy
        // run, and each entry is small: an open transaction holds one pool block at most until it grows).
        static constexpr size_t TAKEN_OVER_KEEP{1000};
        std::unordered_map<CommittedKey, Transaction*, CommittedKeyHash> takenOverTransactions;
        std::deque<CommittedKey> takenOverOrder;

        std::map<LobKey, uint8_t*> orphanedLobs;

    public:
        std::set<Xid> skipXidList;
        std::set<Xid> dumpXidList;
        std::set<XidMap> brokenXidMapList;
        std::string dumpPath;

        explicit TransactionBuffer(Ctx* newCtx);
        ~TransactionBuffer();

        // Key: thread (bits 48-63), conId (bits 32-47, conId is int16_t - always widened as unsigned 16 bit),
        // xid usn+slt (bits 0-31); RAC threads have independent undo usn spaces
        static XidMap makeXidMap(Xid xid, typeConId conId, uint16_t thread);
        void purge();
        [[nodiscard]] Transaction* findTransaction(XmlCtx* xmlCtx, Xid xid, typeConId conId, uint16_t thread, bool old, bool add, bool rollback);
        // "expected" guards against erasing an entry that meanwhile belongs to a newer transaction: with a
        // deferred commit (commit sorter) the same undo slot can be taken over before the flush happens.
        void dropTransaction(Xid xid, typeConId conId, uint16_t thread, const Transaction* expected = nullptr);
        // Park a transaction whose undo slot was taken over while it was open (see takenOverTransactions)
        void stashTakenOverTransaction(Transaction* transaction, XidMap xidMap);
        // Record that this Xid has committed: a second read of its redo is then dropped instead of waiting in
        // Ctx::swappedMemoryInit() for the delivery of the transaction it belongs to. Called where the commit
        // is handed to the commit sorter, the entry lives until the undo slot is taken over by a newer Xid.
        void addCommittedTombstone(Xid xid, typeConId conId, uint16_t thread);
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord);
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackTransactionChunk(Transaction* transaction);
        void mergeBlocks(uint8_t* mergeBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        // Oldest unfinished transaction: its position *and* its redo thread (a sequence number is only meaningful
        // inside the sequence space of its own thread - see Metadata::minThread), plus the earliest unfinished
        // transaction of *every* thread (minTrans, see Metadata::minTrans): a restart has to re-read all of them,
        // not only the oldest one, or the other transactions commit with only their post-restart rows.
        void checkpoint(Seq& minSequence, FileOffset& minFileOffset, Xid& minXid, uint16_t& minThread,
                        std::map<uint16_t, ThreadPos>& minTrans);
        void addOrphanedLob(RedoLogRecord* redoLogRecord1);
        static uint8_t* allocateLob(const RedoLogRecord* redoLogRecord1);
    };
}

#endif
