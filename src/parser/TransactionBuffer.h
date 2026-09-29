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

#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

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
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord);
        void addTransactionChunk(Transaction* transaction, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void rollbackTransactionChunk(Transaction* transaction);
        void mergeBlocks(uint8_t* mergeBuffer, RedoLogRecord* redoLogRecord1, const RedoLogRecord* redoLogRecord2);
        void checkpoint(Seq& minSequence, FileOffset& minFileOffset, Xid& minXid);
        void addOrphanedLob(RedoLogRecord* redoLogRecord1);
        static uint8_t* allocateLob(const RedoLogRecord* redoLogRecord1);
    };
}

#endif
