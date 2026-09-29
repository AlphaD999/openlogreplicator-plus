/* Header for Parser class
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

#ifndef PARSER_H_
#define PARSER_H_

#include <cstddef>

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../reader/Reader.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"
#include "CommitSorter.h"

namespace OpenLogReplicator {
    class Builder;
    class Metadata;
    class Transaction;
    class TransactionBuffer;
    class XmlCtx;

    struct LwnMember {
        uint16_t pageOffset;
        Scn scn;
        uint32_t size;
        typeBlk block;
        typeSubScn subScn;

        bool operator<(const LwnMember& other) const {
            if (scn < other.scn)
                return true;
            if (other.scn < scn)
                return false;
            if (subScn < other.subScn)
                return true;
            if (other.subScn < subScn)
                return false;
            if (block < other.block)
                return true;
            if (block > other.block)
                return false;
            return (pageOffset < other.pageOffset);
        }
    };

    class Parser final : public CommitSorter::Executor {
    protected:
        // One chunk per MEMORY_CHUNK_SIZE_MB: the "512*2 MB" budget of the LWN assembly buffers. The array
        // below is reserved for the smallest allowed block (a smaller block needs proportionally more chunks
        // to hold the same amount of data), the effective limit is maxLwnChunks().
        static constexpr uint64_t MAX_LWN_CHUNKS = static_cast<uint64_t>(512 * 2) / Ctx::MEMORY_CHUNK_SIZE_MB;
        static constexpr uint64_t MAX_RECORDS_IN_LWN = 1048576;

        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        RedoLogRecord zero;
        Transaction* lastTransaction{nullptr};

        uint8_t* lwnChunks[MAX_LWN_CHUNKS * (Ctx::MEMORY_CHUNK_KB_MAX / Ctx::MEMORY_CHUNK_KB_MIN)]{};
        LwnMember* lwnMembers[MAX_RECORDS_IN_LWN + 1]{};
        uint64_t lwnAllocated{0};
        uint64_t lwnAllocatedMax{0};
        Time lwnTimestamp{0};
        Scn lwnScn;
        typeBlk lwnCheckpointBlock{0};

        // Chunks covered by the "512*2 MB" LWN budget at the configured block size (memory."transaction-block-kb").
        [[nodiscard]] uint64_t maxLwnChunks() const {
            return MAX_LWN_CHUNKS * ctx->memoryChunksPerMb;
        }

        void freeLwn();
        void analyzeLwn(LwnMember* lwnMember);
        void appendToTransactionDdl(RedoLogRecord* redoLogRecord1);
        void appendToTransactionBegin(RedoLogRecord* redoLogRecord1);
        void appendToTransactionCommit(RedoLogRecord* redoLogRecord1);
        void appendToTransactionLob(RedoLogRecord* redoLogRecord1);
        void appendToTransactionIndex(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransaction(RedoLogRecord* redoLogRecord1);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1);
        void appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void dumpRedoVector(const uint8_t* data, typeSize recordSize) const;
        void executeCommit(Transaction* transaction) override;
        void executeCheckpoint(const CheckpointEvent& event) override;

    public:
        int group;
        std::string path;
        uint16_t thread{1};
        Seq sequence;
        Scn firstScn{Scn::none()};
        Scn nextScn{Scn::none()};
        Reader* reader{nullptr};

        // Archive slicing (low-water-mark control with more than one redo thread): when set, the archive was
        // only parsed up to "sliceScn", the position was saved in the metadata and the scheduler has to keep
        // this parser to resume it later. See Replicator::processArchivedRedoLogs.
        bool slicePending{false};
        Scn sliceScn{Scn::none()};

        Parser(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer, int newGroup, std::string newPath);
        ~Parser();

        // SCN still to be parsed from this archive: orders the archive list so that the redo threads stay
        // aligned and the low-water-mark gap (and with it the transaction memory kept by the commit sorter)
        // stays small
        [[nodiscard]] Scn orderScn() const {
            return sliceScn != Scn::none() ? sliceScn : firstScn;
        }

        Reader::REDO_CODE parse();
        [[nodiscard]] std::string toString() const;
    };
}

#endif
