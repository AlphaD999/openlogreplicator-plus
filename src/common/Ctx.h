/* Header for Ctx class
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

#ifndef CTX_H_
#define CTX_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <memory>
#include <mutex>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../parser/CommitSorter.h"
#include "types/LobId.h"
#include "types/Scn.h"
#include "types/Xid.h"

namespace OpenLogReplicator {
    class Clock;
    class Metrics;
    class Thread;

    class SwapChunk final {
    public:
        std::vector<uint8_t*> chunks;
        int64_t swappedMin{-1};
        int64_t swappedMax{-1};
        bool release{false};
    };

    // Key of the per transaction swap storage (Ctx::swapChunks). A bare Xid is not unique enough: in a RAC
    // cluster every instance has its own undo space (and in a CDB every container its own undo tablespace),
    // so the same usn/slt/sqn can belong to two transactions that are alive at the same time. With the Xid
    // alone as key the second transaction waited in swappedMemoryInit() for the first one to be delivered -
    // and that can never happen while the parser, the only thread that can deliver, is the one waiting.
    class SwapKey final {
    public:
        uint16_t thread{0};
        typeConId conId{0};
        Xid xid;

        bool operator==(const SwapKey& other) const {
            return thread == other.thread && conId == other.conId && xid == other.xid;
        }

        bool operator!=(const SwapKey& other) const {
            return !(*this == other);
        }

        // Suffix of the swap file of this transaction: it has to be unique per key, two live transactions
        // with the same Xid would otherwise share one file.
        [[nodiscard]] std::string fileNameSuffix() const {
            return xid.toString() + "-t" + std::to_string(thread) + "-c" + std::to_string(conId);
        }

        [[nodiscard]] std::string toString() const {
            return xid.toString() + " (thread: " + std::to_string(thread) + ", conId: " + std::to_string(conId) + ")";
        }
    };

    class SwapKeyHash final {
    public:
        size_t operator()(const SwapKey& key) const noexcept {
            return std::hash<uint64_t>()(key.xid.getData()) ^
                    (static_cast<size_t>(static_cast<uint16_t>(key.thread)) << 1) ^
                    (static_cast<size_t>(static_cast<uint16_t>(key.conId)) << 17);
        }
    };

    class Ctx final {
    public:
        enum class LOCALES : unsigned char {
            TIMESTAMP,
            MOCK
        };

        enum class LOG : unsigned char {
            SILENT,
            ERROR,
            WARNING,
            INFO,
            DEBUG
        };

        enum class MEMORY : unsigned char {
            BUILDER,
            MISC,
            PARSER,
            READER,
            TRANSACTIONS,
            WRITER
        };

        static constexpr uint MEMORY_COUNT{6};

        enum class DISABLE_CHECKS : unsigned char {
            GRANTS           = 1 << 0,
            SUPPLEMENTAL_LOG = 1 << 1,
            BLOCK_SUM        = 1 << 2,
            JSON_TAGS        = 1 << 3
        };

        enum class REDO_FLAGS : unsigned int {
            ARCH_ONLY                     = 1 << 0,
            SCHEMALESS                    = 1 << 1,
            ADAPTIVE_SCHEMA               = 1 << 2,
            DIRECT_DISABLE                = 1 << 3,
            IGNORE_DATA_ERRORS            = 1 << 4,
            SHOW_DDL                      = 1 << 5,
            SHOW_HIDDEN_COLUMNS           = 1 << 6,
            SHOW_GUARD_COLUMNS            = 1 << 7,
            SHOW_NESTED_COLUMNS           = 1 << 8,
            SHOW_UNUSED_COLUMNS           = 1 << 9,
            SHOW_INCOMPLETE_TRANSACTIONS  = 1 << 10,
            SHOW_SYSTEM_TRANSACTIONS      = 1 << 11,
            SHOW_CHECKPOINT               = 1 << 12,
            CHECKPOINT_KEEP               = 1 << 13,
            VERIFY_SCHEMA                 = 1 << 14,
            RAW_COLUMN_DATA               = 1 << 15,
            EXPERIMENTAL_XMLTYPE          = 1 << 16,
            EXPERIMENTAL_JSON             = 1 << 17,
            EXPERIMENTAL_NOT_NULL_MISSING = 1 << 18,
            // Tolerate a transaction that takes over an undo slot whose previous transaction is still
            // open in our state. That happens when replication starts mid-stream (the previous
            // transaction's commit lies before the start position), and without this flag it aborts
            // the whole redo log processing with 50039. With the flag the stale transaction is
            // dropped and recorded as broken, and the new one takes the slot.
            RECOVER_BROKEN_TRANSACTION    = 1 << 19
        };

        enum class TRACE : unsigned int {
            DML          = 1 << 0,
            DUMP         = 1 << 1,
            LOB          = 1 << 2,
            LWN          = 1 << 3,
            THREADS      = 1 << 4,
            SQL          = 1 << 5,
            FILE         = 1 << 6,
            DISK         = 1 << 7,
            PERFORMANCE  = 1 << 8,
            TRANSACTION  = 1 << 9,
            REDO         = 1 << 10,
            ARCHIVE_LIST = 1 << 11,
            SCHEMA_LIST  = 1 << 12,
            WRITER       = 1 << 13,
            CHECKPOINT   = 1 << 14,
            SYSTEM       = 1 << 15,
            LOB_DATA     = 1 << 16,
            SLEEP        = 1 << 17,
            CONDITION    = 1 << 18,
            STREAM       = 1 << 19
        };

        // Granularity of the MB based memory options (memory.min-mb, max-mb, read-buffer-*, ...): the
        // configuration is rounded down to whole MB. The byte accounting itself uses memoryChunkSize.
        static constexpr uint64_t MEMORY_CHUNK_SIZE_MB{1};
        // Size of one pool block, which is the same as the size of one transaction data block: a transaction
        // that holds at least one change owns at least one of them, so this is what a *small* transaction
        // costs. With 1 MB a low-water-mark window holding ~4000 data bearing transactions needs ~4 GB, with
        // 256 KB ~1 GB - the difference between hitting 10017 and not.
        // Configured by memory."transaction-block-kb" (see MEMORY_CHUNK_KB_*); only values that divide
        // MEMORY_CHUNK_SIZE_MB are accepted, so the chunk counts of the MB based options stay integral.
        static constexpr uint64_t MEMORY_CHUNK_KB_MIN{64};
        static constexpr uint64_t MEMORY_CHUNK_KB_MAX{1024};
        static constexpr uint64_t MEMORY_CHUNK_KB_DEFAULT{1024};
        static constexpr uint64_t MEMORY_CHUNK_MIN_MB{32};
        // Minimum length of an archive slice (see Parser::parse): a shorter piece does not release any
        // transaction memory, it only turns into scheduling overhead - slicing at every low-water-mark
        // boundary of a busy archive was measured to shred it into 512 byte pieces.
        static constexpr uint64_t SLICE_MIN_BYTES{1024 * 1024};

        static constexpr typeBlk ZERO_BLK{0xFFFFFFFF};

        static constexpr uint64_t BAD_TIMEZONE{0x7FFFFFFFFFFFFFFF};
        static constexpr int MIN_BLOCK_SIZE{512};
        static constexpr uint MEMORY_ALIGNMENT{4096};
        static constexpr uint MAX_PATH_LENGTH{2048};

        static constexpr typeCol COLUMN_LIMIT{1000};
        static constexpr typeCol COLUMN_LIMIT_23_0{4096};
        static constexpr uint JSON_PARAMETER_LENGTH{256};
        static constexpr uint JSON_TOPIC_LENGTH{256};

        static constexpr uint JSON_USERNAME_LENGTH{128};
        static constexpr uint JSON_PASSWORD_LENGTH{128};
        static constexpr uint JSON_SERVER_LENGTH{4096};
        static constexpr uint JSON_KEY_LENGTH{4096};
        static constexpr uint JSON_CONDITION_LENGTH{16384};
        static constexpr uint JSON_XID_LENGTH{32};
        static constexpr uint JSON_FORMAT_SEPARATOR_LENGTH{128};
        static constexpr uint JSON_TAG_LENGTH{4096};

        static const std::string memoryModules[MEMORY_COUNT];

        // Pool block / transaction data block size in bytes, set by initialize() before any thread starts
        // (see MEMORY_CHUNK_KB_*). Every memory accounting below is in units of this block.
        uint64_t memoryChunkSize{MEMORY_CHUNK_KB_DEFAULT * 1024};
        // How many blocks one MB is: turns a MB based option into a block count.
        uint64_t memoryChunksPerMb{1};

        // Bytes of "chunks" blocks, rounded up to whole MB. A block can be smaller than 1 MB since
        // memory."transaction-block-kb" and reporting 0 MB for a non-zero amount would be misleading.
        [[nodiscard]] uint64_t chunksToMb(uint64_t chunks) const {
            if (chunks == 0)
                return 0;
            constexpr uint64_t mb = MEMORY_CHUNK_SIZE_MB * 1024 * 1024;
            return (chunks * memoryChunkSize + mb - 1) / mb;
        }

        // Number of blocks that MEMORY_CHUNK_SIZE_MB worth of memory is: the cap of a MB based amount.
        [[nodiscard]] uint64_t mbsToChunks(uint64_t mbs) const {
            return (mbs / MEMORY_CHUNK_SIZE_MB) * memoryChunksPerMb;
        }

        uint trace{0};
        uint flags{0};
        // Set when the reader parameter "archives-only" is present in the configuration (either true or false),
        // so that RAC auto-detection can warn when it overrides an explicit value
        bool archivesOnlyConfigured{false};
        // Set when the reader parameter "arch-source" is "asm": archived redo logs stay in ASM and are reached
        // through a filesystem bridge (asmfs, NFS, ...) declared in "path-mapping". Archived redo log paths then
        // start with '+' and must not be filtered out by the SQL layer.
        bool asmArchives{false};
        // Set when the reader parameter "position-source" is "archives": the starting SCN and the starting redo
        // sequence are read through the "archives-server" connection (the instance that owns the archived redo
        // logs) instead of the main connection. Time based positioning always stays on the main connection.
        bool positionSourceArchives{false};
        // True when "position-source" was given explicitly. When it is not, the value above is decided
        // automatically: with "archives-server" configured and no time based position requested, the position is
        // taken from there as well (see the reader block of the configuration parser).
        bool positionSourceSet{false};
        // Set when the source parameter "ddl-filter" is "filtered": a DDL/schema-change message whose object is a
        // table the dictionary knows and that is not a "filter.table" target is dropped (see
        // Builder::ddlOutputAllowed). Only the DDL message is affected - the data dictionary is tracked from the
        // SYS/XDB records, not from the DDL message. Default false: every DDL is reported.
        bool ddlFilter{false};
        uint disableChecks{0};
        LOG logLevel{LOG::INFO};

    protected:
        mutable std::mutex memoryMtx;
        std::condition_variable condOutOfMemory;
        uint8_t** memoryChunks{nullptr};
        uint64_t memoryChunksMin{0};
        uint64_t memoryChunksMax{0};
        uint64_t memoryChunksSwap{0};
        uint64_t memoryChunksAllocated{0};
        uint64_t memoryChunksFree{0};
        uint64_t memoryChunksHWM{0};
        // Set when a chunk is returned after the pool itself has been released (see freeMemoryChunk): only
        // the first one is reported, the rest is silence - it happens while the process is going down.
        bool memoryPoolGoneReported{false};
        // Threads currently waiting for a free chunk. The memory manager reports a possible deadlock when
        // somebody is blocked and no chunk can be swapped out to make room.
        uint64_t memoryBlockedThreads{0};
        uint64_t memoryModulesAllocated[MEMORY_COUNT]{0, 0, 0, 0, 0, 0};

        mutable std::mutex mtx;
        std::condition_variable condMainLoop;
        std::set<Thread*> threads;
        pthread_t mainThread;
        bool outOfMemoryParser{false};
        bool bigEndian{false};

    public:
        uint64_t memoryModulesHWM[MEMORY_COUNT]{0, 0, 0, 0, 0, 0};

        Metrics* metrics{nullptr};
        Clock* clock{nullptr};
        std::string versionStr;
        std::unique_ptr<std::ofstream> dumpStream;
        int64_t dbTimezone{BAD_TIMEZONE};
        int64_t hostTimezone;
        int64_t logTimezone;

        // Memory buffers
        uint64_t memoryChunksReadBufferMax{0};
        uint64_t memoryChunksReadBufferMin{0};
        uint64_t memoryChunksUnswapBufferMin{0};
        uint64_t memoryChunksWriteBufferMax{0};
        uint64_t memoryChunksWriteBufferMin{0};

        // Disk read buffers
        uint64_t bufferSizeMax{0};
        uint64_t bufferSizeFree{0};
        uint64_t bufferSizeHWM{0};
        uint64_t suppLogSize{0};
        // Checkpoint
        uint64_t checkpointIntervalS{600};
        uint64_t checkpointIntervalMb{500};
        uint64_t checkpointKeep{100};
        uint64_t schemaForceInterval{20};
        // Reader
        uint64_t redoReadSleepUs{50000};
        uint64_t redoVerifyDelayUs{0};
        uint64_t archReadSleepUs{10000000};
        uint64_t refreshIntervalUs{10000000};
        // Archive slicing: when this many finished transactions wait in the commit sorter for the low-water-mark,
        // the current archive is sliced so that the other redo threads are parsed first. One memory chunk (1 MB)
        // is held per waiting transaction, so this bounds the memory used by the transactions of one archive.
        // 0 disables slicing (every archive is parsed in one go).
        uint64_t slicePendingCommits{1024};
        // A redo thread that does not advance its parsed SCN for this many microseconds stops holding back
        // the low-water-mark, so the transaction memory kept by the other threads stays bounded while one
        // instance is idle (no log switches for hours). 0 = disabled (default): the strict global SCN order
        // is kept, at the price of an idle instance being able to exhaust "max-mb". See warning 60045.
        uint64_t idleThreadReleaseUs{0};
        // A redo thread whose highest archived redo log sequence does not move for this many microseconds is
        // treated as an instance that stopped producing redo: when one of its archives is missing it no longer
        // holds back the global low-water-mark (warning 60038). The value is derived from the archived redo log
        // list itself, never from SYS.V_$THREAD: on a physical standby V$THREAD.STATUS tells whether the standby
        // instance itself has the thread attached, so the threads of the other instances always look CLOSED
        // while they are in fact archiving. Must stay well above "archive_lag_target" plus the arrival and
        // visibility delay of the archives (900 s default). 0 = disabled: a thread is never released, only waited for.
        uint64_t threadInactiveUs{900000000};
        // Writer
        uint64_t pollIntervalUs{100000};
        uint64_t queueSize{65536};

        std::atomic<uint32_t> version{0}; // Compatibility level of redo logs
        std::atomic<uint> dumpRedoLog{0};
        std::atomic<uint> dumpRawData{0};
        uint archReadTries{10};
        // An archive whose header cannot be decoded (a file that is not a redo log at all: a stale entry of the
        // archive list, a file that was overwritten in ASM, a wrong path mapping) is otherwise fatal: the reader
        // retries archReadTries times and then stops the whole process (10009), so the operator has to restart
        // it and skip that archive by hand - and a restart without a checkpoint starts from "now", which drops
        // every archive in between. With this option the archive is skipped explicitly instead: 10074 names the
        // thread and the sequence, the process keeps running and only that archive's redo is not delivered.
        bool skipUnreadableArchive{false};

        typeCol columnLimit{COLUMN_LIMIT};

        // Transaction buffer
        std::string dumpPath{"."};
        std::string redoCopyPath;
        uint64_t stopLogSwitches{0};
        uint64_t stopCheckpoints{0};
        uint64_t stopTransactions{0};
        typeTransactionSize transactionSizeMax{0};
        std::unordered_map<LobId, Xid> lobIdToXidMap;
        Thread* parserThread{nullptr};
        // Stage 1 of the accounting decoupling: the thread the parse and the delivery path books its memory and
        // its state to. It is the parser (the replicator) - parserThread - unless a thread that runs a delivery
        // for it says otherwise: the writer sets this to itself for the duration of a defect C tick, so that
        // the two threads never write the state of the same Thread object (which is what the old, hard wired
        // parserThread did: the takeover ran Transaction::flush on the writer thread while the blocked parser
        // was writing the same fields). Relaxed on purpose: it is read on hot paths and only has to be visible
        // to the thread that wrote it.
        std::atomic<Thread*> accountThread{nullptr};

        [[nodiscard]] Thread* thread() const {
            Thread* const t = accountThread.load(std::memory_order_relaxed);
            return t != nullptr ? t : parserThread;
        }
        Thread* writerThread{nullptr};
        // Defect C: while the parser is blocked in swappedMemoryInit() it can not drive the commit sorter, and
        // the transaction it waits for is released by exactly that flush - the two wait on each other. The
        // writer thread takes a sorter tick over while this flag is set (see Builder::sleepForWriterWork); in
        // every other situation the parser keeps driving the sorter alone, so the normal path keeps the single
        // threaded order and no lock is taken.
        std::atomic<bool> parserInSwapWait{false};
        // Set while the writer is driving the sorter tick because of the flag above. Only the transition is
        // reported, so that a regression run can tell from the log whether a take-over happened at all.
        std::atomic<bool> sorterTickTakenOver{false};
        // One delivery at a time, over every path that delivers (stage 3: the writer drives the sorter as well
        // as the parser). The callbacks of a flush run outside the sorter lock and write into the builder, so two
        // deliveries in parallel would also step over the accounting thread below and over the deferred releases.
        // Taken by CommitSorter::flushReady/flushAll/tick around the whole delivery.
        std::mutex deliveryMtx;
        // Stage 2 of the accounting decoupling: the delivery that runs on another thread than the parser (the
        // writer, defect C) must not be interrupted by the parser it releases. The release of a swap key is what
        // wakes the parser out of swappedMemoryInit(); if it resumed right there it would use the builder and
        // book memory while the delivery is still doing both. While this is set, swappedMemoryKeyRelease() only
        // collects the keys, and the delivering thread runs them (releaseDeferredSwaps) after its delivery is
        // over and after the accounting thread has been handed back. Only that thread pushes and drains the
        // vector, so it needs no lock of its own.
        bool deferSwapReleases{false};
        std::vector<SwapKey> deferredSwapReleases;

        // Lightweight observability of the two costs above, for builds without metrics (the Prometheus counters
        // of the same name only exist with LINK_LIBRARY_PROMETHEUS, which the production delivery does not
        // enable): redo parsed a second time because a parse pass ended early, and the wait of a re-parse that
        // ran into a transaction that is committed but not delivered yet. Reported as a single line with every
        // checkpoint write (reportParseStats), and only when there is something to report.
        std::atomic<uint64_t> statParsePassesEarly{0};
        std::atomic<uint64_t> statBytesParsedRepeated{0};
        std::atomic<uint64_t> statDuplicateParseWaits{0};
        std::atomic<uint64_t> statDuplicateParseWaitUs{0};
        // "always" reports the line even when all four are zero (the end of a run: the zeros are the answer then).
        void reportParseStats(bool always = false);

        void releaseDeferredSwaps();
        // Executor of the out-of-parser tick. Set by the replicator (which already owns one for its own idle
        // tick): allocating a parser at the moment the pool is exhausted - exactly when the tick is needed -
        // would block on memory.
        // The executor of the commit sorter tick that runs outside the parser (defect C). Owned here on
        // purpose: it is used from the replicator thread and from a writer thread, so it has to outlive both
        // (Ctx does). Created once, at the start of Replicator::run(), before the writer can take a tick over.
        std::unique_ptr<CommitSorter::Executor> sorterExecutor{nullptr};

        // swappedMemoryInit() waits for the same transaction to be delivered. The wait stays unbounded (see the
        // comment there), but it reports itself with this interval: an unbounded silent wait is
        // indistinguishable from an idle run (warning 60050).
        static constexpr uint64_t SWAP_WAIT_REPORT_US{30000000};

        // Telemetry of an operation that is a single blocking call and can be stuck inside it: the archived redo
        // log list query on the connection that holds the list, and the open of an archive through the mount of
        // its diskgroup. Such a call logs nothing at all while it does not return, so a stalled run looks exactly
        // like an idle one (production: four threads silent for eleven minutes after the archives were already
        // listed on the standby, then everything resumed at once). The operation publishes itself here while it
        // is in flight - warning 60053 is emitted from Ctx::slowOpWatchdog() by a thread that is not the one
        // blocked - and reports its total duration when it finally leaves (warning 60054).
        // Only one such operation can be in flight at a time: they are all issued from the same loop.
        static constexpr uint64_t SLOW_OP_REPORT_US{60000000};
        // Reminder interval of warning 60053 while the operation still has not returned
        static constexpr uint64_t SLOW_OP_REMIND_US{600000000};
        static constexpr uint16_t SLOW_OP_NONE{0};
        static constexpr uint16_t SLOW_OP_ARCH_LIST{1};
        static constexpr uint16_t SLOW_OP_ARCH_OPEN{2};
        std::atomic<uint16_t> slowOpKind{SLOW_OP_NONE};
        std::atomic<uint64_t> slowOpSinceUs{0};
        std::atomic<uint64_t> slowOpReportedUs{0};
        mutable std::mutex slowOpMtx;
        std::string slowOpDetail;

        // Per transaction swap storage, keyed by (thread, conId, xid) - see SwapKey
        std::unordered_map<SwapKey, SwapChunk*, SwapKeyHash> swapChunks;
        // Keys whose swap file still has to be unlinked, each together with the entry it was queued for. The
        // pointer is the identity of that entry: while the cleanup is pending, a duplicate parse of the same Xid
        // can take the key over and build a new entry under it (Ctx::swappedMemoryInit), and the delayed cleanup
        // must not remove - and must not unlink the file of - an entry it was never queued for
        // (see MemoryManager::cleanOldTransactions).
        std::vector<std::pair<SwapKey, SwapChunk*>> commitedXids;
        std::condition_variable chunksMemoryManager;
        std::condition_variable chunksTransaction;
        uint64_t swappedMB{0};
        SwapKey swappedFlushXid;
        SwapKey swappedShrinkXid;
        mutable std::mutex swapMtx;
        std::condition_variable reusedTransactions;
        bool version12{false};
        bool hardShutdown{false};
        bool softShutdown{false};
        bool replicatorFinished{false};

        Ctx();
        ~Ctx();

        bool isTraceSet(TRACE mask) const {
            return (trace & static_cast<uint>(mask)) != 0;
        }

        bool isFlagSet(REDO_FLAGS mask) const {
            return (flags & static_cast<uint>(mask)) != 0;
        }

        bool isLogLevelAt(LOG level) const {
            return logLevel >= level;
        }

        bool isDisableChecksSet(DISABLE_CHECKS mask) const {
            return (disableChecks & static_cast<uint>(mask)) != 0;
        }

        void setBigEndian() {
            bigEndian = true;
        }

        [[nodiscard]] bool isBigEndian() const {
            return bigEndian;
        }

        uint16_t read16(const uint8_t* buf) const {
            if (bigEndian)
                return read16Big(buf);
            return read16Little(buf);
        }

        uint32_t read32(const uint8_t* buf) const {
            if (bigEndian)
                return read32Big(buf);
            return read32Little(buf);
        }

        uint64_t read56(const uint8_t* buf) const {
            if (bigEndian)
                return read56Big(buf);
            return read56Little(buf);
        }

        uint64_t read64(const uint8_t* buf) const {
            if (bigEndian)
                return read64Big(buf);
            return read64Little(buf);
        }

        Scn readScn(const uint8_t* buf) const {
            if (bigEndian)
                return readScnBig(buf);
            return readScnLittle(buf);
        }

        Scn readScnR(const uint8_t* buf) const {
            if (bigEndian)
                return readScnRBig(buf);
            return readScnRLittle(buf);
        }

        void write16(uint8_t* buf, uint16_t val) const {
            if (bigEndian)
                write16Big(buf, val);
            else
                write16Little(buf, val);
        }

        void write32(uint8_t* buf, uint32_t val) const {
            if (bigEndian)
                write32Big(buf, val);
            else
                write32Little(buf, val);
        }

        void write56(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write56Big(buf, val);
            else
                write56Little(buf, val);
        }

        void write64(uint8_t* buf, uint64_t val) const {
            if (bigEndian)
                write64Big(buf, val);
            else
                write64Little(buf, val);
        }

        void writeScn(uint8_t* buf, Scn val) const {
            if (bigEndian)
                writeScnBig(buf, val);
            else
                writeScnLittle(buf, val);
        }

        static uint16_t read16Little(const uint8_t* buf) {
            return static_cast<uint16_t>(buf[0]) |
                    (static_cast<uint16_t>(buf[1]) << 8);
        }

        static uint16_t read16Big(const uint8_t* buf) {
            return (static_cast<uint16_t>(buf[0]) << 8) |
                    static_cast<uint16_t>(buf[1]);
        }

        static uint32_t read24Big(const uint8_t* buf) {
            return (static_cast<uint32_t>(buf[0]) << 16) |
                    (static_cast<uint32_t>(buf[1]) << 8) |
                    static_cast<uint32_t>(buf[2]);
        }

        static uint32_t read32Little(const uint8_t* buf) {
            return static_cast<uint32_t>(buf[0]) |
                    (static_cast<uint32_t>(buf[1]) << 8) |
                    (static_cast<uint32_t>(buf[2]) << 16) |
                    (static_cast<uint32_t>(buf[3]) << 24);
        }

        static uint32_t read32Big(const uint8_t* buf) {
            return (static_cast<uint32_t>(buf[0]) << 24) |
                    (static_cast<uint32_t>(buf[1]) << 16) |
                    (static_cast<uint32_t>(buf[2]) << 8) |
                    static_cast<uint32_t>(buf[3]);
        }

        static uint64_t read56Little(const uint8_t* buf) {
            return static_cast<uint64_t>(buf[0]) |
                    (static_cast<uint64_t>(buf[1]) << 8) |
                    (static_cast<uint64_t>(buf[2]) << 16) |
                    (static_cast<uint64_t>(buf[3]) << 24) |
                    (static_cast<uint64_t>(buf[4]) << 32) |
                    (static_cast<uint64_t>(buf[5]) << 40) |
                    (static_cast<uint64_t>(buf[6]) << 48);
        }

        static uint64_t read56Big(const uint8_t* buf) {
            return (static_cast<uint64_t>(buf[0]) << 24) |
                    (static_cast<uint64_t>(buf[1]) << 16) |
                    (static_cast<uint64_t>(buf[2]) << 8) |
                    (static_cast<uint64_t>(buf[3])) |
                    (static_cast<uint64_t>(buf[4]) << 40) |
                    (static_cast<uint64_t>(buf[5]) << 32) |
                    (static_cast<uint64_t>(buf[6]) << 48);
        }

        static uint64_t read64Little(const uint8_t* buf) {
            return static_cast<uint64_t>(buf[0]) |
                    (static_cast<uint64_t>(buf[1]) << 8) |
                    (static_cast<uint64_t>(buf[2]) << 16) |
                    (static_cast<uint64_t>(buf[3]) << 24) |
                    (static_cast<uint64_t>(buf[4]) << 32) |
                    (static_cast<uint64_t>(buf[5]) << 40) |
                    (static_cast<uint64_t>(buf[6]) << 48) |
                    (static_cast<uint64_t>(buf[7]) << 56);
        }

        static uint64_t read64Big(const uint8_t* buf) {
            return (static_cast<uint64_t>(buf[0]) << 56) |
                    (static_cast<uint64_t>(buf[1]) << 48) |
                    (static_cast<uint64_t>(buf[2]) << 40) |
                    (static_cast<uint64_t>(buf[3]) << 32) |
                    (static_cast<uint64_t>(buf[4]) << 24) |
                    (static_cast<uint64_t>(buf[5]) << 16) |
                    (static_cast<uint64_t>(buf[6]) << 8) |
                    static_cast<uint64_t>(buf[7]);
        }

        static Scn readScnLittle(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[5] & 0x80) == 0x80)
                return Scn(buf[0], buf[1], buf[2], buf[3], buf[6], buf[7], buf[4], buf[5] & 0x7F);
            return Scn(buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]);
        }

        static Scn readScnBig(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[4] & 0x80) == 0x80)
                return Scn(buf[3], buf[2], buf[1], buf[0], buf[7], buf[6], buf[5], buf[4] & 0x7F);
            return Scn(buf[3], buf[2], buf[1], buf[0], buf[5], buf[4]);
        }

        static Scn readScnRLittle(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[1] & 0x80) == 0x80)
                return Scn(buf[2], buf[3], buf[4], buf[5], 0, 0, buf[0], buf[1] & 0x7F);
            return Scn(buf[2], buf[3], buf[4], buf[5], buf[0], buf[1]);
        }

        static Scn readScnRBig(const uint8_t* buf) {
            if (buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF && buf[4] == 0xFF && buf[5] == 0xFF)
                return Scn::none();
            if ((buf[0] & 0x80) == 0x80)
                return Scn(buf[5], buf[4], buf[3], buf[2], 0, 0, buf[1], buf[0] & 0x7F);
            return Scn(buf[5], buf[4], buf[3], buf[2], buf[1], buf[0]);
        }

        static void write16Little(uint8_t* buf, uint16_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
        }

        static void write16Big(uint8_t* buf, uint16_t val) {
            buf[0] = (val >> 8) & 0xFF;
            buf[1] = val & 0xFF;
        }

        static void write32Little(uint8_t* buf, uint32_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
        }

        static void write32Big(uint8_t* buf, uint32_t val) {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
        }

        static void write56Little(uint8_t* buf, uint64_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        static void write56Big(uint8_t* buf, uint64_t val) {
            buf[0] = (val >> 24) & 0xFF;
            buf[1] = (val >> 16) & 0xFF;
            buf[2] = (val >> 8) & 0xFF;
            buf[3] = val & 0xFF;
            buf[4] = (val >> 40) & 0xFF;
            buf[5] = (val >> 32) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
        }

        static void write64Little(uint8_t* buf, uint64_t val) {
            buf[0] = val & 0xFF;
            buf[1] = (val >> 8) & 0xFF;
            buf[2] = (val >> 16) & 0xFF;
            buf[3] = (val >> 24) & 0xFF;
            buf[4] = (val >> 32) & 0xFF;
            buf[5] = (val >> 40) & 0xFF;
            buf[6] = (val >> 48) & 0xFF;
            buf[7] = (val >> 56) & 0xFF;
        }

        static void write64Big(uint8_t* buf, uint64_t val) {
            buf[0] = (val >> 56) & 0xFF;
            buf[1] = (val >> 48) & 0xFF;
            buf[2] = (val >> 40) & 0xFF;
            buf[3] = (val >> 32) & 0xFF;
            buf[4] = (val >> 24) & 0xFF;
            buf[5] = (val >> 16) & 0xFF;
            buf[6] = (val >> 8) & 0xFF;
            buf[7] = val & 0xFF;
        }

        static void writeScnLittle(uint8_t* buf, Scn val) {
            if (val.getData() < 0x800000000000) {
                buf[0] = val.getData() & 0xFF;
                buf[1] = (val.getData() >> 8) & 0xFF;
                buf[2] = (val.getData() >> 16) & 0xFF;
                buf[3] = (val.getData() >> 24) & 0xFF;
                buf[4] = (val.getData() >> 32) & 0xFF;
                buf[5] = (val.getData() >> 40) & 0xFF;
            } else {
                buf[0] = val.getData() & 0xFF;
                buf[1] = (val.getData() >> 8) & 0xFF;
                buf[2] = (val.getData() >> 16) & 0xFF;
                buf[3] = (val.getData() >> 24) & 0xFF;
                buf[4] = (val.getData() >> 48) & 0xFF;
                buf[5] = ((val.getData() >> 56) & 0x7F) | 0x80;
                buf[6] = (val.getData() >> 32) & 0xFF;
                buf[7] = (val.getData() >> 40) & 0xFF;
            }
        }

        static void writeScnBig(uint8_t* buf, Scn val) {
            if (val.getData() < 0x800000000000) {
                buf[0] = (val.getData() >> 24) & 0xFF;
                buf[1] = (val.getData() >> 16) & 0xFF;
                buf[2] = (val.getData() >> 8) & 0xFF;
                buf[3] = val.getData() & 0xFF;
                buf[4] = (val.getData() >> 40) & 0xFF;
                buf[5] = (val.getData() >> 32) & 0xFF;
            } else {
                buf[0] = (val.getData() >> 24) & 0xFF;
                buf[1] = (val.getData() >> 16) & 0xFF;
                buf[2] = (val.getData() >> 8) & 0xFF;
                buf[3] = val.getData() & 0xFF;
                buf[4] = ((val.getData() >> 56) & 0x7F) | 0x80;
                buf[5] = (val.getData() >> 48) & 0xFF;
                buf[6] = (val.getData() >> 40) & 0xFF;
                buf[7] = (val.getData() >> 32) & 0xFF;
            }
        }

        void logTrace(TRACE mask, const std::string& message) const {
            if (unlikely((trace & static_cast<uint>(mask)) != 0)) {
                logTraceInt(mask, message);
            }
        }

        void assertDebug(bool condition) {
            if constexpr (CTXASSERT == 1) {
                if (unlikely(!condition)) {
                    printStacktrace();
                    throw std::runtime_error("Assertion failed");
                }
            }
        }

        static void checkJsonFields(const std::string& fileName, const rapidjson::Value& value, const std::vector<std::string>& names);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint16_t getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int16_t getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint32_t getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int32_t getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint64_t getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int64_t getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static uint getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static int getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field);
        [[nodiscard]] static std::string getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field);

        [[nodiscard]] static const rapidjson::Value& getJsonFieldA(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint16_t getJsonFieldU16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int16_t getJsonFieldI16(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint32_t getJsonFieldU32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int32_t getJsonFieldI32(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint64_t getJsonFieldU64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int64_t getJsonFieldI64(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static uint getJsonFieldU(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static int getJsonFieldI(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static const rapidjson::Value& getJsonFieldO(const std::string& fileName, const rapidjson::Value& value, const char* field, uint num);
        [[nodiscard]] static std::string getJsonFieldS(const std::string& fileName, uint maxLength, const rapidjson::Value& value, const char* field,
                                                       uint num);

        void initialize(uint64_t memoryChunkKb, uint64_t memoryMinMb, uint64_t memoryMaxMb, uint64_t memoryReadBufferMaxMb, uint64_t memoryReadBufferMinMb,
                        uint64_t memorySwapMb, uint64_t memoryUnswapBufferMinMb, uint64_t memoryWriteBufferMaxMb, uint64_t memoryWriteBufferMinMb);
        void wakeAllOutOfMemory();
        [[nodiscard]] bool nothingToSwap(Thread* t) const;
        [[nodiscard]] uint64_t getMemoryHWM() const;
        [[nodiscard]] uint64_t getAllocatedMemory() const;
        [[nodiscard]] uint64_t getSwapMemory(Thread* t) const;
        [[nodiscard]] uint64_t getFreeMemory(Thread* t) const;
        [[nodiscard]] uint64_t getMaxMemory() const;
        [[nodiscard]] uint64_t getBlockedThreads() const;
        [[nodiscard]] uint64_t getReservedFreeChunks() const;
        [[nodiscard]] std::string getMemoryModulesText() const;
        [[nodiscard]] uint8_t* getMemoryChunk(Thread* t, MEMORY module, bool swap = false);
        void freeMemoryChunk(Thread* t, MEMORY module, uint8_t* chunk);
        void swappedMemoryInit(Thread* t, const SwapKey& swapKey);
        [[nodiscard]] uint64_t swappedMemorySize(Thread* t, const SwapKey& swapKey) const;
        [[nodiscard]] uint8_t* swappedMemoryGet(Thread* t, const SwapKey& swapKey, int64_t index);
        void swappedMemoryRelease(Thread* t, const SwapKey& swapKey, int64_t index);
        [[nodiscard]] uint8_t* swappedMemoryGrow(Thread* t, const SwapKey& swapKey);
        [[nodiscard]] uint8_t* swappedMemoryShrink(Thread* t, const SwapKey& swapKey);
        void swappedMemoryFlush(Thread* t, const SwapKey& swapKey);
        void swappedMemoryRemove(Thread* t, const SwapKey& swapKey);
        // Drop the swap key of a transaction that has been delivered. It has to happen right there: the entry is
        // what swappedMemoryInit() waits for, so leaving it behind makes a duplicate parse of the same Xid (slice
        // boundary, instance recovery) wait for a delivery that already happened - warning 60050, and nobody can
        // break that wait, because the release would have to come from the transaction that is gone (defect C).
        void swappedMemoryKeyRelease(const SwapKey& swapKey);
        void wontSwap(Thread* t, uint64_t openTransactions, uint64_t swappableTransactions) const;

        void stopHard();
        void stopSoft();
        void mainLoop();
        void mainFinish();
        void printStacktrace();
        void signalHandler(int s);

        bool wakeThreads();
        void spawnThread(Thread* t);
        void finishThread(Thread* t);
        void signalDump() const;
        void usleepInt(uint64_t usec) const;

        void welcome(const std::string& message) const;
        void hint(const std::string& message) const;
        void error(int code, const std::string& message) const;
        void warning(int code, const std::string& message) const;
        void info(int code, const std::string& message) const;
        void debug(int code, const std::string& message) const;
        void logTraceInt(TRACE mask, const std::string& message) const;
        void printMemoryUsageHWM() const;
        void printMemoryUsageCurrent() const;

        void slowOpBegin(uint16_t kind, const std::string& detail);
        void slowOpEnd(uint16_t kind);
        // Called from a thread that is not the one that issued the operation (the checkpoint thread): reports
        // warning 60053 while it is still in flight, so the stall is visible in the log while it happens.
        void slowOpWatchdog();
        static const char* slowOpName(uint16_t kind);

        // RAII: publishes the operation while it is in flight and reports its duration on every path out,
        // including an exception - an error must not leave a stale "in flight" record behind.
        class SlowOp {
            Ctx* ctx;
            uint16_t kind;

        public:
            SlowOp(Ctx* newCtx, uint16_t newKind, const std::string& detail): ctx(newCtx), kind(newKind) {
                ctx->slowOpBegin(kind, detail);
            }

            ~SlowOp() {
                ctx->slowOpEnd(kind);
            }

            SlowOp(const SlowOp&) = delete;
            SlowOp& operator=(const SlowOp&) = delete;
        };
    };
}

#ifndef GLOBALS
extern OpenLogReplicator::Ctx::LOCALES OLR_LOCALES;
#endif

#endif
