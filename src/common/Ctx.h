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

#include <array>
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
        // Report a source that hands out no archive at all after this many microseconds of waiting. The empty
        // list is the one silent case: 60027/60038 need a later archive of the same thread in the list to fire,
        // so a source that returns nothing at all used to look like an idle run (only TRACE::ARCHIVE_LIST said a
        // word - production waited 21 minutes for the next archive without a single warning). 0 disables it.
        // The report repeats with a doubling interval, capped at eight times this value.
        uint64_t archWaitWarnUs{60000000};

        // Parser progress watchdog (diagnostic, off by default): "watchdog-us" > 0 (or the environment variable
        // OLR_WATCHDOG in seconds, which wins) makes a thread that is idle report a parser that did not advance
        // for that long. The report carries the position of the parser and a try_lock probe of the locks that
        // can hold it: a stall inside Parser::parse() used to be completely silent - no trace line, no warning -
        // while the delivery kept running, so it was indistinguishable from an idle run (measured in
        // production: 1,020 s with zero parse progress and zero messages).
        uint64_t watchdogUs{0};
        std::atomic<uint64_t> watchdogProgressUt{0};
        std::atomic<uint64_t> watchdogWarnUt{0};
        std::atomic<uint64_t> watchdogWarnCount{0};
        std::atomic<uint16_t> watchdogThread{0};
        std::atomic<uint64_t> watchdogOffset{0};
        std::atomic<uint64_t> watchdogPending{0};
        // Called from the parse loop and from the places that wait for archives: an expected wait must not look
        // like a stall. Cheap enough to be called per redo block while the watchdog is enabled. (Defined in
        // Ctx.cpp: this header only forward declares Clock.)
        void watchdogNote(uint16_t thread, uint64_t fileOffset, uint64_t pendingCommits);
        // Refresh only the timestamp (a wait that is expected, e.g. no archive to read: 60056 reports it)
        void watchdogNoteIdle();
        // Report a parser without progress (warning 60055); "extraBusy" is what the caller's own lock probe
        // found, the locks of this class are probed here.
        void watchdogCheck(const std::string& extraBusy);
        // Archive slicing: when this many finished transactions wait in the commit sorter for the low-water-mark,
        // the current archive is sliced so that the other redo threads are parsed first. One memory chunk (1 MB)
        // is held per waiting transaction, so this bounds the memory used by the transactions of one archive.
        // 0 disables slicing (every archive is parsed in one go).
        uint64_t slicePendingCommits{1024};
        // g4 credit: how many commits of one redo thread may wait in the commit sorter before its parser stops at
        // the next record head and lets the delivery catch up. The chunk bound next to it can be bypassed by the
        // swap pump - a transaction that has been written out reports a single chunk - so 59,000 buffered commits
        // still fit under half a pool (production 2026-10-23: one thread produced 18,211 commits in 10 seconds,
        // 59,048 were buffered in total, swap entries 18,245, the parse fell from 162 MB/s to 1.6 MB/s).
        // 0 = adapt automatically (AIMD between "CREDIT_MIN" and "CREDIT_MAX"), a large value = off.
        uint64_t pendingCommitsPerThread{4096};
        // g4 stall yield: when the archive the parser is reading delivers no new data for this many microseconds,
        // the parser yields at the next verified record head (the existing slice path: the position is saved and
        // the other redo threads are parsed first). One archive that is still being written - or read slowly
        // through the mount - otherwise stops *all* parsing, because the parse loop is a single thread.
        // 0 disables it (every archive is parsed to its end, or to its next backpressure stop, in one pass).
        uint64_t stallYieldUs{5000000};
        // TEST HOOK (\"source.debug-hold-scn\", 0 = off): pins the low-water-mark of the commit sorter at a fixed SCN, so
        // that every commit above it waits in the sorter - the shape of 2026-10-23 (one redo thread crawling while the
        // others produce) without needing an artificially slow archive, which no rig can produce. It is the only way
        // to exercise the credit (warning 60064) on purpose; the startup line says so in capitals and it must never
        // be set in a production configuration.
        uint64_t debugHoldScn{0};
        // A redo thread that does not advance its parsed SCN for this many microseconds stops holding back
        // the low-water-mark, so the transaction memory kept by the other threads stays bounded while one
        // instance is idle (no log switches for hours). 0 = disabled (default): the strict global SCN order
        // is kept, at the price of an idle instance being able to exhaust "max-mb". See warning 60045.
        uint64_t idleThreadReleaseUs{0};
        // Upper bound, in archives, for the rewind to the oldest unfinished transaction of one redo thread (see
        // SerializerJson: "min-trans"). Re-reading that window is what makes a transaction that was open at shutdown
        // deliver all of its rows - but it also delivers again everything that was already written from that window,
        // and the window is as long as the transaction was open. 0 = no limit (the default: completeness first).
        // A non-zero value trades completeness for a bounded duplicate burst: warning 60063 names the thread, the
        // span and the limit whenever a rewind is skipped because of it.
        uint64_t minTransRewindMaxArchives{0};
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
        // its state to. It is the parser (the replicator) - parserThread - unless the thread that runs a delivery
        // says otherwise: a delivery books everything it releases to itself.
        // It is thread local, and that is the whole point of stage 4: since the delivery runs on its own thread
        // while the parser keeps parsing, a single pointer would make the parser book its own memory and state to
        // the delivery thread (which is what a global atomic here did - the parser read it while the delivery
        // thread had it set). Every thread that delivers sets it for the duration of its delivery
        // (CommitSorter::DeliveryScope), the delivery thread keeps it set for its whole life.
        static thread_local Thread* accountThread;

        [[nodiscard]] Thread* thread() const {
            return accountThread != nullptr ? accountThread : parserThread;
        }

        // Size of the transaction pool in chunks (0 while the pool is not initialized). Read by CommitSorter to
        // derive the backpressure bound: every buffered commit owns at least one chunk of that pool, so a backlog
        // larger than the pool can hold can not be relieved by the swap pump any more. Written once, by
        // Ctx::initialize(), before any thread is spawned - hence the plain read.
        [[nodiscard]] uint64_t getMemoryChunksMax() const {
            return memoryChunksMax;
        }
        Thread* writerThread{nullptr};
        // Defect C: while the parser is blocked in swappedMemoryInit() it publishes no higher water-mark (it is
        // the only thread that does), and the transaction it waits for is released by a delivery that only a
        // higher water-mark would allow - the two wait on each other. The delivery thread watches this flag and
        // delivers the lowest buffered event without waiting for the water-mark while it is set
        // (CommitSorter::tick with flushLowest, see DeliveryExecutor::run).
        std::atomic<bool> parserInSwapWait{false};
        // The same escape for the other way a parser can be blocked: stopped by the per-thread backpressure limit
        // (CommitSorter::waitForBacklog) for longer than BACKPRESSURE_EMERGENCY_US while the delivery has nothing it
        // may flush below the low-water-mark. Set by the waiting parser, cleared when it leaves the wait; the delivery
        // thread treats it like parserInSwapWait and takes the lowest buffered event without the water-mark.
        std::atomic<bool> parserInBacklogWait{false};
        // One delivery at a time, over every path that delivers (stage 4: the delivery thread, and the emergency
        // tick of the writer). The callbacks of a flush run outside the sorter lock and write into the builder, so
        // two deliveries in parallel would also step over the accounting thread below and over the deferred
        // releases. Taken by CommitSorter::flushReady/flushAll/tick around the whole delivery.
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
        // Swap entries that were already gone when the parser asked for them: the same Xid was parsed a second
        // time, took the swap key over, and the delayed cleanup of the previous owner removed the entry right
        // afterwards (defect C). Reported as warning 60057 and counted here, so a run tells "it never happened"
        // from "it happened and was survived" - the same shape as swappedMemoryGrow does it with 60051.
        std::atomic<uint64_t> statSwapEntryMissing{0};
        // Records of a transaction that has already committed, dropped because their redo was read a second
        // time (the tombstone in TransactionBuffer). Counted so that a run tells "the redo was never read
        // twice" from "it was, and the second read was dropped instead of delivering the rows again".
        std::atomic<uint64_t> statCommittedRereadDrops{0};
        // Passes whose saved position turned out not to be the head of an LWN record: they restart the archive
        // from its beginning instead of ending the run (warning 60058). Counted to see how often the saved
        // position is not usable - it is a cost (the archive is read again), not a fault.
        std::atomic<uint64_t> statResumeFallbacks{0};
        // Counters of the silent wait of the delivery: a transaction that was swapped out is read back chunk by
        // chunk (Ctx::swappedMemoryGet), and that wait has no trace line and no warning of its own - which is why
        // the production stall of 2026-10-15 looked like a lock problem (60055 reported "deliveryMtx swapMtx" and
        // nothing else). A single wait long enough to be the reason for a stall is reported by warning 60061, the
        // totals are named by warning 60055.
        std::atomic<uint64_t> statSwapGetWaits{0};
        std::atomic<uint64_t> statSwapGetWaitUs{0};
        // Set by the delivery thread while it is inside a sorter tick, so that a "no progress" report of the parser
        // can tell "blocked by a running delivery" from "blocked by something else".
        std::atomic<uint64_t> deliveryActiveSinceUs{0};
        // Set by the replicator while the parser has no archive to read at all (the archive list returned nothing):
        // no progress is the expected state then and warning 60056 is what reports it - the watchdog (60055) stays
        // quiet, otherwise a run that simply waits for the next archive of a quiet instance looks like a stall.
        std::atomic<bool> parserWaitingForArchive{false};
        // "always" reports the line even when all four are zero (the end of a run: the zeros are the answer then).
        void reportParseStats(bool always = false);
        // One swap entry disappeared while the parser was still using it: counted and reported as warning 60057
        void swappedEntryMissing(const char* stage, const SwapKey& swapKey);

        void releaseDeferredSwaps();
        // Stage 4 puts the delivery on a thread of its own (DeliveryExecutor). That thread is owned by the
        // replicator that runs it (Replicator::deliveryExecutor) and not here: Ctx.h can not include
        // DeliveryExecutor.h (it derives from Thread, and Thread.h includes Ctx.h), and a std::unique_ptr of an
        // incomplete type as a member of Ctx does not compile in the translation units that only see Ctx.h
        // (measured: "invalid application of sizeof to incomplete type" from every TU that instantiates the
        // destructor of the member).

        // swappedMemoryInit() waits for the same transaction to be delivered. The wait stays unbounded (see the
        // comment there), but it reports itself with this interval: an unbounded silent wait is
        // indistinguishable from an idle run (warning 60050).
        static constexpr uint64_t SWAP_WAIT_REPORT_US{30000000};

        // Number of commits the sorter has buffered but not delivered yet, mirrored from the sorter by
        // CommitSorter::getPendingEvents(). Warning 60050 used to report commitedXids.size() as "committed and
        // waiting for delivery", which is a different thing (swap entries waiting for their file to be removed)
        // and sent the analysis of the production stalls in the wrong direction. Read in the 60050 message only.
        std::atomic<uint64_t> statPendingDeliveries{0};

        // Layered view of where the undelivered work sits, mirrored from the sorter by
        // CommitSorter::syncPendingEvents() so that Ctx.h does not have to know CommitSorter. A single "pending
        // commits" number can not tell "parsed but not delivered" (sorter events) from "swapped out and not read
        // back yet" (swapChunks) from "delivered, waiting for the swap file to be removed" (commitedXids).
        std::atomic<uint64_t> statSorterEvents{0};
        std::atomic<uint64_t> statSorterScns{0};
        // g5: events that have left the sorter but whose commit has not reached the writer yet. They are counted in
        // sorter_pending as well (the credit counts them, see CommitSorter::flushUpTo): before g5 they were taken
        // out of the accounting when the flush *collected* them, so a writer that could not send made the backlog
        // disappear from every number - production state reproducible with the black hole target: 50,000 commits
        // buffered, sorter_events=0, credit never fired, the parse ran on. This field is what makes that state
        // visible again.
        std::atomic<uint64_t> statInDelivery{0};
        // g5: messages the writer holds unconfirmed (Writer::currentQueueSize) and its peak. This is the part of the
        // backlog that is *past* the commit sorter: together with the producer queue of librdkafka it is what a
        // stalled client absorbs without the credit noticing (the sorter can be empty while thousands of messages
        // wait here). Published by Writer::createMessage / confirmMessage, read by layersLine only.
        std::atomic<uint64_t> statWriterQueue{0};
        std::atomic<uint64_t> statWriterQueueHwm{0};
        // g5: chunks of the builder output ring, i.e. messages that are built and waiting for the writer/client to
        // confirm them. This is the largest of the buffers past the sorter: the ring grows by a pool chunk per 64 KB
        // of output and used to be bounded only by "memory.max-mb" - a client that stopped reading let it eat the
        // whole pool while sorter_pending stayed at 0 (measured on the rig with a black-holed broker: 50,000 commits
        // delivered, writer_queue pinned at 21, every message sitting in the ring).
        std::atomic<uint64_t> statBuilderOut{0};
        // The ring is held at this many chunks (a quarter of the pool): the delivery thread waits there, the sorter
        // fills, and the credit of the parser stops the parse. 0 = not initialized yet, no bound.
        std::atomic<uint64_t> builderOutLimit{0};
        // Configured override of builderOutLimit ("memory.builder-out-chunks"): 0 = derive it from the pool.
        uint64_t builderOutChunks{0};
        // Reminder timestamp (milliseconds) of warning 60068 (the ring bound was reached).
        std::atomic<uint64_t> builderOutWarnMs{0};

        // True once the builder output ring holds as many chunks as the pool allows for it.
        [[nodiscard]] bool builderOutReached(uint64_t allocated) const {
            const uint64_t limit = builderOutLimit.load(std::memory_order_relaxed);
            return limit != 0 && allocated >= limit;
        }

        // Cost of the last min-tran walk (TransactionBuffer::checkpoint), reported in the "backlog layers" line:
        // transactions walked and microseconds spent. It only runs when a checkpoint file is written (every
        // "interval-s"/"interval-mb"), so this is the number that says whether that walk can explain a stall -
        // it used to run per LWN event and, once a backlog made it O(10k), it was invisible except as a parser
        // that made no progress at all (production 2026-10-22).
        std::atomic<uint64_t> statCheckpointWalkTx{0};
        std::atomic<uint64_t> statCheckpointWalkUs{0};

        // Highest commit SCN whose message has been handed to the writer ("delivered"). Written by the delivery
        // thread, persisted in the checkpoint as "delivered-scn" and restored at startup: the restart rewinds a
        // redo thread to the begin of the transaction that was open (see min-trans), which re-reads and otherwise
        // re-delivers everything already delivered in that window. Production 2026-10-23: 37,000 commits queued
        // again, 20 minutes of delivery_lag_ms that never came down.
        std::atomic<uint64_t> deliveredUpToScn{0};
        // Commits dropped because they were delivered before the restart (the rewind window), commits handed to
        // the writer, the delivery rate over the report interval, and the time the delivery spent waiting for a
        // swapped chunk to come back - the four numbers that tell "the queue is deep" from "the disk is slow".
        std::atomic<uint64_t> statRestartRereadDrops{0};
        std::atomic<uint64_t> statDeliveredCommits{0};
        std::atomic<uint64_t> statDeliveredPerS{0};
        std::atomic<uint64_t> statSwapGetWaitMs{0};

        // "state"."drop-delivered-on-restart": drop the commits of the rewind window that were already handed to
        // the writer before the restart (default on) instead of delivering them a second time. Off = the previous
        // behaviour, which trades the duplicates for nothing: the commits in the writer queue at the stop are lost
        // either way, the checkpoint position is ahead of them.
        bool dropDeliveredOnRestart{true};
        std::array<std::atomic<uint64_t>, 16> statPendingByThread{};
        // Chunks of the transaction pool held by the buffered (committed, not delivered yet) transactions of each
        // thread, mirrored from the sorter by CommitSorter::syncPendingEvents(). This - and not the event count -
        // is what the backpressure of the parser is measured against (CommitSorter::allowanceLocked): the pool is
        // accounted in chunks, and one transaction can own many of them ("transaction-block-kb"), so an event
        // count against a chunk budget is not a memory bound at all.
        std::array<std::atomic<uint64_t>, 16> statChunksByThread{};
        std::atomic<uint64_t> statSorterChunks{0};

        // g4: the credit actually applied (written by CommitSorter: the configured value, or the one AIMD picked).
        std::atomic<uint64_t> creditCurrent{4096};
        // Highest number of buffered commits seen for a single thread since the last layers report - the number that
        // says whether the credit holds (it should stay at the credit plus one record) or whether something still
        // runs away from it. Mirrored by CommitSorter::syncPendingEvents(), reset by layersLine().
        std::array<std::atomic<uint64_t>, 16> statPendingMaxByThread{};
        // Microseconds each thread spent stopped by the credit in the current report interval: the congestion signal
        // of the AIMD controller, and the number that tells "the parser is held on purpose" from "the parser is
        // slow". Reset by the delivery tick that reads it.
        std::array<std::atomic<uint64_t>, 16> statCreditBlockedUs{};
        // g4 swap bound: chunks currently written out to the swap path, and the limit derived from the pool
        // ("memory.max-mb" / 4, 0 = not initialized yet, which means no bound). The swap pump stops selecting new
        // transactions at the limit and the parser waits at a record head instead: unbounded swapping is what
        // turned a backlog into a 0.3 MB/s parse on 2026-10-23 (18,245 swap entries, all four threads crawling at
        // once, because reading back competes with reading archives).
        std::atomic<uint64_t> swappedChunks{0};
        std::atomic<uint64_t> swapChunksLimit{0};
        // Reminder timestamp (milliseconds, see Ctx::getTimeMs) of warning 60065 - the swap bound was reached. The
        // state itself is not stored: it is derived from the two counters above (swapCapReached), so it clears itself
        // as soon as the delivery reads chunks back and the parser may go on.
        std::atomic<uint64_t> swapCapWarnMs{0};
        // Archives yielded because the read stalled (see stallYieldUs): cumulative, and per thread for the 60067
        // report.
        std::atomic<uint64_t> statStallYields{0};
        // Archives waiting in the replicator queue, mirrored by Replicator::run() when it takes one out. A stalled
        // parser only yields when this is not zero: with nothing else to parse, yielding would reopen the same file.
        std::atomic<uint64_t> archivesQueued{0};
        std::array<std::atomic<uint64_t>, 16> statStallYieldsByThread{};

        // The credit to enforce right now: the configured value, or - when "pending-commits-per-thread" is 0 - the
        // one the AIMD controller picked (CommitSorter::adaptCredit). Read at a record head only.
        [[nodiscard]] uint64_t creditToEnforce() const {
            return pendingCommitsPerThread > 0 ? pendingCommitsPerThread : creditCurrent.load(std::memory_order_relaxed);
        }

        // True once as many chunks have been written out as the pool allows. False while the pool is not
        // initialized (limit 0) and while nothing has been swapped out.
        [[nodiscard]] bool swapCapReached() const {
            const uint64_t limit = swapChunksLimit.load(std::memory_order_relaxed);
            return limit != 0 && swappedChunks.load(std::memory_order_relaxed) >= limit;
        }

        // Chunks of one swap entry that are on the disk right now: swappedMin..swappedMax form the range that has
        // been written out and not read back (-1 = nothing on the disk). Only valid under swapMtx.
        [[nodiscard]] static uint64_t swappedOnDisk(const SwapChunk* sc) {
            if (sc == nullptr || sc->swappedMin < 0 || sc->swappedMax < sc->swappedMin)
                return 0;
            return static_cast<uint64_t>(sc->swappedMax - sc->swappedMin + 1);
        }

        // One chunk was written out and is kept on the disk (MemoryManager::swap, discard writes excluded).
        void swappedChunkStored() {
            swappedChunks.fetch_add(1, std::memory_order_relaxed);
        }

        // Chunks left the disk again: read back (MemoryManager::unswap), dropped by a purge/rollback
        // (swappedMemoryRemove) or by a release that found the entry already empty (swappedMemoryKeyRelease).
        // Saturating on purpose: a counter that got stuck above zero would keep the swap pump switched off for the
        // rest of the run, which is much worse than an imprecise bound.
        void swappedChunkFreed(uint64_t count) {
            if (count == 0)
                return;
            uint64_t current = swappedChunks.load(std::memory_order_relaxed);
            while (current > 0 &&
                   !swappedChunks.compare_exchange_weak(current, current > count ? current - count : 0,
                                                        std::memory_order_relaxed)) {
            }
        }

        // One line with those layers plus the swap side (read under swapMtx). Written every 30 s by the delivery
        // tick and appended to warning 60055.
        [[nodiscard]] std::string layersLine() const;

        // Wall clock in milliseconds. Ctx.h only forward declares Clock, so the accessor is defined in Ctx.cpp
        // (same reason as the watchdog helpers below): the writer needs it for the per message delivery lag and
        // the builder for BuilderMsg::commitMs.
        [[nodiscard]] uint64_t getTimeMs() const;

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
