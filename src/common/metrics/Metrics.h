/* Header for Metrics class
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

#ifndef METRICS_H_
#define METRICS_H_

#include <mutex>

namespace OpenLogReplicator {
    class Ctx;

    class Metrics {
    public:
        enum class TAG_NAMES : unsigned char {
            NONE   = 0,
            FILTER = 1 << 0,
            SYS    = 1 << 2
        };

    protected:
        TAG_NAMES tagNames;

    public:
        explicit Metrics(TAG_NAMES newTagNames);
        virtual ~Metrics() = default;

        virtual void initialize(const Ctx* ctx) = 0;
        virtual void shutdown() = 0;
        bool isTagNamesFilter();
        bool isTagNamesSys();

        // bytes_confirmed
        virtual void emitBytesConfirmed(uint64_t counter) = 0;

        // bytes parsed
        virtual void emitBytesParsed(uint64_t counter) = 0;

        // bytes_parsed_repeated: redo bytes parsed a second time because a pass ended before reaching the next
        // LWN boundary (STOPPED/OVERWRITTEN) and the next pass resumes at the last confirmed one
        virtual void emitBytesParsedRepeated(uint64_t counter) = 0;

        // parse_passes_early: how often a pass ends early like that - the frequency that makes the bytes above
        virtual void emitParsePassesEarly(uint64_t counter) = 0;

        // duplicate_parse_waits / duplicate_parse_wait_us: a re-parse ran into the swap key of a transaction
        // that is already committed and not delivered yet, so the parser had to wait for its delivery (defect C)
        virtual void emitDuplicateParseWaits(uint64_t counter) = 0;
        virtual void emitDuplicateParseWaitUs(uint64_t counter) = 0;

        // bytes read
        virtual void emitBytesRead(uint64_t counter) = 0;

        // bytes sent
        virtual void emitBytesSent(uint64_t counter) = 0;

        // checkpoints
        virtual void emitCheckpointsOut(uint64_t counter) = 0;
        virtual void emitCheckpointsSkip(uint64_t counter) = 0;

        // checkpoint_lag
        virtual void emitCheckpointLag(int64_t gauge) = 0;

        // ddl_ops
        virtual void emitDdlOpsAlter(uint64_t counter) = 0;
        virtual void emitDdlOpsCreate(uint64_t counter) = 0;
        virtual void emitDdlOpsDrop(uint64_t counter) = 0;
        virtual void emitDdlOpsOther(uint64_t counter) = 0;
        virtual void emitDdlOpsPurge(uint64_t counter) = 0;
        virtual void emitDdlOpsTruncate(uint64_t counter) = 0;

        // dml_ops
        virtual void emitDmlOpsDeleteOut(uint64_t counter) = 0;
        virtual void emitDmlOpsInsertOut(uint64_t counter) = 0;
        virtual void emitDmlOpsUpdateOut(uint64_t counter) = 0;
        virtual void emitDmlOpsDeleteSkip(uint64_t counter) = 0;
        virtual void emitDmlOpsInsertSkip(uint64_t counter) = 0;
        virtual void emitDmlOpsUpdateSkip(uint64_t counter) = 0;
        virtual void emitDmlOpsDeleteOut(uint64_t counter, const std::string& owner, const std::string& table) = 0;
        virtual void emitDmlOpsInsertOut(uint64_t counter, const std::string& owner, const std::string& table) = 0;
        virtual void emitDmlOpsUpdateOut(uint64_t counter, const std::string& owner, const std::string& table) = 0;
        virtual void emitDmlOpsDeleteSkip(uint64_t counter, const std::string& owner, const std::string& table) = 0;
        virtual void emitDmlOpsInsertSkip(uint64_t counter, const std::string& owner, const std::string& table) = 0;
        virtual void emitDmlOpsUpdateSkip(uint64_t counter, const std::string& owner, const std::string& table) = 0;

        // log_switches
        virtual void emitLogSwitchesArchived(uint64_t counter) = 0;
        virtual void emitLogSwitchesOnline(uint64_t counter) = 0;

        // log_switches_lag
        virtual void emitLogSwitchesLagArchived(int64_t gauge) = 0;
        virtual void emitLogSwitchesLagOnline(int64_t gauge) = 0;

        // memory_allocated_mb
        virtual void emitMemoryAllocatedMb(int64_t gauge) = 0;

        // memory_used_total_mb
        virtual void emitMemoryUsedTotalMb(int64_t gauge) = 0;

        // memory_used_mb
        virtual void emitMemoryUsedMbBuilder(int64_t gauge) = 0;
        virtual void emitMemoryUsedMbMisc(int64_t gauge) = 0;
        virtual void emitMemoryUsedMbParser(int64_t gauge) = 0;
        virtual void emitMemoryUsedMbReader(int64_t gauge) = 0;
        virtual void emitMemoryUsedMbTransactions(int64_t gauge) = 0;
        virtual void emitMemoryUsedMbWriter(int64_t gauge) = 0;

        // messages_confirmed
        virtual void emitMessagesConfirmed(uint64_t counter) = 0;

        // messages sent
        virtual void emitMessagesSent(uint64_t counter) = 0;

        // service_state
        virtual void emitServiceStateInitializing(int64_t gauge) = 0;
        virtual void emitServiceStateReady(int64_t gauge) = 0;
        virtual void emitServiceStateStarting(int64_t gauge) = 0;
        virtual void emitServiceStateReplicating(int64_t gauge) = 0;
        virtual void emitServiceStateFinishing(int64_t gauge) = 0;
        virtual void emitServiceStateAborting(int64_t gauge) = 0;

        // swap_operations
        virtual void emitSwapOperationsMbDiscard(uint64_t counter) = 0;
        virtual void emitSwapOperationsMbRead(uint64_t counter) = 0;
        virtual void emitSwapOperationsMbWrite(uint64_t counter) = 0;

        // swap_usage_mb
        virtual void emitSwapUsageMb(int64_t gauge) = 0;

        // transactions
        virtual void emitTransactionsCommitOut(uint64_t counter) = 0;
        virtual void emitTransactionsRollbackOut(uint64_t counter) = 0;
        virtual void emitTransactionsCommitPartial(uint64_t counter) = 0;
        virtual void emitTransactionsRollbackPartial(uint64_t counter) = 0;
        virtual void emitTransactionsCommitSkip(uint64_t counter) = 0;
        virtual void emitTransactionsRollbackSkip(uint64_t counter) = 0;

        // delivery_lag_ms: the age of a message at the moment the writer hands it over, in milliseconds - the
        // wall clock at the send minus the commit time of the transaction in the database, which is the value the
        // payload carries as "source".commit_ts_ms. It is the delivery side counterpart of checkpoint_lag (which
        // is the parse side): a rising delivery_lag_ms while checkpoint_lag stays flat means the parser keeps up
        // and the delivery or the consumer is the bottleneck.
        virtual void emitDeliveryLagMs(int64_t gauge) = 0;
        // delivery_lag_ms_sum: the same value accumulated, so that rate(delivery_lag_ms_sum)/rate(messages_sent)
        // gives the average lag over a window (a gauge can not be averaged by Prometheus).
        virtual void emitDeliveryLagMsSum(uint64_t counter) = 0;
        // pending_commits: commits parsed and held by the commit sorter, still waiting for their delivery. The
        // backlog of the pipeline: growing while bytes_parsed keeps rising names the delivery side.
        virtual void emitPendingCommits(int64_t gauge) = 0;
        // committed_reread_drops / resume_fallbacks: the slicing diagnosis counters, also reported in the parse
        // statistics line of the log, so that the same numbers are visible where the monitoring is scraped.
        virtual void emitCommittedRereadDrops(uint64_t counter) = 0;
        virtual void emitResumeFallbacks(uint64_t counter) = 0;
    };
}

#endif
