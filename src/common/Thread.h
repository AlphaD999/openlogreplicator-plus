/* Header for Thread class
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

#ifndef THREAD_H_
#define THREAD_H_

#include <atomic>
#include <sys/time.h>

#include "Clock.h"
#include "Ctx.h"
#include "types/Types.h"

namespace OpenLogReplicator {
    class Ctx;

    class Thread {
    protected:
        virtual void run() = 0;

    public:
        enum class CONTEXT : unsigned char {
            NONE,
            CPU,
            OS,
            MUTEX,
            WAIT,
            SLEEP,
            MEM,
            TRAN,
            CHKPT,
            NUM
        };

        enum class REASON : unsigned char {
            NONE,
            // MUTEX
            BUILDER_RELEASE,
            BUILDER_ROTATE,
            BUILDER_COMMIT,
            CHECKPOINT_RUN,
            // 1
            CHECKPOINT_WAKEUP,
            CTX_NOTHING_TO_SWAP,
            CTX_FREE_MEMORY,
            CTX_GET_SWAP,
            CTX_GET_USED,
            // 5
            CTX_MEMORY_INIT,
            CTX_SWAPPED_FLUSH1,
            CTX_SWAPPED_FLUSH2,
            CTX_SWAPPED_GET,
            CTX_SWAPPED_GROW1,
            // 10
            CTX_SWAPPED_GROW2,
            CTX_SWAPPED_RELEASE,
            CTX_SWAPPED_SIZE,
            CTX_SWAPPED_SHRINK1,
            CTX_SWAPPED_SHRINK2,
            // 15
            CTX_SWAPPED_WONT,
            MEMORY_CLEAN,
            MEMORY_RUN1,
            MEMORY_RUN2,
            MEMORY_SWAP1,
            // 20
            MEMORY_SWAP2,
            MEMORY_UNSWAP,
            READER_ALLOCATE1,
            READER_ALLOCATE2,
            READER_CHECK_FINISHED,
            // 25
            READER_CHECK_STATUS,
            READER_CONFIRM,
            READER_CHECK_FREE,
            READER_CHECK_REDO,
            READER_FREE,
            // 30
            READER_FULL,
            READER_MAIN1,
            READER_MAIN2,
            READER_READ1,
            READER_READ2,
            // 35
            READER_SET_READ,
            READER_SLEEP1,
            READER_SLEEP2,
            READER_UPDATE_REDO1,
            READER_UPDATE_REDO2,
            // 40
            READER_UPDATE_REDO3,
            READER_WAKE_UP,
            REPLICATOR_ARCH,
            REPLICATOR_SCHEMA,
            REPLICATOR_UPDATE,
            // 45
            TRANSACTION_DROP,
            TRANSACTION_FIND,
            TRANSACTION_SYSTEM,
            WRITER_CONFIRM,
            WRITER_DONE,
            // 50
            // SLEEP
            CHECKPOINT_NO_WORK,
            MEMORY_EXHAUSTED,
            METADATA_WAIT_WRITER,
            METADATA_WAIT_FOR_REPLICATOR,
            READER_CHECK,
            // 55
            READER_EMPTY,
            READER_BUFFER_FULL,
            READER_FINISHED,
            READER_NO_WORK,
            MEMORY_NO_WORK,
            // 60
            WRITER_NO_WORK,
            MEMORY_BLOCKED,
            // The delivery waits for the client to confirm messages: the builder output ring is at its bound
            // (Ctx::builderOutLimit, warning 60068).
            BUILDER_RING_FULL,
            // 65
            // OTHER
            OS,
            MEM,
            TRAN,
            CHKPT,
            // 67
            // END
            NUM = 255
        };

        Ctx* ctx;
        pthread_t pthread{0};
        std::string alias;
        std::atomic<bool> finished{false};

        explicit Thread(Ctx* newCtx, std::string newAlias);
        virtual ~Thread() = default;

        virtual void wakeUp();
        static void* runStatic(void* thread);

#ifdef THREAD_INFO
        static constexpr bool contextCompiled = true;
#else
        static constexpr bool contextCompiled = false;
#endif
        time_ut contextTimeLast{0};
        time_ut contextTime[static_cast<uint>(CONTEXT::NUM)]{};
        time_ut contextCnt[static_cast<uint>(CONTEXT::NUM)]{};
        uint64_t reasonCnt[static_cast<uint>(REASON::NUM)]{};
        REASON curReason{REASON::NONE};
        CONTEXT curContext{CONTEXT::NONE};
        uint64_t contextSwitches{0};

        virtual std::string getName() const = 0;

        void contextRun() {
            contextStart();
            run();
            contextStop();
        }

        void contextStart() {
            if constexpr (contextCompiled) {
                contextTimeLast = ctx->clock->getTimeUt();
            }
        }

        // Name of a CONTEXT, for the thread dump (Ctx::signalDump) and the reports.
        [[nodiscard]] static const char* contextName(CONTEXT context) {
            switch (context) {
                case CONTEXT::NONE: return "none";
                case CONTEXT::CPU: return "cpu";
                case CONTEXT::OS: return "os";
                case CONTEXT::MUTEX: return "mutex";
                case CONTEXT::WAIT: return "wait";
                case CONTEXT::SLEEP: return "sleep";
                case CONTEXT::MEM: return "mem";
                case CONTEXT::TRAN: return "tran";
                case CONTEXT::CHKPT: return "chkpt";
                default: return "?";
            }
        }

        // Name of the REASON of a wait. Only the ones that matter for a stall are named; anything else prints its
        // number, which is still enough to look it up in this header.
        [[nodiscard]] static std::string reasonName(REASON reason) {
            switch (reason) {
                case REASON::NONE: return "none";
                case REASON::READER_MAIN1: return "READER_MAIN1";
                case REASON::READER_MAIN2: return "READER_MAIN2";
                case REASON::READER_READ1: return "READER_READ1";
                case REASON::READER_READ2: return "READER_READ2";
                case REASON::READER_SET_READ: return "READER_SET_READ";
                case REASON::READER_SLEEP1: return "READER_SLEEP1";
                case REASON::READER_SLEEP2: return "READER_SLEEP2";
                case REASON::READER_UPDATE_REDO1: return "READER_UPDATE_REDO1";
                case REASON::READER_UPDATE_REDO2: return "READER_UPDATE_REDO2";
                case REASON::READER_UPDATE_REDO3: return "READER_UPDATE_REDO3";
                case REASON::READER_WAKE_UP: return "READER_WAKE_UP";
                case REASON::READER_CHECK: return "READER_CHECK";
                case REASON::READER_EMPTY: return "READER_EMPTY";
                case REASON::READER_BUFFER_FULL: return "READER_BUFFER_FULL";
                case REASON::READER_FINISHED: return "READER_FINISHED";
                case REASON::READER_NO_WORK: return "READER_NO_WORK";
                case REASON::REPLICATOR_ARCH: return "REPLICATOR_ARCH";
                case REASON::REPLICATOR_SCHEMA: return "REPLICATOR_SCHEMA";
                case REASON::REPLICATOR_UPDATE: return "REPLICATOR_UPDATE";
                case REASON::WRITER_CONFIRM: return "WRITER_CONFIRM";
                case REASON::WRITER_DONE: return "WRITER_DONE";
                case REASON::WRITER_NO_WORK: return "WRITER_NO_WORK";
                case REASON::MEMORY_BLOCKED: return "MEMORY_BLOCKED";
                case REASON::BUILDER_RING_FULL: return "BUILDER_RING_FULL";
                case REASON::MEMORY_EXHAUSTED: return "MEMORY_EXHAUSTED";
                case REASON::MEMORY_NO_WORK: return "MEMORY_NO_WORK";
                case REASON::METADATA_WAIT_WRITER: return "METADATA_WAIT_WRITER";
                case REASON::METADATA_WAIT_FOR_REPLICATOR: return "METADATA_WAIT_FOR_REPLICATOR";
                case REASON::TRANSACTION_DROP: return "TRANSACTION_DROP";
                case REASON::TRANSACTION_FIND: return "TRANSACTION_FIND";
                case REASON::TRANSACTION_SYSTEM: return "TRANSACTION_SYSTEM";
                case REASON::CHECKPOINT_NO_WORK: return "CHECKPOINT_NO_WORK";
                default: return std::to_string(static_cast<uint>(reason));
            }
        }

        void contextSet(CONTEXT context, REASON reason = REASON::NONE) {
            // The state is recorded in every build: the thread dump (SIGUSR1) and the stall warnings read it, and a
            // release build used to print "context: 0 reason: 0" for every thread - useless exactly when a stall has
            // to be localised (2026-10-26: the parser stopped at an archive boundary and the dump said nothing).
            // Only the timing accounting stays behind the instrumented build, because that is what costs.
            curContext = context;
            curReason = reason;
            if constexpr (contextCompiled) {
                ++contextSwitches;
                const time_ut contextTimeNow = ctx->clock->getTimeUt();
                contextTime[static_cast<uint>(curContext)] += contextTimeNow - contextTimeLast;
                ++contextCnt[static_cast<uint>(curContext)];
                ++reasonCnt[static_cast<uint>(reason)];
                contextTimeLast = contextTimeNow;
            }
        }

        void contextStop() {
            if constexpr (contextCompiled) {
                ++contextSwitches;
                const time_ut contextTimeNow = ctx->clock->getTimeUt();
                contextTime[static_cast<uint>(curContext)] += contextTimeNow - contextTimeLast;
                ++contextCnt[static_cast<uint>(curContext)];

                std::string msg =
                        "thread: " + alias +
                        " cpu: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::CPU)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::CPU)]) +
                        " os: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::OS)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::OS)]) +
                        " mtx: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::MUTEX)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::MUTEX)]) +
                        " wait: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::WAIT)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::WAIT)]) +
                        " sleep: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::SLEEP)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::SLEEP)]) +
                        " mem: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::MEM)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::MEM)]) +
                        " tran: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::TRAN)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::TRAN)]) +
                        " chkpt: " + std::to_string(contextTime[static_cast<uint>(CONTEXT::CHKPT)]) +
                        "/" + std::to_string(contextCnt[static_cast<uint>(CONTEXT::CHKPT)]) +
                        " switches: " + std::to_string(contextSwitches) + " reasons:";
                for (uint reason = static_cast<uint>(REASON::NONE); reason < static_cast<int>(REASON::NUM); ++reason) {
                    if (reasonCnt[reason] == 0)
                        continue;

                    msg += " " + std::to_string(reason) + "/" + std::to_string(reasonCnt[reason]);
                }
                ctx->info(0, msg);
            }
        }
    };
}

#endif
