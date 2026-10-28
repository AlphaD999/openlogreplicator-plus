/* Header for Reader class
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

#ifndef READER_H_
#define READER_H_

#include <atomic>
#include <vector>

#include "../common/Thread.h"
#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class Reader : public Thread {
    public:
        enum class REDO_CODE : unsigned char {
            OK,
            OVERWRITTEN,
            FINISHED,
            STOPPED,
            SHUTDOWN,
            EMPTY,
            ERROR_READ,
            ERROR_WRITE,
            ERROR_SEQUENCE,
            ERROR_CRC,
            ERROR_BLOCK,
            ERROR_BAD_DATA,
            ERROR,
            CNT
        };

    protected:
        static constexpr uint64_t FLAGS_END{0x0008};
        static constexpr uint64_t FLAGS_ASYNC{0x0100};
        static constexpr uint64_t FLAGS_NODATALOSS{0x0200};
        static constexpr uint64_t FLAGS_RESYNC{0x0800};
        static constexpr uint64_t FLAGS_CLOSEDTHREAD{0x1000};
        static constexpr uint64_t FLAGS_MAXPERFORMANCE{0x2000};

        enum class STATUS : unsigned char {
            SLEEPING,
            CHECK,
            UPDATE,
            READ
        };

        static constexpr uint PAGE_SIZE_MAX{4096};
        static constexpr uint BAD_CDC_MAX_CNT{20};
        // How often a position that looked empty is re-read before it is accepted as the end of the archive
        // (see archiveEndConfirmed): the stale answer of a filesystem bridge is transient.
        static constexpr uint END_CHECK_ATTEMPTS{3};

        std::string database;
        int fileCopyDes{-1};
        uint64_t fileSize{0};
        // The size the filesystem reported (before the redo header may have raised or lowered fileSize). Kept
        // apart because it is the only size that reflects the file itself: the header is authoritative when it
        // says more (a bridge can report 0), but a smaller header must not hide data that is really there.
        uint64_t fileSizeStat{0};
        Seq fileCopySequence;
        bool hintDisplayed{false};
        bool configuredBlockSum;
        bool readBlocks{false};
        bool reachedZero{false};
        std::string fileNameWrite;
        int group;
        Seq sequence;
        typeBlk numBlocksHeader{Ctx::ZERO_BLK};
        typeResetlogs resetlogs{0};
        typeActivation activation{0};
        uint8_t* headerBuffer{nullptr};
        uint32_t compatVsn{0};
        uint16_t thread{0};
        Time firstTimeHeader{0};
        Scn firstScn{Scn::none()};
        Scn firstScnHeader{Scn::none()};
        Scn nextScn{Scn::none()};
        Scn nextScnHeader{Scn::none()};
        Time nextTime{0};
        uint blockSize{0};
        uint64_t sumRead{0};
        uint64_t sumTime{0};
        uint64_t bufferScan{0};
        // Ticks spent in the "read buffer full" wait since it last had room again: the throttle of warning
        // 60046 (first report after 200 ticks, then every 1200). It lives in the object and not in the waiting
        // block, because the wait is one tick per pass through the read loop now - a local counter would be
        // reset on every pass and the warning would only ever be produced on the first tick.
        uint64_t bufferFullTicks{0};
        // The file size and the filesystem size as the reader thread last set them, published as atomics: the
        // parser thread prints them too, and a torn or stale read is what made a 60024 of one thread's archive
        // report the size of another thread's archive (measured 2026-10-28: thread 2 seq 15207 reporting
        // 52,630,528 = thread 1 seq 19775). Atomics and not a lock: several of the readers are already inside
        // Reader::mtx, so taking a second mutex there would be a lock-order hazard.
        std::atomic<uint64_t> fileSizePublished{0};
        std::atomic<uint64_t> fileSizeStatPublished{0};
        // Handshake bound (see Ctx::handshakeTimeoutUs): when the wait for the reader to serve
        // checkRedoLog/updateRedoLog started, when 60067 was last written and how many times the handshake has
        // been forced back to sleeping since it last worked. All three are touched only by the parser thread.
        uint64_t handshakeSinceUs{0};
        uint64_t handshakeLastWarnUs{0};
        uint32_t handshakeResets{0};
        // Warning 60024: a block that belongs to another archive. At the end of a file that is how the file
        // ends (an ASM extent handed to another archive, 60052 family) - but in the middle it means the bytes
        // came from somewhere else (the read buffers are shared by every reader, and an archive that is still
        // being written has been read before). Treating that as the end skips the rest of the archive in
        // silence, so the pass stops and comes back instead - unless the same spot keeps failing, which is
        // given up on with a message that says so. Touched only by the reader thread.
        static constexpr uint64_t SEQUENCE_ERROR_SLACK{1024 * 1024};
        static constexpr uint32_t SEQUENCE_ERROR_MAX{3};
        uint64_t sequenceErrorOffset{0};
        Seq sequenceErrorSeq{Seq::zero()};
        uint32_t sequenceErrorCount{0};
        uint lastRead{0};
        time_ut lastReadTime{0};
        time_ut readTime{0};
        time_ut loopTime{0};

        std::mutex mtx;
        std::atomic<uint64_t> bufferStart{0};
        std::atomic<uint64_t> bufferEnd{0};
        std::atomic<STATUS> status{STATUS::SLEEPING};
        std::atomic<REDO_CODE> ret{REDO_CODE::OK};
        std::condition_variable condBufferFull;
        std::condition_variable condReaderSleeping;
        std::condition_variable condParserSleeping;

        virtual void redoClose() = 0;
        virtual REDO_CODE redoOpen() = 0;
        virtual int redoRead(uint8_t* buf, uint64_t offset, uint size) = 0;
        // One read that neither retries nor reports anything, for the archive end verification: reading at
        // the end of a file returns zero bytes, which is a normal answer there and not an error - going
        // through redoRead() instead turns one probe per archive into a storm of 10005 (with the retry sleep
        // of arch-read-sleep-us behind each of them).
        virtual int redoReadProbe(uint8_t* buf, uint64_t offset, uint size);
        // Refresh the file size: the one taken at open time can be stale (a filesystem bridge such as asmfs
        // reports 0 for a file that is not fully visible yet, an online redo log can have grown since)
        virtual void refreshFileSize();
        virtual uint readSize(uint prevRead);
        virtual REDO_CODE reloadHeaderRead();
        REDO_CODE checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint);
        REDO_CODE reloadHeader();
        bool read1();
        bool read2();
        void mainLoop();
        // Reports a handshake the reader does not serve (warning 60067) and, at four times the limit, forces
        // the handshake back to sleeping so the reader can not stay parked in the buffer-full wait of mainLoop
        // while the parser waits for it. Returns true when the caller has to give up and return false.
        bool handshakeWait(std::unique_lock<std::mutex>& lck, const char* what);
        static const char* statusName(STATUS value);
        // Is the end of this archive real? The sizes this reader knows can both be wrong: the one from stat
        // (a filesystem bridge answers a stale, too small value and fileSize is only ever raised, so the
        // wrong value sticks - measured 131072 for a 242688 byte archive) and the one from the redo header
        // (a stale header read gives a too small block count as well). Accepting either as the end skips the
        // tail of the archive in silence: measured "FINISHED, position: 131072 / 242688" lost a whole
        // committed transaction plus a DDL with nothing at all in the log. What is left to consult is the
        // file itself: when data is expected behind the position the parser stopped at, re-read it - real
        // redo means the end is bogus, zeroes mean the padding of a log that was not filled up. Never finish
        // an archive that still holds data: stop instead, the position is saved and the archive is read
        // again with a fresh handle on the next scheduler pass (warning 60044). Not to be used where a
        // non-empty block is the expected state (a block of another redo log, see read1 batch mode).
        bool archiveEndConfirmed(Thread* t);

    public:
        const static char* REDO_MSG[static_cast<uint>(REDO_CODE::CNT)];
        uint8_t** redoBufferList{nullptr};
        std::vector<std::string> paths;
        std::string fileName;
        // A copy of fileName kept under its own lock: the parser thread assigns the name
        // (Replicator::processArchivedRedoLogs) while the reader thread prints it in every diagnostic, and a
        // std::string copied during an assignment is undefined behaviour - that is why 60046 used to name the
        // previous archive. The diagnostics and the 60055 snapshot use this copy; the bare member stays for the
        // code that runs on the reader thread only.
        mutable std::mutex fileNameMtx;
        std::string fileNameSafe;
        void setFileName(const std::string& newFileName);
        [[nodiscard]] std::string fileNameForDiagnostics() const;

        Reader(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum);
        ~Reader() override;

        void initialize();
        void wakeUp() override;
        void run() override;
        void bufferAllocate(uint num);
        void bufferFree(Thread* t, uint num);
        bool bufferIsFree();
        // Makes sure this reader knows which archive it is reading before it accepts data blocks (reads the
        // header block of the file if that has not happened yet). A pass that starts at a saved offset of an
        // archive needs it: the sequence is what every block is checked against, and the bytes at the saved
        // offset are not necessarily this archive's (see checkBlockHeader).
        void ensureArchiveSequence();
        // Tells this reader which archive it is about to open, before the first block of it is read. The caller
        // knows it from the archive list (Replicator::processArchivedRedoLogs). With it every block is checked
        // against the archive it claims to belong to (checkBlockHeader) and the sequence is never taken over from
        // a block header - see the comment there for what that used to cost.
        void setArchiveSequence(Seq newSequence);

    protected:
        // The archive identity the caller declared (setArchiveSequence): it is what every block of an archive is
        // checked against, and it is kept separately because "sequence" is reset while the file is opened
        // (checkRedoLog/updateRedoLog) - losing it there is exactly what made a reader take the sequence over from
        // a block header again. Seq::zero() = no archive has been declared.
        Seq archiveSequence{Seq::zero()};

    public:
        typeSum calcChSum(uint8_t* buffer, uint size) const;
        void printHeaderInfo(std::ostringstream& ss, const std::string& path) const;
        [[nodiscard]] uint getBlockSize() const;
        [[nodiscard]] FileOffset getBufferStart() const;
        [[nodiscard]] FileOffset getBufferEnd() const;
        [[nodiscard]] REDO_CODE getRet() const;
        [[nodiscard]] Scn getFirstScn() const;
        [[nodiscard]] Scn getFirstScnHeader() const;
        [[nodiscard]] Scn getNextScn() const;
        [[nodiscard]] Time getNextTime() const;
        [[nodiscard]] typeBlk getNumBlocks() const;
        [[nodiscard]] int getGroup() const;
        [[nodiscard]] Seq getSequence() const;
        [[nodiscard]] typeResetlogs getResetlogs() const;
        [[nodiscard]] typeActivation getActivation() const;
        [[nodiscard]] uint64_t getSumRead() const;
        [[nodiscard]] uint64_t getSumTime() const;
        [[nodiscard]] uint16_t getThread() const;
        // True when the reader is parked (nothing is inside its read loop), which is the precondition of
        // resumeAt(); STATUS itself stays protected - the scheduler has no business switching states.
        [[nodiscard]] bool isSleeping() const;
        // The published size, rendered for a message: 0 means "the file was not stat'ed yet" (a 60024 on the
        // header block is emitted before redoOpen), which must not be printed as a 0-byte archive.
        [[nodiscard]] std::string fileSizeText() const;

        // ---- slice resume on the same handle (L4) ---------------------------------------------------------
        // The position the last slice of this archive stopped at (0 = none). The scheduler stores it right
        // before it queues the parser again and resumeAt() consumes it on the next pass: continuing on the
        // same file handle instead of closing and opening the archive keeps a slice out of the
        // checkRedoLog/updateRedoLog handshake - the request that deadlocked the two threads on 2026-10-27/28
        // - and spares the open/stat/re-read-header of every slice.
        std::atomic<uint64_t> sliceResumeOffset{0};
        // The sequence that offset belongs to. One archReader serves every redo thread of the instance, so a
        // resume recorded for thread 1 must not be taken by the pass of thread 2 - measured on the rig: without
        // this check thread 2's archive was positioned at thread 1's offset and both passes reported 60024.
        std::atomic<uint32_t> sliceResumeSeq{0};
        // Set when the first block after a resume could not be attributed to this archive (60024 / CRC): the
        // next pass has to reopen the file instead of resuming again, so a resume that keeps landing on a bad
        // position can not spin forever.
        std::atomic<bool> resumeBlocked{false};
        void resumeAt(uint64_t offset);
        // Asks the reader to park *without* a CHECK request (which would close and open the archive): the read
        // loop leaves on its own because its condition includes status == READ. Returns false when the reader
        // is not reading (already parked, or being served elsewhere) or did not park in time - the caller then
        // falls back to the reopening path.
        bool parkForResume(uint64_t timeoutUs);

        void setRet(REDO_CODE newRet);
        void setBufferStartEnd(FileOffset newBufferStart, FileOffset newBufferEnd);
        bool checkRedoLog();
        bool updateRedoLog();
        void setStatusRead();
        void confirmReadData(FileOffset confirmedBufferStart);
        // "stallUs" > 0 gives the wait below a deadline: after that long without a single byte the method returns
        // false with "starving" set instead of waiting for the reader indefinitely. The parse loop is a single
        // thread, so one archive that delivers nothing (still being written, or read slowly through the mount) would
        // otherwise stop every redo thread behind it - the parser uses this to yield at the last confirmed LWN
        // boundary and parse something else first (Ctx::stallYieldUs).
        [[nodiscard]] bool checkFinished(Thread* t, FileOffset confirmedBufferStart, uint64_t stallUs = 0,
                                         bool* starving = nullptr);
        virtual void showHint(Thread* t, std::string origPath, std::string mappedPath) const = 0;

        std::string getName() const override {
            return {"Reader: " + fileName};
        }
    };
}

#endif
