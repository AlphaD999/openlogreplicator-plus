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
