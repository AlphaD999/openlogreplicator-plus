/* Sorter for commit and checkpoint events across redo threads (RAC support)
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

You should have received a copy of the GNU Affero General Public License
along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#include "CommitSorter.h"
#include "Transaction.h"

namespace OpenLogReplicator {
    CommitSorter::~CommitSorter() {
        for (const auto& [_, eventList]: events) {
            for (const Event* event: eventList)
                delete event;
        }
        events.clear();
    }

    void CommitSorter::enqueueCommit(Transaction* transaction, Scn commitScn) {
        auto* event = new Event();
        event->scn = commitScn;
        event->commit = true;
        event->transaction = transaction;
        events[commitScn].push_back(event);
        ++pendingEvents;
    }

    void CommitSorter::enqueueCheckpoint(const CheckpointEvent& checkpoint) {
        auto* event = new Event();
        event->scn = checkpoint.scn;
        event->commit = false;
        event->checkpoint = checkpoint;
        events[checkpoint.scn].push_back(event);
        ++pendingEvents;
    }

    void CommitSorter::setThreadWatermark(uint16_t thread, Scn parsedScn) {
        auto it = threadWatermark.find(thread);
        if (it == threadWatermark.end())
            threadWatermark.try_emplace(thread, parsedScn);
        else if (parsedScn > it->second)
            it->second = parsedScn;
    }

    void CommitSorter::threadClosed(uint16_t thread) {
        closedThreads.insert(thread);
    }

    void CommitSorter::threadReopened(uint16_t thread) {
        closedThreads.erase(thread);
    }

    void CommitSorter::flushReady(Executor* executor) {
        // Low-water-mark: minimum parsed SCN over all active (non-closed) threads
        Scn lowWatermark = Scn::none();
        for (const auto& [thread, parsedScn]: threadWatermark) {
            if (closedThreads.find(thread) != closedThreads.end())
                continue;
            if (lowWatermark == Scn::none() || parsedScn < lowWatermark)
                lowWatermark = parsedScn;
        }
        if (lowWatermark == Scn::none())
            return;

        watermark = lowWatermark;
        flushUpTo(executor, lowWatermark);
    }

    void CommitSorter::flushAll(Executor* executor) {
        flushUpTo(executor, Scn::none());
    }

    void CommitSorter::flushUpTo(Executor* executor, Scn limit) {
        while (!events.empty()) {
            const auto it = events.begin();
            if (limit != Scn::none() && it->first > limit)
                break;

            for (const Event* event: it->second) {
                if (event->commit)
                    executor->executeCommit(event->transaction);
                else
                    executor->executeCheckpoint(event->checkpoint);
                delete event;
                --pendingEvents;
            }
            events.erase(it);
        }
    }
}
