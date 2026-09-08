/* Header for StateDisk class
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

#ifndef STATE_DISK_H_
#define STATE_DISK_H_

#include "State.h"

namespace OpenLogReplicator {
    class StateDisk final : public State {
    protected:
        std::string path;

    public:
        explicit StateDisk(Ctx* newCtx, std::string newPath);
        StateDisk(const StateDisk&) = delete;
        StateDisk& operator=(const StateDisk&) = delete;

        void list(std::set<std::string>& namesList) const override;
        [[nodiscard]] bool read(const std::string& name, uint64_t maxSize, std::string& in) override;
        void write(const std::string& name, Scn scn, const std::ostringstream& out) override;
        void drop(const std::string& name) override;
    };
}

#endif
