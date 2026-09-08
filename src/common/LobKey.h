/* Header for LobKey class
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

#ifndef LOB_KEY_H_
#define LOB_KEY_H_

#include "types/LobId.h"
#include "types/Types.h"

namespace OpenLogReplicator {
    class LobKey final {
    public:
        LobKey(const LobId& newLobId, typeDba newPage);

        bool operator<(const LobKey& other) const;
        bool operator!=(const LobKey& other) const;
        bool operator==(const LobKey& other) const;

        LobId lobId;
        typeDba page;
    };
}

template<>
struct std::hash<OpenLogReplicator::LobKey> {
    size_t operator()(const OpenLogReplicator::LobKey& lobKey) const noexcept;
};

#endif
