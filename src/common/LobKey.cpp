/* Definition of LobKey
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

#include "LobKey.h"

namespace OpenLogReplicator {
    LobKey::LobKey(const LobId& newLobId, typeDba newPage):
            lobId(newLobId),
            page(newPage) {}

    bool LobKey::operator==(const LobKey& other) const {
        return (other.lobId == lobId) && (other.page == page);
    }

    bool LobKey::operator<(const LobKey& other) const {
        if (lobId < other.lobId)
            return true;
        if (other.lobId < lobId)
            return false;
        if (other.page > page)
            return true;
        return false;
    }

    bool LobKey::operator!=(const LobKey& other) const {
        return (other.lobId != lobId) || (other.page != page);
    }
}

size_t std::hash<OpenLogReplicator::LobKey>::operator()(const OpenLogReplicator::LobKey& lobKey) const noexcept {
    return hash<typeDba>()(lobKey.page) ^
            hash<OpenLogReplicator::LobId>()(lobKey.lobId);
}
