/* Header for CharacterSetJA16SJISTILDE class
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

#ifndef CHARACTER_SET_JA16SJISTILDE_H_
#define CHARACTER_SET_JA16SJISTILDE_H_

#include "CharacterSetJA16SJIS.h"

namespace OpenLogReplicator {
    class CharacterSetJA16SJISTILDE final : public CharacterSetJA16SJIS {
    protected:
        [[nodiscard]] typeUnicode readMap(uint64_t byte1, uint64_t byte2) const override;

    public:
        CharacterSetJA16SJISTILDE();
    };
}

#endif
