/* Header for ExpressionString class
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

#ifndef STRING_VALUE_H_
#define STRING_VALUE_H_

#include "Expression.h"

namespace OpenLogReplicator {
    class StringValue final : public Expression {
    public:
        enum class TYPE : unsigned char {
            SESSION_ATTRIBUTE,
            OP,
            VALUE
        };

        TYPE stringType;
        std::string stringValue;

        StringValue(TYPE newStringType, std::string newStringValue);

        bool isString() override { return true; }

        bool evaluateToBool(char op, const AttributeMap* attributes) override;
        std::string evaluateToString(char op, const AttributeMap* attributes) override;
    };
}

#endif
