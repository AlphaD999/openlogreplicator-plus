/* Header for RuntimeException class
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

#ifndef RUNTIME_EXCEPTION_H_
#define RUNTIME_EXCEPTION_H_

#include <ctime>
#include <exception>
#include <sstream>
#include <string>

#include "../types/Types.h"

namespace OpenLogReplicator {
    class RuntimeException final : public std::exception {
    public:
        int code;
        int supCode;
        std::string msg;

        explicit RuntimeException(int newCode, std::string newMsg, int newSupCode = 0);

        // The message std::terminate prints when an exception leaves a thread that has no handler:
        //   terminate called after throwing an instance of 'OpenLogReplicator::RuntimeException'
        //     what():  50072: swap file: ... - write returned: No space left on device
        // Without this override the line reads "what():  std::exception" - the base class text - and a fatal abort
        // leaves no clue at all (production 2026-10-22 05:48: the delivery thread died and neither the code nor the
        // message was anywhere in the log). Kept in a member so the returned pointer stays valid for the lifetime of
        // the exception object, as noexcept what() requires.
        [[nodiscard]] const char* what() const noexcept override;

        RuntimeException(const RuntimeException&) = delete;
        RuntimeException& operator=(const RuntimeException&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const RuntimeException& exception);

    private:
        // "<code>: <msg>", built once at construction (see what()).
        std::string whatMsg;
    };
}

#endif
