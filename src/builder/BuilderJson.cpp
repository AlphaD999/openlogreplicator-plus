/* Memory buffer for handling output buffer in JSON format
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

#include "../common/Clock.h"
#include "../common/DbTable.h"
#include "../common/types/RowId.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderJson.h"

namespace OpenLogReplicator {
    BuilderJson::BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, Format& newFormat, uint64_t newFlushBuffer,
                             bool newEnvelopeOutput, std::string newTopicPrefix, std::string newSchemaChangeTopic,
                             bool newDebeziumSchemaBlock):
            Builder(newCtx, newLocales, newMetadata, newFormat, newFlushBuffer),
            envelopeOutput(newEnvelopeOutput),
            topicPrefix(std::move(newTopicPrefix)),
            schemaChangeTopic(std::move(newSchemaChangeTopic)) {
        debeziumSchemaBlock = newDebeziumSchemaBlock;
        // A configured schema-change topic means DDL events must be collected/reported without SHOW_DDL
        ddlEnabled = envelopeOutput && !schemaChangeTopic.empty();
    }

    void BuilderJson::appendBase64(const uint8_t* data, uint64_t size) {
        uint64_t i = 0;
        while (i + 3 <= size) {
            const uint32_t v = (static_cast<uint32_t>(data[i]) << 16) | (static_cast<uint32_t>(data[i + 1]) << 8) | data[i + 2];
            append(Data::map64((v >> 18) & 0x3F));
            append(Data::map64((v >> 12) & 0x3F));
            append(Data::map64((v >> 6) & 0x3F));
            append(Data::map64(v & 0x3F));
            i += 3;
        }
        const uint64_t rest = size - i;
        if (rest == 1) {
            const uint32_t v = static_cast<uint32_t>(data[i]) << 16;
            append(Data::map64((v >> 18) & 0x3F));
            append(Data::map64((v >> 12) & 0x3F));
            append('=');
            append('=');
        } else if (rest == 2) {
            const uint32_t v = (static_cast<uint32_t>(data[i]) << 16) | (static_cast<uint32_t>(data[i + 1]) << 8);
            append(Data::map64((v >> 18) & 0x3F));
            append(Data::map64((v >> 12) & 0x3F));
            append(Data::map64((v >> 6) & 0x3F));
            append('=');
        }
    }

    void BuilderJson::appendDecimalBase64(int64_t scale) {
        valueBuffer[valueSize] = 0;
        const char* ptr = valueBuffer;
        bool negative = false;
        if (*ptr == '-') {
            negative = true;
            ++ptr;
        } else if (*ptr == '+') {
            ++ptr;
        }

        // Collect the integer and fraction digits of the decimal text
        std::string intDigits;
        std::string fracDigits;
        bool afterDot = false;
        for (; *ptr != 0; ++ptr) {
            if (*ptr == '.') {
                afterDot = true;
                continue;
            }
            if (*ptr < '0' || *ptr > '9')
                continue;
            if (afterDot)
                fracDigits.push_back(*ptr);
            else
                intDigits.push_back(*ptr);
        }

        // Align the fraction to the target scale (BigDecimal.setScale semantics)
        const uint64_t targetScale = (scale > 0) ? static_cast<uint64_t>(scale) : 0;
        if (fracDigits.size() > targetScale)
            fracDigits.resize(targetScale);
        while (fracDigits.size() < targetScale)
            fracDigits.push_back('0');

        std::string digits = intDigits + fracDigits;
        if (digits.empty())
            digits = "0";
        uint64_t firstNonZero = 0;
        while (firstNonZero + 1 < digits.size() && digits[firstNonZero] == '0')
            ++firstNonZero;
        digits.erase(0, firstNonZero);

        // Accumulate into a 128 bit magnitude (NUMBER max precision is 38 digits)
        unsigned __int128 magnitude = 0;
        for (const char c : digits)
            magnitude = (magnitude * 10) + static_cast<unsigned __int128>(c - '0');

        // Two's complement big endian, minimal length: java.math.BigInteger.toByteArray() semantics
        uint8_t bytes[17];
        uint64_t size = 0;
        if (magnitude == 0) {
            bytes[0] = 0;
            size = 1;
        } else if (!negative) {
            uint64_t bits = 1;
            for (uint64_t i = 128; i > 0; --i) {
                if (((magnitude >> (i - 1)) & 1) != 0) {
                    bits = i;
                    break;
                }
            }
            size = (bits + 7) / 8;
            // Keep the sign bit clear (prepend 0x00 when the magnitude fills the top byte)
            if (((magnitude >> (size * 8 - 1)) & 1) != 0)
                ++size;
            for (uint64_t i = 0; i < size; ++i)
                bytes[size - 1 - i] = (i < 16) ? static_cast<uint8_t>((magnitude >> (i * 8)) & 0xFF) : 0x00;
        } else {
            const unsigned __int128 twosComplement = (~magnitude) + 1;
            uint64_t bits = 1;
            for (uint64_t i = 128; i > 0; --i) {
                if (((twosComplement >> (i - 1)) & 1) != 0) {
                    bits = i;
                    break;
                }
            }
            size = (bits + 7) / 8;
            // Keep the sign bit set (extend with 0xFF when the top bit is clear)
            if (size <= 16 && ((twosComplement >> (size * 8 - 1)) & 1) == 0)
                ++size;
            for (uint64_t i = 0; i < size; ++i)
                bytes[size - 1 - i] = (i < 16) ? static_cast<uint8_t>((twosComplement >> (i * 8)) & 0xFF) : 0xFF;
        }

        appendBase64(bytes, size);
    }

    void BuilderJson::appendVariableScaleDecimal() {
        // VariableScaleDecimal holds the scale of the value itself (no column scale available)
        valueBuffer[valueSize] = 0;
        const char* dot = nullptr;
        for (const char* p = valueBuffer; *p != 0; ++p) {
            if (*p == '.') {
                dot = p;
                break;
            }
        }
        int64_t scale = 0;
        if (dot != nullptr) {
            for (const char* p = dot + 1; *p >= '0' && *p <= '9'; ++p)
                ++scale;
        }

        append(std::string_view(R"({"scale":)"));
        appendSDec(scale);
        append(std::string_view(R"(,"value":")"));
        appendDecimalBase64(scale);
        append(std::string_view("\"}"));
    }

    void BuilderJson::columnFloat(const std::string& columnName, double value) {
        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":)"));

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnDouble(const std::string& columnName, long double value) {
        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":)"));

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnString(const std::string& columnName) {
        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":")"));
        appendEscape(valueBuffer, valueSize);
        append('"');
    }

    void BuilderJson::columnNumber(const std::string& columnName, int precision __attribute__((unused)),
            int scale __attribute__((unused))) {
        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":)"));
        valueBuffer[valueSize] = 0;

        if (debeziumSchemaBlock) {
            if (currentColumn != nullptr && currentColumn->type == SysCol::COLTYPE::BOOLEAN) {
                // processValue() emits 0/1, the schema declares "boolean"
                append(valueSize > 0 && valueBuffer[0] == '1' ? std::string_view("true") : std::string_view("false"));
                return;
            }
            if (currentColumn != nullptr && currentColumn->type == SysCol::COLTYPE::NUMBER) {
                const int64_t colPrecision = currentColumn->precision > 0 ? currentColumn->precision : 38;
                const int64_t colScale = currentColumn->scale > 0 ? currentColumn->scale : 0;
                if (!(colScale == 0 && colPrecision < 19)) {
                    // Decimal: unscaled two's complement value, JsonConverter encodes it as base64
                    append('"');
                    appendDecimalBase64(colScale);
                    append('"');
                    return;
                }
            }
            if (currentColumn != nullptr && currentColumn->type == SysCol::COLTYPE::FLOAT) {
                // Oracle FLOAT is a scale-less NUMERIC (2 bytes header + mantissa) => VariableScaleDecimal
                appendVariableScaleDecimal();
                return;
            }
        }

        appendArr(valueBuffer, valueSize);
    }

    void BuilderJson::columnRowId(const std::string& columnName, RowId rowId) {
        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":")"));
        char str[RowId::SIZE + 1];
        rowId.toHex(str);
        appendArr(str, 18);
        append('"');
    }

    void BuilderJson::columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) {
        if (debeziumSchemaBlock) {
            // Debezium binary.handling.mode=bytes: Kafka Connect JsonConverter encodes BYTES as base64
            comma(hasPreviousColumn);
            append('"');
            appendEscape(columnName);
            append(std::string_view(R"(":")"));
            appendBase64(data, size);
            append('"');
            return;
        }

        if (likely(lastBuilderSize + messagePosition + size * 2 + columnName.size() * 3 + 8 < OUTPUT_BUFFER_DATA_SIZE)) {
            if (hasPreviousColumn)
                append<true>(',');
            else
                hasPreviousColumn = true;

            append<true>('"');
            appendEscape<true>(columnName);
            append<true>(std::string_view(R"(":")"));
            for (uint64_t j = 0; j < size; ++j)
                appendHex2<true>(*(data + j));
            append<true>('"');
        } else {
            comma(hasPreviousColumn);
            append('"');
            appendEscape(columnName);
            append(std::string_view(R"(":")"));
            for (uint64_t j = 0; j < size; ++j)
                appendHex2(*(data + j));
            append('"');
        }
    }

    void BuilderJson::columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) {
        if (debeziumSchemaBlock) {
            if (currentColumn != nullptr && (currentColumn->type == SysCol::COLTYPE::TIMESTAMP_WITH_TZ ||
                    currentColumn->type == SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ)) {
                // TIMESTAMP WITH LOCAL TIME ZONE is emitted through columnTimestamp(): the schema declares a
                // ZonedTimestamp, so emit its ISO-8601 representation (the value is UTC => "Z")
                const int64_t p = currentColumn->scale >= 0 ? currentColumn->scale : 6;
                const int digits = p > 9 ? 9 : static_cast<int>(p);
                comma(hasPreviousColumn);
                append('"');
                appendEscape(columnName);
                append(std::string_view(R"(":")"));
                char buffer[24];
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                if (digits > 0) {
                    append('.');
                    char frac[10];
                    uint64_t f = fraction;
                    for (int i = digits - 1; i >= 0; --i) {
                        frac[i] = Data::map10(static_cast<uint>(f % 10));
                        f /= 10;
                    }
                    appendArr(frac, static_cast<uint64_t>(digits));
                }
                append('Z');
                append('"');
                return;
            }
            // Debezium time.precision.mode=adaptive: the unit must match the schema name chosen for the
            // column (Oracle DATE carries no fractional seconds => millis, like the schema above)
            const int64_t precision = (currentColumn != nullptr && currentColumn->type == SysCol::COLTYPE::DATE) ? 0
                    : ((currentColumn != nullptr && currentColumn->scale >= 0) ? currentColumn->scale : 6);
            comma(hasPreviousColumn);
            append('"');
            appendEscape(columnName);
            append(std::string_view(R"(":)"));
            if (precision <= 3)
                appendSDec((timestamp * 1000L) + static_cast<time_t>((fraction + 500000) / 1000000));
            else if (precision <= 6)
                appendSDec((timestamp * 1000000L) + static_cast<time_t>((fraction + 500) / 1000));
            else
                appendSDec((timestamp * 1000000000L) + static_cast<time_t>(fraction));
            return;
        }

        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":)"));
        char buffer[22];

        switch (format.timestampFormat) {
            case Format::TIMESTAMP_FORMAT::UNIX_NANO:
                // 1712345678123456789
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec((timestamp * 1000000000L) + fraction);
                else {
                    // Big number
                    const int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>((timestamp * 1000000000L) + fraction);
                }
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MICRO:
                // 1712345678123457
                appendSDec((timestamp * 1000000L) + ((fraction + 500) / 1000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MILLI:
                // 1712345678123
                appendSDec((timestamp * 1000L) + ((fraction + 500000) / 1000000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX:
                // 1712345678
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_NANO_STRING:
                // "1712345678123456789"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec((timestamp * 1000000000L) + fraction);
                else {
                    // Big number
                    const int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>((timestamp * 1000000000L) + fraction);
                }
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MICRO_STRING:
                // "1712345678123457"
                append('"');
                appendSDec((timestamp * 1000000L) + ((fraction + 500) / 1000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MILLI_STRING:
                // "1712345678123"
                append('"');
                appendSDec((timestamp * 1000L) + ((fraction + 500000) / 1000000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_STRING:
                // "1712345678"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z"
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<9>(fraction);
                append(std::string_view(R"(Z")"));
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<6>(fraction);
                append(std::string_view(R"(Z")"));
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<3>(fraction);
                append(std::string_view(R"(Z")"));
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_TZ:
                // "2024-04-05T19:34:38Z"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append(std::string_view(R"(Z")"));
                break;
            case Format::TIMESTAMP_FORMAT::ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789"
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<9>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<6>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MILLI:
                // "2024-04-05 19:34:38.123"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<3>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601:
                // "2024-04-05 19:34:38"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('"');
                break;
        }
    }

    void BuilderJson::columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const std::string_view& tz) {
        if (debeziumSchemaBlock) {
            // Debezium ZonedTimestamp: ISO-8601 with offset, e.g. 2018-03-27T01:34:56.007891-11:00 (UTC => Z)
            const int64_t precision = (currentColumn != nullptr && currentColumn->scale >= 0) ? currentColumn->scale : 6;
            const int digits = precision > 9 ? 9 : static_cast<int>(precision);
            comma(hasPreviousColumn);
            append('"');
            appendEscape(columnName);
            append(std::string_view(R"(":")"));
            char buffer[24];
            appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
            if (digits > 0) {
                append('.');
                char frac[10];
                uint64_t f = fraction;
                for (int i = digits - 1; i >= 0; --i) {
                    frac[i] = Data::map10(static_cast<uint>(f % 10));
                    f /= 10;
                }
                appendArr(frac, static_cast<uint64_t>(digits));
            }
            // The offset is available whenever the redo data carries one ("+HH:MM"); a time zone name
            // (timezone region) cannot be converted to an offset without a tz database => fall back to UTC.
            if (tz.size() == 6 && (tz[0] == '+' || tz[0] == '-') && tz[3] == ':')
                append(tz);
            else
                append(std::string_view("Z"));
            append('"');
            return;
        }

        comma(hasPreviousColumn);
        append('"');
        appendEscape(columnName);
        append(std::string_view(R"(":)"));
        char buffer[22];

        switch (format.timestampTzFormat) {
            case Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING:
                // "1700000000.123456789,Europe/Warsaw"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec((timestamp * 1000000000L) + fraction);
                else {
                    // Big number
                    const int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>((timestamp * 1000000000L) + fraction);
                }
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_MICRO_STRING:
                // "1700000000.123456,Europe/Warsaw"
                append('"');
                appendSDec((timestamp * 1000000L) + ((fraction + 500) / 1000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_MILLI_STRING:
                // "1700000000.123,Europe/Warsaw"
                append('"');
                appendSDec((timestamp * 1000L) + ((fraction + 500000) / 1000000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_STRING:
                // "1700000000,Europe/Warsaw"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z Europe/Warsaw"
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<9>(fraction);
                append(std::string_view("Z "));
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<6>(fraction);
                append(std::string_view("Z "));
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<3>(fraction);
                append(std::string_view("Z "));
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_TZ:
                // "2024-04-05T19:34:38Z Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, true, false));
                append(std::string_view("Z "));
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789,Europe/Warsaw"
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<9>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456,Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<6>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MILLI:
                // "2024-04-05 19:34:38.123 Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<3>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601:
                // "2024-04-05 19:34:38 Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                appendArr(buffer, Data::epochToIso8601(timestamp, buffer, false, false));
                append(' ');
                append(tz);
                append('"');
                break;
        }
    }

    void BuilderJson::processBeginMessage(Seq sequence, Time timestamp) {
        newTran = false;
        hasPreviousRedo = false;

        if (format.isMessageFormatSkipBegin())
            return;

        builderBegin(sequence, beginScn, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
        append('{');
        hasPreviousValue = false;
        appendHeader(beginScn, timestamp, true, format.isDbFormatAddDml(), true, format.isUserTypeBegin());

        comma(hasPreviousValue);
        if (format.isAttributesFormatBegin())
            appendAttributes();

        if (format.isMessageFormatFull()) {
            append(std::string_view(R"("payload":[)"));
        } else {
            append(std::string_view(R"("payload":[{"op":"begin"}]})"));
            builderCommit();
        }
    }

    void BuilderJson::processCommit() {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if (format.isMessageFormatFull()) {
            append(std::string_view("]}"));
            builderCommit();
        } else if (!format.isMessageFormatSkipCommit()) {
            builderBegin(commitSequence, commitScn, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
            append('{');

            hasPreviousValue = false;
            appendHeader(commitScn, commitTimestamp, false, format.isDbFormatAddDml(), true, format.isUserTypeCommit());

            comma(hasPreviousValue);
            if (format.isAttributesFormatCommit())
                appendAttributes();

            append(std::string_view(R"("payload":[{"op":"commit"}]})"));
            builderCommit();
        }
        num = 0;
    }

    void BuilderJson::processInsert(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        // Debezium envelope: one message per DML, flat structure, no schema block
        if (envelopeOutput) {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, fileOffset);

            // Debezium semantics: ts_ms is the processing time (wall clock), not the Oracle commit time
            processTsUs = ctx->clock->getTimeUt();
            if (debeziumSchemaBlock)
                appendEnvelopeSchemaOpen(table);
            append('{');
            hasPreviousValue = false;
            appendNullBefore();
            appendAfter(lobCtx, xmlCtx, table, fileOffset);
            appendSource(table, obj, scn, dataObj, bdba, slot);
            append(std::string_view(R"(,"op":"c","ts_ms":)"));
            appendDec(static_cast<uint64_t>(processTsUs) / 1000);
            append('}');
            if (debeziumSchemaBlock)
                append('}');
            builderCommit();
            ++num;
            return;
        }

        if (format.isMessageFormatFull()) {
            comma(hasPreviousRedo);
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, fileOffset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true, format.isUserTypeDml());

            comma(hasPreviousValue);
            if (format.isAttributesFormatDml())
                appendAttributes();

            append(std::string_view(R"("payload":[)"));
        }

        append(std::string_view(R"({"op":"c",)"));
        if (format.isMessageFormatAddOffset()) {
            append(std::string_view(R"("offset":)"));
            appendDec(fileOffset.getData());
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, xmlCtx, table, fileOffset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append(std::string_view("]}"));
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processUpdate(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        // Debezium envelope: one message per DML, flat structure, no schema block
        if (envelopeOutput) {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, fileOffset);

            // Debezium semantics: ts_ms is the processing time (wall clock), not the Oracle commit time
            processTsUs = ctx->clock->getTimeUt();
            if (debeziumSchemaBlock)
                appendEnvelopeSchemaOpen(table);
            append('{');
            hasPreviousValue = false;
            appendBefore(lobCtx, xmlCtx, table, fileOffset, true);
            appendAfter(lobCtx, xmlCtx, table, fileOffset);
            appendSource(table, obj, scn, dataObj, bdba, slot);
            append(std::string_view(R"(,"op":"u","ts_ms":)"));
            appendDec(static_cast<uint64_t>(processTsUs) / 1000);
            append('}');
            if (debeziumSchemaBlock)
                append('}');
            builderCommit();
            ++num;
            return;
        }

        if (format.isMessageFormatFull()) {
            comma(hasPreviousRedo);
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, fileOffset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true, format.isUserTypeDml());

            comma(hasPreviousValue);
            if (format.isAttributesFormatDml())
                appendAttributes();

            append(std::string_view(R"("payload":[)"));
        }

        append(std::string_view(R"({"op":"u",)"));
        if (format.isMessageFormatAddOffset()) {
            append(std::string_view(R"("offset":)"));
            appendDec(fileOffset.getData());
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, fileOffset);
        appendAfter(lobCtx, xmlCtx, table, fileOffset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append(std::string_view("]}"));
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processDelete(Seq sequence, Scn scn, Time timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        // Debezium envelope: one message per DML, flat structure, no schema block
        if (envelopeOutput) {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::BEFORE, fileOffset);

            // Debezium semantics: ts_ms is the processing time (wall clock), not the Oracle commit time
            processTsUs = ctx->clock->getTimeUt();
            if (debeziumSchemaBlock)
                appendEnvelopeSchemaOpen(table);
            append('{');
            hasPreviousValue = false;
            appendBefore(lobCtx, xmlCtx, table, fileOffset, true);
            appendNullAfter();
            appendSource(table, obj, scn, dataObj, bdba, slot);
            append(std::string_view(R"(,"op":"d","ts_ms":)"));
            appendDec(static_cast<uint64_t>(processTsUs) / 1000);
            append('}');
            if (debeziumSchemaBlock)
                append('}');
            builderCommit();
            ++num;
            return;
        }

        if (format.isMessageFormatFull()) {
            comma(hasPreviousRedo);
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::BEFORE, fileOffset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true, format.isUserTypeDml());

            comma(hasPreviousValue);
            if (format.isAttributesFormatDml())
                appendAttributes();

            append(std::string_view(R"("payload":[)"));
        }

        append(std::string_view(R"({"op":"d",)"));
        if (format.isMessageFormatAddOffset()) {
            append(std::string_view(R"("offset":)"));
            appendDec(fileOffset.getData());
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, fileOffset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append(std::string_view("]}"));
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processDdl(Seq sequence, Scn scn, Time timestamp, const DbTable* table, typeObj obj) {
        if (newTran)
            processBeginMessage(sequence, timestamp);

        // Envelope mode: DDL events must never mix into data topics (Flink's debezium-json cannot parse them).
        // When a schema-change topic is configured they are published there as {"source":{...},"ddl":"..."};
        // otherwise they are dropped (OLR schema tracking is driven by redo system-table changes independently).
        if (envelopeOutput) {
            if (schemaChangeTopic.empty())
                return;

            // Report DDL only for tables that are actually replicated (present in the dictionary/filter);
            // DDL on system or unlisted objects is never emitted into the schema-change topic
            if (table == nullptr)
                return;

            processTsUs = ctx->clock->getTimeUt();
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::SCHEMA);
            append('{');
            hasPreviousValue = false;
            appendDdlSource(table, obj, scn);
            append(std::string_view(R"(,"ddl":")"));
            if (ddlFirst != nullptr) {
                uint8_t* chunk = ddlFirst;
                while (chunk != nullptr) {
                    uint8_t* chunkNext = *reinterpret_cast<uint8_t**>(chunk);
                    const typeTransactionSize* chunkSize = reinterpret_cast<typeTransactionSize*>(chunk + sizeof(uint8_t*));
                    const uint8_t* chunkData = chunk + sizeof(uint8_t*) + sizeof(uint64_t);
                    appendEscape(reinterpret_cast<const char*>(chunkData), *chunkSize);
                    chunk = chunkNext;
                }
            }
            append('"');
            append('}');
            builderCommit();
            ++num;
            return;
        }

        if (format.isMessageFormatFull()) {
            comma(hasPreviousRedo);
        } else {
            builderBegin(sequence, scn, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDdl(), true, format.isUserTypeDdl());

            comma(hasPreviousValue);
            if (format.isAttributesFormatDml())
                appendAttributes();

            append(std::string_view(R"("payload":[)"));
        }

        append(std::string_view(R"({"op":"ddl",)"));
        appendSchema(table, obj);
        append(std::string_view(R"(,"sql":")"));
        if (ddlFirst != nullptr) {
            uint8_t* chunk = ddlFirst;
            while (chunk != nullptr) {
                uint8_t* chunkNext = *reinterpret_cast<uint8_t**>(chunk);
                const typeTransactionSize* chunkSize = reinterpret_cast<typeTransactionSize*>(chunk + sizeof(uint8_t*));
                const uint8_t* chunkData = chunk + sizeof(uint8_t*) + sizeof(uint64_t);
                appendEscape(reinterpret_cast<const char*>(chunkData), *chunkSize);
                chunk = chunkNext;
            }
        }
        append(std::string_view(R"("})"));

        if (!format.isMessageFormatFull()) {
            append(std::string_view("]}"));
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processCheckpoint(Seq sequence, Scn scn, Time timestamp, FileOffset fileOffset, bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        auto flags = BuilderMsg::OUTPUT_BUFFER::CHECKPOINT;
        if (redo)
            flags = static_cast<BuilderMsg::OUTPUT_BUFFER>(static_cast<uint>(flags) | static_cast<uint>(BuilderMsg::OUTPUT_BUFFER::REDO));
        builderBegin(sequence, scn, 0, flags);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, timestamp, true, false, false, false);

        comma(hasPreviousValue);
        append(std::string_view(R"("payload":[{"op":"chkpt","seq":)"));
        appendDec(sequence.getData());
        append(std::string_view(R"(,"offset":)"));
        appendDec(fileOffset.getData());
        if (redo)
            append(std::string_view(R"(,"redo":true)"));
        append(std::string_view("}]}"));
        builderCommit();
    }

    void BuilderJson::addTagData(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, Format::VALUE_TYPE valueType, FileOffset fileOffset) {
        if (unlikely(table == nullptr || table->tagCols.empty()))
            return;

        const uint64_t messagePositionOld = messagePosition;
        hasPreviousColumn = false;
        // Envelope mode: emit tag columns as a JSON object (Debezium-compatible Kafka key)
        if (envelopeOutput) {
            if (debeziumSchemaBlock)
                appendKeySchemaOpen(table);
            append('{');
        }

        for (uint x = 0; x < table->tagCols.size(); ++x) {
            const typeCol column = table->tagCols[x] - 1;
            if (values[column][static_cast<uint>(valueType)] == nullptr)
                continue;

            if (sizes[column][static_cast<uint>(valueType)] > 0)
                processValue(lobCtx, xmlCtx, table, column, values[column][static_cast<uint>(valueType)], sizes[column][static_cast<uint>(valueType)],
                             fileOffset, true, compressedAfter);
            else
                columnNull(table, column, true);
        }

        if (envelopeOutput) {
            append('}');
            // Close the envelope object opened by the schema block: {"schema":{...},"payload":{...}}
            if (debeziumSchemaBlock)
                append('}');
        }

        if (messagePosition >= messagePositionOld)
            msg->tagSize = messagePosition - messagePositionOld;
        else
            msg->tagSize = messageSize + messagePosition;
    }

    void BuilderJson::appendDdlSource(const DbTable* table, typeObj obj, Scn scn) {
        append(std::string_view(R"("source":{)"));
        bool prev = false;

        comma(prev);
        append(std::string_view(R"("version":"3.6.1.Final")"));
        comma(prev);
        append(std::string_view(R"("connector":"oracle")"));
        comma(prev);
        append(std::string_view(R"("name":")"));
        appendEscape(topicPrefix);
        append('"');
        comma(prev);
        append(std::string_view(R"("ts_ms":)"));
        appendDec(static_cast<uint64_t>(processTsUs) / 1000);
        comma(prev);
        append(std::string_view(R"("snapshot":"false")"));
        comma(prev);
        append(std::string_view(R"("db":")"));
        appendEscape(metadata->context);
        append('"');

        comma(prev);
        if (table != nullptr) {
            append(std::string_view(R"("schema":")"));
            appendEscape(table->owner);
            append(std::string_view(R"(","table":")"));
            appendEscape(table->name);
            append('"');
        } else {
            // DDL on an unknown object: report the object id
            append(std::string_view(R"("schema":null,"table":")"));
            append(std::string_view("OBJ_"));
            appendDec(obj);
            append('"');
        }

        comma(prev);
        append(std::string_view(R"("scn":")"));
        appendDec(scn.getData());
        append('"');
        comma(prev);
        append(std::string_view(R"("commit_scn":")"));
        appendDec(commitScn.getData());
        append('"');
        comma(prev);
        append(std::string_view(R"("ts_us":)"));
        appendDec(static_cast<uint64_t>(processTsUs));
        comma(prev);
        append(std::string_view(R"("ts_ns":)"));
        appendDec(static_cast<uint64_t>(processTsUs) * 1000);
        append('}');
    }

    void BuilderJson::appendNullBefore() {
        append(std::string_view(R"("before":null)"));
    }

    void BuilderJson::appendNullAfter() {
        append(std::string_view(R"(,"after":null)"));
    }

    void BuilderJson::appendSource(const DbTable* table, typeObj obj, Scn scn, typeDataObj dataObj, typeDba bdba, typeSlot slot) {
        const time_t commitTm = commitTimestamp.toEpoch(ctx->hostTimezone);
        const uint64_t commitMs = static_cast<uint64_t>(commitTm) * 1000;
        const time_t beginTm = beginTimestamp.toEpoch(ctx->hostTimezone);
        const uint64_t beginMs = static_cast<uint64_t>(beginTm) * 1000;

        append(std::string_view(R"(,"source":{)"));
        bool prev = false;

        comma(prev);
        append(std::string_view(R"("version":"3.6.1.Final")"));
        comma(prev);
        append(std::string_view(R"("connector":"oracle")"));
        comma(prev);
        append(std::string_view(R"("name":")"));
        appendEscape(topicPrefix);
        append('"');
        comma(prev);
        append(std::string_view(R"("ts_ms":)"));
        appendDec(static_cast<uint64_t>(processTsUs) / 1000);
        comma(prev);
        append(std::string_view(R"("snapshot":"false")"));
        comma(prev);
        append(std::string_view(R"("db":")"));
        appendEscape(metadata->context);
        append('"');
        comma(prev);
        append(std::string_view(R"("sequence":null)"));
        comma(prev);
        append(std::string_view(R"("ts_us":)"));
        appendDec(static_cast<uint64_t>(processTsUs));
        comma(prev);
        append(std::string_view(R"("ts_ns":)"));
        appendDec(static_cast<uint64_t>(processTsUs) * 1000);

        comma(prev);
        if (table != nullptr) {
            append(std::string_view(R"("schema":")"));
            appendEscape(table->owner);
            append(std::string_view(R"(","table":")"));
            appendEscape(table->name);
            append('"');
        } else {
            // No schema information: Debezium reports the table name, OLR only knows the object id
            append(std::string_view(R"("schema":null,"table":")"));
            append(std::string_view("OBJ_"));
            appendDec(obj);
            append('"');
        }

        comma(prev);
        append(std::string_view(R"("txId":")"));
        appendHex16Reversed(lastXid.getData());
        append('"');

        comma(prev);
        append(std::string_view(R"("scn":")"));
        appendDec(scn.getData());
        append('"');
        comma(prev);
        append(std::string_view(R"("commit_scn":")"));
        appendDec(commitScn.getData());
        append('"');

        // Not available from redo log
        comma(prev);
        append(std::string_view(R"("lcr_position":null)"));
        comma(prev);
        append(std::string_view(R"("rs_id":null)"));
        comma(prev);
        append(std::string_view(R"("ssn":0)"));

        comma(prev);
        append(std::string_view(R"("redo_thread":)"));
        appendDec(thread);

        comma(prev);
        const auto itUserName = attributes->find(Attribute::KEY::LOGIN_USER_NAME);
        if (itUserName != attributes->end()) {
            append(std::string_view(R"("user_name":")"));
            appendEscape(itUserName->second);
            append('"');
        } else {
            append(std::string_view(R"("user_name":null)"));
        }

        comma(prev);
        append(std::string_view(R"("redo_sql":null)"));

        comma(prev);
        const RowId rowId(dataObj, bdba, slot);
        char rowIdStr[RowId::SIZE + 1];
        rowId.toString(rowIdStr);
        append(std::string_view(R"("row_id":")"));
        appendArr(rowIdStr, 18);
        append('"');

        comma(prev);
        append(std::string_view(R"("commit_ts_ms":)"));
        appendDec(commitMs);
        comma(prev);
        append(std::string_view(R"("start_scn":")"));
        appendDec(beginScn.getData());
        append('"');
        comma(prev);
        append(std::string_view(R"("start_ts_ms":)"));
        appendDec(beginMs);
        comma(prev);
        append(std::string_view(R"("txSeq":)"));
        appendDec(num);

        append('}');
    }

    // -------------------------------------------------------------------------------------------
    // Debezium standard envelope (schema block), enabled by format option "schema-block": true.
    //
    // The JSON produced by the column*() methods is what the schema must describe, NOT the value
    // Debezium itself would produce: columnNumber emits a bare JSON number, columnRaw emits a hex
    // string, columnTimestamp emits int64 (UNIX_*) or a string (ISO8601 / *_STRING) depending on
    // "format.timestamp", columnTimestampTz always emits a string.
    // -------------------------------------------------------------------------------------------

    // Column schema name prefix: "topic.prefix" when configured, otherwise the database name
    std::string BuilderJson::schemaNamespace() const {
        if (!topicPrefix.empty())
            return topicPrefix;
        return std::string(metadata->context);
    }

    // Columns never emitted by processValue() are skipped in the schema as well (keeps schema and payload aligned)
    bool BuilderJson::isColumnSkipped(const DbColumn* column) {
        if (column->guard && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_GUARD_COLUMNS))
            return true;
        if (column->nested && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_NESTED_COLUMNS))
            return true;
        if (column->hidden && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_HIDDEN_COLUMNS))
            return true;
        if (column->unused && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_UNUSED_COLUMNS))
            return true;
        return false;
    }

    void BuilderJson::appendColumnSchemaDebezium(const DbColumn* column) {
        append('{');
        bool prev = false;

        comma(prev);
        append(std::string_view(R"("field":")"));
        appendEscape(column->name);
        append('"');
        comma(prev);

        // Every column is optional: OLR emits only the columns that carry a value
        switch (column->type) {
            case SysCol::COLTYPE::NUMBER: {
                // Debezium OracleValueConverters.getNumericSchema(): scale present, width = precision - scale
                const int64_t precision = column->precision > 0 ? column->precision : 38;
                const int64_t scale = column->scale > 0 ? column->scale : 0;
                if (scale == 0 && precision < 19) {
                    if (precision < 3)
                        append(std::string_view(R"("type":"int8","optional":true)"));
                    else if (precision < 5)
                        append(std::string_view(R"("type":"int16","optional":true)"));
                    else if (precision < 10)
                        append(std::string_view(R"("type":"int32","optional":true)"));
                    else
                        append(std::string_view(R"("type":"int64","optional":true)"));
                } else {
                    // Decimal: bytes with the unscaled value (JsonConverter encodes them as base64)
                    append(std::string_view(R"("type":"bytes","name":"org.apache.kafka.connect.data.Decimal","version":1,"parameters":{"scale":")"));
                    appendSDec(scale);
                    append(std::string_view(R"(","connect.decimal.precision":")"));
                    appendSDec(precision);
                    append(std::string_view(R"("},"optional":true)"));
                }
                break;
            }

            case SysCol::COLTYPE::FLOAT:
                // Oracle BINARY_FLOAT => FLOAT32 (Debezium: SchemaBuilder.float32())
                append(std::string_view(R"("type":"float","optional":true)"));
                break;

            case SysCol::COLTYPE::DOUBLE:
                // Oracle BINARY_DOUBLE => FLOAT64 (Debezium: SchemaBuilder.float64())
                append(std::string_view(R"("type":"double","optional":true)"));
                break;

            case SysCol::COLTYPE::DATE:
            case SysCol::COLTYPE::TIMESTAMP: {
                // Debezium: TemporalPrecisionMode.ADAPTIVE. Oracle DATE carries scale 0 => millis.
                const int64_t precision = column->type == SysCol::COLTYPE::DATE ? 0
                        : (column->scale >= 0 ? column->scale : 6);
                if (precision <= 3)
                    append(std::string_view(R"("type":"int64","name":"io.debezium.time.Timestamp","version":1,"optional":true)"));
                else if (precision <= 6)
                    append(std::string_view(R"("type":"int64","name":"io.debezium.time.MicroTimestamp","version":1,"optional":true)"));
                else
                    append(std::string_view(R"("type":"int64","name":"io.debezium.time.NanoTimestamp","version":1,"optional":true)"));
                break;
            }

            case SysCol::COLTYPE::TIMESTAMP_WITH_TZ:
            case SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ:
                append(std::string_view(R"("type":"string","name":"io.debezium.time.ZonedTimestamp","version":1,"optional":true)"));
                break;

            case SysCol::COLTYPE::BOOLEAN:
                append(std::string_view(R"("type":"boolean","optional":true)"));
                break;

            case SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND:
            case SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH:
                // Debezium interval.handling.mode=numeric (default) => io.debezium.time.MicroDuration
                append(std::string_view(R"("type":"int64","name":"io.debezium.time.MicroDuration","version":1,"optional":true)"));
                break;

            case SysCol::COLTYPE::JSON:
                append(std::string_view(R"("type":"string","name":"io.debezium.data.Json","version":1,"optional":true)"));
                break;

            case SysCol::COLTYPE::BLOB:
                if (column->xmlType && ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_XMLTYPE))
                    append(std::string_view(R"("type":"string","name":"io.debezium.data.Xml","version":1,"optional":true)"));
                else
                    append(std::string_view(R"("type":"bytes","optional":true)"));
                break;

            case SysCol::COLTYPE::RAW:
                append(std::string_view(R"("type":"bytes","optional":true)"));
                break;

            default:
                // VARCHAR/CHAR/NCHAR/NVARCHAR2/CLOB/NCLOB/ROWID/MROWID => string
                append(std::string_view(R"("type":"string","optional":true)"));
                break;
        }

        append('}');
    }

    void BuilderJson::appendColumnSchema(const DbColumn* column) {
        if (debeziumSchemaBlock) {
            appendColumnSchemaDebezium(column);
            return;
        }

        append('{');
        bool prev = false;

        comma(prev);
        append(std::string_view(R"("field":")"));
        appendEscape(column->name);
        append('"');

        // Every column is optional: OLR emits only the columns that carry a value (changed columns on
        // UPDATE, non null columns, ...), and Kafka Connect requires optional fields to be allowed to
        // be absent.
        comma(prev);
        switch (column->type) {
            case SysCol::COLTYPE::NUMBER:
                // columnNumber() emits a bare JSON number => int64 when integer, double otherwise.
                // NOTE: JsonConverter expects the name "double" for Schema.Type.FLOAT64 (it rejects
                // "float64" with "Unknown schema type"; verified against the real JsonConverter).
                if (column->scale == 0 && column->precision <= 18)
                    append(std::string_view(R"("type":"int64","optional":true)"));
                else
                    append(std::string_view(R"("type":"double","optional":true)"));
                break;

            case SysCol::COLTYPE::FLOAT:
            case SysCol::COLTYPE::DOUBLE:
                append(std::string_view(R"("type":"double","optional":true)"));
                break;

            case SysCol::COLTYPE::DATE:
            case SysCol::COLTYPE::TIMESTAMP:
                // columnTimestamp() follows format.timestampFormat
                switch (format.timestampFormat) {
                    case Format::TIMESTAMP_FORMAT::UNIX_NANO:
                        append(std::string_view(R"("type":"int64","name":"io.debezium.time.NanoTimestamp","version":1,"optional":true)"));
                        break;
                    case Format::TIMESTAMP_FORMAT::UNIX_MICRO:
                        append(std::string_view(R"("type":"int64","name":"io.debezium.time.MicroTimestamp","version":1,"optional":true)"));
                        break;
                    case Format::TIMESTAMP_FORMAT::UNIX_MILLI:
                    case Format::TIMESTAMP_FORMAT::UNIX:
                        append(std::string_view(R"("type":"int64","name":"io.debezium.time.Timestamp","version":1,"optional":true)"));
                        break;
                    default:
                        // ISO8601 / *_STRING variants are emitted as JSON strings
                        append(std::string_view(R"("type":"string","optional":true)"));
                        break;
                }
                break;

            case SysCol::COLTYPE::TIMESTAMP_WITH_TZ:
            case SysCol::COLTYPE::TIMESTAMP_WITH_LOCAL_TZ:
                // columnTimestampTz() emits a string (e.g. "1700000000.123456789,Europe/Warsaw")
                append(std::string_view(R"("type":"string","name":"io.debezium.time.ZonedTimestamp","version":1,"optional":true)"));
                break;

            case SysCol::COLTYPE::INTERVAL_DAY_TO_SECOND:
                if (format.intervalDtsFormat >= Format::INTERVAL_DTS_FORMAT::ISO8601_SPACE)
                    append(std::string_view(R"("type":"string","optional":true)"));
                else
                    append(std::string_view(R"("type":"int64","optional":true)"));
                break;

            case SysCol::COLTYPE::INTERVAL_YEAR_TO_MONTH:
                if (format.intervalYtmFormat >= Format::INTERVAL_YTM_FORMAT::STRING_YM_SPACE)
                    append(std::string_view(R"("type":"string","optional":true)"));
                else
                    append(std::string_view(R"("type":"int64","optional":true)"));
                break;

            default:
                // VARCHAR/CHAR/LONG/CLOB/XMLTYPE -> JSON string (columnString)
                // RAW/LONG_RAW/BLOB/JSON/UROWID -> hex JSON string (columnRaw / columnRowId)
                // BOOLEAN and anything unrecognised -> string is the most tolerant choice
                append(std::string_view(R"("type":"string","optional":true)"));
                break;
        }

        append('}');
    }

    // Emits the field descriptors of the row object ("before"/"after") without enclosing braces.
    // Kafka Connect's JsonConverter requires nested struct descriptors to be FLATTENED: the child
    // schema must be written inline inside the parent field descriptor, i.e.
    //   {"field":"after","type":"struct","optional":true,"name":"...Value","fields":[...]}
    // Wrapping it as {"field":"after","optional":true,"type":{...}} is rejected by JsonConverter
    // with "Schema must contain 'type' field" (verified against the real JsonConverter).
    void BuilderJson::appendValueFields(const DbTable* table) {
        bool prev = false;
        for (typeCol col = 0; col < table->maxSegCol; ++col) {
            const DbColumn* column = table->columns[col];
            if (column == nullptr || isColumnSkipped(column))
                continue;

            comma(prev);
            appendColumnSchema(column);
        }
    }

    // Emits the field descriptors of the "source" object without enclosing braces (flat, see above)
    void BuilderJson::appendSourceFields() {
        // Field list mirrors appendSource() one to one (same names, same JSON types, all optional)
        append(std::string_view(R"({"field":"version","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"connector","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"name","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"ts_ms","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"snapshot","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"db","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"sequence","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"ts_us","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"ts_ns","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"schema","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"table","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"txId","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"scn","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"commit_scn","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"lcr_position","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"rs_id","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"ssn","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"redo_thread","type":"int32","optional":true},)"));
        append(std::string_view(R"({"field":"user_name","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"redo_sql","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"row_id","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"commit_ts_ms","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"start_scn","type":"string","optional":true},)"));
        append(std::string_view(R"({"field":"start_ts_ms","type":"int64","optional":true},)"));
        append(std::string_view(R"({"field":"txSeq","type":"int64","optional":true})"));
    }

    void BuilderJson::appendEnvelopeSchemaOpen(const DbTable* table) {
        // Nested struct descriptors are FLATTENED (verified against the real JsonConverter):
        //   {"field":"after","type":"struct","optional":true,"name":"<ns>.<owner>.<table>.Value","fields":[...]}
        append(std::string_view(R"({"schema":{"type":"struct","name":")"));
        appendEscape(schemaNamespace());
        append('.');
        appendEscape(table->owner);
        append('.');
        appendEscape(table->name);
        append(std::string_view(R"(.Envelope","version":2,"optional":false,"fields":[)"));
        append(std::string_view(R"({"field":"before","type":"struct","optional":true,"name":")"));
        appendEscape(schemaNamespace());
        append('.');
        appendEscape(table->owner);
        append('.');
        appendEscape(table->name);
        append(std::string_view(R"(.Value","fields":[)"));
        appendValueFields(table);
        append(std::string_view(R"(]},{"field":"after","type":"struct","optional":true,"name":")"));
        appendEscape(schemaNamespace());
        append('.');
        appendEscape(table->owner);
        append('.');
        appendEscape(table->name);
        append(std::string_view(R"(.Value","fields":[)"));
        appendValueFields(table);
        append(std::string_view(R"(]},{"field":"source","type":"struct","optional":false,"name":"io.debezium.connector.oracle.Source","version":1,"fields":[)"));
        appendSourceFields();
        append(std::string_view(R"(]},{"field":"op","type":"string","optional":false},{"field":"ts_ms","type":"int64","optional":true}]},"payload":)"));
    }

    void BuilderJson::appendKeySchemaOpen(const DbTable* table) {
        append(std::string_view(R"({"schema":{"type":"struct","name":")"));
        appendEscape(schemaNamespace());
        append('.');
        appendEscape(table->owner);
        append('.');
        appendEscape(table->name);
        append(std::string_view(R"(.Key","optional":false,"fields":[)"));

        bool prev = false;
        for (const typeCol tag : table->tagCols) {
            const typeCol col = tag - 1;
            if (col >= table->maxSegCol)
                continue;
            const DbColumn* column = table->columns[col];
            if (column == nullptr || isColumnSkipped(column))
                continue;

            comma(prev);
            appendColumnSchema(column);
        }

        append(std::string_view(R"(]},"payload":)"));
    }
}
