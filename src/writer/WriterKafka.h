/* Header for WriterKafka class
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

#ifndef WRITER_KAFKA_H_
#define WRITER_KAFKA_H_

#include <librdkafka/rdkafka.h>

#include <map>
#include <unordered_map>
#include "Writer.h"

namespace OpenLogReplicator {
    class WriterKafka final : public Writer {
    protected:
        std::string topic;
        // Per-table topic routing (Debezium-compatible naming): [<prefix>.]<SCHEMA>.<TABLE>
        std::string topicPrefix;
        bool dynamicTopic{false};
        // Debezium-envelope mode: checkpoint messages are internal (position only), never published
        bool envelopeOutput{false};
        // Dedicated topic receiving DDL/schema-change envelope messages (empty = such messages are dropped upstream)
        std::string schemaChangeTopic;
        rd_kafka_topic_t* schemaChangeRkt{nullptr};
        std::unordered_map<typeObj, rd_kafka_topic_t*> topicCache;
        char errStr[512]{};
        std::map<std::string, std::string> properties;
        rd_kafka_t* rk{nullptr};
        rd_kafka_topic_t* rkt{nullptr};
        rd_kafka_conf_t* conf{nullptr};
        static void dr_msg_cb(rd_kafka_t* rkCb, const rd_kafka_message_t* rkMessage, void* opaque);
        static void error_cb(rd_kafka_t* rkCb, int err, const char* reason, void* opaque);
        static void logger_cb(const rd_kafka_t* rkCb, int level, const char* fac, const char* buf);

        rd_kafka_topic_t* getTopicForObj(typeObj obj);
        void sendMessage(BuilderMsg* msg) override;
        std::string getType() const override;
        void pollQueue() override;

    public:
        static constexpr uint64_t MAX_KAFKA_MESSAGE_MB = 953;

        WriterKafka(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, std::string newTopic,
                   std::string newTopicPrefix = "", bool newDynamicTopic = false, bool newEnvelopeOutput = false, std::string newSchemaChangeTopic = "");
        ~WriterKafka() override;

        void addProperty(std::string key, std::string value);
        void initialize() override;
    };
}

#endif
