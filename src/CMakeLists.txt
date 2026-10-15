# Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)
#
# This file is part of OpenLogReplicator.
#
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with this program; see the file LICENSE;
# If not, see <http://www.gnu.org/licenses/>.

list(APPEND ListCommon
        common/ClockHW.cpp
        common/Ctx.cpp
        common/DbLob.cpp
        common/DbTable.cpp
        common/LobCtx.cpp
        common/LobData.cpp
        common/LobKey.cpp
        common/MemoryManager.cpp
        common/Thread.cpp
        common/XmlCtx.cpp
        common/exception/BootException.cpp
        common/exception/ConfigurationException.cpp
        common/exception/DataException.cpp
        common/exception/NetworkException.cpp
        common/exception/RedoLogException.cpp
        common/exception/RuntimeException.cpp
        common/expression/BoolValue.cpp
        common/expression/Expression.cpp
        common/expression/StringValue.cpp
        common/expression/Token.cpp
        common/metrics/Metrics.cpp
        common/types/Data.cpp)

list(APPEND ListReplicator
        replicator/Replicator.cpp
        replicator/ReplicatorBatch.cpp)

list(APPEND ListLocales
        locales/CharacterSet.cpp
        locales/CharacterSet16bit.cpp
        locales/CharacterSet7bit.cpp
        locales/CharacterSet8bit.cpp
        locales/CharacterSetAL16UTF16.cpp
        locales/CharacterSetAL32UTF8.cpp
        locales/CharacterSetJA16EUC.cpp
        locales/CharacterSetJA16EUCTILDE.cpp
        locales/CharacterSetJA16SJIS.cpp
        locales/CharacterSetJA16SJISTILDE.cpp
        locales/CharacterSetKO16KSCCS.cpp
        locales/CharacterSetUTF8.cpp
        locales/CharacterSetZHS16GBK.cpp
        locales/CharacterSetZHS32GB18030.cpp
        locales/CharacterSetZHT16HKSCS31.cpp
        locales/CharacterSetZHT32EUC.cpp
        locales/CharacterSetZHT32TRIS.cpp
        locales/Locales.cpp)

list(APPEND ListBuilder
        builder/Builder.cpp
        builder/BuilderJson.cpp
        builder/SystemTransaction.cpp)

list(APPEND ListParser
        parser/CommitSorter.cpp
        parser/DeliveryExecutor.cpp
        parser/Parser.cpp
        parser/Transaction.cpp
        parser/TransactionBuffer.cpp)

list(APPEND ListReader
        reader/Reader.cpp
        reader/ReaderFilesystem.cpp)

list(APPEND ListMetadata
        metadata/Checkpoint.cpp
        metadata/Metadata.cpp
        metadata/Schema.cpp
        metadata/Serializer.cpp
        metadata/SerializerJson.cpp)

list(APPEND ListState
        state/State.cpp
        state/StateDisk.cpp)

list(APPEND ListWriter
        writer/Writer.cpp
        writer/WriterDiscard.cpp
        writer/WriterFile.cpp)

if (WITH_OCI)
    list(APPEND ListReplicator
            replicator/DatabaseConnection.cpp
            replicator/DatabaseEnvironment.cpp
            replicator/DatabaseStatement.cpp
            replicator/ReplicatorOnline.cpp)
endif ()

if (WITH_RDKAFKA)
    list(APPEND ListWriter
            writer/WriterKafka.cpp)
endif ()

if (WITH_PROMETHEUS)
    list(APPEND ListCommon
            common/metrics/MetricsPrometheus.cpp)
endif ()

if (WITH_PROTOBUF)
    list(APPEND ListCommon
            common/OraProtoBuf.pb.cpp)
    list(APPEND ListBuilder
            builder/BuilderProtobuf.cpp)
    list(APPEND ListStream
            stream/Stream.cpp
            stream/StreamNetwork.cpp)
    list(APPEND ListWriter
            writer/WriterStream.cpp)

    if (WITH_ZEROMQ)
        list(APPEND ListStream
                stream/StreamZeroMQ.cpp)
    endif ()
endif ()

add_library(LibCommon STATIC ${ListCommon})
add_library(LibReplicator STATIC ${ListReplicator})
add_library(LibLocales STATIC ${ListLocales})
add_library(LibBuilder STATIC ${ListBuilder})
add_library(LibParser STATIC ${ListParser})
add_library(LibReader STATIC ${ListReader})
add_library(LibMetadata STATIC ${ListMetadata})
add_library(LibState STATIC ${ListState})
add_library(LibWriter STATIC ${ListWriter})

list(APPEND OLR_LIBS LibCommon)
list(APPEND OLR_LIBS LibReplicator)
list(APPEND OLR_LIBS LibLocales)
list(APPEND OLR_LIBS LibBuilder)
list(APPEND OLR_LIBS LibParser)
list(APPEND OLR_LIBS LibReader)
list(APPEND OLR_LIBS LibMetadata)
list(APPEND OLR_LIBS LibState)
list(APPEND OLR_LIBS LibWriter)

if (WITH_PROTOBUF)
    add_library(LibStream ${ListStream})
    list(APPEND OLR_LIBS LibStream)

    target_sources(StreamClient PUBLIC StreamClient.cpp)
    target_link_libraries(StreamClient LibCommon)
    target_link_libraries(StreamClient LibStream)
endif ()

set(OLR_LIBS ${OLR_LIBS} PARENT_SCOPE)
