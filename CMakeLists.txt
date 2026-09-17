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

cmake_minimum_required(VERSION 3.6)
project(OpenLogReplicator VERSION 2.0.0 LANGUAGES CXX)

set(SCHEMA_VERSION "2.0.0")
string(TIMESTAMP CMAKE_BUILD_TIMESTAMP "%Y-%m-%d %H:%M" UTC)
add_compile_options(-std=c++17)
if (OSX_ARCH)
    set(CMAKE_OSX_ARCHITECTURES ${OSX_ARCH})
endif ()
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(CMAKE_MACOSX_RPATH 1)
    set(MACOSX TRUE)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual  -DCTXASSERT=0")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -pedantic  -fno-omit-frame-pointer -fsanitize=address -fsanitize=shift -fsanitize=shift-exponent -fsanitize=shift-base -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=signed-integer-overflow -fsanitize=bounds -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=bool -fsanitize=enum -fsanitize=vptr -fsanitize=pointer-overflow -fsanitize=builtin -DCTXASSERT=1")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
configure_file(config.h.in ../config.h)

if (CPU_ARCH)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    add_compile_definitions(CPU_ARCH=${CPU_ARCH})
endif()

if (THREAD_INFO)
    add_definitions(-DTHREAD_INFO)
endif()

find_package(Threads REQUIRED)

# RapidJSON
if (WITH_RAPIDJSON)
    include_directories(SYSTEM ${WITH_RAPIDJSON}/include)
else ()
    message(FATAL_ERROR "plase provide WITH_RAPIDJSON parameter with path to rapidjson library")
endif ()

# Oracle, only dynamic
if (WITH_OCI)
    include_directories(SYSTEM ${WITH_OCI}/sdk/include)
    link_directories(${WITH_OCI})
    add_definitions(-DLINK_LIBRARY_OCI)
endif ()

if (WITH_STATIC)
    add_definitions(-DLINK_STATIC)
endif ()

# Protobuf, dynamic or static
if (WITH_PROTOBUF)
    include_directories(SYSTEM ${WITH_PROTOBUF}/include)
    add_definitions(-DLINK_LIBRARY_PROTOBUF)

    if (WITH_STATIC)
        add_library(static_protobuf STATIC IMPORTED)
        set_target_properties(static_protobuf PROPERTIES IMPORTED_LOCATION ${WITH_PROTOBUF}/lib/libprotobuf.a)
    else ()
        link_directories(${WITH_PROTOBUF}/lib)
    endif ()

    # ZeroMQ, just dynamic
    if (WITH_ZEROMQ)
        include_directories(SYSTEM ${WITH_ZEROMQ}/include)
        add_definitions(-DLINK_LIBRARY_ZEROMQ)
        if (MACOSX)
            link_directories(${WITH_ZEROMQ}/lib)
        else ()
            link_directories(${WITH_ZEROMQ}/lib/x86_64-linux-gnu)
        endif ()
    endif ()
endif ()

# Kafka, dynamic or static
if (WITH_RDKAFKA)
    include_directories(SYSTEM ${WITH_RDKAFKA}/include)
    add_definitions(-DLINK_LIBRARY_RDKAFKA)

    if (WITH_STATIC)
        find_package(OpenSSL REQUIRED)
        add_library(static_rdkafka1 STATIC IMPORTED)
        add_library(static_rdkafka2 STATIC IMPORTED)
        set_target_properties(static_rdkafka1 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka.a)
        set_target_properties(static_rdkafka2 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka++.a)
        add_library(static_rdkafka INTERFACE IMPORTED)
        set_property(TARGET static_rdkafka PROPERTY INTERFACE_LINK_LIBRARIES static_rdkafka1 static_rdkafka2 OpenSSL::Crypto OpenSSL::SSL sasl2)
    else ()
        link_directories(${WITH_RDKAFKA}/lib)
    endif ()
endif ()

# Prometheus, only dynamic
if (WITH_PROMETHEUS)
    include_directories(SYSTEM ${WITH_PROMETHEUS}/include)
    link_directories(${WITH_PROMETHEUS}/lib)
    add_definitions(-DLINK_LIBRARY_PROMETHEUS)
endif ()

add_executable(OpenLogReplicator src/OpenLogReplicator.cpp src/main.cpp)

if (WITH_PROTOBUF)
    add_executable(StreamClient ${SOURCE_FILES})
endif ()

add_subdirectory(src)
target_link_libraries(OpenLogReplicator -Wl,--start-group ${OLR_LIBS} -Wl,--end-group)

target_link_libraries(OpenLogReplicator Threads::Threads)

if (WITH_OCI)
    target_link_libraries(OpenLogReplicator nnz11 clntsh)
endif ()

if (WITH_RDKAFKA)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_rdkafka)
    else ()
        target_link_libraries(OpenLogReplicator rdkafka++ rdkafka)
    endif ()
endif ()

if (WITH_PROMETHEUS)
    target_link_libraries(OpenLogReplicator prometheus-cpp-core prometheus-cpp-pull)
endif ()

if (WITH_PROTOBUF)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_protobuf)
        target_link_libraries(StreamClient static_protobuf)
    else ()
        target_link_libraries(OpenLogReplicator protobuf)
        target_link_libraries(StreamClient protobuf)
    endif ()
    target_link_libraries(StreamClient Threads::Threads)

    if (WITH_PROMETHEUS)
        target_link_libraries(StreamClient prometheus-cpp-core prometheus-cpp-pull)
    endif ()

    if (WITH_ZEROMQ)
        target_link_libraries(OpenLogReplicator zmq)
        target_link_libraries(StreamClient zmq)
    endif ()
endif ()

target_include_directories(OpenLogReplicator PUBLIC "${PROJECT_BINARY_DIR}")
