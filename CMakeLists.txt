# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.25)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

project(lance-cpp
        DESCRIPTION "C++ bindings for Lance Columnar format"
        LANGUAGES CXX)
set(PROJECT_VERSION "2.0.0-beta.3")

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(LANCE_CPP_BUILD_STATIC "Build static library" ON)
option(LANCE_CPP_BUILD_SHARED "Build shared library" ON)
option(LANCE_CPP_BUILD_TESTS "Build lance-cpp tests" ON)
option(LANCE_CPP_ENABLE_ASAN "Enable Address Sanitizer" OFF)
option(LANCE_CPP_ENABLE_UBSAN "Enable Undefined Behavior Sanitizer" OFF)

if(LANCE_CPP_BUILD_TESTS)
  enable_testing()
endif()

include(CMakePackageConfigHelpers)
include(FetchContent)
include(GNUInstallDirs)

set(LANCE_CPP_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
set(LANCE_CPP_INSTALL_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}")
set(LANCE_CPP_INSTALL_CONFIGDIR "${LANCE_CPP_INSTALL_LIBDIR}/cmake/LanceCpp")

include(LanceSanitizer)
include(ThirdpartyToolchain)

add_subdirectory(lance-ffi)
add_subdirectory(src)

if(LANCE_CPP_BUILD_TESTS)
  add_subdirectory(tests)
endif()
