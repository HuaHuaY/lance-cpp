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

project(lance_cpp_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

option(LANCE_CPP_BUILD_STATIC_TEST "Build static library test" ON)
option(LANCE_CPP_BUILD_SHARED_TEST "Build shared library test" ON)

find_package(LanceCpp CONFIG REQUIRED)

if(LANCE_CPP_BUILD_STATIC_TEST)
  add_executable(basic_usage_static basic_usage.cpp)
  target_link_libraries(basic_usage_static PRIVATE LanceCpp::lance_cpp_static)
endif()

if(LANCE_CPP_BUILD_SHARED_TEST)
  add_executable(basic_usage_shared basic_usage.cpp)
  target_link_libraries(basic_usage_shared PRIVATE LanceCpp::lance_cpp_shared)
endif()
