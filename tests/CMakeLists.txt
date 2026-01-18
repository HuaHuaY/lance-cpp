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

fetchcontent_declare(googletest
                     GIT_REPOSITORY https://github.com/google/googletest.git
                     GIT_TAG fa8438ae6b70c57010177de47a9f13d7041a6328)
fetchcontent_makeavailable(googletest)

set(LANCE_CPP_TEST_RESOURCES "${CMAKE_CURRENT_SOURCE_DIR}/resources")
configure_file("test_config.hpp.in" "test_config.hpp")
set(LANCE_CPP_TEST_RESOURCES_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}")

function(add_lance_cpp_test TEST_NAME)
  set(OPTIONS)
  set(ONE_VALUE_KEYWORDS)
  set(MULTI_VALUE_KEYWORDS SOURCES)
  cmake_parse_arguments(ARG
                        "${OPTIONS}"
                        "${ONE_VALUE_KEYWORDS}"
                        "${MULTI_VALUE_KEYWORDS}"
                        ${ARGN})

  add_executable(${TEST_NAME} ${ARG_SOURCES})
  target_include_directories(${TEST_NAME} PRIVATE ${LANCE_CPP_TEST_RESOURCES_HEADER_DIR})
  target_link_libraries(${TEST_NAME} PRIVATE GTest::gtest_main
                                             ${LANCE_CPP_VENDOR_DEPENDENCIES})
  if(LANCE_CPP_BUILD_SHARED)
    target_link_libraries(${TEST_NAME} PRIVATE lance_cpp_shared)
  else()
    target_link_libraries(${TEST_NAME} PRIVATE lance_cpp_static)
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

add_lance_cpp_test(dataset_test SOURCES util/test_util.cpp dataset_test.cpp)
