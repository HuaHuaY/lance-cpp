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

set(LANCE_CPP_SOURCES dataset.cpp lib.cpp result.cpp)

get_target_property(LANCE_CXXBRIDGE_INCS lance_cxxbridge INTERFACE_INCLUDE_DIRECTORIES)

if(LANCE_CPP_BUILD_STATIC)
  add_library(lance_cpp_static STATIC ${LANCE_CPP_SOURCES})
  target_link_libraries(lance_cpp_static PUBLIC lance_sanitizer_flags lance_cxxbridge)
  target_include_directories(lance_cpp_static
                             PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
                                    $<INSTALL_INTERFACE:${LANCE_CPP_INSTALL_INCLUDEDIR}>)
  target_include_directories(lance_cpp_static SYSTEM PRIVATE ${LANCE_CXXBRIDGE_INCS})
  install(TARGETS lance_cpp_static lance_cxxbridge lance_sanitizer_flags
          EXPORT LanceCppTargets
          DESTINATION ${LANCE_CPP_INSTALL_LIBDIR})
endif()

if(LANCE_CPP_BUILD_SHARED)
  add_library(lance_cpp_shared SHARED ${LANCE_CPP_SOURCES})
  target_link_libraries(lance_cpp_shared
                        PUBLIC lance_sanitizer_flags
                        PRIVATE lance_cxxbridge)
  target_include_directories(lance_cpp_shared
                             PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
                                    $<INSTALL_INTERFACE:${LANCE_CPP_INSTALL_INCLUDEDIR}>)
  target_include_directories(lance_cpp_shared SYSTEM PRIVATE ${LANCE_CXXBRIDGE_INCS})
  install(TARGETS lance_cpp_shared
          EXPORT LanceCppTargets
          DESTINATION ${LANCE_CPP_INSTALL_LIBDIR})
endif()

install(DIRECTORY ../include/ DESTINATION ${LANCE_CPP_INSTALL_INCLUDEDIR})

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/LanceCppConfigVersion.cmake
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY AnyNewerVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/LanceCppConfigVersion.cmake
        DESTINATION ${LANCE_CPP_INSTALL_CONFIGDIR})

install(EXPORT LanceCppTargets
        NAMESPACE LanceCpp::
        DESTINATION ${LANCE_CPP_INSTALL_CONFIGDIR})
configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/lance-cpp-config.cmake.in
                              ${CMAKE_CURRENT_BINARY_DIR}/LanceCppConfig.cmake
                              INSTALL_DESTINATION ${LANCE_CPP_INSTALL_CONFIGDIR}
                              NO_SET_AND_CHECK_MACRO)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/LanceCppConfig.cmake
        DESTINATION ${LANCE_CPP_INSTALL_CONFIGDIR})
