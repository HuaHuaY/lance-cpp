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

set(LANCE_CPP_SYSTEM_DEPENDENCIES "")
set(LANCE_CPP_VENDOR_DEPENDENCIES "")

set(LANCE_CPP_NANOARROW_BUILD_VERSION "0.7.0")
set(LANCE_CPP_NANOARROW_BUILD_SHA512
    "241142f33358c2e89cf7033865978fbb8b214af920f04e426121c5c0b9f06b4d87b6bc0ade691c38aea80a37e1c00f569b0840518742d4ecfd8838c5b859f467"
)
if(DEFINED ENV{LANCE_CPP_NANOARROW_URL})
  set(LANCE_CPP_NANOARROW_URL "$ENV{LANCE_CPP_NANOARROW_URL}")
else()
  set(LANCE_CPP_NANOARROW_URL
      "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/apache-arrow-nanoarrow-${LANCE_CPP_NANOARROW_BUILD_VERSION}/apache-arrow-nanoarrow-${LANCE_CPP_NANOARROW_BUILD_VERSION}.tar.gz"
  )
endif()

macro(prepare_fetchcontent)
  set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY TRUE)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endmacro()

function(resolve_dependency PACKAGE_NAME PACKAGE_URL PACKAGE_SHA512)
  prepare_fetchcontent()

  fetchcontent_declare(${PACKAGE_NAME}
                       URL ${PACKAGE_URL}
                       URL_HASH "SHA512=${PACKAGE_SHA512}")
  fetchcontent_makeavailable(${PACKAGE_NAME})

  if(${PACKAGE_NAME}_SOURCE_DIR)
    if(TARGET ${PACKAGE_NAME}::${PACKAGE_NAME})
      list(APPEND LANCE_CPP_VENDOR_DEPENDENCIES ${PACKAGE_NAME}::${PACKAGE_NAME})
    else()
      list(APPEND LANCE_CPP_VENDOR_DEPENDENCIES ${PACKAGE_NAME})
      add_library(${PACKAGE_NAME}::${PACKAGE_NAME} ALIAS ${PACKAGE_NAME})
    endif()
    set(LANCE_CPP_VENDOR_DEPENDENCIES
        ${LANCE_CPP_VENDOR_DEPENDENCIES}
        PARENT_SCOPE)
  elseif(${LANCE_CPP_BUILD_STATIC})
    list(APPEND LANCE_CPP_SYSTEM_DEPENDENCIES ${PACKAGE_NAME})
    set(LANCE_CPP_SYSTEM_DEPENDENCIES
        ${LANCE_CPP_SYSTEM_DEPENDENCIES}
        PARENT_SCOPE)
  endif()
endfunction()

resolve_dependency(nanoarrow "${LANCE_CPP_NANOARROW_URL}"
                   "${LANCE_CPP_NANOARROW_BUILD_SHA512}")
