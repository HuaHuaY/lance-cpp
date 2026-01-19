/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "test_util.hpp"

#include <cassert>
#include <cstring>
#include <string>
#include <string_view>

#include "lance/dataset.hpp"
#include "nanoarrow/common/inline_types.h"
#include "nanoarrow/nanoarrow.h"
#include "test_config.hpp"

namespace lance::test {

auto GetResourcePath(std::string_view filename) -> std::string {
  std::string result;
  result.reserve(strlen(LANCE_CPP_TEST_RESOURCES) + 1 + filename.size());
  result += LANCE_CPP_TEST_RESOURCES;
  result += "/";
  result += filename;
  return result;
}

TestDataset::TestDataset(std::string dataset_path) : dataset_path_(std::move(dataset_path)) {}

auto TestDataset::CreateEmptyDataset() const -> Dataset {
  ArrowSchema schema = GetSchema();
  Result<Dataset> ret = Dataset::Create(dataset_path_, schema);
  assert(ret.has_value());
  return std::move(*ret);
}

auto SimpleTestDataset::GetSchema() const -> ArrowSchema {
  static auto create_schema = [](ArrowSchema& schema) -> ArrowErrorCode {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(&schema, 2));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema.children[0], NANOARROW_TYPE_INT32));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema.children[0], "id"));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema.children[1], NANOARROW_TYPE_STRING));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema.children[1], "name"));
    return NANOARROW_OK;
  };
  static ArrowSchema schema = []() -> ArrowSchema {
    ArrowSchema schema;
    NANOARROW_ASSERT_OK(create_schema(schema));  // NOLINT(modernize-use-std-print)
    return schema;
  }();
  return schema;
}

}  // namespace lance::test
