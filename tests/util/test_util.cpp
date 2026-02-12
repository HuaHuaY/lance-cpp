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
#include <format>
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

auto TestDataset::Write(int row_count) const -> Dataset {
  ArrowArrayStream stream = CreateDataStream(row_count);
  Result<Dataset> ret = Dataset::Append(dataset_path_, stream);
  assert(ret.has_value());
  return std::move(*ret);
}

auto SimpleTestDataset::GetSchema() const -> ArrowSchema {
  static auto create_schema = [](ArrowSchema& out) -> ArrowErrorCode {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(&out, 2));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(out.children[0], NANOARROW_TYPE_INT32));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out.children[0], "id"));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(out.children[1], NANOARROW_TYPE_STRING));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out.children[1], "name"));
    return NANOARROW_OK;
  };
  static ArrowSchema base_schema = []() -> ArrowSchema {
    ArrowSchema schema;
    NANOARROW_ASSERT_OK(create_schema(schema));  // NOLINT(modernize-use-std-print)
    return schema;
  }();

  ArrowSchema copy;
  NANOARROW_ASSERT_OK(ArrowSchemaDeepCopy(&base_schema, &copy));  // NOLINT(modernize-use-std-print)
  return copy;
}

auto SimpleTestDataset::CreateDataStream(int row_count) const -> ArrowArrayStream {
  ArrowArrayStream stream;
  ArrowSchema schema = GetSchema();

  auto init_array_stream = [&stream, &schema, row_count]() -> ArrowErrorCode {
    ArrowArray array;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(&array, &schema, nullptr));

    ArrowArray* id_array = array.children[0];
    ArrowArray* name_array = array.children[1];

    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(&array));
    for (int i = 0; i < row_count; ++i) {
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(id_array, i));
      std::string name = std::format("Person {}", i);
      ArrowStringView name_view = {name.c_str(), static_cast<int64_t>(name.size())};
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(name_array, name_view));
      NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(&array));
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(&array, nullptr));

    NANOARROW_RETURN_NOT_OK(ArrowBasicArrayStreamInit(&stream, &schema, 1));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);

    return NANOARROW_OK;
  };

  NANOARROW_ASSERT_OK(init_array_stream());  // NOLINT(modernize-use-std-print)
  return stream;
}

}  // namespace lance::test
