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

#include "lance/dataset.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <string_view>

#include "util/test_util.hpp"

namespace lance::test {

class LanceDatasetTest : public ::testing::Test {
 public:
  auto GetTempPath(std::string_view filename) -> std::string {
    std::filesystem::path path = temp_dir_ / filename;
    return path.string();
  }

 protected:
  void SetUp() override {}

  void TearDown() override {}

 private:
  std::filesystem::path temp_dir_{std::filesystem::temp_directory_path()};
};

TEST_F(LanceDatasetTest, TestCreateEmptyDataset) {
  std::string test_path = GetTempPath("empty_dataset");
  SimpleTestDataset test_dataset(std::move(test_path));
  auto _ = test_dataset.CreateEmptyDataset();

  auto result = Dataset::Open(test_dataset.GetDatasetPath(), {});
  ASSERT_TRUE(result.has_value());
}

}  // namespace lance::test
