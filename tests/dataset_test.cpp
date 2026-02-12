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

#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>

#include "util/test_util.hpp"

namespace lance::test {

class LanceDatasetTest : public ::testing::Test {
 public:
  auto GetTempPath(std::string_view filename) -> std::string {
    return (temp_dir_ / filename).string();
  }

 protected:
  auto SetUp() -> void override { std::filesystem::create_directories(temp_dir_); }

  auto TearDown() -> void override { std::filesystem::remove_all(temp_dir_); }

 private:
  std::filesystem::path temp_dir_{std::filesystem::temp_directory_path() / "lance_cpp_test"};
};

TEST_F(LanceDatasetTest, TestCreateEmptyDataset) {
  std::string test_path = GetTempPath("new_empty_dataset");
  SimpleTestDataset test_dataset(std::move(test_path));
  auto _ = test_dataset.CreateEmptyDataset();

  auto result = Dataset::Open(test_dataset.GetDatasetPath(), {});
  ASSERT_TRUE(result.has_value());
}

TEST_F(LanceDatasetTest, TestDatasetVersion) {
  std::string test_path = GetTempPath("dataset_version");
  SimpleTestDataset test_dataset(std::move(test_path));

  auto dataset = test_dataset.CreateEmptyDataset();
  ASSERT_EQ(dataset.GetVersion(), 1);

  auto latest_version = dataset.GetLatestVersion();
  ASSERT_TRUE(latest_version.has_value());
  ASSERT_EQ(*latest_version, 1);

  auto version_info = dataset.GetVersionInfo();
  ASSERT_EQ(version_info.GetId(), 1);
  auto min_timestamp = std::chrono::sys_days{std::chrono::year{2020} / 1 / 1};
  ASSERT_GT(version_info.GetTimestamp(), min_timestamp);

  auto row_count = dataset.CountRows();
  ASSERT_TRUE(row_count.has_value());
  ASSERT_EQ(*row_count, 0);

  auto dataset_v2 = test_dataset.Write(5);
  ASSERT_EQ(dataset_v2.GetVersion(), 2);

  auto row_count_v2 = dataset_v2.CountRows();
  ASSERT_TRUE(row_count_v2.has_value());
  ASSERT_EQ(*row_count_v2, 5);

  auto dataset_at_v1 = Dataset::Open(test_dataset.GetDatasetPath(), 1, {});
  ASSERT_TRUE(dataset_at_v1.has_value());
  ASSERT_EQ(dataset_at_v1->GetVersion(), 1);

  auto row_count_v1 = dataset_at_v1->CountRows();
  ASSERT_TRUE(row_count_v1.has_value());
  ASSERT_EQ(*row_count_v1, 0);

  auto dataset_v3 = test_dataset.Write(3);
  ASSERT_EQ(dataset_v3.GetVersion(), 3);

  auto row_count_v3 = dataset_v3.CountRows();
  ASSERT_TRUE(row_count_v3.has_value());
  ASSERT_EQ(*row_count_v3, 8);

  auto versions = dataset_v3.ListVersions();
  ASSERT_TRUE(versions.has_value());
  ASSERT_EQ(versions->size(), 3);

  ASSERT_EQ((*versions)[0].GetId(), 1);
  ASSERT_EQ((*versions)[1].GetId(), 2);
  ASSERT_EQ((*versions)[2].GetId(), 3);

  for (size_t i = 1; i < versions->size(); ++i) {
    ASSERT_GE((*versions)[i].GetTimestamp(), (*versions)[i - 1].GetTimestamp());
  }

  auto old_view = Dataset::Open(test_dataset.GetDatasetPath(), 1, {});
  ASSERT_TRUE(old_view.has_value());
  ASSERT_EQ(old_view->GetVersion(), 1);

  auto checkout_result = old_view->CheckoutLatest();
  ASSERT_TRUE(checkout_result.has_value());
  ASSERT_EQ(old_view->GetVersion(), 3);
}

}  // namespace lance::test
