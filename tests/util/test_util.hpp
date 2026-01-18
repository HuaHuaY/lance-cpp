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

#pragma once

#include <string>
#include <string_view>

struct ArrowSchema;

namespace lance {
class Dataset;
}

namespace lance::test {

auto GetResourcePath(std::string_view filename) -> std::string;

class TestDataset {
 public:
  explicit TestDataset(std::string dataset_path);

  [[nodiscard]] auto CreateEmptyDataset() const -> Dataset;

 protected:
  [[nodiscard]] virtual auto GetSchema() const -> ArrowSchema = 0;

 private:
  std::string dataset_path_;
};

class SimpleTestDataset : public TestDataset {
 public:
  using TestDataset::TestDataset;

 protected:
  [[nodiscard]] auto GetSchema() const -> ArrowSchema override;
};

}  // namespace lance::test
