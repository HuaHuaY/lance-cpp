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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "lance/export.hpp"
#include "lance/result.hpp"
#include "lance/version.hpp"

struct ArrowSchema;
struct ArrowArrayStream;

namespace lance {

class LANCE_EXPORT Dataset {
 public:
  ~Dataset();
  Dataset(const Dataset&) = delete;
  auto operator=(const Dataset&) -> Dataset& = delete;
  Dataset(Dataset&&) noexcept;
  auto operator=(Dataset&&) noexcept -> Dataset&;

  static auto Open(std::string_view uri,
                   const std::unordered_map<std::string, std::string>& storage_options)
      -> Result<Dataset>;

  static auto Open(std::string_view uri, uint64_t version,
                   const std::unordered_map<std::string, std::string>& storage_options)
      -> Result<Dataset>;

  static auto Create(std::string_view uri, ArrowSchema& schema) -> Result<Dataset>;

  static auto Append(std::string_view uri, ArrowArrayStream& stream) -> Result<Dataset>;

  [[nodiscard]] auto GetVersion() const -> uint64_t;

  [[nodiscard]] auto GetLatestVersion() const -> Result<uint64_t>;

  [[nodiscard]] auto GetVersionInfo() const -> Version;

  [[nodiscard]] auto ListVersions() const -> Result<std::vector<Version>>;

  [[nodiscard]] auto CountRows() const -> Result<uint64_t>;

  auto CheckoutLatest() -> Result<void>;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit Dataset(std::unique_ptr<Impl> impl);
};

}  // namespace lance
