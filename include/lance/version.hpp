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

#include <chrono>
#include <cstdint>

#include "lance/export.hpp"

namespace lance {

class LANCE_EXPORT Version {
 public:
  using TimePoint = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

  Version(uint64_t id, int64_t timestamp_nanos)
      : id_(id),
        timestamp_(std::chrono::time_point<std::chrono::system_clock>(
            std::chrono::nanoseconds(timestamp_nanos))) {}

  [[nodiscard]] auto GetId() const -> uint64_t { return id_; }

  [[nodiscard]] auto GetTimestamp() const -> TimePoint { return timestamp_; }

 private:
  uint64_t id_;
  TimePoint timestamp_;
};

}  // namespace lance
