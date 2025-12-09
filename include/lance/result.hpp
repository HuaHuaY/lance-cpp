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

#include <expected>
#include <format>
#include <string>
#include <string_view>
#include <utility>

namespace lance {

enum class ErrorKind {
  UnknownError,
  FFIError,
};

constexpr auto ToString(const ErrorKind& kind) -> std::string_view {
  switch (kind) {
    case ErrorKind::UnknownError:
      return "UnknownError";
    case ErrorKind::FFIError:
      return "FFIError";
    default:
      std::unreachable();
  }
}

class [[nodiscard]] Error {
 public:
  template <ErrorKind K, typename... Args>
  static auto Make(const std::format_string<Args...> fmt, Args&&... args) -> Error {
    return {K, std::format(fmt, std::forward<Args>(args)...)};
  };

  template <ErrorKind K>
  static auto FromString(const std::string& msg) -> Error {
    return {K, msg};
  }

  template <typename E>
  static auto From(E&& e) -> Error;

  auto ToString() const -> std::string {
    return std::format("[{}] {}", lance::ToString(kind_), message_);
  }

 private:
  Error(ErrorKind k, std::string msg) : kind_(k), message_(std::move(msg)) {}

  ErrorKind kind_;
  std::string message_;
};

template <typename T, typename E = Error>
using Result = std::expected<T, E>;

}  // namespace lance
