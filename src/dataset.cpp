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

#include <lance_cxxbridge/lib.h>
#include <rust/cxx.h>

#include <expected>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "lance/result.hpp"
#include "lance/version.hpp"

namespace lance {

class Dataset::Impl {
 public:
  static auto Open(std::string_view uri,
                   const std::unordered_map<std::string, std::string>& storage_options)
      -> Result<std::unique_ptr<Impl>> {
    auto rust_uri = rust::Str(uri.data(), uri.size());
    auto rust_options
        = storage_options
          | std::views::transform([](const auto& kv) -> auto {
              return lance_ffi::KV{.key = rust::String(kv.first), .value = rust::String(kv.second)};
            })
          | std::ranges::to<rust::Vec<::lance_ffi::KV>>();

    try {
      auto dataset = lance_ffi::BlockingDataset::open(rust_uri, std::move(rust_options));
      return std::unique_ptr<Impl>(new Impl(std::move(dataset)));
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  static auto Open(std::string_view uri, uint64_t version,
                   const std::unordered_map<std::string, std::string>& storage_options)
      -> Result<std::unique_ptr<Impl>> {
    auto rust_uri = rust::Str(uri.data(), uri.size());
    auto rust_options
        = storage_options
          | std::views::transform([](const auto& kv) -> auto {
              return lance_ffi::KV{.key = rust::String(kv.first), .value = rust::String(kv.second)};
            })
          | std::ranges::to<rust::Vec<::lance_ffi::KV>>();

    try {
      auto dataset = lance_ffi::BlockingDataset::open_with_version(rust_uri, version,
                                                                   std::move(rust_options));
      return std::unique_ptr<Impl>(new Impl(std::move(dataset)));
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  static auto Create(std::string_view uri, ArrowSchema& schema) -> Result<std::unique_ptr<Impl>> {
    auto rust_uri = rust::Str(uri.data(), uri.size());

    try {
      auto dataset = lance_ffi::BlockingDataset::create(rust_uri, &schema);
      return std::unique_ptr<Impl>(new Impl(std::move(dataset)));
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  static auto Append(std::string_view uri, ArrowArrayStream& stream)
      -> Result<std::unique_ptr<Impl>> {
    auto rust_uri = rust::Str(uri.data(), uri.size());

    try {
      auto dataset = lance_ffi::BlockingDataset::append(rust_uri, &stream);
      return std::unique_ptr<Impl>(new Impl(std::move(dataset)));
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  [[nodiscard]] auto GetVersion() const -> uint64_t { return dataset_->version(); }

  [[nodiscard]] auto GetLatestVersion() const -> Result<uint64_t> {
    try {
      return dataset_->latest_version();
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  [[nodiscard]] auto GetVersionInfo() const -> Version {
    auto info = dataset_->get_version();
    return {info.version, info.timestamp_nanos};
  }

  [[nodiscard]] auto ListVersions() const -> Result<std::vector<Version>> {
    try {
      auto versions = dataset_->list_versions();
      std::vector<Version> result;
      result.reserve(versions.size());
      for (const auto& v : versions) {
        result.emplace_back(v.version, v.timestamp_nanos);
      }
      return result;
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  [[nodiscard]] auto CountRows() const -> Result<uint64_t> {
    try {
      return dataset_->count_rows();
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

  auto CheckoutLatest() -> Result<void> {
    try {
      dataset_->checkout_latest();
      return {};
    } catch (const rust::Error& e) {
      return std::unexpected(Error::From(e));
    }
  }

 private:
  explicit Impl(rust::Box<lance_ffi::BlockingDataset> dataset) : dataset_(std::move(dataset)) {}

  rust::Box<lance_ffi::BlockingDataset> dataset_;
};

Dataset::Dataset(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

Dataset::~Dataset() = default;
Dataset::Dataset(Dataset&&) noexcept = default;
auto Dataset::operator=(Dataset&&) noexcept -> Dataset& = default;

auto Dataset::Open(std::string_view uri,
                   const std::unordered_map<std::string, std::string>& storage_options)
    -> Result<Dataset> {
  return Impl::Open(uri, storage_options).transform([](auto&& impl) -> Dataset {
    return Dataset(std::move(impl));
  });
}

auto Dataset::Open(std::string_view uri, uint64_t version,
                   const std::unordered_map<std::string, std::string>& storage_options)
    -> Result<Dataset> {
  return Impl::Open(uri, version, storage_options).transform([](auto&& impl) -> Dataset {
    return Dataset(std::move(impl));
  });
}

auto Dataset::Create(std::string_view uri, ArrowSchema& schema) -> Result<Dataset> {
  return Impl::Create(uri, schema).transform([](auto&& impl) -> Dataset {
    return Dataset(std::move(impl));
  });
}

auto Dataset::Append(std::string_view uri, ArrowArrayStream& stream) -> Result<Dataset> {
  return Impl::Append(uri, stream).transform([](auto&& impl) -> Dataset {
    return Dataset(std::move(impl));
  });
}

auto Dataset::GetVersion() const -> uint64_t { return impl_->GetVersion(); }

auto Dataset::GetLatestVersion() const -> Result<uint64_t> { return impl_->GetLatestVersion(); }

auto Dataset::GetVersionInfo() const -> Version { return impl_->GetVersionInfo(); }

auto Dataset::ListVersions() const -> Result<std::vector<Version>> { return impl_->ListVersions(); }

auto Dataset::CountRows() const -> Result<uint64_t> { return impl_->CountRows(); }

auto Dataset::CheckoutLatest() -> Result<void> { return impl_->CheckoutLatest(); }

}  // namespace lance
