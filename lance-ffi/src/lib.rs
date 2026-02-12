// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub(crate) mod dataset;
pub(crate) mod utils;

use std::sync::LazyLock;

use crate::dataset::BlockingDataset;

pub(crate) static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

#[cxx::bridge]
mod ffi {
    #[namespace = "lance_ffi"]
    pub struct KV {
        pub key: String,
        pub value: String,
    }

    #[namespace = "lance_ffi"]
    #[derive(Clone)]
    pub struct Version {
        pub version: u64,
        pub timestamp_nanos: i64,
    }

    extern "C++" {
        include!("ffi_internal.hpp");

        type ArrowSchema;
        type ArrowArrayStream;
    }

    #[namespace = "lance_ffi"]
    extern "Rust" {
        type BlockingDataset;

        #[Self = "BlockingDataset"]
        pub fn open(path: &str, storage_options: Vec<KV>) -> Result<Box<BlockingDataset>>;

        #[Self = "BlockingDataset"]
        pub fn open_with_version(
            path: &str,
            version: u64,
            storage_options: Vec<KV>,
        ) -> Result<Box<BlockingDataset>>;

        #[Self = "BlockingDataset"]
        pub unsafe fn create(
            uri: &str,
            arrow_schema: *mut ArrowSchema,
        ) -> Result<Box<BlockingDataset>>;

        pub fn latest_version(&self) -> Result<u64>;

        pub fn count_rows(&self) -> Result<u64>;

        pub fn get_version(&self) -> Version;

        pub fn list_versions(&self) -> Result<Vec<Version>>;

        pub fn checkout_latest(&mut self) -> Result<()>;

        #[Self = "BlockingDataset"]
        pub unsafe fn append(
            uri: &str,
            arrow_stream: *mut ArrowArrayStream,
        ) -> Result<Box<BlockingDataset>>;
    }
}
