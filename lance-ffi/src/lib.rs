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

    extern "C++" {
        include!("ffi_internal.hpp");

        type ArrowSchema;
    }

    #[namespace = "lance_ffi"]
    extern "Rust" {
        type BlockingDataset;

        #[Self = "BlockingDataset"]
        pub fn open(path: &str, storage_options: Vec<KV>) -> Result<Box<BlockingDataset>>;

        #[Self = "BlockingDataset"]
        pub unsafe fn create(
            uri: &str,
            arrow_schema: *mut ArrowSchema,
        ) -> Result<Box<BlockingDataset>>;
    }
}
