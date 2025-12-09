// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::LazyLock;

pub(crate) mod dataset;
pub(crate) mod utils;

use crate::dataset::BlockingDataset;

pub(crate) static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

#[cxx::bridge(namespace = "lance_ffi")]
mod ffi {
    pub struct KV {
        pub key: String,
        pub value: String,
    }

    extern "Rust" {
        type BlockingDataset;
        #[Self = "BlockingDataset"]
        pub fn open(path: &str, storage_options: Vec<KV>) -> Result<Box<BlockingDataset>>;
    }
}
