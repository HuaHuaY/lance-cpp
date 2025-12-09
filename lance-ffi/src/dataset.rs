// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance::dataset::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_core::Result;

use crate::RT;
use crate::ffi::KV;
use crate::utils::IntoHashMap;

pub struct BlockingDataset {
    _inner: Dataset,
}

impl BlockingDataset {
    pub fn open(path: &str, storage_options: Vec<KV>) -> Result<Box<Self>> {
        let builder =
            DatasetBuilder::from_uri(path).with_storage_options(storage_options.into_hashmap());
        let inner = RT.block_on(builder.load())?;
        Ok(Box::new(Self { _inner: inner }))
    }
}
