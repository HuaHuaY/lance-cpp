// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::iter::empty;
use std::sync::Arc;

use arrow::array::RecordBatchIterator;
use arrow::datatypes::Schema;
use arrow::ffi::FFI_ArrowSchema;
use lance::dataset::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_core::Result;

use crate::RT;
use crate::ffi::{ArrowSchema, KV};
use crate::utils::IntoHashMap;

#[allow(dead_code)]
pub struct BlockingDataset {
    inner: Dataset,
}

impl BlockingDataset {
    pub fn open(uri: &str, storage_options: Vec<KV>) -> Result<Box<Self>> {
        let builder =
            DatasetBuilder::from_uri(uri).with_storage_options(storage_options.into_hashmap());
        let inner = RT.block_on(builder.load())?;
        Ok(Box::new(Self { inner }))
    }

    pub fn create(uri: &str, arrow_schema: *mut ArrowSchema) -> Result<Box<Self>> {
        let ffi_arrow_schema =
            unsafe { FFI_ArrowSchema::from_raw(arrow_schema as *mut FFI_ArrowSchema) };
        let schema = Arc::new(Schema::try_from(&ffi_arrow_schema)?);
        let reader = RecordBatchIterator::new(empty(), schema);
        let inner = RT.block_on(Dataset::write(reader, uri, None))?;
        Ok(Box::new(Self { inner }))
    }
}
