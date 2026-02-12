// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::iter::empty;
use std::sync::Arc;

use arrow::array::RecordBatchIterator;
use arrow::datatypes::Schema;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use lance::dataset::Dataset;
use lance::dataset::WriteParams;
use lance::dataset::builder::DatasetBuilder;
use lance_core::Result;

use crate::RT;
use crate::ffi::ArrowArrayStream;
use crate::ffi::ArrowSchema;
use crate::ffi::KV;
use crate::ffi::Version;
use crate::utils::IntoHashMap;

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

    pub fn open_with_version(
        uri: &str,
        version: u64,
        storage_options: Vec<KV>,
    ) -> Result<Box<Self>> {
        let builder = DatasetBuilder::from_uri(uri)
            .with_storage_options(storage_options.into_hashmap())
            .with_version(version);
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

    pub fn latest_version(&self) -> Result<u64> {
        RT.block_on(self.inner.latest_version_id())
    }

    pub fn count_rows(&self) -> Result<u64> {
        let count = RT.block_on(self.inner.count_rows(None))?;
        Ok(count as u64)
    }

    pub fn get_version(&self) -> Version {
        let version = self.inner.version();
        Version {
            version: version.version,
            timestamp_nanos: version.timestamp.timestamp_nanos_opt().unwrap_or(0),
        }
    }

    pub fn list_versions(&self) -> Result<Vec<Version>> {
        let versions = RT.block_on(self.inner.versions())?;
        Ok(versions
            .into_iter()
            .map(|v| Version {
                version: v.version,
                timestamp_nanos: v.timestamp.timestamp_nanos_opt().unwrap_or(0),
            })
            .collect())
    }

    pub fn checkout_latest(&mut self) -> Result<()> {
        RT.block_on(self.inner.checkout_latest())
    }

    pub fn append(uri: &str, arrow_stream: *mut ArrowArrayStream) -> Result<Box<Self>> {
        let ffi_stream =
            unsafe { FFI_ArrowArrayStream::from_raw(arrow_stream as *mut FFI_ArrowArrayStream) };
        let reader = ArrowArrayStreamReader::try_new(ffi_stream)?;
        let params = WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        };
        let inner = RT.block_on(Dataset::write(reader, uri, Some(params)))?;
        Ok(Box::new(Self { inner }))
    }
}
