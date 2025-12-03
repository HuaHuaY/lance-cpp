// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::LazyLock;

use lance_core::Result as LanceResult;
use tokio::runtime::Runtime;

pub(crate) mod dataset;

use crate::dataset::Dataset;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn lance_init(path: &str) -> Result<bool>;
        fn lance_cleanup();
    }
}

// Global runtime for async operations
pub(crate) static RT: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create tokio runtime"));

pub fn lance_init(path: &str) -> LanceResult<bool> {
    // Initialize the runtime
    _ = Dataset::new(path)?;
    Ok(true)
}

pub fn lance_cleanup() {
    // Cleanup if needed
}
