// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::LazyLock;
use tokio::runtime::Runtime;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn lance_init() -> bool;
        fn lance_cleanup();
    }
}

// Global runtime for async operations
static RT: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create tokio runtime"));

pub fn lance_init() -> bool {
    // Initialize the runtime
    LazyLock::force(&RT);
    true
}

pub fn lance_cleanup() {
    // Cleanup if needed
}
