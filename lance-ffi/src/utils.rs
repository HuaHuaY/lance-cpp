// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use crate::ffi::KV;

pub trait IntoHashMap {
    fn into_hashmap(self) -> HashMap<String, String>;
}

impl<I> IntoHashMap for I
where
    I: IntoIterator<Item = KV>,
{
    fn into_hashmap(self) -> HashMap<String, String> {
        self.into_iter().map(|kv| (kv.key, kv.value)).collect()
    }
}
