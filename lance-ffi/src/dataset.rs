use std::sync::Arc;

use lance::dataset::Dataset as LanceDataset;
use lance_core::Result as LanceResult;
use tokio::runtime::Handle;

pub struct Dataset {
    _ds: Arc<LanceDataset>,
}

impl Dataset {
    pub fn new(path: &str) -> LanceResult<Self> {
        let ds = Arc::new(Handle::current().block_on(LanceDataset::open(path))?);
        Ok(Dataset { _ds: ds })
    }
}
