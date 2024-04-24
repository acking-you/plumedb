//! Some cache-related wrappers

use anyhow::{anyhow, Result};

use crate::table::sstable::block::Block;

/// Cache the [`Block`] based on `sst_id` and `block_index`
pub struct BlockCache(moka::sync::Cache<(usize, usize), std::sync::Arc<Block>>);

impl BlockCache {
    pub fn new(max_capacity: usize) -> Self {
        type Cache = moka::sync::Cache<(usize, usize), std::sync::Arc<Block>>;
        Self(Cache::new(max_capacity as u64))
    }

    /// Try to get Block from cache
    pub fn try_get_with<F>(&self, key: (usize, usize), init: F) -> Result<std::sync::Arc<Block>>
    where
        F: FnOnce() -> Result<std::sync::Arc<Block>>,
    {
        self.0.try_get_with(key, init).map_err(|e| anyhow!("{e}"))
    }
}
