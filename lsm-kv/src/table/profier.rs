use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use snafu::ResultExt;

use super::mem_table::{MemTable, PutWithWALSnafu, Result as MemResult};
use super::sstable::block::Block;
use super::sstable::{ReadBlockFromCachSnafu, Result as SstResult, SsTable};
use crate::common::profier::{ScopedTimerGuard, WriteProfiler};

impl MemTable {
    pub fn put_bytes_with_profier(
        &self,
        profier: &mut WriteProfiler,
        key: Bytes,
        value: Bytes,
    ) -> MemResult<()> {
        let data_len = key.len() + value.len();
        self.map.insert(key.clone(), value.clone());
        self.approximate_size
            .fetch_add(data_len, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            let key: &[u8] = &key;
            let value: &[u8] = &value;
            let _wal_time_scoped = ScopedTimerGuard::new(&mut profier.write_wal_time);
            wal.put(key.into(), value).context(PutWithWALSnafu)?;
        }
        Ok(())
    }
}

impl SsTable {
    /// Read a block from disk, with block cache.
    pub fn read_block_cached_with_profier(
        &self,
        read_files_time: &mut Duration,
        read_files_bytes: &mut u64,
        read_block_bytes: &mut u64,
        block_idx: usize,
    ) -> SstResult<Arc<Block>> {
        let blk = if let Some(ref block_cache) = self.block_cache {
            block_cache
                .try_get_with((self.id.into(), block_idx), || {
                    let _read_times_guard = ScopedTimerGuard::new(read_files_time);
                    let blk = self.read_block(block_idx)?;
                    *read_files_bytes = blk.block_size();
                    Ok(blk)
                })
                .context(ReadBlockFromCachSnafu)?
        } else {
            let _read_times_guard = ScopedTimerGuard::new(read_files_time);
            let blk = self.read_block(block_idx)?;
            *read_files_bytes = blk.block_size();
            blk
        };
        *read_block_bytes = blk.block_size();
        Ok(blk)
    }
}
