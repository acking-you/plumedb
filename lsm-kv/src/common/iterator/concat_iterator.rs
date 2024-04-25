//! SstConcatIterator is used to traverse the SSTable after compaction

use std::sync::Arc;

use anyhow::{ensure, Result};

use super::StorageIterator;
use crate::common::key::KeySlice;
use crate::common::profier::BlockProfiler;
use crate::table::sstable::sst_iterator::SsTableIterator;
use crate::table::sstable::SsTable;

/// This iterator operates on the assumption that multiple SSTables are sorted and do not contain
/// duplicate keys, which is a property of the compaction SSTable, and can be used to reduce the
/// overhead of [`MergeIterator`]
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    expired_sst_block_profiler: BlockProfiler,
}

impl SstConcatIterator {
    fn check_sst_valid(sstables: &[Arc<SsTable>]) -> Result<()> {
        for sst in sstables {
            ensure!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                ensure!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
        Ok(())
    }

    /// Create iterator and seek to first
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables)?;
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                expired_sst_block_profiler: BlockProfiler::default(),
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
            expired_sst_block_profiler: BlockProfiler::default(),
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    /// Create iterator and seek to key
    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(&sstables)?;
        let idx: usize = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
                expired_sst_block_profiler: BlockProfiler::default(),
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
            expired_sst_block_profiler: BlockProfiler::default(),
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                // do profiler
                self.expired_sst_block_profiler += iter.block_profiler();
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .expect("Must be checked by `is_valid`")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("Must be checked by `is_valid`")
            .value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            assert!(current.is_valid());
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current
            .as_mut()
            .expect("Must be checked by `is_valid`")
            .next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }

    fn block_profiler(&self) -> BlockProfiler {
        let mut profiler = BlockProfiler::default();
        profiler += self.expired_sst_block_profiler;
        if let Some(iter) = self.current.as_ref() {
            profiler += iter.block_profiler();
        }
        profiler
    }

    fn reset_block_profiler(&mut self) {
        self.expired_sst_block_profiler = BlockProfiler::default();
        if let Some(iter) = self.current.as_mut() {
            iter.reset_block_profiler()
        }
    }
}
