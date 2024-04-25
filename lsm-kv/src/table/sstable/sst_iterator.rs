//! iterator for SSTable
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use super::block::Block;
use super::block_iterator::BlockIterator;
use super::SsTable;
use crate::common::iterator::StorageIterator;
use crate::common::key::KeySlice;
use crate::common::profier::BlockProfiler;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    block_profier: BlockProfiler,
}

impl SsTableIterator {
    fn read_block_with_profier(
        table: &Arc<SsTable>,
        blk_idx: usize,
    ) -> Result<(BlockProfiler, Arc<Block>)> {
        let mut read_files_time = Duration::ZERO;
        let mut read_files_bytes: u64 = 0;
        let mut read_block_bytes: u64 = 0;
        let block = table.read_block_cached_with_profier(
            &mut read_files_time,
            &mut read_files_bytes,
            &mut read_block_bytes,
            blk_idx,
        )?;
        if read_files_bytes == 0 {
            Ok((
                BlockProfiler {
                    read_cached_num: 1,
                    read_block_num: 1,
                    read_files_time,
                    read_files_bytes,
                    read_block_bytes,
                },
                block,
            ))
        } else {
            Ok((
                BlockProfiler {
                    read_cached_num: 0,
                    read_block_num: 1,
                    read_files_time,
                    read_files_bytes,
                    read_block_bytes,
                },
                block,
            ))
        }
    }

    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(BlockProfiler, BlockIterator)> {
        let (profier, block) = Self::read_block_with_profier(table, 0)?;
        Ok((profier, BlockIterator::create_and_seek_to_first(block)))
    }

    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_profier, blk_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx: 0,
            block_profier,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (profier, blk_iter) = Self::seek_to_first_inner(&self.table)?;
        self.blk_idx = 0;
        self.blk_iter = blk_iter;
        // do profier
        self.block_profier += profier;
        Ok(())
    }

    fn seek_to_key_inner(
        table: &Arc<SsTable>,
        key: KeySlice,
    ) -> Result<(BlockProfiler, usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let (mut profier, block) = Self::read_block_with_profier(table, blk_idx)?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                let (rprofier, block) = Self::read_block_with_profier(table, blk_idx)?;
                blk_iter = BlockIterator::create_and_seek_to_first(block);
                // do profier
                profier += rprofier;
            }
        }
        Ok((profier, blk_idx, blk_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (block_profier, blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            table,
            blk_idx,
            blk_iter,
            block_profier,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (profier, blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        // do profier
        self.block_profier += profier;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                let (profier, block) = Self::read_block_with_profier(&self.table, self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
                // do profier
                self.block_profier += profier;
            }
        }
        Ok(())
    }

    fn block_profiler(&self) -> BlockProfiler {
        self.block_profier
    }

    fn reset_block_profiler(&mut self) {
        self.block_profier = BlockProfiler::default()
    }
}
