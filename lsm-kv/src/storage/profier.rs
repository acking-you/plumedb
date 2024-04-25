use std::fmt::Display;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use super::lsm_iterator::{FusedIterator, LsmIterator};
use super::lsm_storage::{key_within, range_overlap, LsmStorageInner, WriteBatchRecord};
use super::LsmKV;
use crate::common::iterator::concat_iterator::SstConcatIterator;
use crate::common::iterator::merge_iterator::MergeIterator;
use crate::common::iterator::tow_merge_iterator::TwoMergeIterator;
use crate::common::iterator::StorageIterator;
use crate::common::key::{map_bound, KeySlice};
use crate::common::profier::{ReadProfiler, ReadStatus, Timer, WriteProfiler};
use crate::compact::CompactionOptions;
use crate::table::sstable::sst_iterator::SsTableIterator;
use crate::table::sstable::SsTable;

impl<T: CompactionOptions> Display for LsmStorageInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            write!(
                f,
                "\nL0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            )?;
        }
        for (level, files) in &snapshot.levels {
            write!(f, "\nL{level} ({}): {:?}", files.len(), files)?;
        }
        writeln!(f)
    }
}

impl<T: CompactionOptions> Display for LsmKV<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: CompactionOptions> LsmStorageInner<T> {
    pub fn write_bytes_with_profiler(
        &self,
        profier: &mut WriteProfiler,
        batch: &[WriteBatchRecord<Bytes>],
    ) -> anyhow::Result<()> {
        for record in batch {
            let once_time = Timer::now();
            match record {
                WriteBatchRecord::Del(key) => {
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put_bytes_with_profier(
                            profier,
                            key.clone(),
                            Bytes::from_static(b""),
                        )?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                    profier.write_bytes += key.len() as u64;
                }
                WriteBatchRecord::Put(key, value) => {
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put_bytes_with_profier(
                            profier,
                            key.clone(),
                            value.clone(),
                        )?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                    profier.write_bytes += key.len() as u64;
                    profier.write_bytes += value.len() as u64;
                }
            }
            profier.filled_num += 1;
            profier.write_total_time += once_time.elapsed();
        }
        Ok(())
    }

    /// Get a key from the storage
    pub fn get_with_profiler(
        &self,
        profiler: &mut ReadProfiler,
        key: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let total_time = Timer::now();
        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            profiler.read_status = ReadStatus::MemTable;
            profiler.read_total_time += total_time.elapsed();
            return Ok(Some(value));
        }

        // Search on immutable memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                profiler.read_status = ReadStatus::MemTable;
                profiler.read_total_time += total_time.elapsed();
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                return table.bloom.may_contain(farmhash::fingerprint32(key));
            }
            false
        };

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            } else {
                // profiler bloom filter
                profiler.filter_bytes += table.table_size() as u64;
                profiler.filter_num += 1;
            }
        }

        let l0_iter = MergeIterator::create(l0_iters);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                } else {
                    // profiler bloom filter
                    profiler.filter_bytes += table.table_size() as u64;
                    profiler.filter_num += 1;
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }

        let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;

        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            // do block profiler
            profiler.block_profier = iter.block_profiler();
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }
}
