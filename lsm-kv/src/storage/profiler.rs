use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use parking_lot::MutexGuard;
use tabled::builder::Builder;
use tabled::settings::{Border, Panel, Style};
use tabled::Tabled;

use super::lsm_storage::{key_within, LsmStorageInner, SstStorageState, WriteBatchRecord};
use super::manifest::ManifestRecord;
use super::LsmKV;
use crate::common::file::{MANIFEST_NAME, SST_EXT, WAL_EXT};
use crate::common::iterator::concat_iterator::SstConcatIterator;
use crate::common::iterator::merge_iterator::MergeIterator;
use crate::common::iterator::tow_merge_iterator::TwoMergeIterator;
use crate::common::iterator::StorageIterator;
use crate::common::key::KeySlice;
use crate::common::profier::{
    display_bytes, get_format_tabled, ReadProfiler, ReadStatus, ScopedTimerGuard, Timer,
    WriteProfiler,
};
use crate::compact::CompactionOptions;
use crate::table::mem_table::MemTable;
use crate::table::sstable::sst_iterator::SsTableIterator;
use crate::table::sstable::SsTable;

macro_rules! get_lock_guard_with_profiler {
    ($duration:expr, $guard:expr) => {{
        let _lock_timer = ScopedTimerGuard::new($duration);
        $guard
    }};
}

impl<T: CompactionOptions> LsmStorageInner<T> {
    pub(super) fn try_freeze_profiler(
        &self,
        profier: &mut WriteProfiler,
        estimated_size: usize,
    ) -> anyhow::Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock =
                get_lock_guard_with_profiler!(&mut profier.write_lock_time, self.state_lock.lock());
            let guard =
                get_lock_guard_with_profiler!(&mut profier.read_lock_time, self.state.read());
            // the memtable could have already been frozen, check again to ensure we really need to
            // freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable_profiler(profier, &state_lock)?;
            }
        }
        Ok(())
    }

    pub(super) fn freeze_memtable_with_memtable_profiler(
        &self,
        profier: &mut WriteProfiler,
        memtable: Arc<MemTable>,
    ) -> anyhow::Result<()> {
        let mut guard =
            get_lock_guard_with_profiler!(&mut profier.write_lock_time, self.state.write());

        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable_profiler(
        &self,
        profier: &mut WriteProfiler,
        state_lock_observer: &MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        let freeze_timer = Timer::now();
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(
                MemTable::create_with_wal(memtable_id, self.folder.path_of_wal(memtable_id))
                    .map_err(|e| anyhow!("{}", snafu::Report::from_error(e)))?,
            )
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable_profiler(profier, memtable)?;

        self.manifest.add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.folder.sync_dir()?;

        // do profiler
        profier.freeze_time += freeze_timer.elapsed();

        Ok(())
    }

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
                    let size = {
                        let guard = get_lock_guard_with_profiler!(
                            &mut profier.read_lock_time,
                            self.state.read()
                        );
                        guard.memtable.put_bytes_with_profier(
                            profier,
                            key.clone(),
                            Bytes::from_static(b""),
                        )?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze_profiler(profier, size)?;
                    profier.write_bytes += key.len() as u64;
                }
                WriteBatchRecord::Put(key, value) => {
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size = {
                        let guard = get_lock_guard_with_profiler!(
                            &mut profier.read_lock_time,
                            self.state.read()
                        );
                        guard.memtable.put_bytes_with_profier(
                            profier,
                            key.clone(),
                            value.clone(),
                        )?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze_profiler(profier, size)?;
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
            let guard =
                get_lock_guard_with_profiler!(&mut profiler.read_lock_time, self.state.read());
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

        let mut l0_iters = Vec::with_capacity(snapshot.sst_state.l0_sstables.len());

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

        for table in snapshot.sst_state.l0_sstables.iter() {
            let table = snapshot.sst_state.sstables[table].clone();
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
        let mut level_iters = Vec::with_capacity(snapshot.sst_state.levels.len());
        for (_, level_sst_ids) in &snapshot.sst_state.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sst_state.sstables[table].clone();
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
            profiler.read_status = ReadStatus::SSTable;
            profiler.read_total_time = total_time.elapsed();
            profiler.block_profier = iter.block_profiler();
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }
}

#[derive(Tabled)]
struct LsmFilesStatus {
    #[tabled(display_with("display_files_info"))]
    sst_files: Vec<(String, String)>,
    #[tabled(display_with("display_files_info"))]
    wal_files: Vec<(String, String)>,
    #[tabled(display_with("display_manifest"))]
    manifest: Option<String>,
}

fn display_files_info(files: &[(String, String)]) -> String {
    let mut builder = Builder::new();
    files.iter().for_each(|f| builder.push_record([&f.0, &f.1]));
    builder
        .build()
        .with(Style::modern().frame(Border::inherit(Style::rounded())))
        .to_string()
}

fn display_manifest(manifest: &Option<String>) -> String {
    let mut builder = Builder::new();
    match manifest {
        Some(v) => builder.push_record([v]),
        None => builder.push_record(["None"]),
    }
    builder
        .build()
        .with(Style::modern().frame(Border::inherit(Style::rounded())))
        .to_string()
}

impl SstStorageState {
    fn show_level_status(&self) -> String {
        let mut level_builder = Builder::new();
        level_builder.push_record(["L0".into(), format!("{:?}", self.l0_sstables)]);
        self.levels.iter().enumerate().for_each(|(idx, item)| {
            level_builder.push_record([format!("L{idx}({})", item.0), format!("{:?}", item.1)]);
        });

        level_builder
            .build()
            .with(Panel::header("LevelStatus"))
            .with(Style::modern().frame(Border::inherit(Style::rounded())))
            .to_string()
    }

    fn show_sst_status(&self) -> String {
        let mut sst_builder = Builder::new();
        self.sstables.iter().for_each(|(idx, item)| {
            sst_builder.push_record([format!("SST({})", idx), display_bytes(&item.table_size())]);
        });
        sst_builder
            .build()
            .with(Panel::header("SstStatus"))
            .with(Style::modern().frame(Border::inherit(Style::rounded())))
            .to_string()
    }
}

impl<T: CompactionOptions> LsmStorageInner<T> {
    pub(crate) fn show_options(&self) -> String {
        let mut builder = Builder::new();
        builder.push_record(["lsm_folder".into(), self.folder.to_string()]);
        builder.push_record([
            "lsm_options".into(),
            get_format_tabled(self.options.as_ref()).to_string(),
        ]);
        builder.push_record([
            format!("lsm-compaction({})", T::COMPACTION_TYPE),
            format!("{}", self.options.compaction_options),
        ]);
        builder
            .build()
            .with(Panel::header("Options"))
            .with(Style::modern().frame(Border::inherit(Style::rounded())))
            .to_string()
    }

    pub(crate) fn show_mem_status(&self) -> String {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        // show memtables
        let mut builder = Builder::new();
        builder.push_record([
            format!("cur_memtable({})", snapshot.memtable.id()),
            display_bytes(&snapshot.memtable.approximate_size()),
        ]);
        snapshot.imm_memtables.iter().for_each(|mem| {
            builder.push_record([
                format!("imm_memtable({})", mem.id()),
                display_bytes(&mem.approximate_size()),
            ]);
        });
        let mem_table = builder
            .build()
            .with(Panel::header("MemTableStatus"))
            .with(Style::modern().frame(Border::inherit(Style::rounded())))
            .to_string();
        mem_table
    }

    pub(crate) fn show_level_status(&self) -> String {
        self.state.read().sst_state.show_level_status()
    }

    pub(crate) fn show_sst_status(&self) -> String {
        self.state.read().sst_state.show_sst_status()
    }
}

impl<T: CompactionOptions> LsmKV<T> {
    pub fn show_options(&self) -> String {
        self.inner.show_options()
    }

    pub fn show_mem_status(&self) -> String {
        self.inner.show_mem_status()
    }

    pub fn show_level_status(&self) -> String {
        self.inner.show_level_status()
    }

    pub fn show_sst_status(&self) -> String {
        self.inner.show_sst_status()
    }

    pub fn show_files(&self) -> anyhow::Result<String> {
        let mut wal_files = vec![];
        let mut sst_files = vec![];
        let mut manifest = None;
        for f in self.inner.folder.read_dir()? {
            let f = f?;
            let filename = f
                .file_name()
                .into_string()
                .map_err(|e| anyhow!("os strint convert to String fails:{:?}", e))?;
            let filesize = {
                let filesize = f.metadata()?.size();
                display_bytes(&filesize)
            };
            if filename.ends_with(WAL_EXT) {
                wal_files.push((filename, filesize));
            } else if filename.ends_with(SST_EXT) {
                sst_files.push((filename, filesize));
            } else if filename == MANIFEST_NAME {
                manifest = Some(filesize);
            }
        }
        let lsm_files = LsmFilesStatus {
            wal_files,
            sst_files,
            manifest,
        };
        Ok(get_format_tabled(lsm_files).to_string())
    }
}

#[cfg(test)]
mod tests {
    use tabled::builder::Builder;
    use tabled::settings::Panel;

    #[test]
    fn test_tabled() {
        let message = r#"The terms "the ocean" or "the sea" used without specification refer to the interconnected body of salt water covering the majority of the Earth's surface"#;

        let oceans = ["Atlantic", "Pacific", "Indian", "Southern", "Arctic"];

        let mut builder = Builder::default();
        builder.push_column(["testaafsdfsf"]);
        builder.push_record(["#", "Ocean"]);

        for (i, ocean) in oceans.iter().enumerate() {
            builder.push_record([i.to_string(), ocean.to_string()]);
        }

        let table = builder.build().with(Panel::header(message)).to_string();

        println!("{table}");
    }
}
