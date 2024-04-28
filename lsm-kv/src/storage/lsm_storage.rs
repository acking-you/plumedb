//！Final LSM storage implementation

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::{Mutex, MutexGuard, RwLock};
use tabled::Tabled;

use super::super::common::profier::display_bytes;
use super::lsm_iterator::{FusedIterator, LsmIterator};
// use snafu::{ensure, ResultExt, Snafu};
use super::manifest::{Manifest, ManifestRecord};
use crate::common::cache::BlockCache;
use crate::common::file::{FileFolder, FileObject};
use crate::common::id::{TableId, TableIdBuilder};
use crate::common::iterator::concat_iterator::SstConcatIterator;
use crate::common::iterator::merge_iterator::MergeIterator;
use crate::common::iterator::tow_merge_iterator::TwoMergeIterator;
use crate::common::iterator::StorageIterator;
use crate::common::key::{map_bound, KeySlice};
use crate::compact::{CompactionController, CompactionOptions, CompactionTask, CompactionType};
use crate::table::mem_table::MemTable;
use crate::table::sstable::sst_iterator::SsTableIterator;
use crate::table::sstable::{SsTable, SsTableBuilder};

// #[allow(missing_docs)]
// #[derive(Debug, Snafu)]
// pub enum LsmStorageError {
//     #[snafu(display("The key of a delete operation cannot be empty!"))]
//     DelKeyEmpty,
//     #[snafu(display("The key of a put operation cannot be empty!"))]
//     PutKeyEmpty,
//     #[snafu(display("The value of a put operation cannot be empty!"))]
//     PutValueEmpty,
//     #[snafu(display("Delete batches error"))]
//     DeleteBatch { source: MemTableError },
//     #[snafu(display("Put batches error"))]
//     PutBatch { source: MemTableError },
//     #[snafu(display("WAL sync error when freeze MemTable"))]
//     FreezeMemTableSyncWAL { source: MemTableError },
//     #[snafu(display("WAL create error when freeze MemTable"))]
//     FreezeMemTableCreateWAL { source: MemTableError },
// }

// type Result<T, E = LsmStorageError> = std::result::Result<T, E>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    pub sst_state: SstStorageState,
}

/// Represents the state of the SSTable.
#[derive(Clone)]
pub struct SstStorageState {
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<TableId>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(TableId, Vec<TableId>)>,
    /// SST objects.
    pub sstables: HashMap<TableId, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create<T: CompactionOptions>(options: &LsmStorageOptions<T>) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0.into())),
            imm_memtables: Vec::new(),
            sst_state: SstStorageState {
                l0_sstables: Vec::new(),
                levels: options.compaction_options.get_init_levels(),
                sstables: Default::default(),
            },
        }
    }
}

#[derive(Debug, Clone, Tabled)]
pub struct LsmStorageOptions<T: CompactionOptions> {
    // Block size in bytes
    #[tabled(display_with("display_bytes"))]
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    #[tabled(display_with("display_bytes"))]
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    #[tabled(skip)]
    pub compaction_options: T,
    pub enable_wal: bool,
}

pub(super) fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

pub(super) fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner<T: CompactionOptions> {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) folder: FileFolder,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) sst_id_builder: TableIdBuilder,
    pub(crate) options: Arc<LsmStorageOptions<T>>,
    pub(crate) compaction_controller: T::Controller,
    pub(crate) manifest: Manifest,
}

impl<Options: CompactionOptions> LsmStorageInner<Options> {
    pub(crate) fn next_sst_id(&self) -> TableId {
        self.sst_id_builder.next_id()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub(crate) fn open(
        path: impl AsRef<Path>,
        options: &LsmStorageOptions<Options>,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let folder = FileFolder::new(path);
        let manifest_path = folder.path_of_manifest();

        let mut next_sst_id: TableId = 1.into();
        let mut state = LsmStorageState::create(options);
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;

        let compaction_controller = options.compaction_options.get_compact_controller();

        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    folder.path_of_wal(state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if Options::Controller::FLUSH_TO_L0 {
                            state.sst_state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.sst_state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let task = Options::Task::from_json(&task)?;
                        let (new_state, _) =
                            compaction_controller.recover_compaction_result(&state, &task, &output);
                        // TODO: apply remove again
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            let mut sst_cnt = 0;
            // recover SSTs
            for &table_id in state
                .sst_state
                .l0_sstables
                .iter()
                .chain(state.sst_state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(folder.path_of_sst(table_id))
                        .with_context(|| format!("failed to open SST: {:?}", table_id))?,
                )?;
                state.sst_state.sstables.insert(table_id, Arc::new(sst));
                sst_cnt += 1;
            }
            tracing::info!("{} SSTs opened", sst_cnt);

            next_sst_id += 1;

            // Sort SSTs on each level when it is leveled strategy
            if let CompactionType::Leveled = Options::COMPACTION_TYPE {
                for (_, ssts) in &mut state.sst_state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sst_state
                            .sstables
                            .get(x)
                            .expect("table id must exist")
                            .first_key()
                            .cmp(
                                state
                                    .sst_state
                                    .sstables
                                    .get(y)
                                    .expect("table id must exist")
                                    .first_key(),
                            )
                    })
                }
            }

            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for &id in memtables.iter() {
                    let memtable = MemTable::recover_from_wal(id, folder.path_of_wal(id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                tracing::info!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    folder.path_of_wal(next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            folder,
            block_cache,
            sst_id_builder: TableIdBuilder::new(next_sst_id),
            compaction_controller,
            manifest,
            options: Arc::new(options.clone()),
        };
        storage.folder.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        Ok(self.state.read().memtable.sync_wal()?)
    }

    /// Get a key from the storage
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // Search on immutable memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
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
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }

        let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;

        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(super) fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to
            // freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(super) fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
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
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(
                MemTable::create_with_wal(memtable_id, self.folder.path_of_wal(memtable_id))
                    .map_err(|e| anyhow!("{}", snafu::Report::from_error(e)))?,
            )
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        self.manifest.add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.folder.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let flush_memtable;

        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .context("no imm memtables!")?
                .clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.folder.path_of_sst(sst_id),
        )?);

        // Add the flushed L0 table to the list.
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            // Remove the memtable from the immutable memtables.
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            // Add L0 table
            if Options::Controller::FLUSH_TO_L0 {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.sst_state.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.sst_state.levels.insert(0, (sst_id, vec![sst_id]));
            }
            tracing::info!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sst_state.sstables.insert(sst_id, sst);
            // Update the snapshot.
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.folder.path_of_wal(sst_id))?;
        }

        self.manifest
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        self.folder.sync_dir()?;

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.sst_state.l0_sstables.len());
        for table_id in snapshot.sst_state.l0_sstables.iter() {
            let table = snapshot.sst_state.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                table_iters.push(Box::new(iter));
            }
        }

        let l0_iter = MergeIterator::create(table_iters);
        let mut level_iters = Vec::with_capacity(snapshot.sst_state.levels.len());
        for (_, level_sst_ids) in &snapshot.sst_state.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sst_state.sstables[table].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, MergeIterator::create(level_iters))?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper, Bytes::copy_from_slice),
        )?))
    }
}
