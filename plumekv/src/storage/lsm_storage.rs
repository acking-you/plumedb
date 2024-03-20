//！Final LSM storage implementation
#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use snafu::{ensure, ResultExt, Snafu};

use crate::table::mem_table::{MemTable, MemTableError};

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum LsmStorageError {
    #[snafu(display("The key of a delete operation cannot be empty!"))]
    DelKeyEmpty,
    #[snafu(display("The key of a put operation cannot be empty!"))]
    PutKeyEmpty,
    #[snafu(display("The value of a put operation cannot be empty!"))]
    PutValueEmpty,
    #[snafu(display("Delete batches error"))]
    DeleteBatch { source: MemTableError },
    #[snafu(display("Put batches error"))]
    PutBatch { source: MemTableError },
    #[snafu(display("WAL sync error when freeze MemTable"))]
    FreezeMemTableSyncWAL { source: MemTableError },
    #[snafu(display("WAL create error when freeze MemTable"))]
    FreezeMemTableCreateWAL { source: MemTableError },
}

type Result<T, E = LsmStorageError> = std::result::Result<T, E>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
}

/// batches oprater
pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    /// put
    Put(T, T),
    /// delete
    Del(T),
}

impl LsmStorageState {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
        }
    }
}

/// Options for [`LsmStorageState`]
#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    /// SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    /// Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    /// enable wal for [`MemTable`]
    pub enable_wal: bool,
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    /// To break the unfair writer-first model of [`RwLock`]
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create();

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            next_sst_id: AtomicUsize::new(0),
            options: options.into(),
        };

        Ok(storage)
    }

    /// TODO
    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        // Search on current mem_table
        if let Some(v) = snapshot.memtable.get(key.into()) {
            if v.is_empty() {
                return Ok(None);
            }
            return Ok(Some(v));
        }
        // Search on im_mem_table
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key.into()) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    ensure!(!key.is_empty(), DelKeyEmptySnafu);
                    let size = {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(key.into(), b"")
                            .context(DeleteBatchSnafu)?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    ensure!(!key.is_empty(), PutKeyEmptySnafu);
                    ensure!(!value.is_empty(), PutValueEmptySnafu);
                    let size = {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(key.into(), value)
                            .context(PutBatchSnafu)?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    fn try_freeze(&self, size: usize) -> Result<()> {
        if size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?
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

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable
            .sync_wal()
            .context(FreezeMemTableSyncWALSnafu)?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(
                MemTable::create_with_wal(memtable_id, self.path_of_wal(memtable_id))
                    .context(FreezeMemTableCreateWALSnafu)?,
            )
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }
}
