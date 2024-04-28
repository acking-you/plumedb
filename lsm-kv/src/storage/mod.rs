//! Final KV storage implementation
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod manifest;
pub mod profiler;

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use self::lsm_iterator::{FusedIterator, LsmIterator};
use self::lsm_storage::{LsmStorageInner, LsmStorageOptions, WriteBatchRecord};
use crate::common::profier::{ReadProfiler, WriteProfiler};
use crate::compact::trigger::{spawn_compaction_thread, spawn_flush_thread};
use crate::compact::CompactionOptions;
use crate::table::mem_table::MemTable;

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct LsmKV<T: CompactionOptions> {
    pub(crate) inner: Arc<LsmStorageInner<T>>,
    /// Notifies the L0 flush thread to stop working
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl<T: CompactionOptions> Drop for LsmKV<T> {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl<Options: CompactionOptions> LsmKV<Options> {
    pub fn close(&self) -> anyhow::Result<()> {
        self.inner.folder.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.folder.sync_dir()?;
            return Ok(());
        }

        // create memtable and skip updating manifest
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.folder.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub fn open(
        path: impl AsRef<Path>,
        options: LsmStorageOptions<Options>,
    ) -> anyhow::Result<Arc<Self>> {
        const NOTIFIER_CHAN_CAP: usize = 5;

        let inner = Arc::new(LsmStorageInner::open(path, &options)?);
        let (tx1, rx) = crossbeam_channel::bounded(NOTIFIER_CHAN_CAP);
        let compaction_thread = spawn_compaction_thread(&inner, rx)?;
        let (tx2, rx) = crossbeam_channel::bounded(NOTIFIER_CHAN_CAP);
        let flush_thread = spawn_flush_thread(&inner, rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn get_with_profier(
        &self,
        profier: &mut ReadProfiler,
        key: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        self.inner.get_with_profiler(profier, key)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> anyhow::Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn write_bytes_batch_with_profier(
        &self,
        profiler: &mut WriteProfiler,
        batch: &[WriteBatchRecord<Bytes>],
    ) -> anyhow::Result<()> {
        self.inner.write_bytes_with_profiler(profiler, batch)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> anyhow::Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> anyhow::Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }
}
