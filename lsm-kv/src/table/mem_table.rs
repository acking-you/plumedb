//! Specific implementations of MemTable
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use snafu::{ResultExt, Snafu};

use super::sstable::SsTableBuilder;
use crate::common::id::TableId;
use crate::common::iterator::StorageIterator;
use crate::common::key::{map_bound, KeySlice};
use crate::common::profier::BlockProfiler;
use crate::common::wal::Wal;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum MemTableError {
    #[snafu(display("Failed to crate MemTable with WAL"))]
    CrateWithWAL { source: anyhow::Error },
    #[snafu(display("Failed to recover MemTable with WAL"))]
    RecoverWithWAL { source: anyhow::Error },
    #[snafu(display("Failed to put K-V to WAL"))]
    PutWithWAL { source: anyhow::Error },
    #[snafu(display("Failed to sync K-V to WAL"))]
    SyncWithWAL { source: anyhow::Error },
}

pub(super) type Result<T, E = MemTableError> = std::result::Result<T, E>;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally
/// implemented in other chapters of week 1 and week 2.
pub struct MemTable {
    id: TableId,
    pub(super) approximate_size: Arc<AtomicUsize>,
    pub(super) wal: Option<Wal>,
    pub(super) map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: TableId) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::default()),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: TableId, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path).context(CrateWithWALSnafu)?),
            id,
            approximate_size: Arc::new(AtomicUsize::default()),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: TableId, path: impl AsRef<Path>) -> Result<Self> {
        let skiplist = SkipMap::new();
        let wal = Wal::recover(path, &skiplist).context(RecoverWithWALSnafu)?;
        let map = Arc::new(skiplist);
        Ok(Self {
            map,
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::default()),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::copy_from_slice(key);
        self.map.get(&key).map(|v| v.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// FIXME: approximate_size only increases,
    /// if you keep updating the same key it will also cause the MemTable to be FREEZED,
    /// the current reason for this is that SkipList::insert doesn't return the old entries,
    /// we don't know if the Key existed before and what the exact amount of data was.
    /// You could just call `get` to get the old entry,
    /// but that would be one more access, so it might not be a good solution.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let data_len = key.len() + value.len();
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(data_len, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            wal.put(key.into(), value).context(PutWithWALSnafu)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync().context(SyncWithWALSnafu)?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let mut iter = MemTableIterator::new(
            self.map.clone(),
            |map| {
                let lower = map_bound(lower, Bytes::copy_from_slice);
                let upper = map_bound(upper, Bytes::copy_from_slice);
                map.range((lower, upper))
            },
            (Bytes::from_static(b""), Bytes::from_static(b"")),
        );
        iter.next().expect("MemTable iter next nerver fails");
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(KeySlice::from_slice(&entry.key()[..]), &entry.value()[..]);
        }
        Ok(())
    }

    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0)
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let item = self.with_iter_mut(|iter| match iter.next() {
            Some(entry) => (entry.key().clone(), entry.value().clone()),
            None => (Bytes::from_static(b""), Bytes::from_static(b"")),
        });
        self.with_item_mut(|i| *i = item);
        Ok(())
    }

    fn block_profiler(&self) -> crate::common::profier::BlockProfiler {
        // no block
        BlockProfiler::default()
    }

    fn reset_block_profiler(&mut self) {
        // no block
    }
}
