//! Specific implementations of MemTable
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use snafu::{ResultExt, Snafu};

use crate::common::iterator::StorageIterator;
use crate::common::key::{KeyBytes, KeySlice};
use crate::common::wal::Wal;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
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

type Result<T, E = MemTableError> = std::result::Result<T, E>;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally
/// implemented in other chapters of week 1 and week 2.
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `Bytes` from a bound of `KeySlice`.
pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.key_ref().into()),
        Bound::Excluded(x) => Bound::Excluded(x.key_ref().into()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path.as_ref()).context(CrateWithWALSnafu)?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path.as_ref(), &map).context(RecoverWithWALSnafu)?),
            map,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// SAFETY: Ensure that the lifetime of the key is longer than the function call
    /// Get a value by key. Should not be used in week 3.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key_bytes = KeyBytes::from_bytes(Bytes::from_static(unsafe {
            std::mem::transmute(key.key_ref())
        }));
        self.map.get(&key_bytes).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// FIXME: [`MemTable::approximate_size`] keeps increasing which can be a problem.
    /// For example, if you keep updating the value of the same key, this will also cause it to
    /// increment, eventually freezing the [`MemTable`]. The current reason for this is compromised
    /// by [SkipMap::insert], which doesn't return the old entries, making it impossible to specify
    /// how much data was deleted or updated in a single function call.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let estimated_size = key.key_len() + value.len();
        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            wal.put(key, value).context(PutWithWALSnafu)?;
        }
        Ok(())
    }

    /// Sync WAL
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync().context(SyncWithWALSnafu)?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_key_bound(lower), map_key_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    // pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
    //     for entry in self.map.iter() {
    //         builder.add(entry.key().as_key_slice(), &entry.value()[..]);
    //     }
    //     Ok(())
    // }

    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the approximate size (not length) of the MemTable's stored content
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to
/// week 1, day 2 chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::new()))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::MemTable;

    #[test]
    fn test_mem_table() {
        let memtable = MemTable::create(0);
        memtable.put(b"key1"[..].into(), b"value1").unwrap();
        memtable.put(b"key2"[..].into(), b"value2").unwrap();
        memtable.put(b"key3"[..].into(), b"value3").unwrap();
        assert_eq!(&memtable.get(b"key1"[..].into(),).unwrap()[..], b"value1");
        assert_eq!(&memtable.get(b"key2"[..].into()).unwrap()[..], b"value2");
        assert_eq!(&memtable.get(b"key3"[..].into()).unwrap()[..], b"value3");
    }
}
