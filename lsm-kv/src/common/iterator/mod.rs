//! Defines some iter trait

use super::profier::BlockProfiler;

pub mod concat_iterator;
pub mod merge_iterator;
pub mod tow_merge_iterator;

/// Iterator for storage access
pub trait StorageIterator {
    /// Key type for store
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Get the current key.
    fn key(&self) -> Self::KeyType<'_>;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }

    fn block_profiler(&self) -> BlockProfiler;

    fn reset_block_profiler(&mut self);
}
