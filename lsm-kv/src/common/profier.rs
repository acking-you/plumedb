use std::ops::AddAssign;
use std::time::{Duration, Instant};

/// Guard for profiling
#[derive(Debug)]
pub struct ScopedTimerGuard<'a> {
    now: Instant,
    accumulation: &'a mut Duration,
}

impl<'a> ScopedTimerGuard<'a> {
    /// Create a new guard
    #[inline]
    pub fn new(accumulation: &'a mut Duration) -> Self {
        Self {
            now: Instant::now(),
            accumulation,
        }
    }
}

impl Drop for ScopedTimerGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        *self.accumulation += self.now.elapsed();
    }
}

pub type Timer = Instant;

#[derive(Debug, Clone, Copy, Default)]
pub struct WriteProfiler {
    pub write_wal_time: Duration,
    pub write_total_time: Duration,
    pub write_bytes: u64,
    pub filled_num: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadStatus {
    MemTable,
    SSTable,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BlockProfiler {
    pub read_cached_num: u32,
    pub read_block_num: u32,
    pub read_files_time: Duration,
    pub read_files_bytes: u64,
    pub read_block_bytes: u64,
}

impl AddAssign for BlockProfiler {
    fn add_assign(&mut self, rhs: Self) {
        self.read_cached_num += rhs.read_cached_num;
        self.read_block_num += rhs.read_block_num;
        self.read_files_time += rhs.read_files_time;
        self.read_files_bytes += rhs.read_files_bytes;
        self.read_block_bytes += rhs.read_block_bytes;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadProfiler {
    pub read_status: ReadStatus,
    pub read_total_time: Duration,
    pub block_profier: BlockProfiler,
    pub filter_num: u32,
    pub filter_bytes: u64,
}
