use std::fmt::{Debug, Display};
use std::ops::AddAssign;
use std::time::{Duration, Instant};

use tabled::builder::Builder;
use tabled::settings::{Border, Panel, Style};
use tabled::{Table, Tabled};

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

#[derive(Debug, Clone, Copy, Default, Tabled)]
pub struct WriteProfiler {
    #[tabled(display_with("display_duration"))]
    pub write_wal_time: Duration,
    #[tabled(display_with("display_duration"))]
    pub write_total_time: Duration,
    #[tabled(display_with("display_duration"))]
    pub freeze_time: Duration,
    #[tabled(skip)]
    pub status: WriteStatus,
}

#[derive(Debug, Clone, Copy, Default, Tabled)]
pub struct WriteStatus {
    #[tabled(display_with("display_duration"))]
    pub read_lock_time: Duration,
    /// write lock time (get state lock or get state write lock)
    #[tabled(display_with("display_duration"))]
    pub write_lock_time: Duration,
    pub write_num: u64,
    #[tabled(display_with("display_bytes"))]
    pub write_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadStatus {
    MemTable,
    SSTable,
}

impl Display for ReadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadStatus::MemTable => write!(f, "MemTable"),
            ReadStatus::SSTable => write!(f, "SSTable"),
        }
    }
}

impl Default for ReadStatus {
    fn default() -> Self {
        Self::MemTable
    }
}

#[derive(Debug, Clone, Copy, Default, Tabled)]
pub struct BlockProfiler {
    pub read_cached_num: u32,
    pub read_block_num: u32,
    #[tabled(display_with("display_duration"))]
    pub read_files_time: Duration,
    #[tabled(display_with("display_bytes"))]
    pub read_files_bytes: u64,
    #[tabled(display_with("display_bytes"))]
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

#[derive(Debug, Clone, Copy, Default, Tabled)]
pub struct ReadProfiler {
    pub read_status: ReadStatus,
    #[tabled(display_with("display_duration"))]
    pub read_total_time: Duration,
    #[tabled(display_with("display_duration"))]
    pub read_lock_time: Duration,
    pub filter_num: u32,
    #[tabled(display_with("display_bytes"))]
    pub filter_bytes: u64,
    #[tabled(skip)]
    pub block_profier: BlockProfiler,
}

pub trait DisplayBytes: Display + Clone + Debug + Default + Copy + 'static {
    fn cast_f64(&self) -> f64;
}

impl DisplayBytes for u64 {
    fn cast_f64(&self) -> f64 {
        *self as f64
    }
}

impl DisplayBytes for usize {
    fn cast_f64(&self) -> f64 {
        *self as f64
    }
}

pub fn get_format_read_profiler(read_profiler: &ReadProfiler) -> Table {
    let mut builder = Builder::new();
    builder.push_record([format!("{}", get_format_tabled(read_profiler))]);
    builder.push_record([format!(
        "{}",
        get_format_tabled(read_profiler.block_profier)
    )]);
    let mut table = builder.build();
    table
        .with(Panel::header("ReadProfiler"))
        .with(Style::modern().frame(Border::inherit(Style::rounded())));
    table
}

pub fn get_format_write_profiler(write_profiler: &WriteProfiler) -> Table {
    let mut builder = Builder::new();
    builder.push_record([format!("{}", get_format_tabled(write_profiler))]);
    builder.push_record([format!("{}", get_format_tabled(write_profiler.status))]);
    let mut table = builder.build();
    table
        .with(Panel::header("WriteProfiler"))
        .with(Style::modern().frame(Border::inherit(Style::rounded())));
    table
}

pub fn get_format_block_profiler(block_profiler: &BlockProfiler) -> Table {
    let mut builder = Builder::new();
    builder.push_record([format!("{}", get_format_tabled(block_profiler))]);
    let mut table = builder.build();
    table
        .with(Panel::header("BlockProfiler"))
        .with(Style::modern().frame(Border::inherit(Style::rounded())));
    table
}

pub(crate) fn get_format_tabled<T: Tabled>(item: T) -> Table {
    let mut table = Table::new([item]);
    table.with(Style::modern().frame(Border::inherit(Style::rounded())));
    table
}

pub fn display_bytes<T: DisplayBytes>(&raw_bytes: &T) -> String {
    const BYTES_MULTIPLIER: f64 = 1024.0;
    enum BytesType {
        B,
        Kb,
        Mb,
        Gb,
        Tb,
    }
    impl Display for BytesType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                BytesType::B => write!(f, "B"),
                BytesType::Kb => write!(f, "KB"),
                BytesType::Mb => write!(f, "MB"),
                BytesType::Gb => write!(f, "GB"),
                BytesType::Tb => write!(f, "TB"),
            }
        }
    }

    let mut bytes = raw_bytes.cast_f64();
    let mut bytes_type = BytesType::B;
    if bytes >= BYTES_MULTIPLIER {
        bytes /= BYTES_MULTIPLIER;
        bytes_type = BytesType::Kb;
    }
    if bytes >= BYTES_MULTIPLIER {
        bytes /= BYTES_MULTIPLIER;
        bytes_type = BytesType::Mb;
    }
    if bytes >= BYTES_MULTIPLIER {
        bytes /= BYTES_MULTIPLIER;
        bytes_type = BytesType::Gb;
    }
    if bytes >= BYTES_MULTIPLIER {
        bytes /= BYTES_MULTIPLIER;
        bytes_type = BytesType::Tb;
    }

    format!("{raw_bytes}({bytes:.3}{bytes_type})")
}

fn display_duration(o: &Duration) -> String {
    format!("{o:?}")
}
