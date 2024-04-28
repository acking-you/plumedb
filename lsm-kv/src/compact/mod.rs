//! compaction operator
pub mod leveled;
pub mod trigger;
pub mod utils;

use std::fmt::{Debug, Display};
use std::sync::Arc;

use anyhow::Result;

use crate::common::cache::BlockCache;
use crate::common::file::FileFolder;
use crate::common::id::{TableId, TableIdBuilder};
use crate::storage::lsm_storage::LsmStorageState;
use crate::table::sstable::SsTable;

/// todo
pub trait CompactionTask: Debug + Clone + Send + Sync + 'static {
    fn compact_to_bottom_level(&self) -> bool;

    fn compact(&self, compaction_ctx: CompactionContext<'_>) -> Result<Vec<Arc<SsTable>>>;

    fn to_json(&self) -> Result<String>;

    fn from_json(data: &str) -> Result<Self>;
}

pub enum CompactionType {
    Leveled,
    Tiered,
}

impl Display for CompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionType::Leveled => write!(f, "Leveled"),
            CompactionType::Tiered => write!(f, "Tiered"),
        }
    }
}

/// Leveled compaction must implement this trait
pub trait LeveledCompactionGetter {
    fn get_level_size_multiplier(&self) -> usize {
        unimplemented!("Levled comaction must implement this")
    }
    fn get_level0_file_num_compaction_trigger(&self) -> usize {
        unimplemented!("Levled comaction must implement this")
    }
    fn get_max_levels(&self) -> usize {
        unimplemented!("Levled comaction must implement this")
    }
    fn get_base_level_size_mb(&self) -> usize {
        unimplemented!("Levled comaction must implement this")
    }
}

/// Tiered compaction must implement this trait
pub trait TieredCompactionGetter {
    fn get_num_tiers(&self) -> usize {
        unimplemented!("Tiered comaction must implement this")
    }
    fn get_max_size_amplification_percent(&self) -> usize {
        unimplemented!("Tiered comaction must implement this")
    }
    fn get_size_ratio(&self) -> usize {
        unimplemented!("Tiered comaction must implement this")
    }
    fn get_min_merge_width(&self) -> usize {
        unimplemented!("Tiered comaction must implement this")
    }
}

pub trait CompactionOptionsGetter: LeveledCompactionGetter + TieredCompactionGetter {}

pub trait CompactionOptions:
    CompactionOptionsGetter + Debug + Clone + Send + Sync + Display + 'static
{
    type Controller: CompactionController<Option = Self, Task = Self::Task>;

    type Task: CompactionTask;

    const COMPACTION_TYPE: CompactionType;

    fn get_compact_controller(&self) -> Self::Controller;

    fn get_init_levels(&self) -> Vec<(TableId, Vec<TableId>)>;
}

pub trait CompactionController: Debug + Clone + Send + Sync + 'static {
    type Task: CompactionTask;
    type Option: CompactionOptions<Controller = Self>;

    const FLUSH_TO_L0: bool;

    fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<Self::Task>;

    fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &Self::Task,
        output: &[TableId],
    ) -> (LsmStorageState, Vec<TableId>);

    /// This method is called when recovering from [crate::storage::manifest::Manifest], currently
    /// only for Leveled Compaction, when this method is called and it is a Leveled Compaction,
    /// you need to wait for the SSTable to be fully recovered before sorting it.
    fn recover_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &Self::Task,
        output: &[TableId],
    ) -> (LsmStorageState, Vec<TableId>) {
        self.apply_compaction_result(snapshot, task, output)
    }
}

pub struct CompactionContext<'a> {
    // SSt id builder
    sst_id_builder: &'a TableIdBuilder,
    // filepath builder
    path: &'a FileFolder,
    state: &'a LsmStorageState,
    // Block size in bytes
    block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    target_sst_size: usize,
    block_cache: Arc<BlockCache>,
}
