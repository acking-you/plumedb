use std::collections::HashSet;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::utils::compact_generate_sst_from_iter;
use super::{
    CompactionController, CompactionOptions, CompactionOptionsGetter, CompactionTask,
    CompactionType, LeveledCompactionGetter, TieredCompactionGetter,
};
use crate::common::id::TableId;
use crate::common::iterator::concat_iterator::SstConcatIterator;
use crate::common::iterator::merge_iterator::MergeIterator;
use crate::common::iterator::tow_merge_iterator::TwoMergeIterator;
use crate::storage::lsm_storage::LsmStorageState;
use crate::table::sstable::sst_iterator::SsTableIterator;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<TableId>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<TableId>,
    pub is_lower_level_bottom_level: bool,
}

impl CompactionTask for LeveledCompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        self.is_lower_level_bottom_level
    }

    fn compact(
        &self,
        ctx: super::CompactionContext<'_>,
    ) -> anyhow::Result<Vec<std::sync::Arc<crate::table::sstable::SsTable>>> {
        match self.upper_level {
            Some(_) => {
                let mut upper_ssts = Vec::with_capacity(self.upper_level_sst_ids.len());
                for id in self.upper_level_sst_ids.iter() {
                    upper_ssts.push(
                        ctx.state
                            .sstables
                            .get(id)
                            .with_context(|| {
                                format!("get table_id({id}) sstable fails when create `upper_ssts`")
                            })?
                            .clone(),
                    );
                }
                let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                let mut lower_ssts = Vec::with_capacity(self.lower_level_sst_ids.len());
                for id in self.lower_level_sst_ids.iter() {
                    lower_ssts.push(
                        ctx.state
                            .sstables
                            .get(id)
                            .with_context(|| {
                                format!("get table_id({id}) sstable fails when create `lower_ssts`")
                                    .clone()
                            })?
                            .clone(),
                    );
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                compact_generate_sst_from_iter(
                    &ctx,
                    TwoMergeIterator::create(upper_iter, lower_iter)?,
                    self.compact_to_bottom_level(),
                )
            }
            None => {
                let mut upper_iters = Vec::with_capacity(self.upper_level_sst_ids.len());
                for id in self.upper_level_sst_ids.iter() {
                    upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        ctx.state
                            .sstables
                            .get(id)
                            .with_context(|| {
                                format!(
                                    "get table_id({id}) sstable fails when create `upper_iters`"
                                )
                            })?
                            .clone(),
                    )?));
                }
                let upper_iter = MergeIterator::create(upper_iters);
                let mut lower_ssts = Vec::with_capacity(self.lower_level_sst_ids.len());
                for id in self.lower_level_sst_ids.iter() {
                    lower_ssts.push(
                        ctx.state
                            .sstables
                            .get(id)
                            .with_context(|| {
                                format!(
                                    "get table_id({id}) sstable fails when create `lower_ssts` \
                                     with l0 level"
                                )
                            })?
                            .clone(),
                    );
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                compact_generate_sst_from_iter(
                    &ctx,
                    TwoMergeIterator::create(upper_iter, lower_iter)?,
                    self.compact_to_bottom_level(),
                )
            }
        }
    }

    fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("LevelCompactionTask to json")
    }

    fn from_json(data: &str) -> anyhow::Result<Self> {
        serde_json::from_str(data).context("LevelCompactionTask from json")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

impl LeveledCompactionGetter for LeveledCompactionOptions {
    fn get_level_size_multiplier(&self) -> usize {
        self.level_size_multiplier
    }

    fn get_level0_file_num_compaction_trigger(&self) -> usize {
        self.level0_file_num_compaction_trigger
    }

    fn get_max_levels(&self) -> usize {
        self.max_levels
    }

    fn get_base_level_size_mb(&self) -> usize {
        self.base_level_size_mb
    }
}
impl TieredCompactionGetter for LeveledCompactionOptions {}
impl CompactionOptionsGetter for LeveledCompactionOptions {}

impl CompactionOptions for LeveledCompactionOptions {
    type Controller = LeveledCompactionController;
    type Task = LeveledCompactionTask;

    const COMPACTION_TYPE: CompactionType = CompactionType::Leveled;

    fn get_compact_controller(&self) -> Self::Controller {
        LeveledCompactionController::new(*self)
    }

    fn get_init_levels(
        &self,
    ) -> Vec<(crate::common::id::TableId, Vec<crate::common::id::TableId>)> {
        (1..=self.max_levels)
            .map(|i| (i.into(), Vec::new()))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl CompactionController for LeveledCompactionController {
    type Option = LeveledCompactionOptions;
    type Task = LeveledCompactionTask;

    const FLUSH_TO_L0: bool = true;

    fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<Self::Task> {
        // step 1: compute target level size
        let mut target_level_size: Vec<usize> = (0..self.options.max_levels).map(|_| 0).collect(); // exclude level 0
        let mut real_level_size: Vec<usize> = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| {
                        snapshot
                            .sstables
                            .get(x)
                            .expect("sstable get nerver fails")
                            .table_size()
                    })
                    .sum(),
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        // select base level and compute target level size
        target_level_size[self.options.max_levels - 1] =
            real_level_size[self.options.max_levels - 1].max(base_level_size_bytes);
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = this_level_size;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // Flush L0 SST is the top priority
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            tracing::info!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = real_level_size[level] as f64 / target_level_size[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            tracing::info!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );

            let level = *level;
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap(); // select the oldest sst to compact
            tracing::info!(
                "compaction triggered by priority: {level} out of {:?}, select {selected_sst} for \
                 compaction",
                priorities
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &Self::Task,
        output: &[crate::common::id::TableId],
    ) -> (LsmStorageState, Vec<crate::common::id::TableId>) {
        self.apply_compaction_result_inner::<true>(snapshot, task, output)
    }

    fn recover_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &Self::Task,
        output: &[TableId],
    ) -> (LsmStorageState, Vec<TableId>) {
        self.apply_compaction_result_inner::<false>(snapshot, task, output)
    }
}

impl LeveledCompactionController {
    fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[TableId],
        in_level: usize,
    ) -> Vec<TableId> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .expect("sst id must exist");
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .expect("sst id must exist");
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    fn apply_compaction_result_inner<const NEED_SORT: bool>(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[crate::common::id::TableId],
    ) -> (LsmStorageState, Vec<crate::common::id::TableId>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        assert!(lower_level_sst_ids_set.is_empty());
        new_lower_level_ssts.extend(output);

        // Recovery can not be early sort, when the use of Manifest recovery data when the SST has
        // not really created, you need to be in the SST are created and then sorted!
        if NEED_SORT {
            // SST needs to be reordered with first_key, as the Leveled Compaction strategy may only
            // obtain the Lower Level portion for merging
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .expect("sst must exist")
                    .first_key()
                    .cmp(
                        snapshot
                            .sstables
                            .get(y)
                            .expect("sst must exist")
                            .first_key(),
                    )
            });
        }

        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
