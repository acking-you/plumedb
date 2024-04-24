use std::time::Instant;

use tempfile::tempdir;

use super::common::{check_compaction_ratio, compaction_bench};
use crate::compact::leveled::LeveledCompactionOptions;
use crate::storage::lsm_storage::LsmStorageOptions;
use crate::storage::LsmKV;
use crate::tests::common::INIT_TRACING;

#[test]
fn test_integration() {
    *INIT_TRACING;
    let time = Instant::now();
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20, // 1MB
        compaction_options: LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 2,
            level_size_multiplier: 2,
            base_level_size_mb: 1,
            max_levels: 4,
        },
        enable_wal: false,
        num_memtable_limit: 2,
    };
    let storage = LsmKV::open(&dir, options).unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
    println!("cost time:{:?}", time.elapsed())
}
