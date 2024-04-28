use std::time::{Duration, Instant};

use bytes::BufMut;
use tabled::Table;
use tempfile::tempdir;

use super::common::INIT_TRACING;
use crate::common::profier::ReadProfiler;
use crate::compact::leveled::LeveledCompactionOptions;
use crate::compact::CompactionOptions;
use crate::storage::lsm_storage::LsmStorageOptions;
use crate::storage::LsmKV;
use crate::tests::common::dump_files_in_dir;

#[test]
fn test_simple_compacted_ssts_leveled() {
    *INIT_TRACING;
    let ins = Instant::now();
    *INIT_TRACING;
    test_integration(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_bytes: 1 << 20, // 20MB
    });
    println!("cost time: {:?}", ins.elapsed());
}

/// Provision the storage such that base_level contains 2 SST files (target size is 2MB and each SST
/// is 1MB). This configuration has the effect that compaction will generate a new lower-level
/// containing more than 1 SST files, and leveled compaction should handle this situation correctly:
/// These files might not be sorted by first-key and should NOT be sorted inside the
/// `apply_compaction_result` function, because we don't have any actual SST loaded at the
/// point where this function is called during manifest recovery.
#[test]
fn test_multiple_compacted_ssts_leveled() {
    *INIT_TRACING;
    let compaction_options = LeveledCompactionOptions {
        level_size_multiplier: 4,
        level0_file_num_compaction_trigger: 2,
        max_levels: 2,
        base_level_size_bytes: 2,
    };

    let lsm_storage_options = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20, // 1MB
        compaction_options,
        enable_wal: false,
        num_memtable_limit: 2,
    };
    let dir = tempdir().unwrap();
    let storage = LsmKV::open(&dir, lsm_storage_options.clone()).unwrap();

    // Insert approximately 10MB of data to ensure that at least one compaction is triggered by
    // priority. Insert 500 key-value pairs where each pair is 2KB
    for i in 0..500 {
        let (key, val) = key_value_pair_with_target_size(i, 20 * 1024);
        storage.put(&key, &val).unwrap();
    }

    let mut prev_snapshot = storage.inner.state.read().clone();
    while {
        std::thread::sleep(Duration::from_secs(1));
        let snapshot = storage.inner.state.read().clone();
        let to_cont = prev_snapshot.sst_state.levels != snapshot.sst_state.levels
            || prev_snapshot.sst_state.l0_sstables != snapshot.sst_state.l0_sstables;
        prev_snapshot = snapshot;
        to_cont
    } {
        println!("waiting for compaction to converge");
    }

    storage.close().unwrap();
    assert!(storage.inner.state.read().memtable.is_empty());
    assert!(storage.inner.state.read().imm_memtables.is_empty());

    println!("{}", storage.show_level_status());
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = LsmKV::open(&dir, lsm_storage_options).unwrap();

    for i in 0..500 {
        let (key, val) = key_value_pair_with_target_size(i, 20 * 1024);
        let mut profiler = ReadProfiler::default();
        assert_eq!(
            &storage
                .get_with_profier(&mut profiler, &key)
                .unwrap()
                .unwrap()[..],
            &val
        );
        tracing::info!("profiler:\n {}", Table::new([&profiler]).to_string());
    }
}

/// Create a key value pair where key and value are of target size in bytes
fn key_value_pair_with_target_size(seed: i32, target_size_byte: usize) -> (Vec<u8>, Vec<u8>) {
    let mut key = vec![0; target_size_byte - 4];
    key.put_i32(seed);

    let mut val = vec![0; target_size_byte - 4];
    val.put_i32(seed);

    (key, val)
}

fn test_integration(compaction_options: impl CompactionOptions) {
    let dir = tempdir().unwrap();

    let options = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20, // 1MB
        compaction_options,
        enable_wal: false,
        num_memtable_limit: 2,
    };

    let storage = LsmKV::open(&dir, options.clone()).unwrap();
    for i in 0..=20 {
        storage.put(b"0", format!("v{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            storage.put(b"1", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"1").unwrap();
        }
        if i % 2 == 1 {
            storage.put(b"2", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"2").unwrap();
        }
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
    }
    storage.close().unwrap();
    // ensure all SSTs are flushed
    assert!(storage.inner.state.read().memtable.is_empty());
    assert!(storage.inner.state.read().imm_memtables.is_empty());
    println!("{}", storage.show_level_status());
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = LsmKV::open(&dir, options).unwrap();
    assert_eq!(&storage.get(b"0").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(storage.get(b"2").unwrap(), None);
}
