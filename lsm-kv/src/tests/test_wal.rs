use std::time::Instant;

use tempfile::tempdir;

use super::common::INIT_TRACING;
use crate::compact::leveled::LeveledCompactionOptions;
use crate::compact::CompactionOptions;
use crate::storage::lsm_storage::LsmStorageOptions;
use crate::storage::LsmKV;
use crate::tests::common::dump_files_in_dir;

#[test]
fn test_integration_leveled_wal() {
    let time = Instant::now();
    *INIT_TRACING;
    test_integration(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_bytes: 1 << 20, // 20MB
    });
    println!("cost time:{:?}", time.elapsed())
}

fn test_integration(compaction_options: impl CompactionOptions) {
    let dir = tempdir().unwrap();

    let options = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20, // 1MB
        compaction_options,
        enable_wal: true,
        num_memtable_limit: 2,
    };

    let storage = LsmKV::open(&dir, options.clone()).unwrap();
    for i in 0..=200 {
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
    // ensure some SSTs are not flushed
    assert!(
        !storage.inner.state.read().memtable.is_empty()
            || !storage.inner.state.read().imm_memtables.is_empty()
    );
    println!("{}", storage.show_level_status());
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = LsmKV::open(&dir, options).unwrap();
    assert_eq!(&storage.get(b"0").unwrap().unwrap()[..], b"v200".as_slice());
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"v200".as_slice());
    assert_eq!(storage.get(b"2").unwrap(), None);
}
