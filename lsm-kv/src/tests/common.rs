use std::collections::BTreeMap;
use std::ops::Bound;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use once_cell::sync::Lazy;

use crate::common::config::init_tracing;
use crate::common::iterator::StorageIterator;
use crate::compact::{CompactionOptions, CompactionType};
use crate::storage::LsmKV;

pub static INIT_TRACING: Lazy<()> = Lazy::new(|| {
    println!("init tracing");
    init_tracing::<true>();
});

pub fn dump_files_in_dir(path: impl AsRef<Path>) {
    println!("--- DIR DUMP ---");
    for f in path.as_ref().read_dir().unwrap() {
        let f = f.unwrap();
        print!("{}", f.path().display());
        println!(
            ", size={:.3}KB",
            f.metadata().unwrap().size() as f64 / 1024.0
        );
    }
}

pub fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

pub fn check_lsm_iter_result_by_key<I>(iter: &mut I, expected: Vec<(Bytes, Bytes)>)
where
    I: for<'a> StorageIterator<KeyType<'a> = &'a [u8]>,
{
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key()),
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

pub fn compaction_bench<T: CompactionOptions>(storage: Arc<LsmKV<T>>) {
    let mut key_map = BTreeMap::<usize, usize>::new();
    let gen_key = |i| format!("{:010}", i); // 10B
    let gen_value = |i| format!("{:0110}", i); // 110B
    let mut max_key = 0;
    let overlaps = 20000;
    for iter in 0..10 {
        let range_begin = iter * 5000;
        for i in range_begin..(range_begin + overlaps) {
            // 120B per key, 4MB data populated
            let key: String = gen_key(i);
            let version = key_map.get(&i).copied().unwrap_or_default() + 1;
            let value = gen_value(version);
            key_map.insert(i, version);
            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
            max_key = max_key.max(i);
        }
    }

    std::thread::sleep(Duration::from_secs(1)); // wait until all memtables flush
    while {
        let snapshot = storage.inner.state.read();
        !snapshot.imm_memtables.is_empty()
    } {
        storage.inner.force_flush_next_imm_memtable().unwrap();
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

    let mut expected_key_value_pairs = Vec::new();
    for i in 0..(max_key + 40000) {
        let key = gen_key(i);
        let value = storage.get(key.as_bytes()).unwrap();
        if let Some(val) = key_map.get(&i) {
            let expected_value = gen_value(*val);
            assert_eq!(value, Some(Bytes::from(expected_value.clone())));
            expected_key_value_pairs.push((Bytes::from(key), Bytes::from(expected_value)));
        } else {
            assert!(value.is_none());
        }
    }

    check_lsm_iter_result_by_key(
        &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        expected_key_value_pairs,
    );

    println!("{}", storage.show_level_status());

    println!(
        "This test case does not guarantee your compaction algorithm produces a LSM state as \
         expected. It only does minimal checks on the size of the levels. Please use the \
         compaction simulator to check if the compaction is correctly going on."
    );
}

pub fn check_compaction_ratio<T: CompactionOptions>(storage: Arc<LsmKV<T>>) {
    let state = storage.inner.state.read().clone();
    let mut level_size = Vec::new();
    let options = &storage.inner.options.compaction_options;
    let l0_sst_num = state.sst_state.l0_sstables.len();
    for (_, files) in &state.sst_state.levels {
        let size = match T::COMPACTION_TYPE {
            CompactionType::Leveled => files
                .iter()
                .map(|x| {
                    state
                        .sst_state
                        .sstables
                        .get(x)
                        .as_ref()
                        .unwrap()
                        .table_size() as u64
                })
                .sum::<u64>(),
            CompactionType::Tiered => files.len() as u64,
        };
        level_size.push(size);
    }
    let extra_iterators = 0;
    let num_iters = storage
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .num_active_iterators();
    let num_memtables = storage.inner.state.read().imm_memtables.len() + 1;
    match T::COMPACTION_TYPE {
        CompactionType::Leveled => {
            let level0_file_num_compaction_trigger =
                options.get_level0_file_num_compaction_trigger();
            let level_size_multiplier = options.get_level_size_multiplier();
            let max_levels = options.get_max_levels();
            assert!(l0_sst_num < level0_file_num_compaction_trigger);
            assert!(level_size.len() <= max_levels);
            let last_level_size = *level_size.last().unwrap();
            let mut multiplier = 1.0;
            for idx in (1..level_size.len()).rev() {
                multiplier *= level_size_multiplier as f64;
                let this_size = level_size[idx - 1];
                assert!(
                    // do not add hard requirement on level size multiplier considering bloom
                    // filters...
                    this_size as f64 / last_level_size as f64 <= 1.0 / multiplier + 0.5,
                    "L{}/L_max, {}/{}>>1.0/{}",
                    state.sst_state.levels[idx - 1].0,
                    this_size,
                    last_level_size,
                    multiplier
                );
            }
            assert!(
                num_iters <= l0_sst_num + num_memtables + max_levels + extra_iterators,
                "we found {num_iters} iterators in your implementation, (l0_sst_num={l0_sst_num}, \
                 num_memtables={num_memtables}, max_levels={max_levels}) did you use concat \
                 iterators?"
            );
        }
        CompactionType::Tiered => {
            let num_tiers = options.get_num_tiers();
            let max_size_amplification_percent = options.get_max_size_amplification_percent();
            let size_ratio = options.get_size_ratio();
            let min_merge_width = options.get_min_merge_width();
            let size_ratio_trigger = (100.0 + size_ratio as f64) / 100.0;
            assert_eq!(l0_sst_num, 0);
            assert!(level_size.len() <= num_tiers);
            let mut sum_size = level_size[0];
            for idx in 1..level_size.len() {
                let this_size = level_size[idx];
                if level_size.len() > min_merge_width {
                    assert!(
                        sum_size as f64 / this_size as f64 <= size_ratio_trigger,
                        "violation of size ratio: sum(⬆️L{})/L{}, {}/{}>{}",
                        state.sst_state.levels[idx - 1].0,
                        state.sst_state.levels[idx].0,
                        sum_size,
                        this_size,
                        size_ratio_trigger
                    );
                }
                if idx + 1 == level_size.len() {
                    assert!(
                        sum_size as f64 / this_size as f64
                            <= max_size_amplification_percent as f64 / 100.0,
                        "violation of space amp: sum(⬆️L{})/L{}, {}/{}>{}%",
                        state.sst_state.levels[idx - 1].0,
                        state.sst_state.levels[idx].0,
                        sum_size,
                        this_size,
                        max_size_amplification_percent
                    );
                }
                sum_size += this_size;
            }
            assert!(
                num_iters <= num_memtables + num_tiers + extra_iterators,
                "we found {num_iters} iterators in your implementation, \
                 (num_memtables={num_memtables}, num_tiers={num_tiers}) did you use concat \
                 iterators?"
            );
        }
    }
}
