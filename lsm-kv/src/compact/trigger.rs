use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use super::{CompactionController, CompactionOptions, CompactionTask};
use crate::compact::CompactionContext;
use crate::storage::lsm_storage::LsmStorageInner;
use crate::storage::manifest::ManifestRecord;

fn trigger_compaction<T: CompactionOptions>(storage: &LsmStorageInner<T>) -> Result<()> {
    let snapshot = {
        let state = storage.state.read();
        state.clone()
    };
    let task = storage
        .compaction_controller
        .generate_compaction_task(&snapshot);
    let Some(task) = task else {
        return Ok(());
    };
    tracing::info!("before compaction:\n{}", storage.show_level_status());
    tracing::info!("running compaction task: {:?}", task);
    let sstables = task.compact(CompactionContext {
        sst_id_builder: &storage.sst_id_builder,
        path: &storage.folder,
        state: &snapshot,
        block_size: storage.options.block_size,
        target_sst_size: storage.options.target_sst_size,
        block_cache: storage.block_cache.clone(),
    })?;

    let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
    let ssts_to_remove = {
        let state_lock = storage.state_lock.lock();
        let mut snapshot = storage.state.read().as_ref().clone();
        let mut new_sst_ids = Vec::new();
        for file_to_add in sstables {
            new_sst_ids.push(file_to_add.sst_id());
            let result = snapshot
                .sst_state
                .sstables
                .insert(file_to_add.sst_id(), file_to_add);
            assert!(result.is_none());
        }
        let (mut snapshot, files_to_remove) = storage
            .compaction_controller
            .apply_compaction_result(&snapshot, &task, &output);
        let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
        for file_to_remove in &files_to_remove {
            let result = snapshot.sst_state.sstables.remove(file_to_remove);
            assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
            ssts_to_remove.push(result.unwrap());
        }
        let mut state = storage.state.write();
        *state = Arc::new(snapshot);
        drop(state);
        storage.folder.sync_dir()?;
        storage.manifest.add_record(
            &state_lock,
            ManifestRecord::Compaction(task.to_json()?, new_sst_ids),
        )?;
        ssts_to_remove
    };
    tracing::info!(
        "compaction finished: {} files removed, {} files added, output={:?}",
        ssts_to_remove.len(),
        output.len(),
        output
    );
    for sst in ssts_to_remove {
        std::fs::remove_file(storage.folder.path_of_sst(sst.sst_id()))?;
    }
    storage.folder.sync_dir()?;

    Ok(())
}

pub(crate) fn spawn_compaction_thread<T: CompactionOptions>(
    storage: &Arc<LsmStorageInner<T>>,
    rx: crossbeam_channel::Receiver<()>,
) -> Result<Option<std::thread::JoinHandle<()>>> {
    const COMPACTION_INTERVAL: Duration = Duration::from_millis(50);
    let this = storage.clone();
    let handle = std::thread::spawn(move || {
        let ticker = crossbeam_channel::tick(COMPACTION_INTERVAL);
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => if let Err(e) = trigger_compaction(&this) {
                    tracing::error!("compaction failed: {}", e);
                },
                recv(rx) -> _ => return
            }
        }
    });
    Ok(Some(handle))
}

fn trigger_flush<T: CompactionOptions>(storage: &LsmStorageInner<T>) -> Result<()> {
    let res = {
        let state = storage.state.read();
        state.imm_memtables.len() >= storage.options.num_memtable_limit
    };
    if res {
        storage.force_flush_next_imm_memtable()?;
    }

    Ok(())
}

pub(crate) fn spawn_flush_thread<T: CompactionOptions>(
    storage: &Arc<LsmStorageInner<T>>,
    rx: crossbeam_channel::Receiver<()>,
) -> anyhow::Result<Option<std::thread::JoinHandle<()>>> {
    const FLUSH_INTERVAL: Duration = Duration::from_millis(50);
    let this = storage.clone();
    let handle = std::thread::spawn(move || {
        let ticker = crossbeam_channel::tick(FLUSH_INTERVAL);
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => if let Err(e) = trigger_flush(&this) {
                    tracing::info!("flush failed: {}", e);
                },
                recv(rx) -> _ => return
            }
        }
    });
    Ok(Some(handle))
}
