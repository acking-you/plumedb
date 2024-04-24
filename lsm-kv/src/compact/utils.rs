use std::sync::Arc;

use anyhow::Result;

use super::CompactionContext;
use crate::common::iterator::StorageIterator;
use crate::common::key::KeySlice;
use crate::table::sstable::{SsTable, SsTableBuilder};

pub(crate) fn compact_generate_sst_from_iter<'a>(
    ctx: &CompactionContext<'a>,
    mut iter: impl for<'b> StorageIterator<KeyType<'b> = KeySlice<'b>>,
    compact_to_bottom_level: bool,
) -> Result<Vec<Arc<SsTable>>> {
    let mut builder = None;
    let mut new_sst = Vec::new();

    while iter.is_valid() {
        if builder.is_none() {
            builder = Some(SsTableBuilder::new(ctx.block_size));
        }
        let builder_inner = builder.as_mut().unwrap();
        if compact_to_bottom_level {
            if !iter.value().is_empty() {
                builder_inner.add(iter.key(), iter.value());
            }
        } else {
            builder_inner.add(iter.key(), iter.value());
        }
        iter.next()?;

        if builder_inner.estimated_size() >= ctx.target_sst_size {
            let sst_id = ctx.sst_id_builder.next_id();
            let builder = builder.take().unwrap();
            let sst = Arc::new(builder.build(
                sst_id,
                Some(ctx.block_cache.clone()),
                ctx.path.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
    }
    if let Some(builder) = builder {
        let sst_id = ctx.sst_id_builder.next_id(); // lock dropped here
        let sst = Arc::new(builder.build(
            sst_id,
            Some(ctx.block_cache.clone()),
            ctx.path.path_of_sst(sst_id),
        )?);
        new_sst.push(sst);
    }
    Ok(new_sst)
}
