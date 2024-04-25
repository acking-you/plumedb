//! SSTable-related processing

use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use self::block::{Block, BlockBuilder};
use self::block_meta::BlockMeta;
use self::bloom::Bloom;
use crate::common::cache::BlockCache;
use crate::common::file::FileObject;
use crate::common::id::TableId;
use crate::common::key::{KeyBytes, KeySlice, KeyVec};
use crate::common::size::{CheckSumType, OffsetType, Size};

pub mod block;
pub mod block_iterator;
pub mod block_meta;
mod bloom;
pub mod sst_iterator;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum SStableError {
    #[snafu(display("Failed to read bloom filter offset"))]
    ReadBloomOffset { source: anyhow::Error },
    #[snafu(display("Failed to read bloom filter"))]
    ReadBloom { source: anyhow::Error },
    #[snafu(display("Failed to decode bloom filter"))]
    DecodeBloom { source: anyhow::Error },
    #[snafu(display("Failed to read metadata offset"))]
    ReadMetaOffset { source: anyhow::Error },
    #[snafu(display("Failed to read metadata"))]
    ReadMeta { source: anyhow::Error },
    #[snafu(display("Failed to decode metadata"))]
    DecodeMeta { source: anyhow::Error },
    #[snafu(display("Failed to read block data with checksum"))]
    ReadBlockDataWithCheckSum { source: anyhow::Error },
    #[snafu(display("Checksum miss match"))]
    CheckSumMissMatch,
    #[snafu(display("Failed read block from cache"))]
    ReadBlockFromCach { source: anyhow::Error },
    #[snafu(display("Failed to presist SSTable"))]
    SSTablePersist { source: anyhow::Error },
    #[snafu(display("Metadata is empty when build SSTable"))]
    SSTableMetaDataNotEmpty,
}

pub(super) type Result<T, E = SStableError> = std::result::Result<T, E>;

/// Structure of SSTable
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: OffsetType,
    pub(super) id: TableId,
    pub(super) block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Bloom,
}
impl SsTable {
    /// Open SSTable from a file.
    pub fn open(
        id: TableId,
        block_cache: Option<Arc<BlockCache>>,
        file: FileObject,
    ) -> Result<Self> {
        let len = file.size();
        let offset_raw_size = OffsetType::RAW_SIZE;
        // get bloom filter
        let raw_bloom_offset = file
            .read(len - offset_raw_size, offset_raw_size)
            .context(ReadBloomOffsetSnafu)?;
        let bloom_offset = OffsetType::get_from_buffer(&raw_bloom_offset).into();
        let raw_bloom = file
            .read(bloom_offset, len - bloom_offset - offset_raw_size)
            .context(ReadBloomSnafu)?;
        let bloom = Bloom::decode(&raw_bloom).context(DecodeBloomSnafu)?;
        // get blocks metadata
        let raw_meta_offset = file
            .read(bloom_offset - offset_raw_size, offset_raw_size)
            .context(ReadMetaOffsetSnafu)?;

        let block_meta_offset = OffsetType::get_from_buffer(&raw_meta_offset[..]).into();
        let raw_meta = file
            .read(
                block_meta_offset,
                bloom_offset - block_meta_offset - offset_raw_size,
            )
            .context(ReadMetaSnafu)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..]).context(DecodeMetaSnafu)?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset.into(),
            id,
            block_cache,
            bloom,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset: usize = self.block_meta[block_idx].offset.into();
        let offset_end: usize = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset.into(), |x| x.offset.into());
        let block_len = offset_end - offset - OffsetType::RAW_SIZE;
        let block_data_with_checksum: Vec<u8> = self
            .file
            .read(offset, offset_end - offset)
            .context(ReadBlockDataWithCheckSumSnafu)?;
        let block_data = &block_data_with_checksum[..block_len];
        let checksum = CheckSumType::get_from_buffer(&block_data_with_checksum[block_len..]);
        ensure!(
            checksum == CheckSumType::from_buffer(block_data),
            CheckSumMissMatchSnafu
        );
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id.into(), block_idx), || {
                    self.read_block(block_idx)
                        .map_err(|e| anyhow!("{}", snafu::Report::from_error(e)))
                })
                .context(ReadBlockFromCachSnafu)?;

            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    /// The first key of [`SsTable`] is also the smallest key
    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    /// The last key of [`SsTable`] is also the biggest key
    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    /// file size
    pub fn table_size(&self) -> usize {
        self.file.size()
    }

    /// sstable id
    pub fn sst_id(&self) -> TableId {
        self.id
    }
}

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len().into(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        let checksum = CheckSumType::from_buffer(&encoded_block);
        self.data.extend(encoded_block);
        checksum.put_to_buffer(&mut self.data);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to
    /// manipulate the disk objects.
    pub fn build(
        mut self,
        id: TableId,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let buf = &mut self.data;
        let meta_offset: OffsetType = buf.len().into();
        // put meta datas
        BlockMeta::encode_block_meta(&self.meta, buf);
        meta_offset.put_to_buffer(buf);
        // put bloom filter
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset: OffsetType = buf.len().into();
        bloom.encode(buf);
        bloom_offset.put_to_buffer(buf);
        let file = FileObject::create_and_write(path.as_ref(), buf).context(SSTablePersistSnafu)?;
        Ok(SsTable {
            id,
            file,
            first_key: self
                .meta
                .first()
                .context(SSTableMetaDataNotEmptySnafu)?
                .first_key
                .clone(),
            last_key: self
                .meta
                .last()
                .context(SSTableMetaDataNotEmptySnafu)?
                .last_key
                .clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom,
        })
    }
}
