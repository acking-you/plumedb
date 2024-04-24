//! Used to locate the Block as well as provide some necessary meta information
use anyhow::{ensure, Result};
use bytes::{Buf, BufMut};

use crate::common::key::KeyBytes;
use crate::common::size::{ArrayLenType, CheckSumType, KeyLenType, OffsetType, Size};

/// Provides some meta information about the block, including `offset` for locating the block and
/// `first_key` and `last_key` for quickly determining the extent of the data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: OffsetType,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // Calculate the true size of the insert
        let mut estimated_size = ArrayLenType::RAW_SIZE;
        estimated_size += block_meta
            .iter()
            .map(|meta| {
                OffsetType::RAW_SIZE
                    + KeyLenType::RAW_SIZE * 2
                    + meta.first_key.len()
                    + meta.last_key.len()
            })
            .sum::<usize>();
        estimated_size += CheckSumType::RAW_SIZE;
        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        buf.reserve(estimated_size);
        let original_len = buf.len();
        let block_meta_len: ArrayLenType = block_meta.len().into();
        block_meta_len.put_to_buffer(buf);
        for meta in block_meta {
            meta.offset.put_to_buffer(buf);
            let first_key_len: KeyLenType = meta.first_key.len().into();
            first_key_len.put_to_buffer(buf);
            buf.put_slice(meta.first_key.raw_ref());
            let last_key_len: KeyLenType = meta.last_key.len().into();
            last_key_len.put_to_buffer(buf);
            buf.put_slice(meta.last_key.raw_ref());
        }
        CheckSumType::from_buffer(&buf[original_len + ArrayLenType::RAW_SIZE..]).put_to_buffer(buf);
        assert_eq!(
            estimated_size,
            buf.len() - original_len,
            "The added length must be consistent with the estimated value"
        );
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let array_len: usize = ArrayLenType::get_and_advance(&mut buf).into();
        let checksum = CheckSumType::from_buffer(&buf[..buf.remaining() - CheckSumType::RAW_SIZE]);
        for _ in 0..array_len {
            let offset = OffsetType::get_and_advance(&mut buf);
            let first_key_len = KeyLenType::get_and_advance(&mut buf);
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len.into()));
            let last_key_len = KeyLenType::get_and_advance(&mut buf);
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len.into()));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }

        ensure!(
            CheckSumType::get_and_advance(&mut buf) == checksum,
            "meta checksum mismatched"
        );

        Ok(block_meta)
    }
}
