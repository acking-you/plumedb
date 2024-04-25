//! Block-related operations, an SSTable consists of multiple blocks

use bytes::{BufMut, Bytes};

use crate::common::key::{KeySlice, KeyVec};
use crate::common::size::{
    ArrayLenType, CheckSumType, KeyLenType, OffsetType, OverlapType, Size, ValueLenType,
};

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<OffsetType>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len: ArrayLenType = self.offsets.len().into();
        for offset in &self.offsets {
            offset.put_to_buffer(&mut buf);
        }
        // Adds number of elements at the end of the block
        offsets_len.put_to_buffer(&mut buf);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // get number of elements in the block
        let entry_offsets_len =
            ArrayLenType::get_from_buffer(&data[data.len() - ArrayLenType::RAW_SIZE..]);
        let offset_end = data.len() - ArrayLenType::RAW_SIZE;
        let offset_start =
            offset_end - Into::<usize>::into(entry_offsets_len) * OffsetType::RAW_SIZE;
        let offsets_raw = &data[offset_start..offset_end];
        // get offset array
        let offsets = offsets_raw
            .chunks(OffsetType::RAW_SIZE)
            .map(OffsetType::get_from_buffer)
            .collect();
        // retrieve data
        let data = data[0..offset_start].to_vec();
        Self { data, offsets }
    }

    /// The actual size of the block in the file.
    #[inline]
    pub fn block_size(&self) -> u64 {
        (self.data.len() + self.offsets.len() + CheckSumType::RAW_SIZE) as u64
    }
}

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<OffsetType>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: KeySlice, key: KeySlice) -> OverlapType {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
        }
        i += 1;
    }
    i.into()
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        ArrayLenType::RAW_SIZE /* number of key-value pairs in the block */ +  self.offsets.len() * OffsetType::RAW_SIZE /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size()
            + key.len()
            + value.len()
            + KeyLenType::RAW_SIZE
            + ValueLenType::RAW_SIZE
            + OffsetType::RAW_SIZE
            > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len().into());
        let buf = &mut self.data;
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        let key_len: KeyLenType = (key.len() - Into::<usize>::into(overlap)).into();
        let value_len: ValueLenType = value.len().into();
        // Encode key overlap.
        overlap.put_to_buffer(buf);
        // Encode key length.
        key_len.put_to_buffer(buf);
        // Encode key content.
        buf.put_slice(&key.raw_ref()[overlap.into()..]);
        // Encode value length.
        value_len.put_to_buffer(buf);
        // Encode value content.
        buf.put_slice(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there are no key-value pairs in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
