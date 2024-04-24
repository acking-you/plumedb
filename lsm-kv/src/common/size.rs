//! Some size-related wrappers

use std::hash::Hasher;

use bytes::{Buf, BufMut};

/// The raw length used by the size type
pub trait Size {
    /// The length of the original type used internally
    const RAW_SIZE: usize;
    /// Putting data into an external buffer
    fn put_to_buffer(&self, buf: &mut Vec<u8>);
    /// Getting data from an external buffer and advance pointer
    fn get_and_advance(buf: &mut &[u8]) -> Self;
    /// Getting data from an external buffer
    fn get_from_buffer(buf: &[u8]) -> Self;
}

/// Represents offset
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OffsetType(u32);

impl Size for OffsetType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

/// Represents key length
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyLenType(u32);

impl Size for KeyLenType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

/// Represents overlap index from first key
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OverlapType(u32);

impl Size for OverlapType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

/// Represents key length
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueLenType(u32);

impl Size for ValueLenType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

/// Represents checksum
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CheckSumType(u32);

impl CheckSumType {
    /// compute crc32 checksum
    pub fn from_buffer(buf: &[u8]) -> Self {
        Self(crc32fast::hash(buf))
    }
}

/// For build checksum
#[derive(Debug, Clone)]
pub struct CheckSumBuilder(crc32fast::Hasher);

impl Default for CheckSumBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckSumBuilder {
    /// New builder
    pub fn new() -> Self {
        Self(crc32fast::Hasher::new())
    }

    /// write bytes,such as value or key
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }

    /// write key len
    pub fn write_key_len(&mut self, key_len: KeyLenType) {
        self.0.write_u32(key_len.0)
    }

    /// write value len
    pub fn write_value_len(&mut self, value_len: ValueLenType) {
        self.0.write_u32(value_len.0)
    }

    /// Build checksum
    pub fn finish(self) -> CheckSumType {
        CheckSumType(self.0.finalize())
    }
}

impl Size for CheckSumType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

/// Represents array length type,such as metadata array
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArrayLenType(u32);

impl Size for ArrayLenType {
    const RAW_SIZE: usize = std::mem::size_of::<u32>();

    fn put_to_buffer(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.0)
    }

    fn get_and_advance(buf: &mut &[u8]) -> Self {
        Self(buf.get_u32())
    }

    fn get_from_buffer(mut buf: &[u8]) -> Self {
        Self(buf.get_u32())
    }
}

macro_rules! gen_impl_from_into_raw_type {
    ($custom_type:ty, $raw_type:ty, $inner_type:ty) => {
        impl From<$raw_type> for $custom_type {
            fn from(value: $raw_type) -> Self {
                Self(value as $inner_type)
            }
        }

        impl From<$custom_type> for $raw_type {
            fn from(value: $custom_type) -> Self {
                value.0 as $raw_type
            }
        }
    };
}

gen_impl_from_into_raw_type!(OffsetType, usize, u32);
gen_impl_from_into_raw_type!(OffsetType, u32, u32);
gen_impl_from_into_raw_type!(OffsetType, u64, u32);
gen_impl_from_into_raw_type!(KeyLenType, usize, u32);
gen_impl_from_into_raw_type!(KeyLenType, u32, u32);
gen_impl_from_into_raw_type!(OverlapType, usize, u32);
gen_impl_from_into_raw_type!(OverlapType, u32, u32);
gen_impl_from_into_raw_type!(ValueLenType, usize, u32);
gen_impl_from_into_raw_type!(ValueLenType, u32, u32);
gen_impl_from_into_raw_type!(ArrayLenType, usize, u32);
gen_impl_from_into_raw_type!(ArrayLenType, u32, u32);
