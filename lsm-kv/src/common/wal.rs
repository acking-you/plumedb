//! WAL implementation with checksum
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use super::size::{CheckSumBuilder, CheckSumType, KeyLenType, Size, ValueLenType};
use crate::common::key::KeySlice;

/// Write-Ahead Logging is implemented to prevent data loss
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    /// Create a WAL by opening an existing or creating a non-existing file
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path.as_ref())
                    .with_context(|| {
                        format!("failed to create WAL with path:{:?}", path.as_ref())
                    })?,
            ))),
        })
    }

    /// Read contents of WAL file to recover data to skiplist.
    /// Note that the key and value is represented by u32 for
    /// length in the WAL
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        // FIXME: Is it wrong to read all the data at once if the file is too large?
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            // get key
            let mut checksum_builder = CheckSumBuilder::new();
            let key_len = KeyLenType::get_and_advance(&mut rbuf);
            checksum_builder.write_key_len(key_len);
            let key = Bytes::copy_from_slice(&rbuf[..key_len.into()]);
            checksum_builder.write_bytes(&key);
            rbuf.advance(key_len.into());
            // get value
            let value_len = ValueLenType::get_and_advance(&mut rbuf);
            checksum_builder.write_value_len(value_len);
            let value = Bytes::copy_from_slice(&rbuf[..value_len.into()]);
            checksum_builder.write_bytes(&value);
            rbuf.advance(value_len.into());
            let checksum = CheckSumType::get_and_advance(&mut rbuf);
            ensure!(checksum_builder.finish() == checksum, "checksum mismatch");
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Insert the KV data into the WAL.
    /// Note that the key and value is represented by u32 for
    /// length in the WAL
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + CheckSumType::RAW_SIZE * 2);
        let mut checksum_builder = CheckSumBuilder::new();
        let key_len: KeyLenType = key.len().into();
        let value_len: ValueLenType = value.len().into();
        // put key
        checksum_builder.write_key_len(key_len);
        key_len.put_to_buffer(&mut buf);
        checksum_builder.write_bytes(key.raw_ref());
        buf.put_slice(key.raw_ref());
        // put value
        checksum_builder.write_value_len(value_len);
        value_len.put_to_buffer(&mut buf);
        checksum_builder.write_bytes(value);
        buf.put_slice(value);
        // put checksum
        let checksum = checksum_builder.finish();
        checksum.put_to_buffer(&mut buf);
        file.write_all(&buf)?;
        Ok(())
    }

    /// Flush file
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
