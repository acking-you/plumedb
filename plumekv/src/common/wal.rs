//! WAL implementation with checksum
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::common::key::{KeyBytes, KeySlice};

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
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    /// Read contents of WAL file to recover data to skiplist.
    /// Note that the key and value is represented by u32 for
    /// length in the WAL
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
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
            let mut hasher = crc32fast::Hasher::new();
            let key_len = rbuf.get_u32() as usize;
            hasher.write_u32(key_len as u32);
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            hasher.write(&key);
            rbuf.advance(key_len);
            // get value
            let value_len = rbuf.get_u32() as usize;
            hasher.write_u32(value_len as u32);
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            hasher.write(&value);
            rbuf.advance(value_len);
            let checksum = rbuf.get_u32();
            if hasher.finalize() != checksum {
                bail!("checksum mismatch");
            }
            skiplist.insert(KeyBytes::from_bytes(key), value);
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
            Vec::with_capacity(key.key_len() + value.len() + std::mem::size_of::<u64>());
        let mut hasher = crc32fast::Hasher::new();
        // put key
        hasher.write_u32(key.key_len() as u32);
        buf.put_u32(key.key_len() as u32);
        hasher.write(key.key_ref());
        buf.put_slice(key.key_ref());
        // put value
        hasher.write_u32(value.len() as u32);
        buf.put_u32(value.len() as u32);
        buf.put_slice(value);
        hasher.write(value);
        // put checksum
        buf.put_u32(hasher.finalize());
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
