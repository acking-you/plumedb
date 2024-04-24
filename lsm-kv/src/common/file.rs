//! Some file-related wrappers

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::id::TableId;

/// A file object with length
pub struct FileObject(File, usize);

impl FileObject {
    /// read data from offset and len
    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len];
        self.0.read_exact_at(&mut data[..], offset as u64)?;
        Ok(data)
    }

    /// file size
    pub fn size(&self) -> usize {
        self.1
    }

    /// Create a new file object and write the file to the disk
    pub fn create_and_write<P: AsRef<Path> + Copy>(path: P, data: &[u8]) -> Result<Self> {
        std::fs::write(path, data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            File::options().read(true).write(false).open(path)?,
            data.len(),
        ))
    }

    /// Crate [`FileObject`]
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(file, size as usize))
    }
}

/// Provide file paths to get different types of files
#[derive(Debug, Clone)]
pub struct FilePath(PathBuf);

impl FilePath {
    pub fn exists(&self) -> bool {
        self.0.exists()
    }

    /// New filepath
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_path_buf())
    }

    fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    /// get sst file path
    pub fn path_of_sst(&self, id: TableId) -> PathBuf {
        Self::path_of_sst_static(&self.0, id.into())
    }

    fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    /// get wal file path
    pub fn path_of_wal(&self, id: TableId) -> PathBuf {
        Self::path_of_wal_static(&self.0, id.into())
    }

    /// Sync folder is necessary, check the description below for details:
    /// > Calling fsync() does not necessarily ensure that the entry in the directory containing the
    /// > file has also reached disk. For that an explicit fsync() on a file descriptor for the
    /// > directory
    /// > is also needed. [link](https://unix.stackexchange.com/questions/414749/how-does-fsync-treat-directory-links)
    pub fn sync_dir(&self) -> Result<()> {
        File::open(&self.0)?.sync_all()?;
        Ok(())
    }

    /// Sync WAL file
    pub fn sync_wal(&self, id: TableId) -> Result<()> {
        File::open(self.path_of_wal(id))?.sync_all()?;
        Ok(())
    }

    /// Sync SST file
    pub fn sync_sst(&self, id: TableId) -> Result<()> {
        File::open(self.path_of_sst(id))?.sync_all()?;
        Ok(())
    }
}
