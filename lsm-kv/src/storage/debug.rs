use std::fmt::Display;

use super::lsm_storage::LsmStorageInner;
use super::LsmKV;
use crate::compact::CompactionOptions;

impl<T: CompactionOptions> Display for LsmStorageInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            write!(
                f,
                "\nL0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            )?;
        }
        for (level, files) in &snapshot.levels {
            write!(f, "\nL{level} ({}): {:?}", files.len(), files)?;
        }
        writeln!(f)
    }
}

impl<T: CompactionOptions> Display for LsmKV<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
