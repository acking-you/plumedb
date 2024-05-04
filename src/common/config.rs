use std::path::PathBuf;

use anyhow::Context;
use clap::Subcommand;

pub fn get_user_path() -> anyhow::Result<PathBuf> {
    if cfg!(windows) {
        Ok(std::env::var_os("USERPROFILE")
            .context("USERPROFILE")?
            .into())
    } else {
        Ok(std::env::var_os("HOME").context("HOME")?.into())
    }
}

const LSM_DIR: &str = ".lsm";

pub fn get_lsm_path() -> anyhow::Result<PathBuf> {
    Ok(get_user_path()?.join(LSM_DIR))
}

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum CompactionOptions {
    Leveled {
        /// Multiplier for lower level and upper level storage capacity. By default it 4
        #[arg(long, default_value_t = 4)]
        level_size_multiplier: usize,
        /// Minimum number of level0 files to trigger a compaction. By default it is 2
        #[arg(short, long, default_value_t = 2)]
        level0_file_num_compaction_trigger: usize,
        /// Maximum number of levels. By default it is 2
        #[arg(short, long, default_value_t = 2)]
        max_levels: usize,
        /// Used to calculate the minimum capacity of each level's storage capacity based on the
        /// multiplier, depending on whether the capacity of the bottom level exceeds that
        /// capacity. By default it is 2MB
        #[arg(short, long, default_value_t = 2<<20)]
        base_level_size_bytes: usize,
    },
    Tiered {},
}
