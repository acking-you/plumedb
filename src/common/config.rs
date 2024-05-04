use std::path::PathBuf;

use anyhow::Context;
use clap::Subcommand;
use tracing_subscriber::{layer::SubscriberExt, Layer};

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


pub fn init_tracing<const NO_FORMAT: bool>() {
    if NO_FORMAT {
        no_format_tracing()
    } else {
        format_tracing()
    }
}

fn no_format_tracing() {
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .without_time()
            .with_writer(std::io::stdout)
            .with_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy(),
            ),
    );
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
}

fn format_tracing() {
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .pretty()
            .with_writer(std::io::stdout)
            .with_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy(),
            ),
    );
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
}
