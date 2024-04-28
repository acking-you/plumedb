use std::path::PathBuf;

use anyhow::Context;

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
