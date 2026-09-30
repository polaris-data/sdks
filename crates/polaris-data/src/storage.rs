use std::{env, fs, path::PathBuf};

use directories::BaseDirs;

use crate::errors::PolarisError;

#[derive(Clone, Debug)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub daily_dir: PathBuf,
    pub cache_dir: PathBuf,
}

pub(crate) fn resolve_root(explicit: Option<PathBuf>) -> Result<PathBuf, PolarisError> {
    if let Some(root) = explicit {
        return Ok(expand_home(root));
    }
    if let Ok(root) = env::var("POLARIS_ROOT") {
        return Ok(expand_home(PathBuf::from(root)));
    }

    let base_dirs = BaseDirs::new().ok_or_else(|| {
        PolarisError::InvalidResponse("failed to resolve platform data directory".to_owned())
    })?;
    Ok(base_dirs.data_dir().join("polaris"))
}

fn expand_home(path: PathBuf) -> PathBuf {
    let Some(value) = path.to_str() else {
        return path;
    };
    let Some(home) = BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf()) else {
        return path;
    };
    if value == "~" {
        return home;
    }
    if let Some(suffix) = value.strip_prefix("~/") {
        return home.join(suffix);
    }
    path
}

pub(crate) fn ensure_layout(root: PathBuf) -> Result<StorageLayout, PolarisError> {
    let data_dir = root.join("data");
    let daily_dir = root.join("daily");
    let tmp_dir = root.join("tmp");
    let cache_dir = root.join("cache");
    let locks_dir = root.join("locks");
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&daily_dir)?;
    fs::create_dir_all(&tmp_dir)?;
    fs::create_dir_all(&cache_dir)?;
    fs::create_dir_all(&locks_dir)?;
    Ok(StorageLayout {
        root,
        daily_dir,
        cache_dir,
    })
}
