use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use directories::BaseDirs;
use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::{errors::PolarisError, models::SnapshotEntry};

const SNAPSHOT_EXT: &str = ".jsonl.zst";

#[derive(Clone, Debug)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub data_dir: PathBuf,
    pub daily_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub locks_dir: PathBuf,
}

pub(crate) struct SyncLockGuard {
    file: fs::File,
}

impl Drop for SyncLockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LocalSnapshotFile {
    pub entry: SnapshotEntry,
    pub path: PathBuf,
    pub download_url: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotKeyParts {
    pub tier: String,
    pub source: String,
    pub market: String,
    pub date: String,
    pub timestamp: Option<String>,
    pub hour: Option<u8>,
    pub filename: String,
}

pub(crate) fn resolve_root(explicit: Option<PathBuf>) -> Result<PathBuf, PolarisError> {
    if let Some(root) = explicit {
        return Ok(expand_home(root));
    }
    if let Ok(root) = env::var("POLARIS_ROOT") {
        return Ok(expand_home(PathBuf::from(root)));
    }
    if let Ok(root) = env::var("POLARIS_DATASET_DOWNLOAD_DIR") {
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
        data_dir,
        daily_dir,
        tmp_dir,
        cache_dir,
        locks_dir,
    })
}

pub(crate) fn parse_snapshot_key(key: &str) -> Result<SnapshotKeyParts, PolarisError> {
    if key.contains('/') || key.contains('\\') || key.contains("..") {
        return Err(PolarisError::InvalidResponse(format!(
            "invalid snapshot key '{key}'"
        )));
    }
    let opaque = key.strip_suffix(SNAPSHOT_EXT).unwrap_or(key);
    let parts: Vec<&str> = opaque.split('-').collect();
    if parts.len() < 6 {
        return Err(PolarisError::InvalidResponse(format!(
            "invalid snapshot key '{key}'"
        )));
    }

    let is_timestamped = parts.len() >= 7
        && parts[parts.len() - 1].len() == 6
        && parts[parts.len() - 1].chars().all(|ch| ch.is_ascii_digit())
        && parts[parts.len() - 4].len() == 4
        && parts[parts.len() - 4].chars().all(|ch| ch.is_ascii_digit())
        && parts[parts.len() - 3].len() == 2
        && parts[parts.len() - 2].len() == 2;
    let is_hourly = !is_timestamped
        && parts.len() >= 7
        && parts[parts.len() - 1].len() == 2
        && parts[parts.len() - 1].chars().all(|ch| ch.is_ascii_digit())
        && parts[parts.len() - 4].len() == 4
        && parts[parts.len() - 4].chars().all(|ch| ch.is_ascii_digit())
        && parts[parts.len() - 3].len() == 2
        && parts[parts.len() - 2].len() == 2;

    let (date_index, timestamp, hour) = if is_timestamped {
        let timestamp = parts[parts.len() - 1].to_owned();
        let hour = timestamp[0..2].parse::<u8>().map_err(|err| {
            PolarisError::InvalidResponse(format!("invalid snapshot key '{key}': {err}"))
        })?;
        (parts.len() - 4, Some(timestamp), Some(hour))
    } else if is_hourly {
        let hour = parts[parts.len() - 1].parse::<u8>().map_err(|err| {
            PolarisError::InvalidResponse(format!("invalid snapshot key '{key}': {err}"))
        })?;
        (parts.len() - 4, None, Some(hour))
    } else {
        (parts.len() - 3, None, None)
    };

    let tier = parts[0].to_owned();
    let source = parts[1].to_owned();
    let market = parts[2..date_index].join("-");
    if market.is_empty() {
        return Err(PolarisError::InvalidResponse(format!(
            "invalid snapshot key '{key}'"
        )));
    }
    let date = format!(
        "{}-{}-{}",
        parts[date_index],
        parts[date_index + 1],
        parts[date_index + 2]
    );

    Ok(SnapshotKeyParts {
        tier,
        source,
        market,
        date,
        timestamp,
        hour,
        filename: format!("{opaque}{SNAPSHOT_EXT}"),
    })
}

pub(crate) fn data_file_path(data_dir: &Path, key: &str) -> Result<PathBuf, PolarisError> {
    let parsed = parse_snapshot_key(key)?;
    Ok(data_dir
        .join(parsed.tier)
        .join(parsed.source)
        .join(parsed.market)
        .join(parsed.date)
        .join(parsed.filename))
}

pub(crate) fn temp_file_path(tmp_dir: &Path, key: &str) -> Result<PathBuf, PolarisError> {
    parse_snapshot_key(key)?;
    let digest = Sha256::digest(key.as_bytes());
    Ok(tmp_dir.join(format!("{digest:x}.part")))
}

pub(crate) fn acquire_sync_lock(locks_dir: &Path) -> Result<SyncLockGuard, PolarisError> {
    fs::create_dir_all(locks_dir)?;
    let path = locks_dir.join("sync.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(SyncLockGuard { file })
}

pub(crate) fn list_local_snapshot_entries(
    layout: &StorageLayout,
    source: &str,
    market: &str,
    dates: &BTreeSet<NaiveDate>,
) -> Result<BTreeMap<NaiveDate, Vec<LocalSnapshotFile>>, PolarisError> {
    let mut out = BTreeMap::new();
    if !layout.data_dir.exists() {
        return Ok(out);
    }

    for date in dates {
        let mut matches = Vec::new();
        for tier_entry in fs::read_dir(&layout.data_dir)? {
            let tier_entry = tier_entry?;
            if !tier_entry.file_type()?.is_dir() {
                continue;
            }
            let day_dir = tier_entry
                .path()
                .join(source)
                .join(market)
                .join(date.format("%Y-%m-%d").to_string());
            if !day_dir.exists() {
                continue;
            }

            for file_entry in fs::read_dir(day_dir)? {
                let file_entry = file_entry?;
                if !file_entry.file_type()?.is_file() {
                    continue;
                }
                let path = file_entry.path();
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        PolarisError::InvalidResponse("invalid snapshot filename".to_owned())
                    })?;
                if !name.ends_with(SNAPSHOT_EXT) {
                    continue;
                }
                let key = name.trim_end_matches(SNAPSHOT_EXT).to_owned();
                let parsed = parse_snapshot_key(&key)?;
                matches.push(LocalSnapshotFile {
                    entry: SnapshotEntry {
                        key,
                        source: Some(parsed.source),
                        market: Some(parsed.market),
                        date: Some(parsed.date),
                        start: None,
                        end: None,
                        timestamp: parsed.timestamp,
                        hour: parsed.hour,
                        filename: Some(parsed.filename),
                    },
                    path,
                    download_url: None,
                });
            }
        }
        matches.sort_by(|left, right| left.entry.key.cmp(&right.entry.key));
        out.insert(*date, matches);
    }

    Ok(out)
}
