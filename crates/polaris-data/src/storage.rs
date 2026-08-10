use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use directories::BaseDirs;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{errors::PolarisError, models::SnapshotEntry};

const SNAPSHOT_EXT: &str = ".jsonl.zst";
const COVERAGE_SIDECAR_SUFFIX: &str = ".coverage.json";
const COVERAGE_SIDECAR_VERSION: u8 = 1;

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
    pub coverage: SnapshotCoverage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotCoverage {
    Exact { start_us: i64, end_us: i64 },
    Estimated,
}

impl SnapshotCoverage {
    pub(crate) fn is_estimated(self) -> bool {
        matches!(self, Self::Estimated)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct CoverageSidecar {
    version: u8,
    key: String,
    start_us: i64,
    end_us: i64,
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

pub(crate) fn coverage_sidecar_path(data_path: &Path) -> Result<PathBuf, PolarisError> {
    let filename = data_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| PolarisError::InvalidResponse("invalid snapshot filename".to_owned()))?;
    Ok(data_path.with_file_name(format!("{filename}{COVERAGE_SIDECAR_SUFFIX}")))
}

pub(crate) fn write_coverage_sidecar(
    data_path: &Path,
    key: &str,
    start_us: i64,
    end_us: i64,
) -> Result<(), PolarisError> {
    if start_us >= end_us {
        return Err(PolarisError::InvalidResponse(format!(
            "invalid exact coverage for snapshot '{key}'"
        )));
    }
    let final_path = coverage_sidecar_path(data_path)?;
    if read_coverage_sidecar(data_path, key).is_some() {
        return Ok(());
    }
    let parent = final_path.parent().ok_or_else(|| {
        PolarisError::InvalidResponse("coverage sidecar has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)?;
    let filename = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| PolarisError::InvalidResponse("invalid sidecar filename".to_owned()))?;
    let temp_path = parent.join(format!(".{filename}.{}.part", std::process::id()));
    let payload = CoverageSidecar {
        version: COVERAGE_SIDECAR_VERSION,
        key: key.to_owned(),
        start_us,
        end_us,
    };
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    serde_json::to_writer(&mut file, &payload).map_err(|error| {
        PolarisError::InvalidResponse(format!("failed to encode coverage sidecar: {error}"))
    })?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    if final_path.exists() {
        fs::remove_file(&final_path)?;
    }
    if let Err(error) = fs::rename(&temp_path, &final_path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error.into());
    }
    Ok(())
}

fn read_coverage_sidecar(data_path: &Path, key: &str) -> Option<SnapshotCoverage> {
    let path = coverage_sidecar_path(data_path).ok()?;
    let payload = fs::read(&path).ok()?;
    let sidecar: CoverageSidecar = match serde_json::from_slice(&payload) {
        Ok(value) => value,
        Err(error) => {
            log::warn!(
                "ignoring invalid coverage sidecar {}: {error}",
                path.display()
            );
            return None;
        }
    };
    if sidecar.version != COVERAGE_SIDECAR_VERSION
        || sidecar.key != key
        || sidecar.start_us >= sidecar.end_us
    {
        log::warn!("ignoring invalid coverage sidecar {}", path.display());
        return None;
    }
    Some(SnapshotCoverage::Exact {
        start_us: sidecar.start_us,
        end_us: sidecar.end_us,
    })
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
                let coverage =
                    read_coverage_sidecar(&path, &key).unwrap_or(SnapshotCoverage::Estimated);
                matches.push(LocalSnapshotFile {
                    entry: SnapshotEntry {
                        key: key.clone(),
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
                    coverage,
                });
            }
        }
        matches.sort_by(|left, right| left.entry.key.cmp(&right.entry.key));
        out.insert(*date, matches);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn exact_coverage_sidecar_round_trips_and_replaces_invalid_metadata() {
        let root = TempDir::new().expect("tempdir");
        let data = root.path().join("snapshot.jsonl.zst");
        fs::write(&data, b"snapshot").expect("snapshot");
        let key = "standard-source-market-2024-01-01-000000";
        let sidecar = coverage_sidecar_path(&data).expect("sidecar path");
        fs::write(&sidecar, b"not json").expect("corrupt sidecar");

        write_coverage_sidecar(&data, key, 100, 200).expect("write sidecar");

        assert_eq!(
            read_coverage_sidecar(&data, key),
            Some(SnapshotCoverage::Exact {
                start_us: 100,
                end_us: 200,
            })
        );
        assert!(root.path().read_dir().expect("directory").all(|entry| {
            !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".part")
        }));
    }
}
