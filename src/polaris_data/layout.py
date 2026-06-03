"""Local filesystem layout helpers for snapshot-backed dataset storage."""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from .models import LocalSnapshotEntry

APP_NAME = "polaris"
ROOT_ENV_VAR = "POLARIS_ROOT"
LEGACY_ROOT_ENV_VAR = "POLARIS_DATASET_DOWNLOAD_DIR"


@dataclass(frozen=True)
class LocalDailyArtifactEntry:
    path: str
    exchange: str
    asset: str
    date: str


def resolve_dataset_root(
    *,
    dataset_root: str | os.PathLike[str] | None = None,
    dataset_download_dir: str | os.PathLike[str] | None = None,
) -> Path:
    if dataset_root is not None:
        root = Path(dataset_root).expanduser()
        if dataset_download_dir is not None:
            legacy_root = Path(dataset_download_dir).expanduser()
            if legacy_root != root:
                raise ValueError(
                    "dataset_root and dataset_download_dir must match when both are provided"
                )
        return root

    if dataset_download_dir is not None:
        return Path(dataset_download_dir).expanduser()

    override = os.getenv(ROOT_ENV_VAR)
    if override:
        return Path(override).expanduser()

    legacy_override = os.getenv(LEGACY_ROOT_ENV_VAR)
    if legacy_override:
        return Path(legacy_override).expanduser()

    return default_dataset_root()


def default_dataset_root() -> Path:
    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library" / "Application Support" / APP_NAME
    if os.name == "nt":
        appdata = os.getenv("APPDATA")
        if appdata:
            return Path(appdata) / APP_NAME
        return home / "AppData" / "Roaming" / APP_NAME

    xdg_data_home = os.getenv("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home) / APP_NAME
    return home / ".local" / "share" / APP_NAME


class FileLock(AbstractContextManager["FileLock"]):
    """Cross-process file lock compatible with the shared Polaris dataset root."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: object | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        except Exception:
            handle.close()
            raise

        self._handle = handle
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        handle = self._handle
        if handle is None:
            return

        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None


class LocalDatasetLayout:
    """Filesystem layout shared with the Polaris CLI."""

    def __init__(self, root: Path) -> None:
        self.root = root

    @property
    def data_root(self) -> Path:
        return self.root / "data"

    @property
    def daily_root(self) -> Path:
        return self.root / "daily"

    @property
    def tmp_root(self) -> Path:
        return self.root / "tmp"

    @property
    def cache_root(self) -> Path:
        return self.root / "cache"

    @property
    def lock_path(self) -> Path:
        return self.root / "locks" / "sync.lock"

    def sync_lock(self) -> FileLock:
        return FileLock(self.lock_path)

    def data_path_for_key(self, key: str) -> Path:
        segments = validated_key_segments(key)
        path = self.data_root
        for segment in segments:
            path /= segment
        return path

    def temp_path_for_key(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.tmp_root / f"{digest}.part"

    def daily_path_for_dataset_day(self, exchange: str, asset: str, day: date) -> Path:
        return self.daily_root / exchange / asset / f"{day.isoformat()}.jsonl.zst"

    def daily_temp_path_for_dataset_day(
        self, exchange: str, asset: str, day: date
    ) -> Path:
        digest = hashlib.sha256(
            f"{exchange}:{asset}:{day.isoformat()}".encode("utf-8")
        ).hexdigest()
        return self.tmp_root / f"daily-{digest}.part"

    def list_local_snapshots(self) -> list[LocalSnapshotEntry]:
        files: list[LocalSnapshotEntry] = []
        if not self.data_root.exists():
            return files

        for path in sorted(self.data_root.rglob("*")):
            if not path.is_file():
                continue

            relative = path.relative_to(self.data_root).as_posix()
            filename = path.name
            exchange, asset, day = infer_snapshot_identity(relative, filename)
            start, end = parse_snapshot_times(filename)
            files.append(
                LocalSnapshotEntry(
                    key=relative,
                    path=str(path),
                    filename=filename,
                    exchange=exchange,
                    asset=asset,
                    date=day.isoformat() if day is not None else None,
                    start=start,
                    end=end,
                )
            )

        files.sort(key=lambda item: item.key)
        return files

    def list_local_daily_artifacts(self) -> list[LocalDailyArtifactEntry]:
        files: list[LocalDailyArtifactEntry] = []
        if not self.daily_root.exists():
            return files

        for path in sorted(self.daily_root.rglob("*.jsonl.zst")):
            if not path.is_file():
                continue

            relative = path.relative_to(self.daily_root).parts
            if len(relative) != 3:
                continue

            exchange, asset, filename = relative
            if not filename.endswith(".jsonl.zst"):
                continue

            files.append(
                LocalDailyArtifactEntry(
                    path=str(path),
                    exchange=exchange,
                    asset=asset,
                    date=filename.removesuffix(".jsonl.zst"),
                )
            )

        files.sort(key=lambda item: (item.exchange, item.asset, item.date))
        return files

    def materialize_daily_artifact(
        self,
        snapshot: LocalSnapshotEntry,
        *,
        force: bool = False,
    ) -> Path | None:
        if snapshot.exchange is None or snapshot.asset is None or snapshot.date is None:
            return None

        day = date.fromisoformat(snapshot.date)
        source = Path(snapshot.path)
        target = self.daily_path_for_dataset_day(snapshot.exchange, snapshot.asset, day)
        temp_target = self.daily_temp_path_for_dataset_day(
            snapshot.exchange,
            snapshot.asset,
            day,
        )

        if target.exists() and not force:
            return target

        target.parent.mkdir(parents=True, exist_ok=True)
        temp_target.parent.mkdir(parents=True, exist_ok=True)

        if temp_target.exists():
            temp_target.unlink()

        try:
            try:
                os.link(source, temp_target)
            except OSError:
                shutil.copy2(source, temp_target)
            os.replace(temp_target, target)
        finally:
            if temp_target.exists():
                temp_target.unlink()

        return target


def validated_key_segments(key: str) -> list[str]:
    trimmed = key.strip()
    if not trimmed:
        raise ValueError("snapshot key must not be empty")

    segments: list[str] = []
    for segment in trimmed.split("/"):
        if not segment or segment in {".", ".."} or "\\" in segment:
            raise ValueError(f"invalid remote key segment in {trimmed}")
        segments.append(segment)
    return segments


def infer_snapshot_date_from_key(key: str) -> date | None:
    segments = key.split("/")
    return infer_date_from_segments(segments) or infer_date_from_text(segments[-1])


def parse_snapshot_times(filename: str) -> tuple[datetime | None, datetime | None]:
    start = None
    end = None

    if "_s" in filename and "_e" in filename:
        raw_start = filename.split("_s", 1)[1].split("_e", 1)[0]
        raw_end = filename.split("_e", 1)[1].split(".", 1)[0]
        start = parse_snapshot_timestamp(raw_start)
        end = parse_snapshot_timestamp(raw_end)

    return start, end


def parse_snapshot_timestamp(raw: str) -> datetime | None:
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def infer_snapshot_identity(
    key: str, filename: str
) -> tuple[str | None, str | None, date | None]:
    segments = key.split("/")
    if not segments:
        return None, None, None

    indexed = infer_date_segment_index(segments)
    if indexed is not None:
        index, day = indexed
        exchange = segments[index - 2] if index >= 2 else None
        asset = segments[index - 1] if index >= 1 else None
        return exchange, asset, day

    day = infer_date_from_text(filename)
    if day is not None:
        exchange = segments[-3] if len(segments) >= 3 else None
        asset = segments[-2] if len(segments) >= 2 else None
        return exchange, asset, day

    exchange = segments[-4] if len(segments) >= 4 else None
    asset = segments[-3] if len(segments) >= 3 else None
    return exchange, asset, None


def infer_date_from_segments(segments: Iterable[str]) -> date | None:
    indexed = infer_date_segment_index(list(segments))
    if indexed is None:
        return None
    return indexed[1]


def infer_date_segment_index(segments: list[str]) -> tuple[int, date] | None:
    for index, segment in enumerate(segments):
        day = infer_date_from_text(segment)
        if day is not None:
            return index, day
    return None


def infer_date_from_text(text: str) -> date | None:
    tokens = "".join(ch if ch.isdigit() or ch == "-" else " " for ch in text).split()
    for token in tokens:
        if len(token) != 10:
            continue
        try:
            return date.fromisoformat(token)
        except ValueError:
            continue
    return None
