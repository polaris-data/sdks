"""Local filesystem layout helpers for snapshot-backed dataset storage."""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .models import LocalSnapshotEntry

APP_NAME = "polaris"
ROOT_ENV_VAR = "POLARIS_ROOT"
LEGACY_ROOT_ENV_VAR = "POLARIS_DATASET_DOWNLOAD_DIR"


@dataclass(frozen=True)
class LocalDailyArtifactEntry:
    path: str
    source: str
    market: str
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
        normalized = key.strip()
        if not normalized:
            raise ValueError("snapshot key must not be empty")
        if "/" in normalized or "\\" in normalized or normalized in {".", ".."}:
            raise ValueError(f"invalid snapshot key: {normalized}")
        return self.data_root / normalized

    def temp_path_for_key(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.tmp_root / f"{digest}.part"

    def daily_path_for_dataset_day(
        self,
        source: str,
        market: str,
        day: date,
    ) -> Path:
        return self.daily_root / source / market / f"{day.isoformat()}.jsonl.zst"

    def daily_temp_path_for_dataset_day(
        self,
        source: str,
        market: str,
        day: date,
    ) -> Path:
        digest = hashlib.sha256(
            f"{source}:{market}:{day.isoformat()}".encode("utf-8")
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
            day = infer_date_from_text(relative)
            files.append(
                LocalSnapshotEntry(
                    key=relative,
                    path=str(path),
                    source=None,
                    market=None,
                    date=day.isoformat() if day is not None else None,
                    start=None,
                    end=None,
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

            source, market, filename = relative
            if not filename.endswith(".jsonl.zst"):
                continue

            files.append(
                LocalDailyArtifactEntry(
                    path=str(path),
                    source=source,
                    market=market,
                    date=filename.removesuffix(".jsonl.zst"),
                )
            )

        files.sort(key=lambda item: (item.source, item.market, item.date))
        return files

    def materialize_daily_artifact(
        self,
        snapshot: LocalSnapshotEntry,
        *,
        force: bool = False,
    ) -> Path | None:
        if snapshot.source is None or snapshot.market is None or snapshot.date is None:
            return None

        day = date.fromisoformat(snapshot.date)
        source = Path(snapshot.path)
        target = self.daily_path_for_dataset_day(snapshot.source, snapshot.market, day)
        temp_target = self.daily_temp_path_for_dataset_day(
            snapshot.source,
            snapshot.market,
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


def infer_date_from_text(text: str) -> date | None:
    tokens = "".join(ch if ch.isdigit() or ch == "-" else " " for ch in text).split()
    for token in tokens:
        stripped = token.strip("-")
        if len(stripped) != 10:
            continue
        try:
            return date.fromisoformat(stripped)
        except ValueError:
            continue
    return None
