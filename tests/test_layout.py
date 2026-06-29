from __future__ import annotations

from pathlib import Path

import pytest

from polaris_data.layout import default_dataset_root, resolve_dataset_root, validated_key_segments


def test_resolve_dataset_root_prefers_explicit_value() -> None:
    root = resolve_dataset_root(
        dataset_root="~/polaris-explicit",
        dataset_download_dir="~/polaris-explicit",
    )
    assert root == Path("~/polaris-explicit").expanduser()


def test_resolve_dataset_root_uses_polaris_root_before_legacy_env(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_ROOT", "/tmp/polaris-root")
    monkeypatch.setenv("POLARIS_DATASET_DOWNLOAD_DIR", "/tmp/legacy-root")
    assert resolve_dataset_root() == Path("/tmp/polaris-root")


def test_default_dataset_root_matches_platform_contract() -> None:
    root = default_dataset_root()
    assert root.name == "polaris"


def test_validated_key_segments_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        validated_key_segments("../escape.jsonl.zst")


def test_validated_key_segments_accepts_snapshot_path_schema() -> None:
    assert validated_key_segments(
        "standard-binance-BTC-USDT-2024-01-01"
    ) == (
        "standard",
        "binance",
        "BTC-USDT",
        "2024-01-01",
        "standard-binance-BTC-USDT-2024-01-01.jsonl.zst",
    )
