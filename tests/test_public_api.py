"""Contract tests for the documented Python SDK surface."""

from __future__ import annotations

import importlib.util
import inspect
from dataclasses import fields, is_dataclass
from typing import Any, get_type_hints

import polaris_data
from polaris_data import (
    AccessDeniedError,
    BulkDownloadManifest,
    BulkDownloadSnapshotEntry,
    CatalogAccess,
    CatalogInstrument,
    CatalogMarketEntry,
    CatalogResponse,
    DownloadNotAllowedError,
    LocalSnapshotEntry,
    NotFoundError,
    PolarisClient,
    PolarisError,
    RateLimitedError,
    SnapshotEntry,
    StreamDecodeError,
    UnauthorizedError,
)
from polaris_data.models import JSONDict


def _parameters(callable_: object) -> list[tuple[str, inspect._ParameterKind, object]]:
    return [
        (parameter.name, parameter.kind, parameter.default)
        for parameter in inspect.signature(callable_).parameters.values()
    ]


def test_top_level_exports_are_stable() -> None:
    assert polaris_data.__all__ == [
        "AccessDeniedError",
        "BulkDownloadManifest",
        "BulkDownloadSnapshotEntry",
        "CatalogAccess",
        "CatalogInstrument",
        "CatalogMarketEntry",
        "CatalogResponse",
        "NotFoundError",
        "DownloadNotAllowedError",
        "LocalSnapshotEntry",
        "PolarisClient",
        "PolarisError",
        "RateLimitedError",
        "SnapshotEntry",
        "StreamDecodeError",
        "UnauthorizedError",
    ]


def test_client_constructor_signature_and_defaults_are_stable() -> None:
    positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert _parameters(PolarisClient) == [
        ("api_key", positional, None),
        ("base_url", positional, "https://api.polaris.supply"),
        ("timeout", positional, 30.0),
        ("dataset_root", positional, None),
        ("dataset_download_dir", positional, None),
        ("replay_cache_enabled", positional, True),
        ("replay_cache_dir", positional, None),
    ]


def test_documented_client_method_signatures_and_defaults_are_stable() -> None:
    keyword_only = inspect.Parameter.KEYWORD_ONLY
    positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
    required = inspect.Parameter.empty

    assert _parameters(PolarisClient.health) == [("self", positional, required)]
    assert _parameters(PolarisClient.catalog) == [
        ("self", positional, required),
        ("source", positional, None),
        ("market", positional, None),
        ("q", positional, None),
    ]
    assert _parameters(PolarisClient.list_snapshots) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, required),
        ("to", keyword_only, required),
        ("limit", keyword_only, 1000),
    ]
    assert _parameters(PolarisClient.replay) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("standard", keyword_only, True),
        ("allow_gaps", keyword_only, False),
        ("chunk_size", keyword_only, None),
        ("timeout", keyword_only, None),
        ("parallel", keyword_only, False),
    ]

    historical_methods = [
        PolarisClient.events,
        PolarisClient.trades,
        PolarisClient.l2_snapshots,
        PolarisClient.funding_rates,
        PolarisClient.mark_prices,
        PolarisClient.bbo,
    ]
    for method in historical_methods:
        assert _parameters(method) == [
            ("self", positional, required),
            ("source", keyword_only, required),
            ("market", keyword_only, required),
            ("from_", keyword_only, None),
            ("to", keyword_only, None),
            ("allow_gaps", keyword_only, False),
        ]

    assert _parameters(PolarisClient.raw) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("limit", keyword_only, 1000),
    ]
    assert _parameters(PolarisClient.ohlcv) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("interval", keyword_only, required),
        ("format", keyword_only, None),
        ("allow_gaps", keyword_only, False),
    ]

    for method in [PolarisClient.volume, PolarisClient.vwap]:
        assert _parameters(method) == [
            ("self", positional, required),
            ("source", keyword_only, required),
            ("market", keyword_only, required),
            ("from_", keyword_only, None),
            ("to", keyword_only, None),
            ("interval", keyword_only, required),
            ("allow_gaps", keyword_only, False),
        ]

    assert _parameters(PolarisClient.volatility) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("interval", keyword_only, required),
        ("method", keyword_only, "log_returns"),
        ("allow_gaps", keyword_only, False),
    ]
    assert _parameters(PolarisClient.depth_metrics) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("depth_pct", keyword_only, 0.01),
        ("slippage_notional", keyword_only, 10_000.0),
        ("allow_gaps", keyword_only, False),
    ]


def test_documented_result_annotations_and_models_are_stable() -> None:
    assert inspect.signature(PolarisClient.health).return_annotation == "JSONDict"
    assert inspect.signature(PolarisClient.catalog).return_annotation == "CatalogResponse | JSONDict"
    assert inspect.signature(PolarisClient.list_snapshots).return_annotation == "list[SnapshotEntry]"
    assert inspect.signature(PolarisClient.replay).return_annotation == "Iterator[JSONDict]"
    assert inspect.signature(PolarisClient.ohlcv).return_annotation == "list[JSONDict] | JSONDict"

    assert get_type_hints(CatalogResponse) == {
        "markets": list[CatalogMarketEntry],
        "updatedAt": str,
    }
    assert get_type_hints(BulkDownloadManifest) == {
        "source": str,
        "market": str,
        "date": str,
        "total": int,
        "total_bytes": int,
        "snapshots": list[BulkDownloadSnapshotEntry],
    }
    assert JSONDict == dict[str, Any]
    assert is_dataclass(SnapshotEntry)
    assert is_dataclass(LocalSnapshotEntry)
    assert [field.name for field in fields(SnapshotEntry)] == [
        "key",
        "source",
        "market",
        "date",
        "start",
        "end",
        "hour",
    ]


def test_error_hierarchy_is_stable() -> None:
    for error in [
        UnauthorizedError,
        AccessDeniedError,
        NotFoundError,
        RateLimitedError,
        StreamDecodeError,
        DownloadNotAllowedError,
    ]:
        assert error.__bases__ == (PolarisError,)


def test_removed_legacy_layout_module_is_not_importable() -> None:
    assert importlib.util.find_spec("polaris_data.layout") is None
