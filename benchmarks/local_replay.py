"""Opt-in benchmark for the synchronous local replay hot path.

Run after building the native extension in release mode, for example:
    uv run maturin develop --release
    uv run python benchmarks/local_replay.py --enforce-targets
"""

from __future__ import annotations

import argparse
import io
import json
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterator

import orjson
import zstandard
from polaris_data import PolarisClient

SOURCE = "benchmark"
MARKET = "UNI-USD"
DAY = "2024-01-01"
START_MS = 1_704_067_200_000


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def event_for(index: int, fixture: str, depth: int) -> dict:
    timestamp = START_MS + index
    envelope = {
        "timestamp": timestamp,
        "source": SOURCE,
        "market": MARKET,
    }
    if fixture == "point":
        return envelope | {
            "type": "mark_price",
            "data": {"series": "mark_price", "value": 4.0 + index / 1_000_000},
        }
    if fixture == "trade":
        return envelope | {
            "type": "trade",
            "data": {"price": 4.0 + index / 1_000_000, "quantity": 1.0},
        }
    if index == 0:
        bids = [[4.0 - level / 10_000, 1.0] for level in range(depth)]
        asks = [[4.0001 + level / 10_000, 1.0] for level in range(depth)]
        return envelope | {"type": "orderbook", "data": {"bids": bids, "asks": asks}}
    return envelope | {
        "type": "orderbook_delta",
        "data": {"bids": [[4.0, 1.0 + index % 10]]},
    }


def write_fixture(root: Path, fixture: str, events: int, depth: int) -> Path:
    key = f"standard-{SOURCE}-{MARKET}-{DAY}-000000"
    path = root / "data" / "standard" / SOURCE / MARKET / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    compressor = zstandard.ZstdCompressor(level=1)
    with path.open("wb") as raw, compressor.stream_writer(raw, closefd=False) as output:
        for index in range(events):
            output.write(orjson.dumps(event_for(index, fixture, depth)))
            output.write(b"\n")
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": START_MS * 1_000,
                "end_us": (START_MS + events) * 1_000,
            },
            separators=(",", ":"),
        )
    )
    return path


def direct_rows(path: Path) -> Iterator[dict]:
    with path.open("rb") as raw, zstandard.ZstdDecompressor().stream_reader(raw) as decoded:
        for line in io.BufferedReader(decoded):
            if line.strip():
                yield orjson.loads(line)


def run_worker(
    root: Path,
    path: Path,
    backend: str,
    materialize: bool,
    events: int,
) -> None:
    # Warm the compressed file without including the warm-up in any timing.
    with path.open("rb") as source:
        while source.read(1024 * 1024):
            pass

    rss_before = peak_rss_bytes()
    construct_started = time.perf_counter()
    client: PolarisClient | None = None
    if backend == "direct-json":
        rows = direct_rows(path)
    else:
        client = PolarisClient(dataset_root=root, base_url="http://127.0.0.1:1")
        method = client.events if backend == "events" else client.replay
        rows = method(
            source=SOURCE,
            market=MARKET,
            from_=START_MS * 1_000,
            to=(START_MS + events) * 1_000,
            materialize_orderbooks=materialize,
        )
    construct_seconds = time.perf_counter() - construct_started

    first_started = time.perf_counter()
    first = next(rows, None)
    first_seconds = time.perf_counter() - first_started
    iteration_started = time.perf_counter()
    count = int(first is not None)
    timestamp_checksum = 0 if first is None else int(first["timestamp"])
    for row in rows:
        count += 1
        timestamp_checksum ^= int(row["timestamp"])
    iteration_seconds = time.perf_counter() - iteration_started
    close = getattr(rows, "close", None)
    if close is not None:
        close()
    if client is not None:
        client.close()

    print(
        json.dumps(
            {
                "backend": backend,
                "materialize_orderbooks": materialize,
                "rows": count,
                "timestamp_checksum": timestamp_checksum,
                "construct_seconds": construct_seconds,
                "first_event_seconds": first_seconds,
                "iteration_seconds": iteration_seconds,
                "steady_events_per_second": max(count - 1, 0) / iteration_seconds,
                "peak_rss_bytes": peak_rss_bytes(),
                "peak_rss_delta_bytes": max(peak_rss_bytes() - rss_before, 0),
            }
        )
    )


def child_sample(script: Path, args: argparse.Namespace, root: Path, path: Path, backend: str, materialize: bool) -> dict:
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--root",
        str(root),
        "--path",
        str(path),
        "--backend",
        backend,
        "--fixture",
        args.fixture,
        "--events",
        str(args.events),
        "--depth",
        str(args.depth),
    ]
    if materialize:
        command.append("--materialize")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(
            f"benchmark worker failed ({result.returncode}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def summarize(samples: list[dict]) -> dict:
    keys = [
        "construct_seconds",
        "first_event_seconds",
        "iteration_seconds",
        "steady_events_per_second",
        "peak_rss_bytes",
        "peak_rss_delta_bytes",
    ]
    summary = {f"median_{key}": statistics.median(float(sample[key]) for sample in samples) for key in keys}
    summary["rows"] = samples[0]["rows"]
    summary["samples"] = samples
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", choices=["point", "trade", "shallow-book", "deep-book"], default="trade")
    parser.add_argument("--events", type=int, default=788_383)
    parser.add_argument("--depth", type=int, default=2_000)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--enforce-targets", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--backend", choices=["direct-json", "events", "replay"], help=argparse.SUPPRESS)
    parser.add_argument("--materialize", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.events <= 0 or args.depth <= 0 or args.runs <= 0:
        parser.error("events, depth, and runs must be positive")
    if args.worker:
        assert args.root is not None and args.path is not None and args.backend is not None
        run_worker(args.root, args.path, args.backend, args.materialize, args.events)
        return 0

    fixture_kind = "shallow-book" if args.fixture == "shallow-book" else args.fixture
    depth = 20 if fixture_kind == "shallow-book" else args.depth
    script = Path(__file__).resolve()
    failed = False
    with tempfile.TemporaryDirectory(prefix="polaris-local-replay-") as temporary:
        root = Path(temporary)
        path = write_fixture(root, "shallow-book" if "book" in fixture_kind else fixture_kind, args.events, depth)
        results: dict[str, dict] = {}
        for backend in ["direct-json", "events", "replay"]:
            for materialize in ([False] if backend == "direct-json" else [False, True]):
                label = f"{backend}:materialize={materialize}"
                samples = [child_sample(script, args, root, path, backend, materialize) for _ in range(args.runs)]
                results[label] = summarize(samples)
        sdk = results["events:materialize=False"]
        baseline = results["direct-json:materialize=False"]
        sdk_rate = float(sdk["median_steady_events_per_second"])
        baseline_rate = float(baseline["median_steady_events_per_second"])
        first_ms = float(sdk["median_first_event_seconds"]) * 1_000
        comparable = [
            results["direct-json:materialize=False"]["samples"][0],
            results["events:materialize=False"]["samples"][0],
            results["replay:materialize=False"]["samples"][0],
        ]
        output_matches = len(
            {(sample["rows"], sample["timestamp_checksum"]) for sample in comparable}
        ) == 1
        results["targets"] = {
            "first_event_ms": first_ms,
            "events_per_second": sdk_rate,
            "direct_speed_ratio": baseline_rate / sdk_rate,
            "first_under_10ms": first_ms < 10,
            "at_least_500k_events_per_second": sdk_rate >= 500_000,
            "within_2x_direct": baseline_rate / sdk_rate <= 2,
            "unmaterialized_output_matches": output_matches,
        }
        if args.enforce_targets:
            failed = not all(
                results["targets"][key]
                for key in [
                    "first_under_10ms",
                    "at_least_500k_events_per_second",
                    "within_2x_direct",
                    "unmaterialized_output_matches",
                ]
            )
        print(json.dumps(results, indent=2, sort_keys=True))
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
