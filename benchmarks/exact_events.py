"""Benchmark exact dictionary and Arrow-batch replay on representative events.

The source file is read once before timing so every reported sample explicitly
measures an OS-page-cache-warm replay. Run after building the native extension:

    uv run python benchmarks/exact_events.py --events 788383 --runs 3
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import orjson
import zstandard

from polaris_data import PolarisClient

SOURCE = "benchmark"
MARKET = "BTC-USD"
DAY = "2024-01-01"
START_US = 1_704_067_200_000_000
FIXTURES = ("trade", "point", "shallow-book", "deep-book")
MODES = ("iterator", "batches")


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def event_for(index: int, fixture: str, depth: int) -> dict:
    envelope = {
        "timestamp": START_US + index,
        "source": SOURCE,
        "market": MARKET,
        "sequence": index,
        "sequence_scope": "benchmark-channel",
    }
    if fixture == "trade":
        return envelope | {
            "type": "trade",
            "data": {
                "price": 40_000.0 + index / 100_000,
                "quantity": 0.01,
                "side": "buy" if index % 2 == 0 else "sell",
            },
        }
    if fixture == "point":
        return envelope | {
            "type": "point",
            "data": {"series": "mark_price", "value": 40_000.0 + index / 100_000},
        }
    if index == 0:
        return envelope | {
            "type": "orderbook",
            "data": {
                "bids": [[40_000.0 - level * 0.01, 1.0] for level in range(depth)],
                "asks": [[40_000.01 + level * 0.01, 1.0] for level in range(depth)],
            },
        }
    return envelope | {
        "type": "orderbook_delta",
        "data": {"bids": [[40_000.0, 1.0 + index % 10]]},
    }


def write_fixture(root: Path, fixture: str, events: int) -> Path:
    depth = 20 if fixture == "shallow-book" else 2_000
    key = f"standard-{SOURCE}-{MARKET}-{DAY}-000000"
    path = root / "data" / "standard" / SOURCE / MARKET / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, zstandard.ZstdCompressor(level=1).stream_writer(
        raw, closefd=False
    ) as output:
        for index in range(events):
            output.write(orjson.dumps(event_for(index, fixture, depth)))
            output.write(b"\n")
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": START_US,
                "end_us": START_US + events,
            },
            separators=(",", ":"),
        )
    )
    return path


def run_worker(root: Path, path: Path, mode: str, events: int, batch_size: int) -> None:
    if mode == "batches":
        import pyarrow  # noqa: F401 - exclude optional runtime startup from replay timing

    with path.open("rb") as source:
        while source.read(1024 * 1024):
            pass

    rss_before = peak_rss_bytes()
    started = time.perf_counter()
    with PolarisClient(dataset_root=root, base_url="http://127.0.0.1:1") as client:
        rows = client.events(
            source=SOURCE,
            market=MARKET,
            from_=START_US,
            to=START_US + events,
            materialize_orderbooks=False,
            output="batches" if mode == "batches" else "iterator",
            batch_size=batch_size,
        )
        count = 0
        checksum = 0
        if mode == "iterator":
            for row in rows:
                count += 1
                checksum ^= int(row["timestamp"])
        else:
            for batch in rows:
                count += batch.num_rows
                ordinal = batch.column("replay_ordinal")
                checksum ^= int(ordinal[0].as_py())
                checksum ^= int(ordinal[-1].as_py())
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "mode": mode,
                "cache_state": "os-page-cache-warm",
                "rows": count,
                "checksum": checksum,
                "seconds": elapsed,
                "rows_per_second": count / elapsed,
                "peak_rss_delta_bytes": max(peak_rss_bytes() - rss_before, 0),
            }
        )
    )


def sample(script: Path, root: Path, path: Path, mode: str, args: argparse.Namespace) -> dict:
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--worker",
            "--root",
            str(root),
            "--path",
            str(path),
            "--mode",
            mode,
            "--events",
            str(args.events),
            "--batch-size",
            str(args.batch_size),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", choices=(*FIXTURES, "all"), default="all")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=32_768)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=MODES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.events <= 0 or args.batch_size <= 0 or args.runs <= 0:
        parser.error("events, batch size, and runs must be positive")
    if args.worker:
        assert args.root is not None and args.path is not None and args.mode is not None
        run_worker(args.root, args.path, args.mode, args.events, args.batch_size)
        return 0

    fixtures = FIXTURES if args.fixture == "all" else (args.fixture,)
    script = Path(__file__).resolve()
    output: dict[str, object] = {
        "hardware": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
        },
        "events": args.events,
        "batch_size": args.batch_size,
        "runs": args.runs,
        "fixtures": {},
    }
    with tempfile.TemporaryDirectory(prefix="polaris-exact-events-") as directory:
        for fixture in fixtures:
            root = Path(directory) / fixture
            path = write_fixture(root, fixture, args.events)
            fixture_results = {}
            for mode in MODES:
                samples = [sample(script, root, path, mode, args) for _ in range(args.runs)]
                if {sample["rows"] for sample in samples} != {args.events}:
                    raise RuntimeError(f"row-count mismatch for {fixture}/{mode}")
                fixture_results[mode] = {
                    "median_rows_per_second": statistics.median(
                        float(sample["rows_per_second"]) for sample in samples
                    ),
                    "max_peak_rss_delta_bytes": max(
                        int(sample["peak_rss_delta_bytes"]) for sample in samples
                    ),
                    "samples": samples,
                }
            output["fixtures"][fixture] = fixture_results  # type: ignore[index]
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
