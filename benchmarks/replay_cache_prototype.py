"""Prototype an Arrow IPC replay cache without changing SDK cache behavior.

This benchmark measures first-use cache construction and repeated memory-mapped
reads. It intentionally remains a benchmark until real datasets establish cache
keying, invalidation, and disk-cost requirements.
"""

from __future__ import annotations

import argparse
import json
import platform
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from exact_events import FIXTURES, MARKET, SOURCE, START_US, write_fixture
from polaris_data import PolarisClient


def build_cache(
    root: Path,
    cache_path: Path,
    events: int,
    batch_size: int,
    compression: str | None,
) -> dict:
    started = time.perf_counter()
    rows = 0
    with PolarisClient(dataset_root=root, base_url="http://127.0.0.1:1") as client:
        batches = client.events(
            source=SOURCE,
            market=MARKET,
            from_=START_US,
            to=START_US + events,
            materialize_orderbooks=False,
            output="batches",
            batch_size=batch_size,
        )
        first = next(batches, None)
        if first is None:
            raise RuntimeError("empty exact replay")
        options = ipc.IpcWriteOptions(compression=compression)
        with cache_path.open("wb") as output, ipc.new_file(
            output, first.schema, options=options
        ) as writer:
            writer.write_batch(first)
            rows += first.num_rows
            for batch in batches:
                writer.write_batch(batch)
                rows += batch.num_rows
    elapsed = time.perf_counter() - started
    return {
        "rows": rows,
        "seconds": elapsed,
        "rows_per_second": rows / elapsed,
        "bytes": cache_path.stat().st_size,
        "compression": compression or "none",
    }


def read_cache(cache_path: Path) -> dict:
    started = time.perf_counter()
    rows = 0
    checksum = 0
    with pa.memory_map(str(cache_path), "r") as source:
        reader = ipc.open_file(source)
        for index in range(reader.num_record_batches):
            batch = reader.get_batch(index)
            rows += batch.num_rows
            checksum += int(pc.sum(batch.column("replay_ordinal")).as_py())
    elapsed = time.perf_counter() - started
    return {
        "rows": rows,
        "checksum": checksum,
        "seconds": elapsed,
        "rows_per_second": rows / elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", choices=FIXTURES, default="trade")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=32_768)
    parser.add_argument("--warm-runs", type=int, default=3)
    args = parser.parse_args()
    if args.events <= 0 or args.batch_size <= 0 or args.warm_runs <= 0:
        parser.error("events, batch size, and warm runs must be positive")

    with tempfile.TemporaryDirectory(prefix="polaris-replay-cache-") as directory:
        root = Path(directory) / "dataset"
        source_path = write_fixture(root, args.fixture, args.events)
        caches = {}
        for compression in (None, "zstd"):
            label = compression or "none"
            cache_path = Path(directory) / f"exact-{label}.arrow"
            caches[label] = {
                "build": build_cache(
                    root,
                    cache_path,
                    args.events,
                    args.batch_size,
                    compression,
                ),
                "warm_reads": [
                    read_cache(cache_path) for _ in range(args.warm_runs)
                ],
            }
        output = {
            "hardware": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
            },
            "fixture": args.fixture,
            "events": args.events,
            "batch_size": args.batch_size,
            "compressed_source_bytes": source_path.stat().st_size,
            "caches": caches,
        }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
