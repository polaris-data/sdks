"""Opt-in DataFrame and RecordBatch benchmark over a local trade fixture.

The ``list-json-normalize`` mode intentionally measures the worst-case
convenience pattern of materializing every row dictionary before asking Pandas
to normalize it. It is not representative of the final DataFrame's steady-state
memory usage.

Run after building the native extension in release mode:
    uv run maturin develop --release
    uv run python benchmarks/columnar.py
"""

from __future__ import annotations

import argparse
import json
import resource
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
START_MS = 1_704_067_200_000
DEFAULT_ROWS = 788_383
MODES = ("iterator", "list-json-normalize", "batches", "dataframe")


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def write_fixture(root: Path, rows: int) -> Path:
    key = f"standard-{SOURCE}-{MARKET}-{DAY}-000000"
    path = root / "data" / "standard" / SOURCE / MARKET / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    compressor = zstandard.ZstdCompressor(level=1)
    with path.open("wb") as raw, compressor.stream_writer(raw, closefd=False) as output:
        for index in range(rows):
            output.write(
                orjson.dumps(
                    {
                        "timestamp": START_MS + index,
                        "source": SOURCE,
                        "market": MARKET,
                        "type": "trade",
                        "data": {
                            "price": 40_000.0 + (index % 10_000) / 100,
                            "quantity": 0.001 + (index % 100) / 10_000,
                            "side": "buy" if index % 2 == 0 else "sell",
                        },
                    }
                )
            )
            output.write(b"\n")
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": START_MS * 1_000,
                "end_us": (START_MS + rows) * 1_000,
            },
            separators=(",", ":"),
        )
    )
    return path


def trade_query(client: PolarisClient, rows: int, **kwargs):
    return client.trades(
        source=SOURCE,
        market=MARKET,
        from_=START_MS * 1_000,
        to=(START_MS + rows) * 1_000,
        **kwargs,
    )


def run_worker(root: Path, mode: str, rows: int, batch_size: int) -> None:
    pd = None
    pc = None
    if mode in {"list-json-normalize", "dataframe"}:
        import pandas as pd
    if mode in {"batches", "dataframe"}:
        import pyarrow  # noqa: F401 - warm optional runtime before RSS baseline
    if mode == "batches":
        import pyarrow.compute as pc

    rss_before = peak_rss_bytes()
    started = time.perf_counter()
    with PolarisClient(dataset_root=root, base_url="http://127.0.0.1:1") as client:
        if mode == "iterator":
            count = 0
            checksum = 0.0
            for row in trade_query(client, rows):
                count += 1
                checksum += float(row["data"]["price"])
        elif mode == "list-json-normalize":
            assert pd is not None
            frame = pd.json_normalize(list(trade_query(client, rows)))
            count = len(frame)
            checksum = float(frame["data.price"].sum())
        elif mode == "batches":
            assert pc is not None
            count = 0
            checksum = 0.0
            for batch in trade_query(
                client,
                rows,
                output="batches",
                batch_size=batch_size,
            ):
                count += batch.num_rows
                checksum += pc.sum(batch.column("price")).as_py()
        elif mode == "dataframe":
            frame = trade_query(
                client,
                rows,
                output="dataframe",
                batch_size=batch_size,
            )
            count = len(frame)
            checksum = float(frame["price"].sum())
        else:  # pragma: no cover - guarded by argparse
            raise ValueError(mode)
    elapsed = time.perf_counter() - started
    peak = peak_rss_bytes()
    print(
        json.dumps(
            {
                "mode": mode,
                "rows": count,
                "price_checksum": checksum,
                "seconds": elapsed,
                "rows_per_second": count / elapsed,
                "peak_rss_bytes": peak,
                "incremental_rss_bytes": max(peak - rss_before, 0),
            }
        )
    )


def run_child(script: Path, root: Path, mode: str, rows: int, batch_size: int) -> dict:
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--worker",
            "--root",
            str(root),
            "--mode",
            mode,
            "--rows",
            str(rows),
            "--batch-size",
            str(batch_size),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--batch-size", type=int, default=65_536)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--mode", choices=MODES)
    args = parser.parse_args()

    if args.worker:
        if args.root is None or args.mode is None:
            parser.error("--root and --mode are required with --worker")
        run_worker(args.root, args.mode, args.rows, args.batch_size)
        return

    if args.rows <= 0 or args.batch_size <= 0:
        parser.error("--rows and --batch-size must be positive")
    with tempfile.TemporaryDirectory(prefix="polaris-columnar-") as directory:
        root = Path(directory)
        write_fixture(root, args.rows)
        results = [
            run_child(Path(__file__), root, mode, args.rows, args.batch_size)
            for mode in MODES
        ]
    expected_counts = {result["rows"] for result in results}
    if expected_counts != {args.rows}:
        raise RuntimeError(f"row-count mismatch: {sorted(expected_counts)}")
    checksums = [float(result["price_checksum"]) for result in results]
    if max(checksums) - min(checksums) > max(abs(checksums[0]) * 1e-12, 1e-9):
        raise RuntimeError(f"checksum mismatch: {checksums}")
    print(json.dumps({"rows": args.rows, "batch_size": args.batch_size, "results": results}, indent=2))


if __name__ == "__main__":
    main()
