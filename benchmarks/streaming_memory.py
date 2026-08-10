"""Opt-in end-to-end throughput and peak-RSS benchmark for historical streams."""

from __future__ import annotations

import argparse
import json
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import zstandard
from polaris_data import OrderbookBuilder, PolarisClient

SOURCE = "benchmark"
MARKET = "BTC-USD"
START_MS = 1_704_067_200_000


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def write_fixture(root: Path, depth: int, deltas: int) -> Path:
    path = root / "daily" / SOURCE / MARKET / "2024-01-01.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    bids = [[100_000.0 - index * 0.5, 1.0] for index in range(depth)]
    asks = [[100_000.5 + index * 0.5, 1.0] for index in range(depth)]
    with zstandard.open(path, "wt", encoding="utf-8") as output:
        output.write(
            json.dumps(
                {
                    "timestamp": START_MS,
                    "source": SOURCE,
                    "market": MARKET,
                    "type": "orderbook",
                    "data": {"bids": bids, "asks": asks},
                },
                separators=(",", ":"),
            )
            + "\n"
        )
        for index in range(1, deltas + 1):
            output.write(
                json.dumps(
                    {
                        "timestamp": START_MS + index,
                        "source": SOURCE,
                        "market": MARKET,
                        "type": "orderbook_delta",
                        "data": {"bids": [[100_000.0, 1.0 + index % 10]]},
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )
    return path


def run_worker(root: Path, mode: str, deltas: int) -> None:
    from_us = START_MS * 1_000
    to_us = (START_MS + deltas + 1) * 1_000
    started = time.perf_counter()
    with PolarisClient(dataset_root=root) as client:
        if mode == "l2_builder":
            builder = OrderbookBuilder()
            updates = client.l2_updates(
                source=SOURCE,
                market=MARKET,
                from_=from_us,
                to=to_us,
            )
            count = sum(1 for update in updates if builder.update(update))
            if builder.snapshot(SOURCE, MARKET) is None:
                raise RuntimeError("lazy orderbook builder did not initialize")
        else:
            if mode == "events":
                rows = client.events(
                    source=SOURCE,
                    market=MARKET,
                    from_=from_us,
                    to=to_us,
                    materialize_orderbooks=False,
                )
            elif mode == "bbo":
                rows = client.bbo(
                    source=SOURCE,
                    market=MARKET,
                    from_=from_us,
                    to=to_us,
                )
            elif mode == "l2_updates":
                rows = client.l2_updates(
                    source=SOURCE,
                    market=MARKET,
                    from_=from_us,
                    to=to_us,
                )
            else:
                rows = client.l2_snapshots(
                    source=SOURCE,
                    market=MARKET,
                    from_=from_us,
                    to=to_us,
                    materialize_orderbooks=True,
                )
            count = sum(1 for _ in rows)
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "mode": mode,
                "deltas": deltas,
                "rows": count,
                "seconds": elapsed,
                "rows_per_second": count / elapsed,
                "peak_rss_bytes": peak_rss_bytes(),
            }
        )
    )


def run_child(script: Path, root: Path, mode: str, deltas: int) -> dict[str, object]:
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--worker",
            "--root",
            str(root),
            "--mode",
            mode,
            "--deltas",
            str(deltas),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def run_samples(
    script: Path, root: Path, mode: str, deltas: int, runs: int
) -> dict[str, object]:
    samples = [run_child(script, root, mode, deltas) for _ in range(runs)]
    row_counts = {int(sample["rows"]) for sample in samples}
    if len(row_counts) != 1:
        raise RuntimeError(f"inconsistent row counts for {mode}: {sorted(row_counts)}")
    seconds = [float(sample["seconds"]) for sample in samples]
    throughput = [float(sample["rows_per_second"]) for sample in samples]
    peak_rss = [int(sample["peak_rss_bytes"]) for sample in samples]
    return {
        "runs": runs,
        "rows": row_counts.pop(),
        "median_seconds": statistics.median(seconds),
        "median_rows_per_second": statistics.median(throughput),
        "min_rows_per_second": min(throughput),
        "max_rows_per_second": max(throughput),
        "peak_rss_bytes": max(peak_rss),
        "samples": samples,
    }


def parse_minimum_throughput(values: list[str]) -> dict[str, float]:
    minimums: dict[str, float] = {}
    for value in values:
        try:
            mode, raw_rate = value.split("=", 1)
            rate = float(raw_rate)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"expected MODE=ROWS_PER_SECOND, got {value!r}"
            ) from error
        if mode not in {"events", "bbo", "l2_updates", "l2_builder", "l2"}:
            raise argparse.ArgumentTypeError(f"unknown benchmark mode {mode!r}")
        if rate <= 0:
            raise argparse.ArgumentTypeError("minimum throughput must be positive")
        minimums[mode] = rate
    return minimums


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--depth", type=int, default=3_000)
    parser.add_argument("--short-deltas", type=int, default=100_000)
    parser.add_argument("--long-deltas", type=int, default=1_000_000)
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="isolated process runs per mode and input size; use 3+ for stable medians",
    )
    parser.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=0.75,
        help="minimum long/short throughput ratio (default: 0.75)",
    )
    parser.add_argument(
        "--min-rps",
        action="append",
        default=[],
        metavar="MODE=ROWS_PER_SECOND",
        help="optional absolute throughput floor; may be repeated",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["events", "bbo", "l2_updates", "l2_builder", "l2"],
        default=["events", "bbo", "l2_updates", "l2_builder"],
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--mode",
        choices=["events", "bbo", "l2_updates", "l2_builder", "l2"],
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--deltas", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        assert (
            args.root is not None and args.mode is not None and args.deltas is not None
        )
        run_worker(args.root, args.mode, args.deltas)
        return 0

    if args.depth <= 0 or args.short_deltas <= 0 or args.long_deltas <= 0:
        parser.error("depth and delta counts must be positive")
    if args.short_deltas >= args.long_deltas:
        parser.error("--short-deltas must be less than --long-deltas")
    if args.runs <= 0:
        parser.error("--runs must be positive")
    if not 0 < args.min_throughput_ratio <= 1:
        parser.error("--min-throughput-ratio must be in the range (0, 1]")
    try:
        minimum_throughput = parse_minimum_throughput(args.min_rps)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))

    script = Path(__file__).resolve()
    with tempfile.TemporaryDirectory(prefix="polaris-stream-benchmark-") as temporary:
        root = Path(temporary)
        print(
            f"Generating {args.depth:,}-level book with {args.long_deltas:,} deltas..."
        )
        write_fixture(root, args.depth, args.long_deltas)
        failed = False
        for mode in args.modes:
            short = run_samples(script, root, mode, args.short_deltas, args.runs)
            long = run_samples(script, root, mode, args.long_deltas, args.runs)
            short_rss = int(short["peak_rss_bytes"])
            long_rss = int(long["peak_rss_bytes"])
            allowance = max(int(short_rss * 0.20), 64 * 1024 * 1024)
            bounded = long_rss - short_rss <= allowance
            short_rps = float(short["median_rows_per_second"])
            long_rps = float(long["median_rows_per_second"])
            throughput_ratio = long_rps / short_rps
            speed_stable = throughput_ratio >= args.min_throughput_ratio
            minimum_rps = minimum_throughput.get(mode)
            speed_floor_met = minimum_rps is None or long_rps >= minimum_rps
            passed = bounded and speed_stable and speed_floor_met
            failed |= not passed
            print(
                json.dumps(
                    {
                        "mode": mode,
                        "short": short,
                        "long": long,
                        "memory_bounded": bounded,
                        "throughput_ratio": throughput_ratio,
                        "speed_stable": speed_stable,
                        "minimum_rows_per_second": minimum_rps,
                        "speed_floor_met": speed_floor_met,
                        "passed": passed,
                    }
                )
            )
        return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
