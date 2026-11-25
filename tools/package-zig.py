#!/usr/bin/env python3
"""Stage Zig native archives and verify the standalone Zig SDK archive."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "crates/polaris-data"
SOURCE = ("build.zig", "build.zig.zon", "zig", "include", "README.md", ".zigversion")
TARGETS = {
    "x86_64-unknown-linux-gnu": "x86_64-linux-gnu.2.17",
    "aarch64-unknown-linux-gnu": "aarch64-linux-gnu.2.17",
    "x86_64-unknown-linux-musl": "x86_64-linux-musl",
    "aarch64-unknown-linux-musl": "aarch64-linux-musl",
    "x86_64-apple-darwin": "x86_64-macos.10.12",
    "aarch64-apple-darwin": "aarch64-macos.11.0",
    "x86_64-pc-windows-msvc": "x86_64-windows-msvc",
    "aarch64-pc-windows-msvc": "aarch64-windows-msvc",
}
ZIG = os.environ.get("ZIG", "zig")


def build_target(target, destination):
    name = "polaris_zig_core.lib" if target.endswith("-msvc") else "libpolaris_zig_core.a"
    with tempfile.TemporaryDirectory() as work:
        subprocess.run(
            [ZIG, "build", "-Doptimize=ReleaseSafe", f"-Dtarget={TARGETS[target]}", "--prefix", work, "-j2"],
            cwd=ROOT, check=True,
        )
        folder = destination / target
        folder.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(work) / "lib" / name, folder / name)


def stage_source():
    destination = CRATE / "zig-source"
    destination.mkdir(exist_ok=True)
    for name in SOURCE:
        source = ROOT / name
        if source.is_dir():
            shutil.copytree(source, destination / name, dirs_exist_ok=True)
        else:
            shutil.copyfile(source, destination / name)


def reproducible(member):
    member.uid = member.gid = member.mtime = 0
    member.uname = member.gname = ""
    member.mode = 0o755 if member.isdir() else 0o644
    return member


def source_archive(destination):
    version = re.search(r'\.version = "([^"]+)"', (ROOT / "build.zig.zon").read_text())[1]
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / f"polaris-data-zig-{version}.tar.gz"
    with path.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for name in sorted(SOURCE):
                archive.add(ROOT / name, arcname=name, filter=reproducible)
    return path


def check_archive(archive):
    with tempfile.TemporaryDirectory() as work:
        consumer = Path(work) / "consumer"
        shutil.copytree(ROOT / "tests/zig-consumer", consumer)
        subprocess.run([ZIG, "fetch", "--save=polaris_data", archive.resolve().as_uri()], cwd=consumer, check=True)
        subprocess.run([ZIG, "build", "--prefix", work], cwd=consumer, check=True)
        with subprocess.Popen(
            [sys.executable, str(ROOT / "tests/fixture_server.py")],
            stdout=subprocess.PIPE, text=True,
        ) as server:
            try:
                uri = server.stdout.readline().strip()
                if not uri.startswith("http://"):
                    raise SystemExit("Fixture server did not start")
                executable = "polaris-consumer.exe" if os.name == "nt" else "polaris-consumer"
                subprocess.run([Path(work) / "bin" / executable, uri], timeout=30, check=True)
            finally:
                server.terminate()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_subparsers(dest="action", required=True)
    targets = action.add_parser("archives")
    targets.add_argument("--target", action="append", choices=TARGETS)
    targets.add_argument("--out", type=Path, default=CRATE / "prebuilt")
    action.add_parser("stage-source")
    archive = action.add_parser("source-archive")
    archive.add_argument("--out", type=Path, default=ROOT / "dist")
    archive.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.action == "archives":
        with ThreadPoolExecutor(max_workers=2) as workers:
            list(workers.map(lambda target: build_target(target, args.out), args.target or TARGETS))
    elif args.action == "stage-source":
        stage_source()
    else:
        path = source_archive(args.out)
        if args.check:
            check_archive(path)
        print(path)


if __name__ == "__main__":
    main()
