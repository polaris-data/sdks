#!/usr/bin/env python3
"""Install the pinned Zig and Binaryen release tools for SDK builds."""
import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ZIG_VERSION = (ROOT / ".zigversion").read_text().strip()
BINARYEN_VERSION = 133


def download(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": "polaris-sdk-build"})
    with urllib.request.urlopen(req, timeout=120) as response, path.open("wb") as out:
        shutil.copyfileobj(response, out)


def install(destination, url, checksum, suffix):
    if destination.is_dir():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as work:
        work = Path(work)
        archive = work / f"release{suffix}"
        download(url, archive)
        with archive.open("rb") as body:
            digest = hashlib.sha256()
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                digest.update(chunk)
            actual = digest.hexdigest()
        if actual != checksum:
            raise SystemExit(f"Checksum mismatch for {url}")
        unpacked = work / "unpacked"
        unpacked.mkdir()
        subprocess.run(["tar", "-xf", archive, "-C", unpacked], check=True)
        folder, = unpacked.iterdir()
        folder.rename(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaryen", action="store_true")
    parser.add_argument("--destination", type=Path, default=ROOT / ".context" / "tools")
    args = parser.parse_args()
    machine = platform.machine().lower()
    arch = {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
    system = {"Darwin": "macos", "Linux": "linux", "Windows": "windows"}[platform.system()]
    zig = args.destination / f"zig-{ZIG_VERSION}"
    if not zig.is_dir():
        with urllib.request.urlopen("https://ziglang.org/download/index.json", timeout=30) as response:
            info = json.load(response)[ZIG_VERSION][f"{arch}-{system}"]
        install(zig, info["tarball"], info["shasum"], ".zip" if system == "windows" else ".tar.xz")
    paths = [zig]
    if args.binaryen:
        binaryen = args.destination / f"binaryen-{BINARYEN_VERSION}"
        if not binaryen.is_dir():
            tag = f"version_{BINARYEN_VERSION}"
            asset_arch = "arm64" if arch == "aarch64" else arch
            base = f"https://github.com/WebAssembly/binaryen/releases/download/{tag}"
            name = f"binaryen-{tag}-{asset_arch}-{system}.tar.gz"
            with urllib.request.urlopen(f"{base}/{name}.sha256", timeout=30) as response:
                checksum = response.read().decode().split()[0]
            install(binaryen, f"{base}/{name}", checksum, ".tar.gz")
        paths.append(binaryen / "bin")
    gh_path = os.environ.get("GITHUB_PATH")
    if gh_path:
        with open(gh_path, "a", encoding="utf8") as output:
            for path in paths:
                print(path.resolve(), file=output)
    else:
        for path in paths:
            print(path.resolve())


if __name__ == "__main__":
    main()
