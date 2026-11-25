#!/usr/bin/env python3
"""Install the packaged crate as a dependency in a clean project."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("crate", type=Path)
    args = parser.parse_args()
    if args.crate.stat().st_size >= 10_000_000:
        raise SystemExit("Bundled Zig archives exceed the crates.io package size limit")
    with tempfile.TemporaryDirectory() as temp:
        work = Path(temp)
        unpacked = work / "unpacked"
        with tarfile.open(args.crate) as archive:
            archive.extractall(unpacked, filter="data")
        package, = unpacked.iterdir()
        (work / "Cargo.toml").write_text(
            '[package]\nname = "polaris-consumer"\nversion = "0.0.0"\nedition = "2024"\n'
            f'[dependencies]\npolaris-data = {{ path = {json.dumps(str(package))} }}\n'
        )
        (work / "src").mkdir()
        (work / "src/main.rs").write_text("""
use polaris_data::{blocking::PolarisClient, DataQuery, StreamQuery, SubscriptionRequest};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args().nth(1).expect("fixture endpoint");
    let client = PolarisClient::builder().base_url(url).api_key("fixture-key").build()?;
    assert!(client.health()?.ok);
    let data = client.data_rows(DataQuery { table: "standard.trades".into(), ..Default::default() })?;
    assert_eq!(data.collect::<Result<Vec<_>, _>>()?.len(), 2);
    let pages = client.stream(StreamQuery { subscriptions: vec![
        SubscriptionRequest { table: "standard.trades".into(), ..Default::default() }
    ] })?;
    assert_eq!(pages.map(|page| page.map(|p| p.cursor)).collect::<Result<Vec<_>, _>>()?, ["stream:1", "stream:2"]);
    Ok(())
}
""")
        # Published crates should link their matching archive without a Zig executable.
        env = dict(os.environ, ZIG="zig-must-not-run")
        with subprocess.Popen(
            [sys.executable, ROOT / "tests/fixture_server.py"], stdout=subprocess.PIPE, text=True
        ) as fixture:
            try:
                endpoint = fixture.stdout.readline().strip()
                subprocess.run(["cargo", "run", "--quiet", "--", endpoint], cwd=work, env=env, check=True, timeout=180)
            finally:
                fixture.terminate()


if __name__ == "__main__":
    main()
