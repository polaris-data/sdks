#!/usr/bin/env python3
"""Install a Python SDK artifact in a clean venv and exercise the shared fixture."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]

SNIPPET = """
import sys
from polaris_data import OrderbookBuilder, PolarisClient, _native
assert _native.__native__
with PolarisClient('fixture-key', base_url=sys.argv[1]) as client:
    assert client.health()['ok']
    assert len(list(client.data_rows('standard.trades'))) == 2
    with client.stream([{'table':'standard.trades'}]) as pages:
        assert [page['cursor'] for page in pages] == ['stream:1', 'stream:2']
books = OrderbookBuilder()
assert books.update({
    'timestamp': 1, 'source': 'fixture', 'market': 'BTC', 'type': 'orderbook',
    'data': {'bids': [[99, 1]], 'asks': []},
})
assert books.snapshot('fixture', 'BTC')['bids'][0]['price'] == 99
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as temp:
        venv = Path(temp) / "venv"
        subprocess.run([sys.executable, "-m", "venv", venv], check=True)
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        subprocess.run([python, "-m", "pip", "install", args.artifact], cwd=temp, check=True)
        with subprocess.Popen(
            [sys.executable, ROOT / "tests/fixture_server.py"], stdout=subprocess.PIPE, text=True
        ) as fixture:
            try:
                endpoint = fixture.stdout.readline().strip()
                subprocess.run([python, "-I", "-c", SNIPPET, endpoint], cwd=temp, check=True, timeout=30)
            finally:
                fixture.terminate()


if __name__ == "__main__":
    main()
