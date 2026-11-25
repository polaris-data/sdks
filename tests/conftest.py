from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "sdk/python/python"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
