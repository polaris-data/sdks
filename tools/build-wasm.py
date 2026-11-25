#!/usr/bin/env python3
"""Build the browser and Node core from the same freestanding Zig binary."""
import base64
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "typescript" / "src"
zig = os.environ.get("ZIG", "zig")
wasm2js = os.environ.get("WASM2JS", "wasm2js")
expected = (ROOT / ".zigversion").read_text().strip()
actual = subprocess.check_output([zig, "version"], text=True).strip()
if actual != expected:
    raise SystemExit(f"Expected Zig {expected}, got {actual}")

binaryen = subprocess.check_output([wasm2js, "--version"], text=True)
if not binaryen.startswith("wasm2js version 133 "):
    raise SystemExit(f"Expected Binaryen 133, got {binaryen.strip()}")

subprocess.run([zig, "build", "wasm"], cwd=ROOT, check=True)
image = ROOT / "zig-out/bin/polaris.wasm"
binary = base64.b64encode(image.read_bytes()).decode()
(SOURCE / "wasm-image.ts").write_text(f"export default {json.dumps(binary)};\n")
fallback = SOURCE / "wasm-fallback.mjs"
subprocess.run(
    [wasm2js, str(image), "-O", "--enable-bulk-memory-opt", "--llvm-memory-copy-fill-lowering", "-o", str(fallback)],
    cwd=ROOT, check=True,
)
# Convert the instantiation emitted by the pinned compiler to a lazy factory.
source = fallback.read_text()
boundary = "\nvar retasmFunc = asmFunc("
if boundary not in source:
    raise SystemExit("Unexpected wasm2js module format")
fallback.write_text(source.split(boundary, 1)[0] + "\nexport function makeCore() { return asmFunc({}); }\n")
