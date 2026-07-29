# Migrating to the native Python SDK

Version 0.9 keeps the documented `polaris_data` Python API while moving HTTP,
authentication, snapshot coverage, downloads, decoding, storage locking, and
analytics into the shared Rust engine.

## Removed constructor hooks

The undocumented `transport=` and `http_client=` constructor arguments were
deprecated in the final 0.8 release and are removed in 0.9. Tests should use a
deterministic local HTTP server and pass its URL through `base_url=`.

## Runtime and platform support

- CPython 3.9 and newer use `abi3` native wheels.
- Published wheels always include `polaris_data._native`.
- There is no pure-Python fallback.
- Unsupported targets require a working Rust toolchain for a source build.
- PyPy and free-threaded CPython wheels are not part of the initial 0.9 release.

## Data compatibility

- Numeric query timestamps remain microseconds for compatibility.
- Decoded event timestamps and derived timestamps are milliseconds.
- Snapshot data uses `data/`, materialized days use `daily/`, partial downloads
  use `tmp/`, replay artifacts use `cache/`, and cross-process locks use
  `locks/`.
- Standardized methods are strict by default. `allow_gaps=True` returns covered
  rows and emits the existing Python warning.
