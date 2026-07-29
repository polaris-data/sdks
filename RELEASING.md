# Releasing Polaris SDKs

The Python and Rust packages are versioned and published independently. Release
tags must point to a commit contained in `main`; the release workflows reject
version mismatches and tags created from other branches.

## Version sources

For a Python release, update these values together:

- `[project].version` in `pyproject.toml`
- `[package].version` in `crates/polaris-python/Cargo.toml`
- `__version__` in `python/polaris_data/__init__.py`
- The local project entries in `uv.lock` and `Cargo.lock`

For a Rust release, update these values together:

- `[package].version` in `crates/polaris-data/Cargo.toml`
- The `polaris-data` entry in `Cargo.lock`

The prepared releases are Python `0.9.0` and Rust `0.7.0`.

## One-time registry setup

Both GitHub environments require `HilliamT` as an approving reviewer, with
self-review allowed.

### crates.io

1. Create a crates.io API token restricted to publishing `polaris-data`.
2. Store it as the `CARGO_REGISTRY_TOKEN` secret in the `crates-io` GitHub
   environment.

### PyPI

Configure a trusted publisher for the existing `polaris-data` project:

- Owner: `polaris-data`
- Repository: `sdks`
- Workflow: `release-python.yml`
- Environment: `pypi`

No long-lived PyPI token is stored in GitHub. The `pypi` environment grants its
publish job permission to request a short-lived trusted-publishing token.

## Preflight

Run these checks from a clean checkout:

```bash
python3 scripts/check-release-version.py rust-v0.7.0
python3 scripts/check-release-version.py python-v0.9.0
uv lock --check
cargo metadata --locked --no-deps --format-version 1 > /dev/null
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo publish -p polaris-data --locked --dry-run
```

Build and inspect the Python artifacts with the same maturin version used in
the release workflow:

```bash
mkdir -p dist
uvx --from maturin==1.14.1 maturin build \
  --release --locked --compatibility pypi --out dist
uvx --from maturin==1.14.1 maturin sdist --out dist
uvx --from twine twine check dist/*
```

Install the wheel and source distribution into separate clean virtual
environments. In each environment, verify:

```bash
python -c \
  'import polaris_data; from polaris_data import _native; assert polaris_data.__version__ == "0.9.0"; assert _native.__native__'
```

Before tagging, verify that Python `0.9.0` and Rust `0.7.0` are still
unpublished and that CI passes on the selected `main` commit.

## Publish Rust first

Create an annotated tag at the selected commit and push only that tag:

```bash
git fetch origin main
git tag -a rust-v0.7.0 origin/main -m "Release Rust SDK 0.7.0"
git push origin rust-v0.7.0
```

Wait for validation, approve the `crates-io` deployment, and verify the
published crate before continuing:

```bash
cargo info polaris-data@0.7.0
```

## Publish Python second

Tag the exact commit published by the verified Rust release:

```bash
release_sha=$(git rev-list -n 1 rust-v0.7.0)
git tag -a python-v0.9.0 "$release_sha" -m "Release Python SDK 0.9.0"
git push origin python-v0.9.0
```

Wait for every wheel and the source distribution to build, approve the `pypi`
deployment, and then install from PyPI in a clean environment. Verify the
package version and native extension with the import command from the preflight
section.
