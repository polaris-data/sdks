# Source provenance

This monorepo combines two previously separate SDK implementations.

- The Rust engine under `crates/polaris-data` was imported from
  `polaris-data/polaris-rs` at commit
  `c78eb71f7f0f6420a237d4ab941cd52a3eadf287`.
- The Python facade, compatibility tests, and legacy differential oracle were
  imported from `polaris-data/polaris-py` at the `origin/main` snapshot present
  when this workspace was created.

The legacy Python implementation lives only at `tests/legacy_client.py`; it is
not packaged or imported at runtime. It remains available as a differential
oracle while the native 0.9 release line is validated.

When the standalone repositories are retired, their READMEs and repository
metadata should point contributors to
`https://github.com/polaris-data/polaris-sdk`.
