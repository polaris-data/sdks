#!/usr/bin/env python3
"""Validate a release tag against all package version sources."""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TAG_PATTERN = re.compile(
    r"^(?P<ecosystem>python|rust|typescript)-v(?P<major>0|[1-9]\d*)"
    r"\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$"
)
TOML_SECTION_PATTERN = re.compile(r"^\s*\[([^\]]+)\]\s*(?:#.*)?$")


def toml_string(path: str, section: str, key: str) -> str:
    manifest_path = ROOT / path
    current_section = ""
    value_pattern = re.compile(
        rf'^\s*{re.escape(key)}\s*=\s*"([^"]+)"\s*(?:#.*)?$'
    )
    for line in manifest_path.read_text().splitlines():
        section_match = TOML_SECTION_PATTERN.fullmatch(line)
        if section_match:
            current_section = section_match.group(1)
            continue
        if current_section == section:
            value_match = value_pattern.fullmatch(line)
            if value_match:
                return value_match.group(1)
    raise ValueError(f"could not find [{section}].{key} in {manifest_path}")


def python_runtime_version() -> str:
    init_path = ROOT / "python/polaris_data/__init__.py"
    module = ast.parse(init_path.read_text(), filename=str(init_path))
    for statement in module.body:
        if (
            isinstance(statement, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in statement.targets
            )
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            return statement.value.value
    raise ValueError(f"could not find a literal __version__ assignment in {init_path}")


def json_string(path: str, *keys: str) -> str:
    manifest_path = ROOT / path
    value: object = json.loads(manifest_path.read_text())
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            dotted_key = ".".join(keys)
            raise ValueError(f"could not find {dotted_key} in {manifest_path}")
        value = value[key]
    if not isinstance(value, str):
        dotted_key = ".".join(keys)
        raise ValueError(f"{dotted_key} in {manifest_path} is not a string")
    return value


def version_sources(ecosystem: str) -> dict[str, str]:
    if ecosystem == "python":
        return {
            "pyproject.toml [project]": toml_string(
                "pyproject.toml", "project", "version"
            ),
            "crates/polaris-python/Cargo.toml [package]": toml_string(
                "crates/polaris-python/Cargo.toml", "package", "version"
            ),
            "python/polaris_data/__init__.py": python_runtime_version(),
        }
    if ecosystem == "rust":
        return {
            "crates/polaris-data/Cargo.toml [package]": toml_string(
                "crates/polaris-data/Cargo.toml", "package", "version"
            )
        }
    return {
        "typescript/package.json": json_string(
            "typescript/package.json", "version"
        ),
        "typescript/package-lock.json": json_string(
            "typescript/package-lock.json", "version"
        ),
        "typescript/package-lock.json packages[\"\"]": json_string(
            "typescript/package-lock.json", "packages", "", "version"
        ),
    }


def main() -> int:
    if len(sys.argv) != 2:
        print(
            f"usage: {Path(sys.argv[0]).name} "
            "(python-vX.Y.Z|rust-vX.Y.Z|typescript-vX.Y.Z)",
            file=sys.stderr,
        )
        return 2

    tag = sys.argv[1]
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        print(
            f"invalid release tag {tag!r}; expected "
            "python-vX.Y.Z, rust-vX.Y.Z, or typescript-vX.Y.Z",
            file=sys.stderr,
        )
        return 2

    ecosystem = match.group("ecosystem")
    expected = ".".join(
        (match.group("major"), match.group("minor"), match.group("patch"))
    )
    mismatches = {
        source: actual
        for source, actual in version_sources(ecosystem).items()
        if actual != expected
    }
    if mismatches:
        print(f"{tag} does not match package metadata:", file=sys.stderr)
        for source, actual in mismatches.items():
            print(f"  {source}: expected {expected}, found {actual}", file=sys.stderr)
        return 1

    print(f"verified {tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
