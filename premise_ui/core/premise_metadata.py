"""Lightweight accessors for Premise metadata without importing heavy modules."""

from __future__ import annotations

import ast
from pathlib import Path

import yaml


def _premise_package_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "premise"


def load_premise_constants() -> dict:
    constants_path = _premise_package_dir() / "iam_variables_mapping" / "constants.yaml"
    with open(constants_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_premise_version() -> str:
    init_path = _premise_package_dir() / "__init__.py"
    module = ast.parse(init_path.read_text(encoding="utf-8"))

    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__version__":
                    value = ast.literal_eval(node.value)
                    if isinstance(value, tuple):
                        return ".".join(map(str, value))
                    return str(value)

    return "unknown"
