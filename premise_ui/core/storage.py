"""Persistence helpers for the Premise UI scaffold."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from premise_ui.core.manifests import GuiProjectManifest


def _ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: str | Path, payload: dict[str, Any]) -> Path:
    json_path = _ensure_parent(Path(path).expanduser().resolve())
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return json_path


def read_json(path: str | Path) -> dict[str, Any]:
    json_path = Path(path).expanduser().resolve()
    with open(json_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_project(path: str | Path, project: GuiProjectManifest) -> Path:
    return write_json(path, project.to_dict())


def load_project(path: str | Path) -> GuiProjectManifest:
    return GuiProjectManifest.from_dict(read_json(path))


def clone_project(source_path: str | Path, target_path: str | Path) -> Path:
    project = load_project(source_path).cloned()
    return save_project(target_path, project)


def list_run_artifacts(run_dir: str | Path) -> list[str]:
    base_dir = Path(run_dir).expanduser().resolve()
    if not base_dir.exists():
        return []
    return sorted(
        str(path.relative_to(base_dir))
        for path in base_dir.rglob("*")
        if path.is_file()
    )
