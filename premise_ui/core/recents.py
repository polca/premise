"""Global recent-project and recent-path persistence for the Premise UI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from premise_ui.core.manifests import utc_now_iso
from premise_ui.core.paths import USER_UI_RECENTS_FILE
from premise_ui.core.storage import read_json, write_json

RECENTS_FILE = USER_UI_RECENTS_FILE
MAX_RECENT_PROJECTS = 8
MAX_RECENT_PATHS = 12


def _default_state() -> dict[str, list[dict[str, Any]]]:
    return {"projects": [], "paths": []}


def _normalize_state(state: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(state, dict):
        return _default_state()
    return {
        "projects": list(state.get("projects", [])),
        "paths": list(state.get("paths", [])),
    }


def _write_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    write_json(RECENTS_FILE, state)
    return state


def load_recent_state() -> dict[str, list[dict[str, Any]]]:
    if not RECENTS_FILE.exists():
        return _default_state()

    try:
        return _normalize_state(read_json(RECENTS_FILE))
    except Exception:
        return _default_state()


def clear_recent_state() -> dict[str, list[dict[str, Any]]]:
    return _write_state(_default_state())


def _resolve_path(path: str, *, base_path: str | None = None) -> str:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return str(candidate.resolve())

    if base_path:
        base = Path(base_path).expanduser()
        if base.suffix:
            base = base.parent
        return str((base / candidate).resolve())

    return str(candidate.resolve())


def _remember_entry(
    items: list[dict[str, Any]],
    entry: dict[str, Any],
    *,
    key_fields: tuple[str, ...],
    limit: int,
) -> list[dict[str, Any]]:
    filtered = []
    for item in items:
        if all(item.get(field) == entry.get(field) for field in key_fields):
            continue
        filtered.append(item)
    return [entry, *filtered][:limit]


def remember_recent_project(path: str, *, label: str | None = None) -> dict[str, list[dict[str, Any]]]:
    resolved = _resolve_path(path)
    state = load_recent_state()
    entry = {
        "path": resolved,
        "label": label or Path(resolved).stem or resolved,
        "last_used_at": utc_now_iso(),
    }
    state["projects"] = _remember_entry(
        state["projects"],
        entry,
        key_fields=("path",),
        limit=MAX_RECENT_PROJECTS,
    )
    return _write_state(state)


def remember_recent_path(
    path: str,
    *,
    kind: str,
    label: str | None = None,
    base_path: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    resolved = _resolve_path(path, base_path=base_path)
    state = load_recent_state()
    entry = {
        "path": resolved,
        "kind": kind,
        "label": label or Path(resolved).name or resolved,
        "last_used_at": utc_now_iso(),
    }
    state["paths"] = _remember_entry(
        state["paths"],
        entry,
        key_fields=("path", "kind"),
        limit=MAX_RECENT_PATHS,
    )
    return _write_state(state)


def recents_payload() -> dict[str, list[dict[str, Any]]]:
    state = load_recent_state()
    return {
        "recent_projects": state["projects"],
        "recent_paths": state["paths"],
    }
