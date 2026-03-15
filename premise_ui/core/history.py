"""Per-project run history helpers for the Premise UI."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from premise_ui.core.manifests import GuiProjectManifest, RunManifest, utc_now_iso
from premise_ui.core.storage import (
    list_run_artifacts,
    load_project,
    read_json,
    save_project,
)


def _project_dir(project_path: str | Path) -> Path:
    return Path(project_path).expanduser().resolve().parent


def _relative_to_project(project_path: str | Path, target_path: str | Path) -> str:
    project_dir = _project_dir(project_path)
    target = Path(target_path).expanduser().resolve()
    try:
        return str(target.relative_to(project_dir))
    except ValueError:
        return str(target)


def _resolve_run_dir(project_path: str | Path, run_dir_ref: str) -> Path:
    candidate = Path(run_dir_ref).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (_project_dir(project_path) / candidate).resolve()


def _project_snapshot(
    project: GuiProjectManifest | dict[str, Any] | None,
) -> dict[str, Any] | None:
    if project is None:
        return None

    manifest = (
        project
        if isinstance(project, GuiProjectManifest)
        else GuiProjectManifest.from_dict(project)
    )
    return {
        "schema_version": manifest.schema_version,
        "project_name": manifest.project_name,
        "workflow": manifest.workflow,
        "config": deepcopy(manifest.config),
        "scenario_sets": deepcopy(manifest.scenario_sets),
    }


def _scenario_summary(manifest: RunManifest) -> list[dict[str, Any]]:
    summary = []
    for scenario in manifest.scenarios:
        summary.append(
            {
                "model": scenario.get("model"),
                "pathway": scenario.get("pathway"),
                "year": scenario.get("year"),
            }
        )
    return summary


def _initial_history_entry(
    manifest: RunManifest,
    *,
    project_snapshot: dict[str, Any] | None,
    run_dir: str | Path,
    dry_run: bool,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    export = manifest.config.get("export", {})
    return {
        "run_id": manifest.run_id,
        "created_at": manifest.created_at,
        "updated_at": utc_now_iso(),
        "status": "queued",
        "workflow": manifest.workflow,
        "project_name": manifest.project_name,
        "dry_run": dry_run,
        "export_type": export.get("type"),
        "scenario_count": len(manifest.scenarios),
        "scenarios": _scenario_summary(manifest),
        "run_dir": (
            _relative_to_project(manifest.project_path, run_dir)
            if manifest.project_path
            else str(Path(run_dir).expanduser().resolve())
        ),
        "artifacts": [],
        "artifact_count": 0,
        "warnings": list(warnings or []),
        "project_snapshot": project_snapshot,
    }


def remember_project_run(
    project_path: str | None,
    *,
    manifest: RunManifest,
    run_dir: str | Path,
    dry_run: bool,
    project_snapshot: dict[str, Any] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any] | None:
    if not project_path:
        return None

    project_file = Path(project_path).expanduser().resolve()
    if not project_file.exists():
        return None

    project = load_project(project_file)
    entry = _initial_history_entry(
        manifest,
        project_snapshot=_project_snapshot(project_snapshot),
        run_dir=run_dir,
        dry_run=dry_run,
        warnings=warnings,
    )
    project.run_history = [
        entry,
        *[
            item
            for item in project.run_history
            if item.get("run_id") != manifest.run_id
        ],
    ]
    save_project(project_file, project)
    return entry


def sync_project_run_history(
    project_path: str | None,
    *,
    run_id: str,
    manifest: dict[str, Any] | None = None,
    run_dir: str | Path | None = None,
    status: str,
    events: list[dict[str, Any]] | None = None,
    process_returncode: int | None = None,
) -> dict[str, Any] | None:
    if not project_path:
        return None

    project_file = Path(project_path).expanduser().resolve()
    if not project_file.exists():
        return None

    project = load_project(project_file)
    index = next(
        (
            position
            for position, item in enumerate(project.run_history)
            if item.get("run_id") == run_id
        ),
        None,
    )
    if index is None:
        if manifest is None or run_dir is None:
            return None

        run_manifest = RunManifest.from_dict(manifest)
        entry = _initial_history_entry(
            run_manifest,
            project_snapshot=None,
            run_dir=run_dir,
            dry_run=False,
        )
        project.run_history = [entry, *project.run_history]
        index = 0

    entry = dict(project.run_history[index])
    resolved_run_dir = None
    if run_dir is not None:
        resolved_run_dir = Path(run_dir).expanduser().resolve()
    elif entry.get("run_dir"):
        resolved_run_dir = _resolve_run_dir(project_file, str(entry["run_dir"]))

    artifacts = (
        list_run_artifacts(resolved_run_dir)
        if resolved_run_dir
        else entry.get("artifacts", [])
    )
    entry["updated_at"] = utc_now_iso()
    entry["status"] = status
    entry["artifacts"] = artifacts
    entry["artifact_count"] = len(artifacts)

    if process_returncode is not None:
        entry["process_returncode"] = process_returncode

    if events:
        last_event = events[-1]
        entry["last_event"] = {
            "timestamp": last_event.get("timestamp"),
            "event_type": last_event.get("event_type"),
            "phase": last_event.get("phase"),
            "message": last_event.get("message"),
        }

    if resolved_run_dir and (resolved_run_dir / "result.json").is_file():
        try:
            entry["result"] = read_json(resolved_run_dir / "result.json")
        except Exception:
            pass

    if status in {"completed", "failed", "cancelled"} and "completed_at" not in entry:
        entry["completed_at"] = utc_now_iso()

    project.run_history[index] = entry
    save_project(project_file, project)
    return entry
