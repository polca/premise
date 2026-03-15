"""Support-bundle export helpers for the Premise UI."""

from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from premise_ui.core.diagnostics import load_run_diagnostics
from premise_ui.core.manifests import GuiProjectManifest, utc_now_iso
from premise_ui.core.redaction import redact_value
from premise_ui.core.storage import load_project
from premise_ui.worker.events import read_events


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _project_payload(project_path: str | None, run_id: str | None) -> tuple[dict | None, dict | None]:
    if not project_path:
        return None, None

    project_file = Path(project_path).expanduser().resolve()
    if not project_file.exists():
        return None, None

    project = load_project(project_file)
    history_entry = None
    for entry in project.run_history:
        if entry.get("run_id") == run_id:
            history_entry = dict(entry)
            break

    if history_entry and history_entry.get("project_snapshot"):
        project_config = history_entry.get("project_snapshot")
    else:
        project_config = {
            "schema_version": project.schema_version,
            "project_name": project.project_name,
            "workflow": project.workflow,
            "config": project.config,
            "scenario_sets": project.scenario_sets,
        }

    if history_entry is not None:
        history_entry.pop("project_snapshot", None)

    return redact_value(project_config), redact_value(history_entry)


def write_support_bundle(
    run_dir: str | Path,
    *,
    run_id: str | None = None,
    project_path: str | None = None,
) -> Path:
    base_dir = Path(run_dir).expanduser().resolve()
    diagnostics = load_run_diagnostics(base_dir)
    manifest = diagnostics.get("manifest") or {}
    resolved_run_id = run_id or manifest.get("run_id") or base_dir.name
    resolved_project_path = project_path or manifest.get("project_path")
    project_config, history_entry = _project_payload(resolved_project_path, resolved_run_id)

    summary = {
        "bundle_version": 1,
        "generated_at": utc_now_iso(),
        "run_id": resolved_run_id,
        "project_config": project_config,
        "project_history_entry": history_entry,
        "metadata": diagnostics.get("metadata"),
        "manifest": manifest,
        "result": diagnostics.get("result"),
        "diagnostics": diagnostics.get("diagnostics"),
        "events": redact_value(read_events(base_dir / "events.jsonl")),
        "available_files": diagnostics.get("available_files", []),
    }

    stdout_log = redact_value(_read_text(base_dir / "stdout.log"))
    stderr_log = redact_value(_read_text(base_dir / "stderr.log"))

    bundle_path = base_dir / f"support-bundle-{resolved_run_id}.zip"
    with ZipFile(bundle_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(
            "summary.json",
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
        )
        archive.writestr("stdout.log", stdout_log)
        archive.writestr("stderr.log", stderr_log)

    return bundle_path
