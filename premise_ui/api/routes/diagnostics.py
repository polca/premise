"""Diagnostics routes for the Premise UI scaffold."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from premise_ui.api.models import RunDiagnosticsRequest
from premise_ui.core.diagnostics import load_run_diagnostics
from premise_ui.core.paths import USER_UI_DATA_DIR

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])


@router.get("/summary")
def diagnostics_summary() -> dict:
    return {
        "ui_data_dir": str(USER_UI_DATA_DIR),
        "detail": "Run diagnostics are available via `/api/diagnostics/run-details`.",
    }


def _resolve_run_dir(request: RunDiagnosticsRequest, api_request: Request) -> Path:
    if request.run_id:
        run_dir_value = api_request.app.state.run_dirs.get(request.run_id)
        if run_dir_value is not None:
            return Path(run_dir_value).expanduser().resolve()

    if request.run_dir:
        candidate = Path(request.run_dir).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        if request.project_path:
            project_path = Path(request.project_path).expanduser().resolve()
            return (project_path.parent / candidate).resolve()

    raise HTTPException(
        status_code=400,
        detail="Provide a known `run_id`, or both `project_path` and `run_dir`.",
    )


@router.post("/run-details")
def run_details(request: RunDiagnosticsRequest, api_request: Request) -> dict:
    run_dir = _resolve_run_dir(request, api_request)
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail=f"Run directory not found: {run_dir}")

    payload = load_run_diagnostics(run_dir)
    payload["run_id"] = request.run_id or payload.get("manifest", {}).get("run_id")
    return payload
