"""Job management routes for the Premise UI scaffold."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from premise_ui.api.models import (
    JobCancelRequest,
    JobEnqueueRequest,
    JobValidateRequest,
    ProjectRunRequest,
)
from premise_ui.core.history import remember_project_run, sync_project_run_history
from premise_ui.core.job_runtime import (
    advance_queue as advance_job_queue,
    pop_queued_job,
    queue_or_start_job,
    queue_position,
    status_from_events,
)
from premise_ui.core.manifests import (
    GuiProjectManifest,
    RunManifest,
    build_run_manifest_from_project,
)
from premise_ui.core.paths import run_dir_for
from premise_ui.core.storage import list_run_artifacts, read_json, write_json
from premise_ui.core.support_bundle import write_support_bundle
from premise_ui.core.validation import validate_run_manifest_payload
from premise_ui.worker.events import EventWriter, read_events

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


def _manifest_path(run_dir: Path) -> Path:
    return run_dir / "run_manifest.json"


def _job_state(run_id: str, api_request: Request) -> dict[str, Any]:
    run_dir_value = api_request.app.state.run_dirs.get(run_id)
    if run_dir_value is None:
        raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}")

    run_dir = Path(run_dir_value)
    manifest = read_json(run_dir / "run_manifest.json")
    events = read_events(run_dir / "events.jsonl")
    process = api_request.app.state.processes.get(run_id)
    process_returncode = process.poll() if process is not None else None
    queued_position = queue_position(api_request.app.state.job_queue, run_id)
    status = status_from_events(
        events,
        process_returncode=process_returncode,
        is_active=api_request.app.state.active_run_id == run_id and process_returncode is None,
        queue_position=queued_position,
    )
    return {
        "run_id": run_id,
        "run_dir": run_dir,
        "manifest": manifest,
        "events": events,
        "process": process,
        "process_returncode": process_returncode,
        "queue_position": queued_position,
        "status": status,
    }


def _sync_history_for_run(run_id: str, api_request: Request) -> dict[str, Any]:
    state = _job_state(run_id, api_request)
    sync_project_run_history(
        state["manifest"].get("project_path"),
        run_id=run_id,
        manifest=state["manifest"],
        run_dir=state["run_dir"],
        status=state["status"],
        events=state["events"],
        process_returncode=state["process_returncode"],
    )
    return state


def _spawn_job(job: dict[str, Any], api_request: Request) -> dict[str, Any]:
    command = [sys.executable, "-m", "premise_ui.worker", job["manifest_path"]]
    if job["dry_run"]:
        command.append("--dry-run")

    process = subprocess.Popen(command, cwd=job["run_dir"])
    api_request.app.state.processes[job["run_id"]] = process
    api_request.app.state.active_run_id = job["run_id"]
    return job


def _advance_queue(api_request: Request) -> None:
    advance_job_queue(
        api_request.app.state,
        sync_finished_run=lambda run_id: _sync_history_for_run(run_id, api_request),
        spawn_job=lambda job: _spawn_job(job, api_request),
    )


def _prepare_job(
    manifest: RunManifest,
    *,
    dry_run: bool,
    api_request: Request,
    warnings: list[str],
    project_snapshot: dict | None = None,
) -> dict[str, Any]:
    run_dir = run_dir_for(manifest.project_path, manifest.run_id)
    manifest_path = _manifest_path(run_dir)
    write_json(manifest_path, manifest.to_dict())

    api_request.app.state.run_dirs[manifest.run_id] = str(run_dir)
    EventWriter(run_dir, manifest.run_id).emit(
        "job_queued",
        phase="queue",
        message="Run queued for worker execution.",
    )
    remember_project_run(
        manifest.project_path,
        manifest=manifest,
        run_dir=run_dir,
        dry_run=dry_run,
        project_snapshot=project_snapshot,
        warnings=warnings,
    )

    return {
        "run_id": manifest.run_id,
        "run_dir": str(run_dir),
        "manifest_path": str(manifest_path),
        "dry_run": dry_run,
        "warnings": warnings,
        "run_manifest": manifest.to_dict(),
    }


def _queue_or_start(
    manifest: RunManifest,
    *,
    dry_run: bool,
    api_request: Request,
    warnings: list[str],
    project_snapshot: dict | None = None,
) -> dict[str, Any]:
    _advance_queue(api_request)

    job = _prepare_job(
        manifest,
        dry_run=dry_run,
        api_request=api_request,
        warnings=warnings,
        project_snapshot=project_snapshot,
    )
    status, queued_position = queue_or_start_job(
        api_request.app.state,
        job,
        spawn_job=lambda queued_job: _spawn_job(queued_job, api_request),
    )
    job["status"] = status
    job["queue_position"] = queued_position
    return job


def _project_from_request(request: ProjectRunRequest) -> GuiProjectManifest:
    if request.project:
        return GuiProjectManifest.from_dict(request.project)
    if request.path:
        return GuiProjectManifest.from_dict(read_json(request.path))
    raise HTTPException(
        status_code=400,
        detail="Either `path` or `configuration` must be provided.",
    )


def _resolve_run_dir(
    run_id: str,
    api_request: Request,
    *,
    project_path: str | None = None,
    run_dir: str | None = None,
) -> Path:
    run_dir_value = api_request.app.state.run_dirs.get(run_id)
    if run_dir_value is not None:
        return Path(run_dir_value).expanduser().resolve()

    if run_dir:
        candidate = Path(run_dir).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        if project_path:
            project_file = Path(project_path).expanduser().resolve()
            return (project_file.parent / candidate).resolve()

    raise HTTPException(
        status_code=404,
        detail=f"Unknown run: {run_id}",
    )


def _resolve_artifact_path(run_dir: Path, artifact_path: str) -> Path:
    candidate = (run_dir / artifact_path).resolve()
    try:
        candidate.relative_to(run_dir.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Artifact path escapes the run directory.") from exc

    if not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_path}")

    return candidate


@router.post("/validate")
def validate_job(request: JobValidateRequest) -> dict:
    errors, warnings = validate_run_manifest_payload(request.run_manifest)
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }


@router.post("/enqueue")
def enqueue_job(request: JobEnqueueRequest, api_request: Request) -> dict:
    errors, warnings = validate_run_manifest_payload(request.run_manifest)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors, "warnings": warnings})

    manifest = RunManifest.from_dict(request.run_manifest)
    return _queue_or_start(
        manifest,
        dry_run=request.dry_run,
        api_request=api_request,
        warnings=warnings,
    )


@router.post("/enqueue-project")
def enqueue_project_job(request: ProjectRunRequest, api_request: Request) -> dict:
    project = _project_from_request(request)
    manifest = build_run_manifest_from_project(project, project_path=request.path)
    errors, warnings = validate_run_manifest_payload(manifest.to_dict())
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors, "warnings": warnings})

    return _queue_or_start(
        manifest,
        dry_run=request.dry_run,
        api_request=api_request,
        warnings=warnings,
        project_snapshot=project.to_dict(),
    )


@router.post("/cancel")
def cancel_job(request: JobCancelRequest, api_request: Request) -> dict:
    queued_job = pop_queued_job(api_request.app.state.job_queue, request.run_id)
    if queued_job is not None:
        run_dir = Path(queued_job["run_dir"])
        EventWriter(run_dir, request.run_id).emit(
            "job_cancelled",
            level="warning",
            phase="queue",
            message="Queued run cancelled before worker startup.",
        )
        write_json(
            run_dir / "diagnostics.json",
            {
                "kind": "cancelled",
                "message": "Queued run cancelled before worker startup.",
                "cancelled_before_start": True,
            },
        )
        sync_project_run_history(
            queued_job["run_manifest"].get("project_path"),
            run_id=request.run_id,
            manifest=queued_job["run_manifest"],
            run_dir=run_dir,
            status="cancelled",
            events=read_events(run_dir / "events.jsonl"),
            process_returncode=None,
        )
        return {"run_id": request.run_id, "status": "cancelled"}

    process = api_request.app.state.processes.get(request.run_id)
    if process is None:
        raise HTTPException(status_code=404, detail=f"Unknown run: {request.run_id}")

    if process.poll() is None:
        process.terminate()
    return {"run_id": request.run_id, "status": "cancelling"}


@router.get("/{run_id}")
def job_status(run_id: str, api_request: Request) -> dict:
    _advance_queue(api_request)
    state = _sync_history_for_run(run_id, api_request)

    process_state = state["process_returncode"]
    status = state["status"]

    return {
        "run_id": run_id,
        "status": status,
        "queue_position": state["queue_position"],
        "process_returncode": process_state,
        "manifest": state["manifest"],
        "events": state["events"],
    }


@router.get("/{run_id}/artifacts")
def job_artifacts(run_id: str, api_request: Request) -> dict:
    run_dir_value = api_request.app.state.run_dirs.get(run_id)
    if run_dir_value is None:
        raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}")

    run_dir = Path(run_dir_value)
    return {"run_id": run_id, "artifacts": list_run_artifacts(run_dir)}


@router.get("/{run_id}/artifact", response_model=None)
def job_artifact(
    run_id: str,
    api_request: Request,
    path: str = Query(..., description="Artifact path relative to the run directory."),
    project_path: str | None = None,
    run_dir: str | None = None,
) -> FileResponse:
    resolved_run_dir = _resolve_run_dir(
        run_id,
        api_request,
        project_path=project_path,
        run_dir=run_dir,
    )
    artifact = _resolve_artifact_path(resolved_run_dir, path)
    return FileResponse(artifact, filename=artifact.name)


@router.get("/{run_id}/support-bundle")
def support_bundle(
    run_id: str,
    api_request: Request,
    project_path: str | None = None,
    run_dir: str | None = None,
) -> FileResponse:
    resolved_run_dir = _resolve_run_dir(
        run_id,
        api_request,
        project_path=project_path,
        run_dir=run_dir,
    )
    bundle_path = write_support_bundle(
        resolved_run_dir,
        run_id=run_id,
        project_path=project_path,
    )
    return FileResponse(
        bundle_path,
        media_type="application/zip",
        filename=bundle_path.name,
    )


@router.websocket("/events")
async def job_events(websocket: WebSocket) -> None:
    await websocket.accept()
    run_id = websocket.query_params.get("run_id")
    if not run_id:
        await websocket.send_json({"detail": "run_id query parameter is required."})
        await websocket.close(code=1008)
        return

    run_dir_value = websocket.app.state.run_dirs.get(run_id)
    if run_dir_value is None:
        await websocket.send_json({"detail": f"Unknown run: {run_id}"})
        await websocket.close(code=1008)
        return

    run_dir = Path(run_dir_value)
    events_path = run_dir / "events.jsonl"
    sent_count = 0

    try:
        while True:
            events = read_events(events_path)
            if sent_count < len(events):
                for event in events[sent_count:]:
                    await websocket.send_json(event)
                sent_count = len(events)
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return
