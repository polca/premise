"""Configuration persistence routes for the Premise UI scaffold."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from premise_ui.api.models import (
    ProjectCloneRequest,
    ProjectPathRequest,
    ProjectRunRequest,
    ProjectSaveRequest,
)
from premise_ui.core.manifests import (
    GuiProjectManifest,
    build_run_manifest_from_project,
)
from premise_ui.core.recents import remember_recent_project
from premise_ui.core.storage import clone_project, load_project, save_project

router = APIRouter(prefix="/api/projects", tags=["projects"])


def _project_from_request(request: ProjectRunRequest) -> GuiProjectManifest:
    if request.project:
        return GuiProjectManifest.from_dict(request.project)
    if request.path:
        return load_project(request.path)
    raise HTTPException(
        status_code=400,
        detail="Either `path` or `configuration` must be provided.",
    )


@router.get("/template")
def project_template(workflow: str = "new_database") -> dict:
    return {"project": GuiProjectManifest.template(workflow=workflow).to_dict()}


@router.post("/open")
def open_project(request: ProjectPathRequest) -> dict:
    project = load_project(request.path)
    remember_recent_project(request.path, label=project.project_name)
    return {"path": request.path, "project": project.to_dict()}


@router.post("/save")
def save_project_route(request: ProjectSaveRequest) -> dict:
    project = GuiProjectManifest.from_dict(request.project)
    saved_path = save_project(request.path, project)
    remember_recent_project(str(saved_path), label=project.project_name)
    return {"path": str(saved_path), "project": project.to_dict()}


@router.post("/clone")
def clone_project_route(request: ProjectCloneRequest) -> dict:
    cloned = clone_project(request.source_path, request.target_path)
    project = load_project(cloned)
    remember_recent_project(str(cloned), label=project.project_name)
    return {"path": str(cloned), "project": project.to_dict()}


@router.post("/history")
def project_history_route(request: ProjectPathRequest) -> dict:
    project = load_project(request.path)
    return {"path": request.path, "run_history": project.run_history}


@router.post("/run-manifest")
def build_run_manifest_route(request: ProjectRunRequest) -> dict:
    project = _project_from_request(request)
    manifest = build_run_manifest_from_project(project, project_path=request.path)
    return {
        "path": request.path,
        "project": project.to_dict(),
        "run_manifest": manifest.to_dict(),
    }
