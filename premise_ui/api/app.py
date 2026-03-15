"""FastAPI application factory for the local Premise UI service."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import FileResponse, JSONResponse

from premise_ui import __version__ as UI_VERSION
from premise_ui.api.routes.capabilities import router as capabilities_router
from premise_ui.api.routes.credentials import router as credentials_router
from premise_ui.api.routes.diagnostics import router as diagnostics_router
from premise_ui.api.routes.dialogs import router as dialogs_router
from premise_ui.api.routes.discovery import router as discovery_router
from premise_ui.api.routes.health import router as health_router
from premise_ui.api.routes.jobs import router as jobs_router
from premise_ui.api.routes.projects import router as projects_router
from premise_ui.api.routes.recents import router as recents_router
from premise_ui.api.routes.scenario_explorer import router as scenario_explorer_router
from premise_ui.core.paths import frontend_dist_dir


def create_app() -> FastAPI:
    app = FastAPI(title="Premise UI", version=UI_VERSION)
    app.state.processes = {}
    app.state.run_dirs = {}
    app.state.job_queue = []
    app.state.active_run_id = None

    app.include_router(health_router)
    app.include_router(capabilities_router)
    app.include_router(credentials_router)
    app.include_router(dialogs_router)
    app.include_router(discovery_router)
    app.include_router(projects_router)
    app.include_router(recents_router)
    app.include_router(jobs_router)
    app.include_router(diagnostics_router)
    app.include_router(scenario_explorer_router)

    dist_dir = frontend_dist_dir()
    index_file = dist_dir / "index.html"

    @app.get("/", include_in_schema=False, response_model=None)
    def root() -> Response:
        if index_file.exists():
            return FileResponse(index_file)
        return JSONResponse(
            {"detail": "Premise UI frontend assets are not bundled yet."},
            status_code=503,
        )

    @app.get("/{asset_path:path}", include_in_schema=False, response_model=None)
    def spa(asset_path: str) -> Response:
        if asset_path.startswith("api/"):
            return JSONResponse({"detail": "Not found."}, status_code=404)

        candidate = (dist_dir / asset_path).resolve()
        try:
            candidate.relative_to(dist_dir.resolve())
        except ValueError:
            return JSONResponse({"detail": "Not found."}, status_code=404)

        if candidate.is_file():
            return FileResponse(candidate)

        if index_file.exists():
            return FileResponse(index_file)

        return JSONResponse(
            {"detail": "Premise UI frontend assets are not bundled yet."},
            status_code=503,
        )

    return app
