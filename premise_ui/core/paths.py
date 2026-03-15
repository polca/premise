"""Filesystem path helpers for the Premise UI scaffold."""

from __future__ import annotations

import os
from pathlib import Path

import platformdirs

_USER_UI_DATA_DIR_OVERRIDE = os.environ.get("PREMISE_UI_DATA_DIR")
USER_UI_DATA_DIR = (
    Path(_USER_UI_DATA_DIR_OVERRIDE).expanduser().resolve()
    if _USER_UI_DATA_DIR_OVERRIDE
    else platformdirs.user_data_path(appname="premise-ui", appauthor="pylca")
)
USER_UI_DATA_DIR.mkdir(parents=True, exist_ok=True)

USER_UI_RUNS_DIR = USER_UI_DATA_DIR / "runs"
USER_UI_RUNS_DIR.mkdir(parents=True, exist_ok=True)

USER_UI_RECENTS_FILE = USER_UI_DATA_DIR / "recents.json"
USER_UI_CREDENTIALS_FILE = USER_UI_DATA_DIR / "credentials.json"


def frontend_dist_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "frontend" / "dist"


def project_runs_dir(project_path: str | None) -> Path:
    if project_path:
        project_file = Path(project_path).expanduser().resolve()
        run_dir = project_file.parent / ".premise-ui" / "runs"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    return USER_UI_RUNS_DIR


def run_dir_for(project_path: str | None, run_id: str) -> Path:
    run_dir = project_runs_dir(project_path) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir
