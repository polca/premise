"""Discovery routes for local Premise UI resources."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException

from premise_ui.api.models import BrightwayProjectRequest, ScenarioPreviewRequest
from premise_ui.core.scenario_catalog import (
    clear_local_iam_scenarios,
    current_download_job,
    get_download_all_known_iam_scenarios_status,
    list_local_iam_scenarios,
    start_download_all_known_iam_scenarios,
)
from premise_ui.core.scenario_storylines import load_iam_storyline_catalog

router = APIRouter(prefix="/api/discovery", tags=["discovery"])


def _project_name(project: object) -> str:
    name = getattr(project, "name", None)
    if name:
        return str(name)

    value = str(project)
    if value.startswith("Project: "):
        return value.split("Project: ", 1)[1]
    return value


def _year_from_col(column: object) -> int | None:
    value = str(column).strip()
    if value.isdigit():
        return int(value)
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _read_preview_dataframe(path: Path, *, rows: int) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".mif"}:
        return pd.read_csv(path, nrows=rows)
    return pd.read_excel(path, nrows=rows)


def _preview_label(row: pd.Series) -> str:
    parts: list[str] = []
    lowered = {str(column).strip().lower(): column for column in row.index}
    for key in ("model", "scenario", "pathway", "region", "variable"):
        column = lowered.get(key)
        if column is None:
            continue
        value = row[column]
        if pd.notna(value) and str(value).strip():
            parts.append(str(value).strip())

    if not parts:
        parts.append("Preview series")

    return " / ".join(parts)


def _preview_series(df: pd.DataFrame, years: list[int]) -> list[dict]:
    if not years:
        return []

    year_columns = [
        column for column in df.columns if _year_from_col(column) in set(years)
    ]
    if not year_columns:
        return []

    series: list[dict] = []
    for _, row in df.iterrows():
        points: list[dict[str, float | int]] = []
        for column in year_columns:
            numeric_value = pd.to_numeric(row[column], errors="coerce")
            if pd.isna(numeric_value):
                continue
            year = _year_from_col(column)
            if year is None:
                continue
            points.append({"year": year, "value": float(numeric_value)})

        if len(points) < 2:
            continue

        lowered = {str(column).strip().lower(): column for column in row.index}
        unit_column = lowered.get("unit")
        unit = None
        if unit_column is not None and pd.notna(row[unit_column]):
            unit = str(row[unit_column]).strip() or None

        series.append(
            {
                "label": _preview_label(row),
                "unit": unit,
                "points": points,
            }
        )
        if len(series) == 4:
            break

    return series


@router.post("/brightway")
def brightway_discovery() -> dict:
    try:
        import bw2data
    except ImportError:
        return _empty_brightway_state()

    return _brightway_state_from_module(bw2data)


def _empty_brightway_state() -> dict:
    return {
        "available": False,
        "current_project": None,
        "projects": [],
        "databases": [],
    }


def _brightway_state_from_module(bw2data: object) -> dict:
    current_project = getattr(getattr(bw2data, "projects", None), "current", None)
    projects: list[str] = []
    try:
        projects = [_project_name(project) for project in bw2data.projects]
    except TypeError:
        projects = []
    except Exception:
        projects = []

    databases = getattr(bw2data, "databases", [])
    try:
        database_names = sorted(map(str, databases))
    except Exception:
        database_names = []

    return {
        "available": True,
        "current_project": _project_name(current_project) if current_project else None,
        "projects": projects,
        "databases": database_names,
    }


@router.post("/brightway/project")
def select_brightway_project(request: BrightwayProjectRequest) -> dict:
    try:
        import bw2data
    except ImportError as exc:
        raise HTTPException(
            status_code=503,
            detail="Brightway discovery is unavailable in this environment.",
        ) from exc

    requested_project = _project_name(request.project_name)
    state = _brightway_state_from_module(bw2data)
    if requested_project not in state["projects"]:
        raise HTTPException(
            status_code=404,
            detail=f"Brightway project not found: {request.project_name}",
        )

    try:
        bw2data.projects.set_current(requested_project)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to switch Brightway project: {exc}",
        ) from exc

    return _brightway_state_from_module(bw2data)


@router.post("/scenario-preview")
def scenario_preview(request: ScenarioPreviewRequest) -> dict:
    path = Path(request.path).expanduser().resolve()
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in {".csv", ".mif", ".xls", ".xlsx"}:
        raise HTTPException(
            status_code=400,
            detail="Unsupported IAM file type. Expected csv, mif, xls, or xlsx.",
        )

    header_df = _read_preview_dataframe(path, rows=0)
    preview_df = _read_preview_dataframe(path, rows=12)

    year_columns = []
    for column in header_df.columns:
        year = _year_from_col(column)
        if year is not None and 2005 <= year <= 2100:
            year_columns.append(year)

    model = None
    pathway = None
    if "_" in path.stem:
        model, pathway = path.stem.split("_", 1)

    return {
        "path": str(path),
        "file_name": path.name,
        "suffix": suffix,
        "inferred_model": model,
        "inferred_pathway": pathway,
        "years": sorted(set(year_columns)),
        "columns": [str(column) for column in header_df.columns],
        "series": _preview_series(preview_df, sorted(set(year_columns))),
    }


@router.post("/iam-scenarios/download-all")
def download_all_iam_scenarios() -> dict:
    try:
        return start_download_all_known_iam_scenarios()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to download IAM scenarios: {exc}",
        ) from exc


@router.get("/iam-scenarios/local")
def local_iam_scenarios() -> dict:
    try:
        return {"scenarios": list_local_iam_scenarios()}
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list local IAM scenarios: {exc}",
        ) from exc


@router.get("/iam-storylines")
def iam_storylines() -> dict:
    try:
        return load_iam_storyline_catalog()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load IAM scenario descriptions: {exc}",
        ) from exc


@router.get("/iam-scenarios/download-all/{job_id}")
def download_all_iam_scenarios_status(job_id: str) -> dict:
    payload = get_download_all_known_iam_scenarios_status(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Download job not found: {job_id}")
    return payload


@router.post("/iam-scenarios/clear")
def clear_iam_scenarios() -> dict:
    active_job = current_download_job()
    if active_job is not None:
        raise HTTPException(
            status_code=409,
            detail="Cannot clear local IAM scenario files while a download is running.",
        )

    try:
        return clear_local_iam_scenarios()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear IAM scenarios: {exc}",
        ) from exc
