"""Scenario Explorer routes for the local Premise UI service."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module

from fastapi import APIRouter, HTTPException

from premise_ui.api.models import (
    ScenarioExplorerCompareRequest,
    ScenarioExplorerSummaryRequest,
)

router = APIRouter(prefix="/api/scenario-explorer", tags=["scenario-explorer"])


@lru_cache(maxsize=1)
def _scenario_explorer_core():
    try:
        return import_module("premise_ui.core.scenario_explorer")
    except Exception as exc:
        raise RuntimeError(
            f"Scenario Explorer backend is unavailable in this environment: {exc}"
        ) from exc


def get_scenario_explorer_catalog() -> dict:
    return _scenario_explorer_core().get_scenario_explorer_catalog()


def summarize_scenario_explorer_sector(*args, **kwargs) -> dict:
    return _scenario_explorer_core().summarize_scenario_explorer_sector(*args, **kwargs)


def compare_scenario_explorer_sector(*args, **kwargs) -> dict:
    return _scenario_explorer_core().compare_scenario_explorer_sector(*args, **kwargs)


@router.get("/catalog")
def scenario_explorer_catalog() -> dict:
    try:
        return get_scenario_explorer_catalog()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build Scenario Explorer catalog: {exc}",
        ) from exc


@router.post("/sector-summary")
def scenario_explorer_sector_summary(request: ScenarioExplorerSummaryRequest) -> dict:
    try:
        return summarize_scenario_explorer_sector(
            request.scenario_paths,
            request.sector,
            group_names=request.group_names,
            regions=request.regions,
            variables=request.variables,
            year_start=request.year_start,
            year_end=request.year_end,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to summarize IAM sector data: {exc}",
        ) from exc


@router.post("/compare")
def scenario_explorer_compare(request: ScenarioExplorerCompareRequest) -> dict:
    try:
        return compare_scenario_explorer_sector(
            request.scenario_paths,
            request.sector,
            compare_mode=request.compare_mode,
            baseline_year=request.baseline_year,
            baseline_scenario_id=request.baseline_scenario_id,
            group_names=request.group_names,
            regions=request.regions,
            variables=request.variables,
            year_start=request.year_start,
            year_end=request.year_end,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare IAM sector data: {exc}",
        ) from exc
