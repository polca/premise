"""Scenario Explorer helpers backed by Premise IAM summary extraction."""

from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

from premise_ui.core.credentials import iam_key_value
from premise_ui.core.scenario_catalog import SUPPORTED_SCENARIO_SUFFIXES, list_local_iam_scenarios

SUMMARY_REFERENCE_YEAR = 2030
SUPPORTED_COMPARE_MODES = {"overlay", "indexed", "delta", "percent_change"}


@lru_cache(maxsize=1)
def _scenario_summary_module():
    return import_module("premise.scenario_summary")


def get_scenario_explorer_catalog() -> dict[str, list[dict[str, Any]]]:
    """Return installed local IAM scenarios and supported explorer sectors."""
    scenario_summary = _scenario_summary_module()

    return {
        "scenarios": list_local_iam_scenarios(),
        "sectors": scenario_summary.get_sector_catalog(),
    }


def summarize_scenario_explorer_sector(
    scenario_paths: list[str],
    sector: str,
    *,
    group_names: list[str] | None = None,
    regions: list[str] | None = None,
    variables: list[str] | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
) -> dict[str, Any]:
    """Summarize one sector across one or more local IAM scenario files."""
    scenario_summary = _scenario_summary_module()
    scenarios = [_load_scenario_descriptor(path) for path in scenario_paths]
    summary = scenario_summary.summarize_sector(scenarios, sector)
    return scenario_summary.filter_sector_summary(
        summary,
        group_names=group_names,
        regions=regions,
        variables=variables,
        year_start=year_start,
        year_end=year_end,
    )


def compare_scenario_explorer_sector(
    scenario_paths: list[str],
    sector: str,
    *,
    compare_mode: str = "overlay",
    baseline_year: int | None = None,
    baseline_scenario_id: str | None = None,
    group_names: list[str] | None = None,
    regions: list[str] | None = None,
    variables: list[str] | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
) -> dict[str, Any]:
    """Return a comparison payload for the requested sector."""

    if compare_mode not in SUPPORTED_COMPARE_MODES:
        raise NotImplementedError(
            f"Scenario Explorer compare mode '{compare_mode}' is not implemented yet."
        )

    summary = summarize_scenario_explorer_sector(
        scenario_paths,
        sector,
        group_names=group_names,
        regions=regions,
        variables=variables,
        year_start=year_start,
        year_end=year_end,
    )

    if compare_mode == "overlay":
        return {
            "compare_mode": compare_mode,
            "baseline_year": baseline_year,
            "baseline_scenario_id": _normalize_baseline_scenario_id(
                summary, baseline_scenario_id
            ),
            "baseline_scenario_label": _baseline_scenario_label(
                summary, baseline_scenario_id
            ),
            "summary": summary,
        }

    if compare_mode == "indexed":
        indexed_summary, resolved_year = _indexed_summary(summary, baseline_year)
        return {
            "compare_mode": compare_mode,
            "baseline_year": resolved_year,
            "baseline_scenario_id": None,
            "baseline_scenario_label": None,
            "summary": indexed_summary,
        }

    if len(summary.get("scenarios", [])) < 2:
        raise ValueError(
            f"Scenario Explorer compare mode '{compare_mode}' requires at least two scenarios."
        )

    compared_summary, resolved_baseline_id = _scenario_delta_summary(
        summary,
        compare_mode=compare_mode,
        baseline_scenario_id=baseline_scenario_id,
    )
    return {
        "compare_mode": compare_mode,
        "baseline_year": baseline_year,
        "baseline_scenario_id": resolved_baseline_id,
        "baseline_scenario_label": _baseline_scenario_label(summary, resolved_baseline_id),
        "summary": compared_summary,
    }


def _load_scenario_descriptor(path: str) -> dict[str, Any]:
    resolved_path = _resolve_scenario_path(path)
    model, pathway = _parse_scenario_path(resolved_path)
    stat = resolved_path.stat()
    return {
        "scenario_id": f"{model}::{pathway}::{resolved_path.stem}",
        "model": model,
        "pathway": pathway,
        "path": str(resolved_path),
        "iam data": _load_iam_data_cached(
            str(resolved_path),
            stat.st_mtime_ns,
            stat.st_size,
        ),
    }


def _resolve_scenario_path(path: str) -> Path:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"IAM scenario file not found: {resolved_path}")
    if resolved_path.suffix.lower() not in SUPPORTED_SCENARIO_SUFFIXES:
        raise ValueError(
            "Unsupported IAM file type. Expected csv, mif, xls, or xlsx."
        )
    return resolved_path


def _parse_scenario_path(path: Path) -> tuple[str, str]:
    if "_" not in path.stem:
        raise ValueError(
            f"Could not infer IAM model and pathway from file name: {path.name}"
        )
    model, pathway = path.stem.split("_", 1)
    if not model or not pathway:
        raise ValueError(
            f"Could not infer IAM model and pathway from file name: {path.name}"
        )
    return model, pathway


def _reference_point(points: list[dict[str, Any]], baseline_year: int | None) -> dict[str, Any] | None:
    if not points:
        return None

    ordered = sorted(points, key=lambda point: point["year"])
    if baseline_year is None:
        return ordered[0]

    return min(
        ordered,
        key=lambda point: (abs(int(point["year"]) - baseline_year), int(point["year"])),
    )


def _indexed_summary(
    summary: dict[str, Any],
    baseline_year: int | None,
) -> tuple[dict[str, Any], int | None]:
    indexed = deepcopy(summary)
    resolved_years: set[int] = set()

    for scenario in indexed.get("scenarios", []):
        scenario["comparison_label"] = f"{scenario['model'].upper()} / {scenario['pathway']}"
        for group in scenario.get("groups", []):
            transformed_series = []
            for series in group.get("series", []):
                reference = _reference_point(series.get("points", []), baseline_year)
                if reference is None or reference["value"] == 0:
                    continue
                resolved_years.add(int(reference["year"]))
                transformed_points = [
                    {
                        "year": point["year"],
                        "value": (point["value"] / reference["value"]) * 100,
                    }
                    for point in series.get("points", [])
                ]
                transformed = deepcopy(series)
                transformed["unit"] = "Index (baseline = 100)"
                transformed["points"] = transformed_points
                transformed_series.append(transformed)
            group["series"] = transformed_series

    indexed["label"] = "Index (baseline = 100)"
    indexed["explanation"] = (
        f"{summary.get('explanation', '')} Indexed to 100 at the selected baseline year."
    ).strip()
    _recompute_summary(indexed)
    return indexed, min(resolved_years) if resolved_years else baseline_year


def _scenario_delta_summary(
    summary: dict[str, Any],
    *,
    compare_mode: str,
    baseline_scenario_id: str | None,
) -> tuple[dict[str, Any], str]:
    baseline = _resolve_baseline_scenario(summary, baseline_scenario_id)
    baseline_series = _group_series_map(baseline)

    compared = deepcopy(summary)
    compared["scenarios"] = []

    for scenario in summary.get("scenarios", []):
        if scenario["scenario_id"] == baseline["scenario_id"]:
            continue

        transformed_scenario = deepcopy(scenario)
        transformed_scenario["comparison_label"] = (
            f"{scenario['model'].upper()} / {scenario['pathway']} vs "
            f"{baseline['model'].upper()} / {baseline['pathway']}"
        )
        transformed_groups = []

        for group in scenario.get("groups", []):
            baseline_group_series = baseline_series.get(group["name"], {})
            transformed_series = []
            for series in group.get("series", []):
                baseline_points = baseline_group_series.get(series["variable"])
                if not baseline_points:
                    continue
                points = _compare_points(
                    series.get("points", []),
                    baseline_points,
                    compare_mode=compare_mode,
                )
                if not points:
                    continue
                transformed = deepcopy(series)
                transformed["points"] = points
                transformed["unit"] = (
                    "%"
                    if compare_mode == "percent_change"
                    else summary.get("label")
                )
                transformed_series.append(transformed)

            if transformed_series:
                next_group = deepcopy(group)
                next_group["series"] = transformed_series
                transformed_groups.append(next_group)

        transformed_scenario["groups"] = transformed_groups
        compared["scenarios"].append(transformed_scenario)

    compared["label"] = (
        "%"
        if compare_mode == "percent_change"
        else summary.get("label")
    )
    compared["explanation"] = (
        f"{summary.get('explanation', '')} "
        f"Displayed as {compare_mode.replace('_', ' ')} against "
        f"{baseline['model'].upper()} / {baseline['pathway']}."
    ).strip()
    _recompute_summary(compared)
    return compared, baseline["scenario_id"]


def _compare_points(
    scenario_points: list[dict[str, Any]],
    baseline_points: list[dict[str, Any]],
    *,
    compare_mode: str,
) -> list[dict[str, Any]]:
    baseline_map = {point["year"]: point["value"] for point in baseline_points}
    points = []
    for point in scenario_points:
        year = point["year"]
        if year not in baseline_map:
            continue
        baseline_value = baseline_map[year]
        if compare_mode == "delta":
            value = point["value"] - baseline_value
        elif compare_mode == "percent_change":
            if baseline_value == 0:
                continue
            value = ((point["value"] - baseline_value) / baseline_value) * 100
        else:
            raise NotImplementedError(
                f"Scenario Explorer compare mode '{compare_mode}' is not implemented yet."
            )
        points.append({"year": year, "value": value})
    return points


def _group_series_map(scenario: dict[str, Any]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    groups: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for group in scenario.get("groups", []):
        groups[group["name"]] = {
            series["variable"]: deepcopy(series.get("points", []))
            for series in group.get("series", [])
        }
    return groups


def _normalize_baseline_scenario_id(
    summary: dict[str, Any], baseline_scenario_id: str | None
) -> str | None:
    if not summary.get("scenarios"):
        return None
    if baseline_scenario_id:
        if any(
            scenario["scenario_id"] == baseline_scenario_id
            for scenario in summary["scenarios"]
        ):
            return baseline_scenario_id
        raise ValueError(f"Baseline scenario not found: {baseline_scenario_id}")
    return summary["scenarios"][0]["scenario_id"]


def _resolve_baseline_scenario(
    summary: dict[str, Any], baseline_scenario_id: str | None
) -> dict[str, Any]:
    resolved_id = _normalize_baseline_scenario_id(summary, baseline_scenario_id)
    if resolved_id is None:
        raise ValueError("No scenarios available for Scenario Explorer comparison.")

    for scenario in summary.get("scenarios", []):
        if scenario["scenario_id"] == resolved_id:
            return scenario

    raise ValueError(f"Baseline scenario not found: {baseline_scenario_id}")


def _baseline_scenario_label(
    summary: dict[str, Any], baseline_scenario_id: str | None
) -> str | None:
    resolved_id = _normalize_baseline_scenario_id(summary, baseline_scenario_id)
    if resolved_id is None:
        return None
    for scenario in summary.get("scenarios", []):
        if scenario["scenario_id"] == resolved_id:
            return f"{scenario['model'].upper()} / {scenario['pathway']}"
    return None


def _recompute_summary(summary: dict[str, Any]) -> None:
    summary["scenarios"] = [
        scenario
        for scenario in summary.get("scenarios", [])
        if _recompute_scenario_fields(scenario)
    ]
    summary["regions"] = sorted(
        {
            group["name"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "region"
        }
    )
    summary["subscenarios"] = sorted(
        {
            group["name"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "subscenario"
        }
    )
    summary["variables"] = sorted(
        {
            series["variable"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
        }
    )
    summary["years"] = sorted(
        {
            point["year"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
            for point in series["points"]
        }
    )


def _recompute_scenario_fields(scenario: dict[str, Any]) -> bool:
    scenario["groups"] = [
        group for group in scenario.get("groups", []) if _recompute_group_fields(group)
    ]
    if not scenario["groups"]:
        return False

    scenario["regions"] = sorted(
        {
            group["name"]
            for group in scenario["groups"]
            if group["group_type"] == "region"
        }
    )
    scenario["subscenarios"] = sorted(
        {
            group["name"]
            for group in scenario["groups"]
            if group["group_type"] == "subscenario"
        }
    )
    scenario["variables"] = sorted(
        {
            series["variable"]
            for group in scenario["groups"]
            for series in group["series"]
        }
    )
    scenario["years"] = sorted(
        {
            point["year"]
            for group in scenario["groups"]
            for series in group["series"]
            for point in series["points"]
        }
    )
    return True


def _recompute_group_fields(group: dict[str, Any]) -> bool:
    group["series"] = [series for series in group.get("series", []) if series.get("points")]
    if not group["series"]:
        return False

    group["variables"] = sorted({series["variable"] for series in group["series"]})
    group["years"] = sorted(
        {
            point["year"]
            for series in group["series"]
            for point in series["points"]
        }
    )
    return True


@lru_cache(maxsize=16)
def _load_iam_data_cached(path: str, mtime_ns: int, size: int):
    del mtime_ns
    del size

    from premise.data_collection import IAMDataCollection

    resolved_path = Path(path)
    model, pathway = _parse_scenario_path(resolved_path)
    return IAMDataCollection(
        model=model,
        pathway=pathway,
        year=SUMMARY_REFERENCE_YEAR,
        filepath_iam_files=resolved_path.parent,
        key=iam_key_value(),
        system_model="cutoff",
    )
