"""LCIA regression helpers for Brightway integration tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import bw2calc
import bw2data
import pytest
import yaml


REFERENCE_SCORES = Path(__file__).parent / "data" / "lcia_regression_scores.yaml"


def load_lcia_regression_data() -> dict[str, Any]:
    """Load deterministic LCIA reference scores for integration tests."""

    with open(REFERENCE_SCORES, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def get_lcia_regression_method(case_key: str) -> tuple[str, ...]:
    """Return the LCIA method tuple configured for a regression test case."""

    data = load_lcia_regression_data()
    try:
        return tuple(data["cases"][case_key]["method"])
    except KeyError as exc:
        available = ", ".join(sorted(data.get("cases", {})))
        raise KeyError(
            f"No LCIA regression case {case_key!r}. Available cases: {available}"
        ) from exc


def find_activity(database: bw2data.Database, activity: dict[str, str]):
    """Find exactly one activity matching the reference activity metadata."""

    matches = [
        node
        for node in database
        if node.get("name") == activity["name"]
        and node.get("reference product") == activity["reference product"]
        and node.get("location") == activity["location"]
        and node.get("unit") == activity["unit"]
    ]

    if len(matches) != 1:
        raise AssertionError(
            f"Expected one match in {database.name} for {activity}; "
            f"found {len(matches)}"
        )

    return matches[0]


def demand_keys(node) -> list[Any]:
    """Return demand-key candidates accepted by different Brightway versions."""

    candidates = []
    if getattr(node, "id", None) is not None:
        candidates.append(node.id)
    if getattr(node, "key", None) is not None:
        candidates.append(node.key)
    candidates.append(node)
    return candidates


def score_activity(node, method: tuple[str, ...]) -> float:
    """Run LCIA for one unit of an activity, across Brightway demand APIs."""

    exceptions = []

    for key in demand_keys(node):
        try:
            lca = bw2calc.LCA({key: 1}, method)
            lca.lci()
            lca.lcia()
            return float(lca.score)
        except (KeyError, TypeError, ValueError) as exc:
            exceptions.append(exc)

    raise AssertionError(
        f"Could not score activity {node!r} with method {method!r}: "
        + "; ".join(str(exc) for exc in exceptions)
    )


def score_database(
    database_name: str, method: tuple[str, ...], activities: dict[str, dict[str, str]]
) -> dict[str, float]:
    """Score all configured regression activities in a Brightway database."""

    database = bw2data.Database(database_name)

    return {
        activity_key: score_activity(find_activity(database, activity), method)
        for activity_key, activity in activities.items()
    }


def assert_lcia_regression_scores(
    case_key: str, database_names: list[str] | tuple[str, ...]
) -> None:
    """Assert deterministic LCIA scores for generated Brightway databases."""

    data = load_lcia_regression_data()
    tolerance = data.get("tolerance", {})
    relative_tolerance = float(tolerance.get("relative", 1e-3))
    absolute_tolerance = float(tolerance.get("absolute", 1e-9))

    try:
        case = data["cases"][case_key]
    except KeyError as exc:
        available = ", ".join(sorted(data.get("cases", {})))
        raise KeyError(
            f"No LCIA regression case {case_key!r}. Available cases: {available}"
        ) from exc

    method = tuple(case["method"])
    expected_by_database = case["scores"]
    failures = []

    for database_name in database_names:
        if database_name not in expected_by_database:
            failures.append(f"{case_key}/{database_name}: no reference scores")
            continue

        actual_scores = score_database(database_name, method, data["activities"])
        expected_scores = expected_by_database[database_name]

        for activity_key, expected_score in expected_scores.items():
            actual_score = actual_scores[activity_key]
            if actual_score != pytest.approx(
                expected_score,
                rel=relative_tolerance,
                abs=absolute_tolerance,
            ):
                failures.append(
                    f"{case_key}/{database_name}/{activity_key}: "
                    f"expected {expected_score:.12g}, got {actual_score:.12g}"
                )

    if failures:
        raise AssertionError("LCIA regression score mismatch:\n" + "\n".join(failures))
