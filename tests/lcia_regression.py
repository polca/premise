from pathlib import Path

import bw2calc
import bw2data
import pytest
import yaml


REFERENCE_SCORES = Path(__file__).parent / "data" / "lcia_regression_scores.yaml"


def _load_reference_scores():
    with open(REFERENCE_SCORES, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def get_lcia_regression_method(case_key):
    """Return the exact LCIA method tuple for an ecoinvent test case."""
    data = _load_reference_scores()

    try:
        method = tuple(data["cases"][case_key]["method"])
    except KeyError as exc:
        raise AssertionError(f"No LCIA regression method configured for {case_key}") from exc

    if method not in bw2data.methods:
        raise AssertionError(
            f"LCIA regression method {method!r} is not registered in project "
            f"{bw2data.projects.current!r}."
        )

    return method


def _find_activity(database, activity):
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
            f"Expected one activity match in {database.name!r} for "
            f"{activity['name']!r} / {activity['reference product']!r} / "
            f"{activity['location']!r} / {activity['unit']!r}; found {len(matches)}."
        )

    return matches[0]


def assert_lcia_regression_scores(case_key, database_names):
    """Check deterministic LCIA scores for generated Brightway databases."""
    data = _load_reference_scores()
    activities = data["activities"]
    tolerance = data.get("tolerance", {})
    rel = tolerance.get("relative", 1e-6)
    abs_ = tolerance.get("absolute", 1e-9)

    try:
        expected_scores = data["cases"][case_key]["scores"]
    except KeyError as exc:
        raise AssertionError(f"No LCIA regression scores configured for {case_key}") from exc

    method = get_lcia_regression_method(case_key)

    for database_name in database_names:
        if database_name not in bw2data.databases:
            raise AssertionError(f"Database {database_name!r} does not exist.")

        database = bw2data.Database(database_name)
        database_scores = expected_scores.get(database_name)
        if database_scores is None:
            raise AssertionError(
                f"No LCIA regression scores configured for {case_key}/{database_name}"
            )

        lca = None
        for activity_key, activity in activities.items():
            if activity_key not in database_scores:
                raise AssertionError(
                    f"No LCIA regression score configured for "
                    f"{case_key}/{database_name}/{activity_key}"
                )

            node = _find_activity(database, activity)
            if lca is None:
                lca = bw2calc.LCA({node.id: 1}, method)
                lca.lci()
                lca.lcia()
            else:
                lca.redo_lcia({node.id: 1})

            expected = database_scores[activity_key]
            assert lca.score == pytest.approx(expected, rel=rel, abs=abs_), (
                f"LCIA regression score changed for "
                f"{case_key}/{database_name}/{activity_key}: "
                f"expected {expected}, got {lca.score}, method={method!r}"
            )
