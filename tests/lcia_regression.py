from pathlib import Path

import bw2calc
import bw2data
import pytest
import yaml

REFERENCE_SCORES = Path(__file__).parent / "data" / "lcia_regression_scores.yaml"


def _load_reference_scores():
    with open(REFERENCE_SCORES, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _ecoinvent_method_prefix(case_key):
    parts = case_key.split("-")
    if len(parts) >= 2 and parts[0] == "ecoinvent":
        return "-".join(parts[:2])
    return None


def _format_methods(methods):
    return ", ".join(repr(method) for method in sorted(methods))


def _candidate_prefix(candidate, method):
    if not method:
        return candidate
    return candidate[: -len(method)]


def _matches_case_prefix(candidate, method, case_prefix):
    for prefix_part in _candidate_prefix(candidate, method):
        if prefix_part == case_prefix:
            return True
        if isinstance(prefix_part, str) and (
            prefix_part.startswith(f"{case_prefix}-")
            or prefix_part.startswith(f"{case_prefix} ")
        ):
            return True
    return False


def _has_ecoinvent_prefix(candidate, method):
    return any(
        isinstance(prefix_part, str) and prefix_part.startswith("ecoinvent-")
        for prefix_part in _candidate_prefix(candidate, method)
    )


def _resolve_lcia_method(method, case_key, available_methods):
    """Resolve an LCIA method tuple against registered Brightway methods.

    ecoinvent method names can be registered either as the raw method tuple or
    with an ecoinvent-version prefix, depending on the importer/version.
    """
    available_methods = [tuple(candidate) for candidate in available_methods]

    if method in available_methods:
        return method

    case_prefix = _ecoinvent_method_prefix(case_key)
    if case_prefix and method[:1] == (case_prefix,):
        unprefixed_method = method[1:]
        if unprefixed_method in available_methods:
            return unprefixed_method

    matching_suffix = [
        candidate
        for candidate in available_methods
        if len(candidate) > len(method) and candidate[-len(method) :] == method
    ]

    if case_prefix:
        prefixed_method = (case_prefix,) + method
        if prefixed_method in available_methods:
            return prefixed_method

        case_matches = [
            candidate
            for candidate in matching_suffix
            if _matches_case_prefix(candidate, method, case_prefix)
        ]
        if len(case_matches) == 1:
            return case_matches[0]
        if len(case_matches) > 1:
            raise AssertionError(
                f"Multiple LCIA regression methods match {method!r} for "
                f"{case_key}: {_format_methods(case_matches)}."
            )

        other_ecoinvent_matches = [
            candidate
            for candidate in matching_suffix
            if _has_ecoinvent_prefix(candidate, method)
        ]
        if other_ecoinvent_matches:
            raise AssertionError(
                f"LCIA regression method {method!r} is not registered for "
                f"{case_prefix!r}; found only methods for another ecoinvent "
                f"prefix: {_format_methods(other_ecoinvent_matches)}."
            )

    if len(matching_suffix) == 1:
        return matching_suffix[0]
    if len(matching_suffix) > 1:
        raise AssertionError(
            f"Multiple LCIA regression methods match {method!r} for "
            f"{case_key}: {_format_methods(matching_suffix)}."
        )

    raise AssertionError(
        f"LCIA regression method {method!r} is not registered in project "
        f"{bw2data.projects.current!r}."
    )


def get_lcia_regression_method(case_key):
    """Return the registered LCIA method tuple for an ecoinvent test case."""
    data = _load_reference_scores()

    try:
        method = tuple(data["cases"][case_key]["method"])
    except KeyError as exc:
        raise AssertionError(
            f"No LCIA regression method configured for {case_key}"
        ) from exc

    return _resolve_lcia_method(method, case_key, bw2data.methods)


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
        raise AssertionError(
            f"No LCIA regression scores configured for {case_key}"
        ) from exc

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
