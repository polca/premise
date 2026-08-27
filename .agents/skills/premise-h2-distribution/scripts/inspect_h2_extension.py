#!/usr/bin/env python3
"""Inspect H2-distribution wiring without importing Premise."""

from __future__ import annotations

import argparse
import ast
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - environment-dependent message
    yaml = None


REQUIRED_PATHS = (
    "premise/fuels/hydrogen.py",
    "premise/fuels/base.py",
    "premise/fuels/config.py",
    "premise/fuels/h2_decision_tree/hydrogen_distribution_shares.yaml",
    "premise/fuels/h2_decision_tree/hydrogen_consumer_routing.yaml",
    "premise/data/utils/logging/reporting.yaml",
    "premise/data/additional_inventories/lci-hydrogen-distribution.xlsx",
    "premise/data/additional_inventories/lci-hydrogen-transport.xlsx",
    "tests/test_hydrogen.py",
    "tests/test_fuels.py",
)

MODE_LOG_COLUMNS = {
    "compressed_gaseous_truck": ("hydrogen distribution compressed gaseous truck"),
    "compressed_gaseous_pipeline": (
        "hydrogen distribution compressed gaseous pipeline"
    ),
    "liquid_hydrogen_truck": "hydrogen distribution liquid truck",
    "liquid_ammonia_ship": "hydrogen distribution liquid ammonia ship",
    "liquid_hydrogen_ship": "hydrogen distribution liquid hydrogen ship",
}


def run_git(repo: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def parse_python(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def literal_assignment(tree: ast.AST, name: str) -> Any:
    for node in getattr(tree, "body", []):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(
            isinstance(target, ast.Name) and target.id == name for target in targets
        ):
            continue
        try:
            return ast.literal_eval(node.value)
        except (TypeError, ValueError):
            return None
    return None


def load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required; run this script with the Premise environment."
        )
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return value


def test_count(path: Path) -> int:
    tree = parse_python(path)
    return sum(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
        for node in tree.body
    )


def add_issue(issues: list[str], message: str) -> None:
    if message not in issues:
        issues.append(message)


def validate_generic_intervals(rules: list[dict[str, Any]], errors: list[str]) -> None:
    by_basis: dict[str, list[tuple[float, float, str]]] = {}
    for rule in rules:
        if rule.get("match", {}) != {}:
            continue
        basis = rule.get("basis")
        condition = rule.get("condition", {})
        if not isinstance(basis, str) or not isinstance(condition, dict):
            continue
        lower = condition.get("min_demand", 0)
        upper = condition.get("max_demand", math.inf)
        if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
            continue
        by_basis.setdefault(basis, []).append(
            (float(lower), float(upper), str(rule.get("name", "<unnamed>")))
        )

    for basis, intervals in by_basis.items():
        cursor = 0.0
        for lower, upper, name in sorted(intervals):
            if lower != cursor:
                add_issue(
                    errors,
                    f"Generic rules for {basis!r} have a gap/overlap before "
                    f"{name!r}: expected lower bound {cursor:g}, got {lower:g}.",
                )
            if upper <= lower:
                add_issue(
                    errors,
                    f"Rule {name!r} has a non-positive demand interval.",
                )
            cursor = upper
        if not math.isinf(cursor):
            add_issue(
                errors,
                f"Generic rules for {basis!r} do not cover unbounded demand.",
            )


def inspect_repo(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    missing = [path for path in REQUIRED_PATHS if not (repo / path).is_file()]
    if missing:
        raise ValueError(
            "Not a complete premise_h2-distribution checkout; missing: "
            + ", ".join(missing)
        )

    hydrogen_tree = parse_python(repo / "premise/fuels/hydrogen.py")
    base_tree = parse_python(repo / "premise/fuels/base.py")
    init_tree = parse_python(repo / "premise/__init__.py")
    distribution = load_yaml(
        repo / "premise/fuels/h2_decision_tree/hydrogen_distribution_shares.yaml"
    )
    routing = load_yaml(
        repo / "premise/fuels/h2_decision_tree/hydrogen_consumer_routing.yaml"
    )
    reporting = load_yaml(repo / "premise/data/utils/logging/reporting.yaml")

    errors: list[str] = []
    warnings: list[str] = []
    rules = distribution.get("rules", [])
    if not isinstance(rules, list) or not rules:
        errors.append("Distribution YAML must contain a non-empty rules list.")
        rules = []

    transport = literal_assignment(hydrogen_tree, "HYDROGEN_TRANSPORT_ACTIVITIES") or {}
    conversion_map = (
        literal_assignment(hydrogen_tree, "HYDROGEN_TRANSPORT_CONVERSION_MAP") or {}
    )
    conversions = (
        literal_assignment(hydrogen_tree, "HYDROGEN_CONVERSION_ACTIVITIES") or {}
    )
    distances = (
        literal_assignment(hydrogen_tree, "HYDROGEN_TRANSPORT_DISTANCES_KM") or {}
    )
    log_columns = literal_assignment(base_tree, "HYDROGEN_LOG_COLUMNS") or []

    names: set[str] = set()
    configured_modes: set[str] = set()
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            add_issue(errors, f"Rule at index {index} is not a mapping.")
            continue
        name = rule.get("name")
        if not isinstance(name, str) or not name:
            add_issue(errors, f"Rule at index {index} has no non-empty name.")
        elif name in names:
            add_issue(errors, f"Duplicate distribution rule name: {name!r}.")
        else:
            names.add(name)
        if not isinstance(rule.get("basis"), str):
            add_issue(errors, f"Rule {name!r} has no string basis.")
        shares = rule.get("shares", {})
        if not isinstance(shares, dict):
            add_issue(errors, f"Rule {name!r} shares must be a mapping.")
            continue
        total = 0.0
        for mode, share in shares.items():
            configured_modes.add(str(mode))
            if mode not in transport:
                add_issue(
                    errors,
                    f"Rule {name!r} uses unknown transport mode {mode!r}.",
                )
            if not isinstance(share, (int, float)) or not 0 <= share <= 1:
                add_issue(
                    errors,
                    f"Rule {name!r} has invalid share {share!r} for {mode!r}.",
                )
                continue
            total += float(share)
        if total > 1 + 1e-12:
            add_issue(errors, f"Rule {name!r} shares sum to {total:g} (> 1).")
        elif total < 1 - 1e-12:
            add_issue(
                warnings,
                f"Rule {name!r} shares sum to {total:g}; verify the remainder "
                "is intentionally documented as onsite production.",
            )

    validate_generic_intervals(rules, errors)

    for mode in configured_modes:
        if mode != "compressed_gaseous_pipeline" and mode not in distances:
            add_issue(errors, f"Transport mode {mode!r} has no distance.")
        expected_log_column = MODE_LOG_COLUMNS.get(mode)
        if expected_log_column is None:
            add_issue(
                warnings,
                f"Transport mode {mode!r} has no audit-column mapping in the "
                "inspector.",
            )
        elif expected_log_column not in log_columns:
            add_issue(
                warnings,
                f"Transport mode {mode!r} is not represented in "
                "HYDROGEN_LOG_COLUMNS.",
            )

    for mode, chain in conversion_map.items():
        if mode not in transport:
            add_issue(errors, f"Conversion map uses unknown mode {mode!r}.")
        for conversion in chain:
            if conversion not in conversions:
                add_issue(
                    errors,
                    f"Mode {mode!r} uses unknown conversion {conversion!r}.",
                )

    sectors = routing.get("sectors", {})
    if not isinstance(sectors, dict) or not sectors:
        errors.append("Consumer-routing YAML has no non-empty sectors mapping.")
        sectors = {}
    markets: list[str] = []
    for sector, config in sectors.items():
        market = config.get("market") if isinstance(config, dict) else None
        if not isinstance(market, str) or not market:
            add_issue(errors, f"Routing sector {sector!r} has no market name.")
        else:
            markets.append(market)
    if len(markets) != len(set(markets)):
        errors.append("Consumer-routing sector market names are not unique.")

    report_columns = reporting.get("premise_fuel", {}).get("columns", {})
    if not isinstance(report_columns, dict):
        errors.append("reporting.yaml has no premise_fuel columns mapping.")
        report_columns = {}
    for column in log_columns:
        if column not in report_columns:
            add_issue(
                errors,
                f"Hydrogen log column {column!r} is absent from reporting.yaml.",
            )

    new_database_text = (repo / "premise/new_database.py").read_text(encoding="utf-8")
    for filename in (
        "lci-hydrogen-distribution.xlsx",
        "lci-hydrogen-transport.xlsx",
    ):
        if filename not in new_database_text:
            add_issue(errors, f"Inventory {filename!r} has no new_database hook.")

    version = literal_assignment(init_tree, "__version__")
    if isinstance(version, tuple):
        version = ".".join(str(item) for item in version)

    upstream_ref = None
    remote_refs = run_git(
        repo, "for-each-ref", "--format=%(refname:short)", "refs/remotes/upstream"
    )
    if remote_refs:
        candidates = set(remote_refs.splitlines())
        for candidate in ("upstream/master", "upstream/main"):
            if candidate in candidates:
                upstream_ref = candidate
                break

    dirty = run_git(repo, "status", "--porcelain")
    return {
        "repository": str(repo),
        "package_version": version,
        "git": {
            "branch": run_git(repo, "branch", "--show-current"),
            "commit": run_git(repo, "rev-parse", "HEAD"),
            "dirty_file_count": len(dirty.splitlines()) if dirty else 0,
            "origin": run_git(repo, "remote", "get-url", "origin"),
            "upstream": run_git(repo, "remote", "get-url", "upstream"),
            "local_upstream_ref": upstream_ref,
            "upstream_merge_base": (
                run_git(repo, "merge-base", "HEAD", upstream_ref)
                if upstream_ref
                else None
            ),
        },
        "distribution_rule_count": len(rules),
        "distribution_modes": sorted(configured_modes),
        "routing_sectors": sorted(map(str, sectors)),
        "hydrogen_test_count": test_count(repo / "tests/test_hydrogen.py"),
        "fuel_test_count": test_count(repo / "tests/test_fuels.py"),
        "errors": errors,
        "warnings": warnings,
    }


def as_markdown(data: dict[str, Any]) -> str:
    git = data["git"]
    lines = [
        "# H2-distribution extension inspection",
        "",
        f"- Repository: `{data['repository']}`",
        f"- Premise version: `{data['package_version']}`",
        f"- Git branch: `{git['branch']}`",
        f"- Git commit: `{git['commit']}`",
        f"- Dirty files: `{git['dirty_file_count']}`",
        f"- Origin: `{git['origin']}`",
        f"- Upstream: `{git['upstream']}`",
        f"- Local upstream ref: `{git['local_upstream_ref']}`",
        f"- Upstream merge base: `{git['upstream_merge_base']}`",
        f"- Distribution rules: `{data['distribution_rule_count']}`",
        "- Distribution modes: "
        + ", ".join(f"`{mode}`" for mode in data["distribution_modes"]),
        "- Routing sectors: "
        + ", ".join(f"`{sector}`" for sector in data["routing_sectors"]),
        f"- H2 tests: `{data['hydrogen_test_count']}`",
        f"- Fuels tests: `{data['fuel_test_count']}`",
        "",
        "## Contract errors",
        "",
    ]
    lines.extend(f"- {item}" for item in data["errors"])
    if not data["errors"]:
        lines.append("- None")
    lines.extend(["", "## Contract warnings", ""])
    lines.extend(f"- {item}" for item in data["warnings"])
    if not data["warnings"]:
        lines.append("- None")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "repository",
        nargs="?",
        default=".",
        help="Path to the premise_h2-distribution checkout (default: current dir)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    args = parser.parse_args()

    try:
        data = inspect_repo(Path(args.repository))
    except (OSError, RuntimeError, SyntaxError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(json.dumps(data, indent=2) if args.json else as_markdown(data))
    return 1 if data["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
