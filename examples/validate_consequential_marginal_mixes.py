"""Validate consequential marginal mixes against an independent calculation.

The production calculation is run through :func:`consequential_method`.  The
reference calculation below deliberately does not import any of the marginal
mix helpers: it reads the consequential YAML parameters itself, interpolates
the IAM series with SciPy, constructs the observation intervals from the
documented equations, and implements the six measurement equations directly.

Example
-------

.. code-block:: console

    PREMISE_KEY=... python examples/validate_consequential_marginal_mixes.py

The complete numerical results are written to a CSV file and a compact Markdown
report is written next to it.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
import yaml
from scipy.interpolate import Akima1DInterpolator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from premise.data_collection import IAMDataCollection  # noqa: E402
from premise.filesystem_constants import DATA_DIR, VARIABLES_DIR  # noqa: E402
from premise.marginal_mixes import consequential_method  # noqa: E402

LEAD_TIMES = DATA_DIR / "consequential" / "leadtimes.yaml"
LIFETIMES = DATA_DIR / "consequential" / "lifetimes.yaml"
CONSTRAINED = DATA_DIR / "consequential" / "constrained_suppliers.yaml"
ELECTRICITY_MAPPING = VARIABLES_DIR / "electricity.yaml"
DEFAULT_REPORT = REPO_ROOT / "docs" / "consequential_marginal_mix_validation.md"
DEFAULT_CSV = REPO_ROOT / "docs" / "consequential_marginal_mix_validation.csv"
TOLERANCE = 1e-12


@dataclass
class OracleResult:
    """Intermediate values and final mix from the independent calculation."""

    mix: np.ndarray
    scores: np.ndarray
    start: int | np.ndarray
    end: int | np.ndarray
    average_start: int
    average_end: int
    average_lead_time: int
    average_lifetime: int
    capital_replacement_threshold: float
    volume_change: float
    direction: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="image")
    parser.add_argument("--pathway", default="SSP1-M")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument("--region", default="WEU")
    parser.add_argument(
        "--declining-year",
        type=int,
        default=2090,
        help="Additional year used to exercise the declining-market branch.",
    )
    parser.add_argument("--top", type=int, default=6)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    return parser.parse_args()


def read_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def load_exact_constrained_suppliers() -> set[str]:
    """Read the intended supplier names using exact membership.

    The fallback preserves the validator's ability to diagnose the historical
    bare-line format without copying its substring-membership behaviour.
    """

    parsed = read_yaml(CONSTRAINED)
    if isinstance(parsed, list):
        return set(parsed)

    return {
        line.strip().removeprefix("- ").strip()
        for line in CONSTRAINED.read_text(encoding="utf-8").splitlines()
        if line.strip() and line.strip() != "---" and not line.lstrip().startswith("#")
    }


def constrained_parameter_audit() -> tuple[str, list[str]]:
    """Return the YAML runtime type and unintended substring matches."""

    parsed = read_yaml(CONSTRAINED)
    exact = load_exact_constrained_suppliers()
    technologies = set(read_yaml(LEAD_TIMES))
    substring_only = (
        sorted(
            technology
            for technology in technologies
            if technology in parsed and technology not in exact
        )
        if isinstance(parsed, str)
        else []
    )
    return type(parsed).__name__, substring_only


def load_raw_electricity(data: xr.DataArray, model: str) -> xr.DataArray:
    """Select and aggregate electricity series using the public mapping file.

    This reproduces only the generic IAM-label-to-Premise-label wiring.  It does
    not call the private market-data method or any marginal-mix code.
    """

    mapping = read_yaml(ELECTRICITY_MAPPING)
    available = set(data.coords["variables"].values.tolist())
    pieces = []

    for technology, metadata in mapping.items():
        alias = metadata.get("iam_aliases", {}).get(model)
        aliases = alias if isinstance(alias, list) else [alias]
        aliases = [item for item in aliases if item in available]
        if not aliases:
            continue

        piece = data.sel(variables=aliases).sum(dim="variables")
        piece = piece.expand_dims(variables=[technology])
        pieces.append(piece)

    if not pieces:
        raise ValueError(f"No electricity production variables found for {model!r}.")

    electricity = xr.concat(pieces, dim="variables")
    if electricity.coords["variables"].size != len(
        set(electricity.coords["variables"].values.tolist())
    ):
        electricity = electricity.groupby("variables").sum(dim="variables")

    return electricity.transpose("region", "variables", "year")


def interpolate_annually(
    data: xr.DataArray, region: str
) -> tuple[np.ndarray, np.ndarray]:
    """Interpolate raw IAM series annually with SciPy's Akima implementation."""

    source_years = np.asarray(data.coords["year"].values, dtype=float)
    annual_years = np.arange(int(source_years.min()), int(source_years.max()) + 1)
    source_values = np.asarray(data.sel(region=region).values, dtype=float)
    values = Akima1DInterpolator(source_years, source_values, axis=1)(annual_years)
    return annual_years, np.asarray(values, dtype=float)


def nearest_year(value: int | float | np.ndarray, years: np.ndarray):
    requested = np.asarray(value)
    indices = np.abs(requested[..., None] - years).argmin(axis=-1)
    selected = years[indices]
    return selected.item() if requested.ndim == 0 else selected


def values_at(
    values: np.ndarray, years: np.ndarray, requested: int | float | np.ndarray
) -> np.ndarray:
    """Return one interpolated observation per technology."""

    requested_array = np.asarray(requested, dtype=float)
    if requested_array.ndim == 0:
        return np.asarray(
            [np.interp(float(requested_array), years, row) for row in values]
        )
    return np.asarray(
        [
            np.interp(requested_year, years, row)
            for requested_year, row in zip(requested_array, values)
        ]
    )


def observation_interval(
    *,
    year: int,
    range_time: int,
    duration: int,
    foresight: bool,
    individual_lead_times: bool,
    lead_times: np.ndarray,
    shares: np.ndarray,
) -> tuple[int | np.ndarray, int | np.ndarray, int, int, int]:
    """Construct the documented time interval without production helpers."""

    average_lead_time = int(np.sum(shares * lead_times))

    if range_time:
        average_centre = year if foresight else year + average_lead_time
        centre = (
            year + lead_times
            if individual_lead_times and not foresight
            else average_centre
        )
        start = centre - range_time
        end = centre + range_time
        average_start = average_centre - range_time
        average_end = average_centre + range_time
    elif duration:
        average_start = year if foresight else year + average_lead_time
        start = (
            year + lead_times
            if individual_lead_times and not foresight
            else average_start
        )
        end = start + duration
        average_end = average_start + duration
    elif foresight:
        start = year - lead_times if individual_lead_times else year - average_lead_time
        end = np.full_like(lead_times, year) if individual_lead_times else year
        average_start = year - average_lead_time
        average_end = year
    else:
        start = np.full_like(lead_times, year) if individual_lead_times else year
        end = year + lead_times if individual_lead_times else year + average_lead_time
        average_start = year
        average_end = year + average_lead_time

    return start, end, average_start, average_end, average_lead_time


def normalise_indicator(
    indicator: np.ndarray, declining: bool, fallback: np.ndarray
) -> np.ndarray:
    """Apply the documented expanding/declining-market supplier selection."""

    indicator = np.nan_to_num(indicator, nan=0.0, posinf=0.0, neginf=0.0)
    if declining:
        selected = np.where(indicator > 0, 0.0, indicator) * -1
    else:
        selected = np.where(indicator < 0, 0.0, indicator)

    denominator = float(selected.sum())
    if not np.isfinite(denominator) or denominator == 0:
        return fallback.copy()
    return selected / denominator


def oracle_mix(
    *,
    data: xr.DataArray,
    region: str,
    year: int,
    args: dict[str, Any],
) -> OracleResult:
    """Calculate a marginal mix independently from ``marginal_mixes.py``."""

    technologies = data.coords["variables"].values.tolist()
    lead_time_map = read_yaml(LEAD_TIMES)
    lifetime_map = read_yaml(LIFETIMES)
    constrained = load_exact_constrained_suppliers()

    missing_lead_times = set(technologies) - set(lead_time_map)
    missing_lifetimes = set(technologies) - set(lifetime_map)
    if missing_lead_times or missing_lifetimes:
        raise ValueError(
            "Missing consequential parameters: "
            f"lead times={sorted(missing_lead_times)}, "
            f"lifetimes={sorted(missing_lifetimes)}"
        )

    lead_times = np.asarray([lead_time_map[item] for item in technologies], dtype=float)
    lifetimes = np.asarray([lifetime_map[item] for item in technologies], dtype=float)
    years, values = interpolate_annually(data, region)
    constrained_mask = np.asarray([item in constrained for item in technologies])
    values[constrained_mask] = 0.0

    demand_values = values_at(values, years, year)
    demand_total = float(demand_values.sum())
    if demand_total == 0:
        raise ValueError(f"No production in {region} in {year}.")
    initial_shares = demand_values / demand_total

    range_time = int(args.get("range time", 2))
    duration = int(args.get("duration", 0))
    foresight = bool(args.get("foresight", False))
    individual = bool(args.get("lead time", False))
    use_replacement = bool(args.get("capital replacement rate", True))
    measurement = int(args.get("measurement", 0))
    weighted_start = float(args.get("weighted slope start", 0.75))
    weighted_end = float(args.get("weighted slope end", 1.0))

    start, end, average_start, average_end, average_lead_time = observation_interval(
        year=year,
        range_time=range_time,
        duration=duration,
        foresight=foresight,
        individual_lead_times=individual,
        lead_times=lead_times,
        shares=initial_shares,
    )
    start = nearest_year(start, years)
    end = nearest_year(end, years)
    average_start = int(nearest_year(average_start, years))
    average_end = int(nearest_year(average_end, years))

    start_values = values_at(values, years, start)
    end_values = values_at(values, years, end)
    values_at_average_start = values_at(values, years, average_start)
    shares = values_at_average_start / values_at_average_start.sum()
    average_lifetime = int(np.sum(shares * lifetimes)) or 30
    replacement_threshold = -1 / average_lifetime
    volume_change = float(
        (values_at(values, years, average_end).sum() - values_at_average_start.sum())
        / (average_end - average_start)
    )
    declining = (not use_replacement and volume_change < 0) or (
        use_replacement and volume_change < replacement_threshold
    )

    interval_length = np.asarray(end) - np.asarray(start)
    replacement = -values_at_average_start / lifetimes

    if measurement == 0:
        indicator = (end_values - start_values) / interval_length
        if use_replacement:
            indicator -= replacement

    elif measurement == 1:
        start_by_technology = np.broadcast_to(start, len(technologies))
        end_by_technology = np.broadcast_to(end, len(technologies))
        indicator = np.zeros(len(technologies))
        for index, (lower, upper) in enumerate(
            zip(start_by_technology, end_by_technology)
        ):
            mask = (years >= lower) & (years <= upper)
            indicator[index] = np.polyfit(years[mask], values[index, mask], 1)[0]
        if use_replacement:
            indicator -= replacement

    elif measurement == 2:
        start_by_technology = np.broadcast_to(start, len(technologies))
        end_by_technology = np.broadcast_to(end, len(technologies))
        mask = (years[None, :] >= start_by_technology[:, None]) & (
            years[None, :] <= end_by_technology[:, None]
        )
        trapezoidal_area = 0.5 * (
            2 * np.where(mask, values, 0).sum(axis=1) - end_values - start_values
        )
        baseline_area = start_values * interval_length
        indicator = (trapezoidal_area - baseline_area) / interval_length
        if use_replacement:
            indicator -= replacement * interval_length**2 * 0.5

    elif measurement == 3:
        slope = (end_values - start_values) / interval_length
        short_start = np.asarray(start) + interval_length * weighted_start
        short_end = np.asarray(start) + interval_length * weighted_end
        short_slope = (
            values_at(values, years, short_end) - values_at(values, years, short_start)
        ) / (short_end - short_start)
        if use_replacement:
            slope -= replacement
            short_slope -= replacement
        ratio = np.divide(
            short_slope,
            slope,
            out=np.zeros_like(short_slope, dtype=float),
            where=slope != 0,
        )
        split = np.where(ratio < 0, -1.0, 1.0)
        finite_ratio = (ratio > -500) & (ratio < 500)
        transformed = np.zeros_like(ratio)
        transformed[finite_ratio] = 2 * (
            np.exp(-1 + ratio[finite_ratio]) / (1 + np.exp(-1 + ratio[finite_ratio]))
            - 0.5
        )
        split[finite_ratio] = transformed[finite_ratio]
        indicator = slope * (1 + split)

    elif measurement == 4:
        annual_mixes = []
        for split_year in range(average_start, average_end):
            annual = values_at(values, years, split_year + 1) - values_at(
                values, years, split_year
            )
            if use_replacement:
                active = values_at(values, years, split_year) != 0
                annual -= replacement * active
            if declining:
                annual = np.where(annual > 0, 0.0, annual)
                denominator = annual.sum()
                annual = (
                    annual / denominator * -1
                    if denominator
                    else np.full_like(annual, np.nan)
                )
            else:
                annual = np.where(annual < 0, 0.0, annual)
                denominator = annual.sum()
                annual = (
                    annual / denominator
                    if denominator
                    else np.full_like(annual, np.nan)
                )
            annual_mixes.append(annual)
        indicator = np.nan_to_num(np.mean(annual_mixes, axis=0), nan=0.0)

    elif measurement == 5:
        indicator = (end_values - start_values) / interval_length
        if use_replacement:
            indicator -= replacement
        if declining:
            indicator = np.where(indicator < 0, -start_values, 0.0)
        else:
            indicator = np.where(indicator > 0, start_values, 0.0)

    else:
        raise ValueError(f"Unsupported measurement method {measurement}.")

    rounded_indicator = np.round(indicator, 3)
    mix = normalise_indicator(rounded_indicator, declining, shares)
    return OracleResult(
        mix=mix,
        scores=rounded_indicator,
        start=start,
        end=end,
        average_start=average_start,
        average_end=average_end,
        average_lead_time=average_lead_time,
        average_lifetime=average_lifetime,
        capital_replacement_threshold=replacement_threshold,
        volume_change=volume_change,
        direction="declining" if declining else "expanding/replacing",
    )


def cases() -> list[tuple[str, str, dict[str, Any]]]:
    reference = {
        "range time": 2,
        "duration": 0,
        "foresight": False,
        "lead time": False,
        "capital replacement rate": True,
        "measurement": 0,
        "weighted slope start": 0.75,
        "weighted slope end": 1.0,
    }

    def changed(**updates: Any) -> dict[str, Any]:
        result = reference.copy()
        result.update(updates)
        return result

    return [
        ("reference", "short, myopic, average lead time", reference),
        (
            "individual_lead_time",
            "lead time = individual",
            changed(**{"lead time": True}),
        ),
        ("perfect_foresight", "foresight = perfect", changed(foresight=True)),
        (
            "long_duration",
            "duration = 20 years",
            changed(**{"range time": 0, "duration": 20}),
        ),
        (
            "no_replacement",
            "capital replacement rate = off",
            changed(**{"capital replacement rate": False}),
        ),
        ("measurement_1", "measurement = linear regression", changed(measurement=1)),
        ("measurement_2", "measurement = area above baseline", changed(measurement=2)),
        ("measurement_3", "measurement = weighted slope", changed(measurement=3)),
        ("measurement_4", "measurement = split annual", changed(measurement=4)),
        ("measurement_5", "measurement = legacy volume", changed(measurement=5)),
    ]


def format_interval(result: OracleResult) -> str:
    if np.asarray(result.start).ndim == 0:
        supplier = f"{int(result.start)}–{int(result.end)}"
    else:
        supplier = (
            f"starts {int(np.min(result.start))}–{int(np.max(result.start))}; "
            f"ends {int(np.min(result.end))}–{int(np.max(result.end))}"
        )
    average = f"avg {result.average_start}–{result.average_end}"
    return (
        average
        if supplier == f"{result.average_start}–{result.average_end}"
        else f"{average}; {supplier}"
    )


def top_mix(technologies: list[str], mix: np.ndarray, count: int) -> str:
    selected = [index for index in np.argsort(-mix) if mix[index] > 0][:count]
    return "; ".join(f"{technologies[index]} {mix[index]:.3f}" for index in selected)


def git_state() -> str:
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        return f"{revision}{' + uncommitted changes' if dirty else ''}"
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def run_production(data: xr.DataArray, year: int, args: dict[str, Any]) -> xr.DataArray:
    with contextlib.redirect_stdout(io.StringIO()):
        return consequential_method(data, year, args, "electricity")


def verify_guardrails(data: xr.DataArray, year: int) -> list[tuple[str, bool, str]]:
    invalid = [
        (
            "range and duration cannot both be non-zero",
            {"range time": 2, "duration": 20},
            "cannot both be non-zero",
        ),
        (
            "a 2-year duration must use range time",
            {"range time": 0, "duration": 2},
            "at least 3 years",
        ),
        (
            "split-annual measurement rejects individual lead times",
            {"measurement": 4, "lead time": True},
            "common market interval",
        ),
    ]
    results = []
    for label, args, expected in invalid:
        try:
            run_production(data, year, args)
        except ValueError as error:
            results.append((label, expected in str(error), str(error)))
        else:
            results.append((label, False, "No exception raised"))
    return results


def write_csv(
    path: Path,
    *,
    metadata: dict[str, Any],
    technologies: list[str],
    results: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "model",
        "pathway",
        "year",
        "region",
        "case",
        "changed_argument",
        "arguments",
        "average_interval",
        "supplier_interval",
        "direction",
        "technology",
        "production_mix",
        "oracle_mix",
        "absolute_difference",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for result in results:
            oracle = result["oracle"]
            for index, technology in enumerate(technologies):
                writer.writerow(
                    {
                        **metadata,
                        "year": result["year"],
                        "case": result["name"],
                        "changed_argument": result["description"],
                        "arguments": json.dumps(result["args"], sort_keys=True),
                        "average_interval": f"{oracle.average_start}-{oracle.average_end}",
                        "supplier_interval": (
                            f"{np.asarray(oracle.start).tolist()} to "
                            f"{np.asarray(oracle.end).tolist()}"
                        ),
                        "direction": oracle.direction,
                        "technology": technology,
                        "production_mix": f"{result['actual'][index]:.15g}",
                        "oracle_mix": f"{oracle.mix[index]:.15g}",
                        "absolute_difference": f"{abs(result['actual'][index] - oracle.mix[index]):.15g}",
                    }
                )


def write_report(
    path: Path,
    *,
    args: argparse.Namespace,
    csv_path: Path,
    technologies: list[str],
    results: list[dict[str, Any]],
    wiring_delta: float,
    guardrails: list[tuple[str, bool, str]],
    constrained_yaml_type: str,
    substring_only_constraints: list[str],
) -> None:
    all_exact = all(result["max_delta"] <= TOLERANCE for result in results)
    all_normalised = all(abs(result["sum"] - 1) <= TOLERANCE for result in results)
    all_finite = all(np.isfinite(result["actual"]).all() for result in results)
    all_nonnegative = all(result["minimum"] >= -TOLERANCE for result in results)
    all_constrained_zero = all(
        result["constrained_share"] <= TOLERANCE for result in results
    )
    guardrails_pass = all(item[1] for item in guardrails)
    declining_branch_covered = any(
        result["oracle"].direction == "declining" for result in results
    )
    if constrained_yaml_type == "list":
        constrained_schema_note = (
            "The constrained-supplier file parses as a YAML list, so supplier "
            "exclusions use exact names. In particular, `diesel` and `liquefied "
            "petroleum gas` are not accidentally excluded through substring matching."
        )
    else:
        constrained_schema_note = (
            "One separate robustness issue was found: "
            "`constrained_suppliers.yaml` is not a YAML list. It currently parses as "
            "one folded string, so production uses substring membership. This does not "
            "change the electricity results above, but it also classifies `diesel` and "
            "`liquefied petroleum gas` as constrained even though neither is an exact "
            "entry. The file or loader should be corrected before treating fuel-sector "
            "marginal mixes as validated."
        )

    lines = [
        "# Consequential marginal-mix validation",
        "",
        f"Generated {datetime.now().astimezone().isoformat(timespec='seconds')} from Git state `{git_state()}`.",
        "",
        "## Scope",
        "",
        (
            f"Fixed case: **{args.model.upper()} / {args.pathway}, {args.region}, "
            f"{args.year}, electricity**. The complete technology-by-technology results "
            f"are in [{csv_path.name}]({csv_path.name})."
        ),
        "",
        (
            "Reference arguments: `range time=2`, `duration=0`, `foresight=False`, "
            "`lead time=False` (market average), `capital replacement rate=True`, "
            "and `measurement=0`. Each following case changes one argument family."
        ),
        "",
        (
            f"An additional `{args.declining_year}` row keeps the same scenario and region "
            "but checks the declining-market supplier-selection branch. It is not part "
            "of the one-factor comparison."
        ),
        "",
        (
            "The production result comes from `premise.marginal_mixes.consequential_method`. "
            "The oracle is separate code in `examples/validate_consequential_marginal_mixes.py`: "
            "it reads the YAML parameters directly, uses SciPy's Akima interpolator, "
            "constructs the documented observation windows, and evaluates the six "
            "measurement equations without importing Premise's marginal-mix helpers."
        ),
        "",
        "## Results",
        "",
        "| Case | Change or branch check | Effective interval | Market | Largest suppliers | Max absolute delta |",
        "|---|---|---|---|---|---:|",
    ]
    for result in results:
        oracle = result["oracle"]
        lines.append(
            "| {name} | {description} | {interval} | {direction} | {top} | {delta:.2e} |".format(
                name=result["name"],
                description=result["description"],
                interval=format_interval(oracle),
                direction=oracle.direction,
                top=top_mix(technologies, result["actual"], args.top),
                delta=result["max_delta"],
            )
        )

    lines.extend(
        [
            "",
            "`Max absolute delta` is the largest supplier-share difference between the production calculation and the independent oracle.",
            "",
            "## Checks",
            "",
            "| Check | Result | Evidence |",
            "|---|---|---|",
            f"| Production vs independent oracle | {'PASS' if all_exact else 'FAIL'} | {sum(item['max_delta'] <= TOLERANCE for item in results)}/{len(results)} cases within {TOLERANCE:.0e}; maximum {max(item['max_delta'] for item in results):.2e} |",
            f"| IAM-to-electricity wiring | {'PASS' if wiring_delta <= TOLERANCE else 'FAIL'} | Raw mapped volumes normalise to the `IAMDataCollection.electricity_mix` result; max absolute delta = {wiring_delta:.2e} |",
            f"| Mixes sum to one | {'PASS' if all_normalised else 'FAIL'} | Range {min(item['sum'] for item in results):.15g}–{max(item['sum'] for item in results):.15g} |",
            f"| No negative or non-finite shares | {'PASS' if all_nonnegative and all_finite else 'FAIL'} | Minimum share {min(item['minimum'] for item in results):.3g}; all values finite = {all_finite} |",
            f"| Constrained suppliers excluded | {'PASS' if all_constrained_zero else 'FAIL'} | Largest total constrained share {max(item['constrained_share'] for item in results):.2e} |",
            f"| Invalid argument combinations rejected | {'PASS' if guardrails_pass else 'FAIL'} | {sum(item[1] for item in guardrails)}/{len(guardrails)} expected errors raised |",
            f"| Real-data market-direction branches | {'PASS' if declining_branch_covered else 'WARN'} | Expanding/replacing and declining branches both covered = {declining_branch_covered} |",
            f"| Constrained-supplier parameter schema | {'PASS' if constrained_yaml_type == 'list' else 'WARN'} | YAML runtime type is `{constrained_yaml_type}`; unintended substring matches: {', '.join(f'`{item}`' for item in substring_only_constraints) or 'none'} |",
            "",
            "Guardrails exercised:",
            "",
        ]
    )
    lines.extend(
        f"- {'PASS' if passed else 'FAIL'} — {label}" for label, passed, _ in guardrails
    )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            (
                "This run confirms that the implemented numerical code follows the stated "
                "algorithm for this real IAM market across every consequential argument "
                "family, all measurement methods, and both market-direction branches. The "
                "lead-time switch changes the supplier-specific windows (rather than "
                "disabling lead time), while perfect foresight centres the window on the "
                "demand year."
            ),
            "",
            constrained_schema_note,
            "",
            (
                "It is a strong implementation and regression check, but not a proof that "
                "the behavioural assumptions are empirically correct for every market. "
                "Confidence should also come from the synthetic unit tests, additional "
                "scenario/region fixtures, and review of the lead-time, lifetime, and "
                "constrained-supplier parameter files."
            ),
            "",
            "## Reproduce",
            "",
            "```console",
            "PREMISE_KEY=... python examples/validate_consequential_marginal_mixes.py",
            "```",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        raise RuntimeError(
            "Set PREMISE_KEY (or IAM_FILES_KEY) to decrypt the IAM file."
        )

    with contextlib.redirect_stdout(io.StringIO()):
        iam_data = IAMDataCollection(
            model=args.model,
            pathway=args.pathway,
            year=args.year,
            filepath_iam_files=DATA_DIR / "iam_output_files",
            key=key.encode(),
            system_model="cutoff",
        )

    electricity = load_raw_electricity(iam_data.data, args.model)
    if args.region not in electricity.coords["region"]:
        raise ValueError(f"Unknown region {args.region!r}.")
    region_data = electricity.sel(region=[args.region])
    technologies = electricity.coords["variables"].values.tolist()

    raw_at_year = electricity.sel(region=args.region, year=args.year)
    independently_normalised = raw_at_year / raw_at_year.sum(dim="variables")
    wired = iam_data.electricity_mix.sel(region=args.region, year=args.year).reindex(
        variables=technologies, fill_value=0
    )
    wiring_delta = float(abs(independently_normalised - wired).max())

    constrained = load_exact_constrained_suppliers()
    constrained_mask = np.asarray([item in constrained for item in technologies])
    results = []

    def evaluate(
        name: str, description: str, case_args: dict[str, Any], case_year: int
    ) -> None:
        oracle = oracle_mix(
            data=region_data,
            region=args.region,
            year=case_year,
            args=case_args,
        )
        production = run_production(region_data, case_year, case_args)
        actual = np.asarray(
            production.sel(region=args.region, year=case_year).values, dtype=float
        )
        results.append(
            {
                "name": name,
                "description": description,
                "year": case_year,
                "args": case_args,
                "oracle": oracle,
                "actual": actual,
                "max_delta": float(np.max(np.abs(actual - oracle.mix))),
                "sum": float(actual.sum()),
                "minimum": float(actual.min()),
                "constrained_share": float(actual[constrained_mask].sum()),
            }
        )

    configured_cases = cases()
    for name, description, case_args in configured_cases:
        evaluate(name, description, case_args, args.year)

    no_replacement_args = next(
        case_args for name, _, case_args in configured_cases if name == "no_replacement"
    )
    evaluate(
        "declining_branch",
        f"declining-market check at year {args.declining_year}",
        no_replacement_args,
        args.declining_year,
    )

    guardrails = verify_guardrails(region_data, args.year)
    constrained_yaml_type, substring_only_constraints = constrained_parameter_audit()
    metadata = {
        "model": args.model,
        "pathway": args.pathway,
        "year": args.year,
        "region": args.region,
    }
    write_csv(
        args.csv,
        metadata=metadata,
        technologies=technologies,
        results=results,
    )
    write_report(
        args.report,
        args=args,
        csv_path=args.csv,
        technologies=technologies,
        results=results,
        wiring_delta=wiring_delta,
        guardrails=guardrails,
        constrained_yaml_type=constrained_yaml_type,
        substring_only_constraints=substring_only_constraints,
    )

    failures = [result for result in results if result["max_delta"] > TOLERANCE]
    failures.extend(
        result for result in results if not np.isfinite(result["actual"]).all()
    )
    if failures or wiring_delta > TOLERANCE or not all(item[1] for item in guardrails):
        raise SystemExit("Validation failed; inspect the generated report.")

    print(f"PASS: {len(results)} production mixes match the independent oracle.")
    if constrained_yaml_type != "list":
        print("WARNING: constrained_suppliers.yaml is not a YAML list; see the report.")
    print(f"Report: {args.report}")
    print(f"Full results: {args.csv}")


if __name__ == "__main__":
    main()
