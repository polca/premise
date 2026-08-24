"""Calculation engine for the European hydrogen-market LCIA example.

The public :func:`run_analysis` function performs every Brightway calculation and
returns named pandas tables. Plotting intentionally remains in the notebook.
"""

from __future__ import annotations

import re
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import bw2calc as bc
import bw2data as bd
import numpy as np
import pandas as pd
from premise_gwp import add_premise_gwp

try:
    import config as cfg
    from mapping import (
        CONVERSION_NAMES,
        classify_market_branch,
        classify_production_input,
        impact_category_label,
        is_hydrogen_market,
        normalized,
    )
except ImportError:  # pragma: no cover - supports package-style imports
    from . import config as cfg
    from .mapping import (
        CONVERSION_NAMES,
        classify_market_branch,
        classify_production_input,
        impact_category_label,
        is_hydrogen_market,
        normalized,
    )


@dataclass
class AnalysisResults:
    """All metadata and numerical tables produced for one Brightway database."""

    project: str
    database_name: str
    selected: dict
    market_order: list[str]
    sector_order: list[str]
    lcia_methods: list[tuple]
    ef31_methods: list[tuple]
    method_units: dict[tuple, str]
    tables: dict[str, pd.DataFrame]
    output_dir: Path
    export_paths: dict[str, Path] = field(default_factory=dict)

    def table(self, name: str) -> pd.DataFrame:
        """Return a named result table with a helpful error on misspelling."""
        if name not in self.tables:
            available = ", ".join(sorted(self.tables))
            raise KeyError(
                f"Unknown result table {name!r}. Available tables: {available}"
            )
        return self.tables[name]


def safe_path_component(value: str) -> str:
    """Return a stable filesystem-safe label for database-specific result folders."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._")
    return cleaned or "analysis"


def _reference_output_amount(activity) -> float:
    production = [
        exc for exc in activity.production() if exc.input.key == activity.key
    ]
    if len(production) != 1:
        raise ValueError(
            f"Expected one reference production exchange for {activity.key}; "
            f"found {len(production)}."
        )
    amount = float(production[0].get("amount", 0.0))
    if amount == 0:
        raise ZeroDivisionError(
            f"Reference production amount is zero for {activity.key}."
        )
    return amount


def _collect_market_branches(activity, demand_amount, path=(), visited=()):
    """Unwrap pass-through hydrogen markets into production/distribution branches."""
    if activity.key in visited:
        raise RuntimeError(
            f"Hydrogen-market pass-through cycle detected at {activity.key}."
        )
    scale = demand_amount / _reference_output_amount(activity)
    current_path = (*path, activity.key)
    branches = []
    market_biosphere = []
    for exchange in activity.technosphere():
        provider = exchange.input
        provider_demand = scale * float(exchange.get("amount", 0.0))
        if is_hydrogen_market(provider):
            nested_branches, nested_biosphere = _collect_market_branches(
                provider,
                provider_demand,
                current_path,
                (*visited, activity.key),
            )
            branches.extend(nested_branches)
            market_biosphere.extend(nested_biosphere)
        else:
            branches.append(
                {
                    "provider": provider,
                    "demand amount": provider_demand,
                    "market path": current_path,
                }
            )
    for exchange in activity.biosphere():
        market_biosphere.append(
            {
                "exchange": exchange,
                "scaled amount": scale * float(exchange.get("amount", 0.0)),
                "market path": current_path,
            }
        )
    return branches, market_biosphere


def _pipeline_conversion_inputs(activity, demand_amount):
    if (
        normalized(activity.get("name"))
        != "hydrogen supply, distributed by pipeline"
    ):
        return []
    scale = demand_amount / _reference_output_amount(activity)
    inputs = []
    for exchange in activity.technosphere():
        provider = exchange.input
        exchange_name = normalized(
            exchange.get("name") or provider.get("name")
        )
        if exchange_name in CONVERSION_NAMES:
            inputs.append(
                {
                    "provider": provider,
                    "demand amount": scale
                    * float(exchange.get("amount", 0.0)),
                    "component": CONVERSION_NAMES[exchange_name],
                }
            )
    return inputs


def _method_cf_lookup(method):
    return {
        int(flow_id): float(cf) for flow_id, cf in bd.Method(method).load()
    }


def _direct_biosphere_rows(activity, demand_amount, cf_lookup):
    scale = demand_amount / _reference_output_amount(activity)
    rows = []
    for exchange in activity.biosphere():
        flow = exchange.input
        flow_name = str(flow.get("name", ""))
        scaled_amount = scale * float(exchange.get("amount", 0.0))
        if flow_name.lower() == "hydrogen":
            contribution_type = "Hydrogen leakage"
        elif flow_name.lower() == "ammonia":
            contribution_type = "Ammonia leakage"
        else:
            contribution_type = "Other direct emissions"
        rows.append(
            {
                "flow": flow_name,
                "flow key": flow.key,
                "flow unit": flow.get("unit", ""),
                "physical amount": scaled_amount,
                "contribution type": contribution_type,
                "score": scaled_amount * cf_lookup.get(int(flow.id), 0.0),
            }
        )
    return rows


class _DemandScorer:
    """Reuse one Brightway matrix factorization for many branch demands."""

    def __init__(self, seed_activity, method):
        self.lca = bc.LCA({seed_activity: 1.0}, method)
        self.lca.lci()
        self.lca.lcia()
        self.cache = {}

    def unit_score(self, activity):
        if activity.key not in self.cache:
            self.lca.redo_lcia({int(activity.id): 1.0})
            self.cache[activity.key] = float(self.lca.score)
        return self.cache[activity.key]

    def score(self, activity, amount):
        return float(amount) * self.unit_score(activity)


def _score_fields(row, total_score):
    row["share of market (%)"] = (
        100.0 * row["score"] / total_score if total_score != 0 else np.nan
    )
    return row


def select_markets(database):
    """Select the exact generic market and every available European sector market."""

    def exact_matches(name, location):
        return sorted(
            [
                activity
                for activity in database
                if activity.get("name") == name
                and activity.get("reference product") == cfg.REFERENCE_PRODUCT
                and activity.get("unit") == cfg.UNIT
                and activity.get("location") == location
            ],
            key=lambda activity: str(activity.key),
        )

    generic_matches = exact_matches(
        cfg.GENERIC_MARKET_NAME, cfg.TARGET_LOCATION
    )
    if len(generic_matches) != 1:
        raise LookupError(
            f"Expected exactly one generic market at {cfg.TARGET_LOCATION!r}; "
            f"found {len(generic_matches)}."
        )
    selected = {"Generic": generic_matches[0]}
    missing = []
    sector_locations = {}
    for label, name in cfg.EXPECTED_SECTOR_MARKETS.items():
        chosen = None
        for location in (cfg.TARGET_LOCATION, *cfg.SECTOR_LOCATION_FALLBACKS):
            matches = exact_matches(name, location)
            if len(matches) > 1:
                raise LookupError(
                    f"Multiple exact matches for {name!r} at {location!r}."
                )
            if matches:
                chosen = matches[0]
                break
        if chosen is None:
            missing.append(label)
        else:
            selected[label] = chosen
            sector_locations[label] = chosen.get("location")
    if len(selected) == 1:
        raise LookupError(
            "No European sector-specific hydrogen markets were found."
        )
    if missing:
        warnings.warn(
            "No market was generated for these European sectors in this scenario: "
            + ", ".join(missing),
            stacklevel=2,
        )
    fallback_locations = sorted(
        set(sector_locations.values()) - {cfg.TARGET_LOCATION}
    )
    if fallback_locations:
        warnings.warn(
            f"Sector markets are unavailable at {cfg.TARGET_LOCATION}; using mapped "
            f"IAM location(s) {fallback_locations}.",
            stacklevel=2,
        )
    sector_order = [
        label for label in cfg.EXPECTED_SECTOR_MARKETS if label in selected
    ]
    market_order = ["Generic", *sector_order]
    selection_df = pd.DataFrame(
        [
            {
                "comparison label": label,
                "name": selected[label].get("name"),
                "reference product": selected[label].get("reference product"),
                "location": selected[label].get("location"),
                "unit": selected[label].get("unit"),
                "database key": selected[label].key,
            }
            for label in market_order
        ]
    )
    return selected, market_order, sector_order, selection_df


def select_methods():
    ef31_methods = sorted(
        [
            method
            for method in bd.methods
            if len(method) >= 2
            and method[0] == "EF v3.1"
            and not any(
                term.lower() in " | ".join(method).lower()
                for term in cfg.EXCLUDED_METHOD_TERMS
            )
        ]
    )
    if not ef31_methods:
        raise LookupError(
            "No EF v3.1 methods were found in the current Brightway project."
        )
    missing_ced = [
        method for method in cfg.CED_METHODS if method not in bd.methods
    ]
    if missing_ced:
        raise LookupError(
            f"Required CED methods are unavailable: {missing_ced}"
        )
    lcia_methods = [*ef31_methods, cfg.PREMISE_GWP_METHOD, *cfg.CED_METHODS]
    rows = []
    for method in lcia_methods:
        metadata = bd.Method(method).metadata
        rows.append(
            {
                "method": method,
                "impact category": impact_category_label(method),
                "indicator": method[-1],
                "unit": metadata.get("unit", ""),
            }
        )
    methods_df = pd.DataFrame(rows)
    method_units = {row["method"]: row["unit"] for row in rows}
    return ef31_methods, lcia_methods, method_units, methods_df


def _reverse_activity_index(lca):
    if hasattr(lca, "dicts") and hasattr(lca.dicts, "activity"):
        reverse = lca.dicts.activity.reversed
        return dict(reverse) if not isinstance(reverse, dict) else reverse
    if hasattr(lca, "activity_dict"):
        return {index: key for key, index in lca.activity_dict.items()}
    raise AttributeError(
        "Could not find the Brightway activity index on the LCA object."
    )


def _get_activity_from_key(key):
    try:
        return bd.get_activity(key)
    except Exception:
        return bd.get_node(id=key)


def _top_process_rows(lca, market_label, activity, method, unit, top_n):
    process_scores = np.asarray(
        lca.characterized_inventory.sum(axis=0)
    ).ravel()
    reverse = _reverse_activity_index(lca)
    nonzero = np.flatnonzero(process_scores)
    ordered = nonzero[np.argsort(np.abs(process_scores[nonzero]))[::-1]]
    kept = ordered[:top_n]
    rows = []
    for rank, column in enumerate(kept, start=1):
        process = _get_activity_from_key(reverse[int(column)])
        score = float(process_scores[column])
        rows.append(
            {
                "market": market_label,
                "market location": activity.get("location"),
                "impact category": impact_category_label(method),
                "indicator": method[-1],
                "method": method,
                "unit": unit,
                "rank": rank,
                "process": process.get("name", str(process)),
                "process location": process.get("location", ""),
                "process key": process.key,
                "score": score,
                "share (%)": (
                    100.0 * score / lca.score if lca.score != 0 else np.nan
                ),
            }
        )
    other_score = float(process_scores.sum() - process_scores[kept].sum())
    rows.append(
        {
            "market": market_label,
            "market location": activity.get("location"),
            "impact category": impact_category_label(method),
            "indicator": method[-1],
            "method": method,
            "unit": unit,
            "rank": top_n + 1,
            "process": "Other",
            "process location": "",
            "process key": None,
            "score": other_score,
            "share (%)": (
                100.0 * other_score / lca.score if lca.score != 0 else np.nan
            ),
        }
    )
    return rows


def calculate_lcia(selected, market_order, lcia_methods, method_units):
    score_records = []
    contribution_records = []
    for market_label in market_order:
        activity = selected[market_label]
        lca = bc.LCA({activity: cfg.FUNCTIONAL_UNIT_KG}, lcia_methods[0])
        lca.lci()
        for method in lcia_methods:
            lca.switch_method(method)
            lca.lcia()
            unit = method_units[method]
            score_records.append(
                {
                    "market": market_label,
                    "market name": activity.get("name"),
                    "location": activity.get("location"),
                    "impact category": impact_category_label(method),
                    "indicator": method[-1],
                    "method": method,
                    "unit": unit,
                    "score per kg H2": float(lca.score),
                }
            )
            if method == cfg.PREMISE_GWP_METHOD:
                contribution_records.extend(
                    _top_process_rows(
                        lca,
                        market_label,
                        activity,
                        method,
                        unit,
                        cfg.TOP_N_PROCESSES,
                    )
                )
    scores_df = pd.DataFrame(score_records)
    contributions_df = pd.DataFrame(contribution_records)
    expected_count = len(selected) * len(lcia_methods)
    if len(scores_df) != expected_count:
        raise AssertionError(
            f"Expected {expected_count} LCIA results; found {len(scores_df)}."
        )
    reconstructed = (
        contributions_df.groupby(["market", "method"], as_index=False)["score"]
        .sum()
        .rename(columns={"score": "reconstructed score"})
    )
    check = scores_df[
        scores_df["method"].isin([cfg.PREMISE_GWP_METHOD])
    ].merge(reconstructed, on=["market", "method"], validate="one_to_one")
    differences = (
        check["score per kg H2"] - check["reconstructed score"]
    ).abs()
    tolerance = 1e-9 * np.maximum(1.0, check["score per kg H2"].abs())
    if not np.all(differences <= tolerance):
        raise AssertionError(
            "Process contributions do not reconstruct one or more totals."
        )
    return scores_df, contributions_df


def analyze_hydrogen_life_cycle_stages(
    selected,
    market_order,
    method,
    scores_df,
    impact_category,
    unit,
    functional_unit=cfg.FUNCTIONAL_UNIT_KG,
    reconciliation_rtol=cfg.RECONCILIATION_RTOL,
):
    """Build Layer 1 and Layer 2 contribution tables for one LCIA method."""
    if reconciliation_rtol <= 0:
        raise ValueError("reconciliation_rtol must be positive.")
    scorer = _DemandScorer(selected[market_order[0]], method)
    cf_lookup = _method_cf_lookup(method)
    score_lookup = (
        scores_df[scores_df["method"].isin([method])]
        .set_index("market")["score per kg H2"]
        .to_dict()
    )
    layer1_rows = []
    production_input_rows = []
    distribution_rows = []
    audit_rows = []
    for market_label in market_order:
        market = selected[market_label]
        market_total = float(score_lookup[market_label])
        branches, market_biosphere = _collect_market_branches(
            market, functional_unit
        )
        distribution_nonleakage = 0.0
        distribution_leakage = defaultdict(float)
        distribution_leakage_amount = defaultdict(float)
        for branch in branches:
            provider = branch["provider"]
            demand_amount = branch["demand amount"]
            stage, substage, short_name, rule = classify_market_branch(
                provider
            )
            branch_score = scorer.score(provider, demand_amount)
            direct_rows = _direct_biosphere_rows(
                provider, demand_amount, cf_lookup
            )
            leakage_score = sum(
                row["score"]
                for row in direct_rows
                if row["contribution type"]
                in {"Hydrogen leakage", "Ammonia leakage"}
            )
            audit_rows.append(
                {
                    "market": market_label,
                    "market location": market.get("location"),
                    "provider": provider.get("name"),
                    "provider product": provider.get("reference product"),
                    "provider unit": provider.get("unit"),
                    "provider location": provider.get("location"),
                    "provider key": provider.key,
                    "demand amount": demand_amount,
                    "stage": stage,
                    "substage": substage,
                    "component": short_name,
                    "classification rule": rule,
                }
            )
            if stage == "Production":
                layer1_rows.append(
                    _score_fields(
                        {
                            "market": market_label,
                            "layer 1 group": "Production technology",
                            "component": short_name,
                            "contribution type": "Technology excluding leakage",
                            "score": branch_score - leakage_score,
                        },
                        market_total,
                    )
                )
                scale = demand_amount / _reference_output_amount(provider)
                for exchange in provider.technosphere():
                    input_provider = exchange.input
                    input_demand = scale * float(exchange.get("amount", 0.0))
                    production_input_rows.append(
                        _score_fields(
                            {
                                "market": market_label,
                                "technology": short_name,
                                "input group": classify_production_input(
                                    input_provider
                                ),
                                "input process": input_provider.get("name"),
                                "input location": input_provider.get(
                                    "location"
                                ),
                                "input key": input_provider.key,
                                "contribution type": "Upstream input",
                                "score": scorer.score(
                                    input_provider, input_demand
                                ),
                            },
                            market_total,
                        )
                    )
                for row in direct_rows:
                    production_input_rows.append(
                        _score_fields(
                            {
                                "market": market_label,
                                "technology": short_name,
                                "input group": row["contribution type"],
                                "input process": row["flow"],
                                "input location": "biosphere",
                                "input key": row["flow key"],
                                "contribution type": row["contribution type"],
                                "physical amount": row["physical amount"],
                                "physical unit": row["flow unit"],
                                "score": row["score"],
                            },
                            market_total,
                        )
                    )
                for leakage_type in ("Hydrogen leakage", "Ammonia leakage"):
                    leakage_rows = [
                        row
                        for row in direct_rows
                        if row["contribution type"] == leakage_type
                    ]
                    if leakage_rows:
                        layer1_rows.append(
                            _score_fields(
                                {
                                    "market": market_label,
                                    "layer 1 group": "Production technology",
                                    "component": f"{leakage_type} — {short_name}",
                                    "contribution type": leakage_type,
                                    "score": sum(
                                        row["score"] for row in leakage_rows
                                    ),
                                },
                                market_total,
                            )
                        )
            else:
                conversion_inputs = _pipeline_conversion_inputs(
                    provider, demand_amount
                )
                conversion_score = sum(
                    scorer.score(item["provider"], item["demand amount"])
                    for item in conversion_inputs
                )
                process_nonleakage = (
                    branch_score - leakage_score - conversion_score
                )
                distribution_nonleakage += (
                    process_nonleakage + conversion_score
                )
                distribution_rows.append(
                    _score_fields(
                        {
                            "market": market_label,
                            "substage": substage,
                            "process": short_name,
                            "process key": provider.key,
                            "contribution type": "Process excluding leakage",
                            "score": process_nonleakage,
                        },
                        market_total,
                    )
                )
                for item in conversion_inputs:
                    input_provider = item["provider"]
                    input_score = scorer.score(
                        input_provider, item["demand amount"]
                    )
                    distribution_rows.append(
                        _score_fields(
                            {
                                "market": market_label,
                                "substage": "Conversion",
                                "process": item["component"],
                                "process key": input_provider.key,
                                "contribution type": "Process excluding leakage",
                                "score": input_score,
                            },
                            market_total,
                        )
                    )
                    audit_rows.append(
                        {
                            "market": market_label,
                            "market location": market.get("location"),
                            "provider": input_provider.get("name"),
                            "provider product": input_provider.get(
                                "reference product"
                            ),
                            "provider unit": input_provider.get("unit"),
                            "provider location": input_provider.get(
                                "location"
                            ),
                            "provider key": input_provider.key,
                            "demand amount": item["demand amount"],
                            "stage": "Distribution",
                            "substage": "Conversion",
                            "component": item["component"],
                            "classification rule": "pipeline compression input",
                        }
                    )
                for leakage_type in ("Hydrogen leakage", "Ammonia leakage"):
                    leakage = [
                        row
                        for row in direct_rows
                        if row["contribution type"] == leakage_type
                    ]
                    if leakage:
                        leakage_value = sum(row["score"] for row in leakage)
                        physical_amount = sum(
                            row["physical amount"] for row in leakage
                        )
                        distribution_leakage[leakage_type] += leakage_value
                        distribution_leakage_amount[
                            leakage_type
                        ] += physical_amount
                        distribution_rows.append(
                            _score_fields(
                                {
                                    "market": market_label,
                                    "substage": substage,
                                    "process": f"{leakage_type} — {short_name}",
                                    "process key": provider.key,
                                    "contribution type": leakage_type,
                                    "physical amount": physical_amount,
                                    "physical unit": leakage[0]["flow unit"],
                                    "score": leakage_value,
                                },
                                market_total,
                            )
                        )
        for item in market_biosphere:
            flow = item["exchange"].input
            flow_name = str(flow.get("name", ""))
            if flow_name.lower() not in {"hydrogen", "ammonia"}:
                raise ValueError(
                    f"Unclassified direct market emission {flow_name!r} in {market_label}."
                )
            leakage_type = (
                "Hydrogen leakage"
                if flow_name.lower() == "hydrogen"
                else "Ammonia leakage"
            )
            leakage_score = item["scaled amount"] * cf_lookup.get(
                int(flow.id), 0.0
            )
            distribution_leakage[leakage_type] += leakage_score
            distribution_leakage_amount[leakage_type] += item["scaled amount"]
            distribution_rows.append(
                _score_fields(
                    {
                        "market": market_label,
                        "substage": "Transport",
                        "process": f"{leakage_type} — market",
                        "process key": market.key,
                        "contribution type": leakage_type,
                        "physical amount": item["scaled amount"],
                        "physical unit": flow.get("unit", ""),
                        "score": leakage_score,
                    },
                    market_total,
                )
            )
        layer1_rows.append(
            _score_fields(
                {
                    "market": market_label,
                    "layer 1 group": "Hydrogen distribution",
                    "component": "Hydrogen distribution",
                    "contribution type": "Distribution excluding leakage",
                    "score": distribution_nonleakage,
                },
                market_total,
            )
        )
        for leakage_type, leakage_score in distribution_leakage.items():
            layer1_rows.append(
                _score_fields(
                    {
                        "market": market_label,
                        "layer 1 group": "Hydrogen distribution",
                        "component": f"{leakage_type} — distribution",
                        "contribution type": leakage_type,
                        "physical amount": distribution_leakage_amount[
                            leakage_type
                        ],
                        "physical unit": "kilogram",
                        "score": leakage_score,
                    },
                    market_total,
                )
            )
    layer1_df = pd.DataFrame(layer1_rows)
    production_inputs_df = pd.DataFrame(production_input_rows)
    distribution_df = pd.DataFrame(distribution_rows)
    audit_df = pd.DataFrame(audit_rows)
    production_groups_df = (
        production_inputs_df.groupby(
            ["market", "technology", "input group", "contribution type"],
            as_index=False,
            dropna=False,
        )["score"]
        .sum()
        .merge(
            scores_df[scores_df["method"].isin([method])][
                ["market", "score per kg H2"]
            ],
            on="market",
            validate="many_to_one",
        )
    )
    production_groups_df["share of market (%)"] = np.where(
        production_groups_df["score per kg H2"] != 0,
        100.0
        * production_groups_df["score"]
        / production_groups_df["score per kg H2"],
        np.nan,
    )
    reconciliation_rows = []
    for market_label in market_order:
        total = float(score_lookup[market_label])
        layer1_sum = float(
            layer1_df.loc[layer1_df["market"] == market_label, "score"].sum()
        )
        production_sum = float(
            production_groups_df.loc[
                production_groups_df["market"] == market_label, "score"
            ].sum()
        )
        distribution_sum = float(
            distribution_df.loc[
                distribution_df["market"] == market_label, "score"
            ].sum()
        )
        reconciliation_rows.append(
            {
                "market": market_label,
                "method": method,
                "impact category": impact_category,
                "unit": unit,
                "total score": total,
                "Layer 1 reconstructed score": layer1_sum,
                "Layer 1 difference": layer1_sum - total,
                "Layer 2 reconstructed score": production_sum
                + distribution_sum,
                "Layer 2 difference": production_sum
                + distribution_sum
                - total,
            }
        )
    reconciliation_df = pd.DataFrame(reconciliation_rows)
    tolerance = reconciliation_rtol * np.maximum(
        1.0, reconciliation_df["total score"].abs()
    )
    if not (reconciliation_df["Layer 1 difference"].abs() <= tolerance).all():
        raise AssertionError(
            "Layer 1 contributions do not reconstruct all LCIA totals."
        )
    if not (reconciliation_df["Layer 2 difference"].abs() <= tolerance).all():
        raise AssertionError(
            "Layer 2 contributions do not reconstruct all LCIA totals."
        )
    for frame in (
        layer1_df,
        production_inputs_df,
        production_groups_df,
        distribution_df,
    ):
        frame["method"] = [method] * len(frame)
        frame["impact category"] = impact_category
        frame["unit"] = unit
    return {
        "layer1": layer1_df,
        "production inputs detailed": production_inputs_df,
        "production input groups": production_groups_df,
        "distribution processes": distribution_df,
        "classification audit": audit_df,
        "reconciliation": reconciliation_df,
    }


def _comparison_to_baseline(scores_df, baseline_label, comparison_labels):
    baseline = scores_df[scores_df["market"] == baseline_label][
        ["method", "score per kg H2"]
    ].rename(columns={"score per kg H2": "baseline score per kg H2"})
    compared = scores_df[scores_df["market"].isin(comparison_labels)].merge(
        baseline, on="method", validate="many_to_one"
    )
    compared["absolute difference"] = (
        compared["score per kg H2"] - compared["baseline score per kg H2"]
    )
    compared["difference (%)"] = np.where(
        compared["baseline score per kg H2"] != 0,
        100.0
        * compared["absolute difference"]
        / compared["baseline score per kg H2"],
        np.nan,
    )
    compared["baseline"] = baseline_label
    return compared


def _spider_ratios(scores_df, ef31_methods, market_order):
    categories = [impact_category_label(method) for method in ef31_methods]
    ef_scores = (
        scores_df[scores_df["method"].isin(ef31_methods)]
        .pivot(
            index="impact category", columns="market", values="score per kg H2"
        )
        .reindex(index=categories, columns=market_order)
    )
    baseline = ef_scores["Generic"].replace(0, np.nan)
    return ef_scores.divide(baseline, axis=0)


def _hotspot_tables(
    selected, market_order, ef31_methods, scores_df, method_units
):
    candidates = [*ef31_methods, cfg.PREMISE_GWP_METHOD]
    methods_by_category = {}
    for category in cfg.SPECIFIC_IMPACT_CATEGORY_ORDER:
        matches = [
            method
            for method in candidates
            if impact_category_label(method) == category
        ]
        if len(matches) != 1:
            raise LookupError(
                f"Expected exactly one method for hotspot category {category!r}; "
                f"found {len(matches)}: {matches}"
            )
        methods_by_category[category] = matches[0]
    layer1_frames = []
    reconciliation_frames = []
    for category, method in methods_by_category.items():
        result = analyze_hydrogen_life_cycle_stages(
            selected,
            market_order,
            method,
            scores_df,
            category,
            method_units[method],
        )
        layer1_frames.append(result["layer1"])
        reconciliation_frames.append(result["reconciliation"])
    layer1 = pd.concat(layer1_frames, ignore_index=True)
    reconciliation = pd.concat(reconciliation_frames, ignore_index=True)
    groups = (
        layer1.groupby(
            [
                "market",
                "method",
                "impact category",
                "unit",
                "layer 1 group",
                "component",
            ],
            as_index=False,
            dropna=False,
        )["score"]
        .sum()
        .rename(columns={"score": "absolute contribution"})
    )
    totals = scores_df[scores_df["method"].isin(methods_by_category.values())][
        ["market", "method", "score per kg H2"]
    ]
    groups = groups.merge(
        totals, on=["market", "method"], validate="many_to_one"
    )
    groups["share of total impact (%)"] = np.where(
        groups["score per kg H2"] != 0,
        100.0 * groups["absolute contribution"] / groups["score per kg H2"],
        np.nan,
    )
    groups["impact category"] = pd.Categorical(
        groups["impact category"],
        categories=cfg.SPECIFIC_IMPACT_CATEGORY_ORDER,
        ordered=True,
    )
    groups = groups.sort_values(
        ["impact category", "market", "layer 1 group", "component"]
    ).reset_index(drop=True)
    return groups, reconciliation


def _regional_steel_analysis(database, method_units):
    activities = sorted(
        [
            activity
            for activity in database
            if activity.get("name") == cfg.STEEL_MARKET_NAME
            and activity.get("reference product") == cfg.REFERENCE_PRODUCT
            and activity.get("unit") == cfg.UNIT
            and str(activity.get("location", "")).upper()
            != cfg.EXCLUDED_STEEL_LOCATION
        ],
        key=lambda activity: (activity.get("location", ""), str(activity.key)),
    )
    if not activities:
        raise LookupError(
            f"No regional {cfg.STEEL_MARKET_NAME!r} activities were found."
        )
    location_counts = pd.Series(
        [activity.get("location") for activity in activities]
    ).value_counts()
    duplicates = location_counts[location_counts > 1].index.tolist()
    if duplicates:
        raise LookupError(
            "Duplicate steel-market locations: " + ", ".join(duplicates)
        )
    selected = {activity.get("location"): activity for activity in activities}
    order = list(selected)
    selection = pd.DataFrame(
        [
            {
                "region": region,
                "name": activity.get("name"),
                "reference product": activity.get("reference product"),
                "unit": activity.get("unit"),
                "database key": activity.key,
            }
            for region, activity in selected.items()
        ]
    )
    records = []
    for region, activity in selected.items():
        lca = bc.LCA(
            {activity: cfg.FUNCTIONAL_UNIT_KG}, cfg.PREMISE_GWP_METHOD
        )
        lca.lci()
        lca.lcia()
        records.append(
            {
                "market": region,
                "market name": activity.get("name"),
                "location": activity.get("location"),
                "impact category": impact_category_label(
                    cfg.PREMISE_GWP_METHOD
                ),
                "indicator": cfg.PREMISE_GWP_METHOD[-1],
                "method": cfg.PREMISE_GWP_METHOD,
                "unit": method_units[cfg.PREMISE_GWP_METHOD],
                "score per kg H2": float(lca.score),
            }
        )
    scores = pd.DataFrame(records)
    stage = analyze_hydrogen_life_cycle_stages(
        selected,
        order,
        cfg.PREMISE_GWP_METHOD,
        scores,
        impact_category_label(cfg.PREMISE_GWP_METHOD),
        method_units[cfg.PREMISE_GWP_METHOD],
    )
    return selected, order, selection, scores, stage


def export_tables(results: AnalysisResults) -> dict[str, Path]:
    """Write every configured table available in ``results`` to CSV."""
    results.output_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for label, filename in cfg.EXPORT_FILENAMES.items():
        if label not in results.tables:
            continue
        path = results.output_dir / filename
        results.tables[label].to_csv(path, index=False)
        paths[label] = path
    if len(set(paths.values())) != len(paths):
        raise AssertionError(
            "Two result tables resolve to the same export path."
        )
    results.export_paths = paths
    return paths


def run_analysis(
    project: str,
    database_name: str,
    *,
    output_dir: str | Path | None = None,
    export: bool = True,
    run_regional_steel: bool = False,
) -> AnalysisResults:
    """Run the complete LCIA workflow for one prospective database."""
    bd.projects.set_current(project)
    add_premise_gwp()
    if cfg.PREMISE_GWP_METHOD not in bd.methods:
        raise LookupError(
            f"premise_gwp did not install the expected method: {cfg.PREMISE_GWP_METHOD}"
        )
    if database_name not in bd.databases:
        available = "\n".join(f"  - {name}" for name in bd.databases)
        raise KeyError(
            f"Database {database_name!r} is unavailable. Available databases:\n{available}"
        )
    database = bd.Database(database_name)
    selected, market_order, sector_order, selection_df = select_markets(
        database
    )
    ef31_methods, lcia_methods, method_units, methods_df = select_methods()
    scores_df, contributions_df = calculate_lcia(
        selected, market_order, lcia_methods, method_units
    )
    comparison_df = _comparison_to_baseline(scores_df, "Generic", sector_order)
    spider_df = _spider_ratios(scores_df, ef31_methods, market_order)
    hotspot_df, hotspot_reconciliation_df = _hotspot_tables(
        selected, market_order, ef31_methods, scores_df, method_units
    )
    stage = analyze_hydrogen_life_cycle_stages(
        selected,
        market_order,
        cfg.PREMISE_GWP_METHOD,
        scores_df,
        impact_category_label(cfg.PREMISE_GWP_METHOD),
        method_units[cfg.PREMISE_GWP_METHOD],
    )
    tables = {
        "selection": selection_df,
        "methods": methods_df,
        "scores": scores_df,
        "RER comparison": comparison_df,
        "EF 3.1 spider ratios": spider_df.rename_axis(
            columns="market"
        ).reset_index(),
        "Hotspot process groups": hotspot_df,
        "Hotspot reconciliation": hotspot_reconciliation_df,
        "contributions": contributions_df,
        "Stage Layer 1": stage["layer1"],
        "Production inputs detailed": stage["production inputs detailed"],
        "Production input groups": stage["production input groups"],
        "Distribution processes": stage["distribution processes"],
        "Stage classification audit": stage["classification audit"],
        "Stage reconciliation": stage["reconciliation"],
    }
    if run_regional_steel:
        _, steel_order, steel_selection, steel_scores, steel_stage = (
            _regional_steel_analysis(database, method_units)
        )
        tables.update(
            {
                "Steel-region selection": steel_selection,
                "Steel-region scores": steel_scores,
                "Steel-region Layer 1": steel_stage["layer1"],
                "Steel-region distribution": steel_stage[
                    "distribution processes"
                ],
                "Steel-region reconciliation": steel_stage["reconciliation"],
            }
        )
        tables["Steel-region order"] = pd.DataFrame({"region": steel_order})
    resolved_output_dir = Path(output_dir) if output_dir else cfg.RESULTS_DIR
    results = AnalysisResults(
        project=project,
        database_name=database_name,
        selected=selected,
        market_order=market_order,
        sector_order=sector_order,
        lcia_methods=lcia_methods,
        ef31_methods=ef31_methods,
        method_units=method_units,
        tables=tables,
        output_dir=resolved_output_dir.resolve(),
    )
    if export:
        export_tables(results)
    return results


__all__ = [
    "AnalysisResults",
    "analyze_hydrogen_life_cycle_stages",
    "export_tables",
    "run_analysis",
    "safe_path_component",
    "select_markets",
    "select_methods",
]
