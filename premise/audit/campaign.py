"""Sequential auditing of written graphs with a matched reference sample."""

from collections import defaultdict
from dataclasses import dataclass, field, replace
import json
import gc
from pathlib import Path
import random
import re
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from ..materials.metals_rules import configured_metal_products

from .brightway import audit_brightway_database
from .engine import compare_audits
from .graph import identity
from .models import AuditConfig
from .report import csv_rows


@dataclass(frozen=True)
class AuditCase:
    """One existing database; the first case defines the discovery sample."""

    label: str
    database: str
    config: AuditConfig = field(default_factory=AuditConfig)

    def __post_init__(self):
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", self.label) is None:
            raise ValueError("Audit labels must be simple directory-safe names")
        if not self.database:
            raise ValueError("Audit cases require a database name")


def audit_campaign(cases, *, output=None, crosscheck_size=250, progress=None):
    """Audit existing Brightway databases, preserving a matched discovery panel.

    The caller selects the Brightway project. No scenarios are built or
    repaired. Later cases use matching identities from the first case; smaller
    panels are balanced across the discovery panel and sector labels. Missing
    identities are recorded, never represented as zero scores. Use
    ``AuditReport.read`` to reload completed cases for further comparisons.
    """
    import bw2data as bd

    cases = tuple(cases)
    if not cases or len({c.label for c in cases}) != len(cases):
        raise ValueError("Campaigns require nonempty, uniquely labelled cases")
    if any(c.database not in bd.databases for c in cases):
        raise ValueError("Every campaign database must exist in the current project")
    root = Path(output) if output is not None else None
    if root is not None:
        root.mkdir(parents=True, exist_ok=False)
    reports = {}
    master = None
    comparisons = []
    for case in cases:
        config = case.config
        cohort = None
        if master is not None and not config.explicit_sample:
            candidates = defaultdict(list)
            for node in bd.Database(case.database):
                candidates[identity(node)].append(node)
            matched = [
                r
                for r in master.sample
                if len(candidates.get(tuple(r["activity"]), ())) == 1
            ]
            cohort = dict(
                reference=cases[0].label,
                available_reference_matches=len(matched),
                unmatched_reference_activities=[
                    r["activity"]
                    for r in master.sample
                    if len(candidates.get(tuple(r["activity"]), ())) != 1
                ],
            )
            if matched:
                if len(matched) > config.sample_size:
                    groups = defaultdict(list)
                    rng = random.Random(config.seed)
                    for row in matched:
                        groups[(row["panel"], row["stratum"])].append(row)
                    for rows in groups.values():
                        rng.shuffle(rows)
                    selected = []
                    while len(selected) < config.sample_size:
                        for key in sorted(groups):
                            if groups[key] and len(selected) < config.sample_size:
                                selected.append(groups[key].pop())
                    matched = selected
                config = replace(
                    config, explicit_sample=tuple(tuple(r["activity"]) for r in matched)
                )
                cohort["selection"] = (
                    "Matched discovery identities; panel/sector-balanced subset where needed"
                )
            else:
                cohort["selection"] = "Independent panel: no discovery identity matched"

        def notify(event):
            if progress is not None:
                progress(dict(event, case=case.label))

        report = audit_brightway_database(
            case.database,
            config=config,
            crosscheck_size=crosscheck_size,
            progress=notify,
            provenance=dict(campaign_case=case.label),
        )
        if cohort is not None:
            report.metadata["reference_cohort"] = cohort
        if master is None:
            master = report
        else:
            comparisons.extend(
                dict(row, case=case.label, reference=cases[0].label)
                for row in compare_audits(
                    master, report, threshold_percent=config.comparison_percent
                )
            )
        reports[case.label] = report
        if root is not None:
            report.write(root / case.label)
        gc.collect()
    if root is not None:
        csv_rows(root / "comparisons.csv", comparisons)
        (root / "campaign.json").write_text(
            json.dumps(
                dict(
                    reference=cases[0].label,
                    cases=[
                        dict(
                            label=c.label,
                            database=c.database,
                            sampled=len(reports[c.label].sample),
                            errors=sum(
                                f.issue.severity == "error"
                                for f in reports[c.label].findings
                            ),
                            warnings=sum(
                                f.issue.severity == "warning"
                                for f in reports[c.label].findings
                            ),
                        )
                        for c in cases
                    ],
                    qualification="Comparison triggers are not independent proof of defects; matched control panels have discovery-sample selection.",
                ),
                indent=2,
            ),
            encoding="utf-8",
        )
    return reports


RTOL = 1e-10
ATOL = 1e-12
ACTIVITY_FIELDS = ("name", "reference product", "location", "unit")
PROVIDER_FIELDS = ("name", "product", "location", "unit")


def activity_key(activity: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """Return the stable semantic identity used across independent builds."""

    return tuple(str(activity.get(field) or "") for field in ACTIVITY_FIELDS)


def provider_key(exchange: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return tuple(str(exchange.get(field) or "") for field in PROVIDER_FIELDS)


def collect_direct_metal_inputs(
    database: Iterable[Mapping[str, Any]],
    products: Iterable[tuple[str, str]] | None = None,
) -> tuple[
    dict[tuple[tuple[str, str, str, str], tuple[str, str, str, str]], float],
    frozenset[tuple[str, str, str, str]],
]:
    """Aggregate configured direct technosphere metal inputs for every dataset."""

    products = frozenset(products or configured_metal_products())
    product_labels = {product for _name, product in products}
    amounts: dict[
        tuple[tuple[str, str, str, str], tuple[str, str, str, str]], float
    ] = defaultdict(float)
    activities = set()
    for dataset in database:
        dataset_key = activity_key(dataset)
        activities.add(dataset_key)
        for exchange in dataset.get("exchanges", ()):
            if exchange.get("type") != "technosphere":
                continue
            if exchange.get("product") not in product_labels:
                continue
            amounts[(dataset_key, provider_key(exchange))] += float(
                exchange.get("amount", 0.0)
            )
    return dict(amounts), frozenset(activities)


def _close(left: float, right: float) -> bool:
    return bool(np.isclose(left, right, rtol=RTOL, atol=ATOL))


def _decision_index(
    decisions: Sequence[Mapping[str, Any]],
) -> dict[tuple[tuple[str, str, str, str], str], list[Mapping[str, Any]]]:
    index = defaultdict(list)
    for decision in decisions:
        activity = decision.get("activity", {})
        index[
            (
                activity_key(activity),
                str(decision.get("provider product") or ""),
            )
        ].append(decision)
    return index


def compare_direct_metal_inputs(
    source: Iterable[Mapping[str, Any]],
    before: Iterable[Mapping[str, Any]],
    after: Iterable[Mapping[str, Any]],
    *,
    decisions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return a complete, classified before/after direct-metal comparison."""

    products = configured_metal_products()
    source_amounts, source_activities = collect_direct_metal_inputs(source, products)
    before_amounts, before_activities = collect_direct_metal_inputs(before, products)
    after_amounts, after_activities = collect_direct_metal_inputs(after, products)
    if before_activities != after_activities:
        missing_after = sorted(before_activities - after_activities)
        extra_after = sorted(after_activities - before_activities)
        raise ValueError(
            "Before/after activity sets differ; compare like-for-like updated builds. "
            f"Missing after={missing_after[:5]!r}; extra after={extra_after[:5]!r}."
        )

    decision_index = _decision_index(decisions)
    all_keys = sorted(set(source_amounts) | set(before_amounts) | set(after_amounts))
    totals_by_product = defaultdict(lambda: [0.0, 0.0, 0.0])
    for (dataset, provider), amount in source_amounts.items():
        totals_by_product[(dataset, provider[1])][0] += amount
    for (dataset, provider), amount in before_amounts.items():
        totals_by_product[(dataset, provider[1])][1] += amount
    for (dataset, provider), amount in after_amounts.items():
        totals_by_product[(dataset, provider[1])][2] += amount

    rows = []
    for dataset, provider in all_keys:
        source_amount = source_amounts.get((dataset, provider), 0.0)
        before_amount = before_amounts.get((dataset, provider), 0.0)
        after_amount = after_amounts.get((dataset, provider), 0.0)
        changed = not _close(before_amount, after_amount)
        matching_decisions = decision_index.get((dataset, provider[1]), ())
        last_decision = matching_decisions[-1] if matching_decisions else None
        classification = "unchanged"
        justification = "No direct metal-input difference."

        if changed:
            preserve = [
                item
                for item in matching_decisions
                if item.get("reason code") == "metals.material_rule.preserved_source"
            ]
            source_total, before_total, after_total = totals_by_product[
                (dataset, provider[1])
            ]
            if preserve and _close(after_total, source_total):
                classification = "epr_preserve_source"
                justification = preserve[-1].get("explanation", "")
            elif matching_decisions and _close(before_total, after_total):
                classification = "provider_selection_normalized"
                justification = "Provider location follows the configured preference."
            elif last_decision is not None and _close(
                after_total,
                float(last_decision.get("target direct amount", float("nan"))),
            ):
                classification = (
                    "missing_rule_now_applied"
                    if _close(before_total, 0.0)
                    else "material_rule_output_corrected"
                )
                justification = last_decision.get("explanation", "")
            else:
                classification = "unexpected"
                justification = "No material decision explains this difference."

        delta = after_amount - before_amount
        relative_delta = None
        if not _close(before_amount, 0.0):
            relative_delta = delta / abs(before_amount)
        rows.append(
            {
                "dataset name": dataset[0],
                "dataset reference product": dataset[1],
                "dataset location": dataset[2],
                "dataset unit": dataset[3],
                "provider name": provider[0],
                "provider product": provider[1],
                "provider location": provider[2],
                "provider unit": provider[3],
                "source amount": source_amount,
                "before amount": before_amount,
                "after amount": after_amount,
                "absolute delta": delta,
                "relative delta": relative_delta,
                "classification": classification,
                "justification": justification,
                "material rule id": (
                    last_decision.get("material rule id") if last_decision else None
                ),
                "reason code": (
                    last_decision.get("reason code") if last_decision else None
                ),
                "valid": classification != "unexpected",
            }
        )
    return rows
