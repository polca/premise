"""Dataset-wise comparison of direct metal inputs across database builds."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from .metals_rules import load_material_rules

RTOL = 1e-10
ATOL = 1e-12
ACTIVITY_FIELDS = ("name", "reference product", "location", "unit")
PROVIDER_FIELDS = ("name", "product", "location", "unit")


def activity_key(activity: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """Return the stable semantic identity used across independent builds."""

    return tuple(str(activity.get(field) or "") for field in ACTIVITY_FIELDS)


def provider_key(exchange: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return tuple(str(exchange.get(field) or "") for field in PROVIDER_FIELDS)


def configured_metal_products() -> frozenset[tuple[str, str]]:
    """Return provider name/product pairs covered by enabled material rules."""

    return frozenset(
        (provider.name, provider.reference_product)
        for rule in load_material_rules().enabled_rules
        if rule.provider is not None
        for provider in (
            rule.provider,
            rule.provider.for_version("3.11"),
            rule.provider.for_version("3.12"),
        )
    )


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


def write_comparison_bundle(
    rows: Sequence[Mapping[str, Any]],
    output_directory: str | Path,
    *,
    metadata: Mapping[str, Any] | None = None,
    decisions: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[Path, Path, Path]:
    """Write complete, unexpected-only, and metadata validation artifacts."""

    directory = Path(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    complete_path = directory / "dataset_metal_input_differences.csv"
    unexpected_path = directory / "unexpected_differences.csv"
    metadata_path = directory / "build_metadata.json"
    headers = (
        list(rows[0])
        if rows
        else [
            "dataset name",
            "dataset reference product",
            "dataset location",
            "dataset unit",
            "provider name",
            "provider product",
            "provider location",
            "provider unit",
            "source amount",
            "before amount",
            "after amount",
            "absolute delta",
            "relative delta",
            "classification",
            "justification",
            "material rule id",
            "reason code",
            "valid",
        ]
    )
    with complete_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    with unexpected_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(row for row in rows if not row.get("valid"))

    identity_headers = headers[:8]
    for label, amount_field in (
        ("source", "source amount"),
        ("before_fix", "before amount"),
        ("after_fix", "after amount"),
    ):
        path = directory / f"dataset_metal_inputs_{label}.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=[*identity_headers, "amount"])
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        **{header: row.get(header) for header in identity_headers},
                        "amount": row.get(amount_field),
                    }
                )

    changed_rows = [row for row in rows if row.get("classification") != "unchanged"]
    with (directory / "difference_justifications.csv").open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(changed_rows)

    decision_counts = defaultdict(int)
    for decision in decisions or ():
        activity = decision.get("activity", {})
        key = (activity_key(activity), decision.get("material rule id"))
        decision_counts[key] += 1
    provenance_headers = [
        "dataset name",
        "dataset reference product",
        "dataset location",
        "dataset unit",
        "material rule id",
        "decision count",
        "valid",
    ]
    with (directory / "provenance_cardinality.csv").open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=provenance_headers)
        writer.writeheader()
        for key, count in sorted(
            decision_counts.items(), key=lambda item: str(item[0])
        ):
            writer.writerow(
                {
                    "dataset name": key[0][0],
                    "dataset reference product": key[0][1],
                    "dataset location": key[0][2],
                    "dataset unit": key[0][3],
                    "material rule id": key[1],
                    "decision count": count,
                    "valid": count == 1,
                }
            )

    classifications = defaultdict(int)
    for row in rows:
        classifications[row.get("classification")] += 1
    summary_lines = [
        "# Premise metals validation summary",
        "",
        f"- Compared direct metal inputs: {len(rows)}",
        f"- Changed inputs: {len(changed_rows)}",
        f"- Unexpected differences: {sum(not row.get('valid') for row in rows)}",
        f"- Material decisions: {len(decisions or ())}",
        f"- Repeated dataset/rule decisions: {sum(count > 1 for count in decision_counts.values())}",
        "",
        "## Classifications",
        "",
        "| Classification | Rows |",
        "|---|---:|",
    ]
    summary_lines.extend(
        f"| {classification} | {count} |"
        for classification, count in sorted(classifications.items())
    )
    (directory / "summary.md").write_text(
        "\n".join(summary_lines) + "\n", encoding="utf-8"
    )
    metadata_path.write_text(
        json.dumps(dict(metadata or {}), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return complete_path, unexpected_path, metadata_path


__all__ = [
    "ATOL",
    "RTOL",
    "activity_key",
    "collect_direct_metal_inputs",
    "compare_direct_metal_inputs",
    "configured_metal_products",
    "provider_key",
    "write_comparison_bundle",
]
