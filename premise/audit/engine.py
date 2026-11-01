"""Reusable auditing of physical inventories, distinct from inventory repairs."""

from __future__ import annotations

from collections import Counter
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
import hashlib
import random
import time

import numpy as np

from .graph import AuditGraph, Factorization
from .indicators import characterize, default_indicators
from .models import AuditConfig, AuditFinding, AuditReport
from .rules import (
    RuleBook,
    check_carbon,
    check_contracts,
    check_resources_land_water,
    default_contracts,
)
from .sampling import choose_sample
from .budgets import check_budgets
from ..validation.validation_framework import ValidationIssue


def audit_database(
    inventory,
    *,
    config=None,
    biosphere=None,
    indicators=None,
    provenance=None,
    target_database=None,
    progress=None,
    _matrix_adapter=None,
):
    """Audit a list of inventory dictionaries or an :class:`InventoryStore`.

    This function never repairs exchanges, downloads data or changes Brightway
    projects. Use :func:`audit_brightway_database` for an already written graph.
    Missing providers block scoring rather than being silently assigned zero.
    Physical findings distinguish exact contracts from coverage/plausibility
    concerns; absence of findings does not certify complete scientific validity.

    Parameters
    ----------
    inventory:
        Complete linked inventory, including required technosphere suppliers.
    config: AuditConfig, optional
        Sampling, model context, independent physical contracts and tolerances.
    biosphere: mapping, optional
        ``(database, code) -> flow metadata``. When supplied, linked flow identity
        and units are checked independently of exchange names.
    indicators: sequence of Indicator, optional
        Defaults to physical carbon/water/land and 65 RELICS indicators.
    provenance: mapping, optional
        Caller-supplied build metadata. Do not include credentials.
    target_database: str, optional
        Restrict sampling to this database while including suppliers in solves.
    """
    started = time.monotonic()

    def notify(phase, completed=None, total=None):
        if progress is not None:
            progress(dict(phase=phase, completed=completed, total=total))

    config = config or AuditConfig()
    implementation = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        implementation.update(path.name.encode())
        implementation.update(path.read_bytes())
    indicators = tuple(indicators) if indicators is not None else default_indicators()
    if not indicators or len({s.key for s in indicators}) != len(indicators):
        raise ValueError("Indicators must be nonempty and have unique keys")
    graph = AuditGraph(inventory, biosphere)
    if _matrix_adapter is not None and not graph.blocked:
        _matrix_adapter(graph)
    notify("graph", len(graph.nodes), len(graph.nodes))
    CF, direct, coverage = characterize(graph, indicators)
    direct_positive = (CF @ graph.B_positive).tocsc()
    direct_negative = (CF @ graph.B_negative).tocsc()
    book = RuleBook(graph)
    findings = list(graph.findings)
    metadata = dict(
        ruleset_version=1,
        audit_implementation=implementation.hexdigest(),
        physical_contracts=[
            asdict(c)
            for c in (
                (*default_contracts(), *config.contracts)
                if config.include_default_contracts
                else config.contracts
            )
        ],
        exchange_budgets=[asdict(b) for b in config.budgets],
        provenance=provenance or {},
        version=config.version,
        system_model=config.system_model,
        activities=len(graph.nodes),
        biosphere_flows=len(graph.flows),
        requested_sample=config.sample_size,
        seed=config.seed,
        scoring_status="blocked" if graph.blocked else "pending",
        default_contracts=config.include_default_contracts,
        functional_unit="+1 reference-product unit; interpret negative reference outputs explicitly",
        signed_contribution_scope="positive/negative split first-tier supplier scores plus raw direct biosphere contributions; not gross physical extraction",
        numerical_tolerances=dict(
            rtol=config.numerical_rtol, atol=config.numerical_atol
        ),
        scientific_scope="Contract checks, carbon-origin tracing and conservative coverage screens; not certification",
        relics_version="0.0.6 (bundled BSD-3-Clause mapping)",
    )
    empty = np.empty((0, len(indicators)))

    def blocked(message=None):
        if message:
            findings.append(
                AuditFinding(
                    ValidationIssue("NUMERICAL.SOLVE", "error", message),
                    "numerical",
                    "high",
                )
            )
        metadata.update(
            scoring_status="blocked", elapsed_seconds=time.monotonic() - started
        )
        return AuditReport(
            metadata,
            tuple(s.to_dict() for s in indicators),
            (),
            tuple(findings),
            tuple(coverage),
            book.coverage(),
            empty,
            empty,
            empty,
            empty,
        )

    if not graph.nodes or graph.blocked:
        return blocked("No resolvable inventory graph" if not graph.nodes else None)
    try:
        factor = Factorization(graph.A)
        all_scores = np.stack(
            [factor.solve(row.toarray().ravel(), transpose=True) for row in direct]
        )
        notify("adjoint_scores", len(indicators), len(indicators))
        if not np.all(np.isfinite(all_scores)):
            return blocked("Nonfinite adjoint scores; inventory may be singular")
    except (RuntimeError, ValueError) as exc:
        return blocked(f"Technosphere factorization/solve failed: {exc}")
    adjoint_difference = abs(graph.A.T @ all_scores.T - direct.T.toarray())
    adjoint_residual = np.max(adjoint_difference, axis=0)
    adjoint_scale = abs(graph.A.T) @ abs(all_scores.T) + abs(direct.T.toarray())
    adjoint_tolerance_ratio = float(
        np.max(
            adjoint_difference
            / np.maximum(
                config.numerical_atol + config.numerical_rtol * adjoint_scale,
                np.finfo(float).tiny,
            )
        )
    )
    scaled_adjoint_residual = adjoint_residual / np.maximum(
        1.0, np.max(abs(direct.toarray()), axis=1)
    )
    check_carbon(graph, book, config)
    check_contracts(graph, book, config, indicators, direct, all_scores)
    check_resources_land_water(graph, book, config, indicators, all_scores)
    check_budgets(graph, book, config)
    notify("physical_rules", len(graph.nodes), len(graph.nodes))
    findings.extend(book.findings)
    physical_finding_count = len(book.findings)
    eligible = (
        {i for i, n in enumerate(graph.nodes) if n.get("database") == target_database}
        if target_database
        else None
    )
    if eligible is not None and not eligible:
        raise ValueError("The target database has no process activities")
    indices, sample = choose_sample(
        graph, config, all_scores, indicators, eligible, findings
    )
    shape = len(indices), len(indicators)
    totals, local, positive, negative = (np.empty(shape) for _ in range(4))
    max_error = np.zeros(len(indicators))
    forward_residuals = []
    forward_residual_ratios = []
    explanations = []
    selected_findings = {}
    for finding in findings:
        if finding.indicator is not None and finding.issue.activity_id is not None:
            selected_findings.setdefault(finding.issue.activity_id, set()).add(
                finding.indicator
            )
    indicator_index = {s.key: j for j, s in enumerate(indicators)}
    rhs = np.zeros(len(graph.nodes))
    for p, i in enumerate(indices):
        if p % 250 == 0:
            notify("forward_scores", p, len(indices))
        sample[p]["reference_output"] = float(graph.production[i])
        sample[p]["demand"] = 1.0
        rhs[i] = 1.0
        supply = factor.solve(rhs)
        if not np.all(np.isfinite(supply)):
            return blocked(f"Nonfinite forward inventory for {graph.keys[i]}")
        totals[p] = direct @ supply
        local[p] = direct[:, i].toarray().ravel() / graph.production[i]
        # A solved LCI can already cancel positive and negative supplies of
        # the very same upstream process. Split first-tier supplier scores
        # instead, preserving both branches without pretending to reconstruct
        # physical gross mining throughput from an allocated graph.
        local_parts = [
            matrix[:, i].toarray().ravel() / graph.production[i]
            for matrix in (direct_positive, direct_negative)
        ]
        positive[p] = sum(np.maximum(v, 0) for v in local_parts)
        negative[p] = sum(np.minimum(v, 0) for v in local_parts)
        for provider, amount, exc in getattr(graph, "scoring_inputs", graph.inputs)[i]:
            part = amount / graph.production[i] * all_scores[:, provider]
            positive[p] += np.maximum(part, 0)
            negative[p] += np.minimum(part, 0)
        error = abs(totals[p] - all_scores[:, i])
        max_error = np.maximum(max_error, error)
        tolerance = config.numerical_atol + config.numerical_rtol * np.maximum(
            abs(totals[p]), abs(all_scores[:, i])
        )
        book.status("NUMERICAL.FORWARD_ADJOINT")
        if np.any(error > tolerance):
            book.emit(
                "NUMERICAL.FORWARD_ADJOINT",
                i,
                "Forward and adjoint indicator scores disagree",
                category="numerical",
                confidence="high",
                severity="error",
                actual=float(max(error)),
                evidence=dict(
                    indicators=[
                        indicators[j].key for j in np.flatnonzero(error > tolerance)
                    ]
                ),
            )
        if p % 50 == 0:
            residual = graph.A @ supply - rhs
            # Componentwise backward error avoids interpreting heterogeneous
            # physical matrix scales as a universal absolute residual bound.
            scale = abs(graph.A) @ abs(supply) + abs(rhs)
            forward_residuals.append(
                float(max(abs(residual)) / max(1.0, float(max(scale))))
            )
            tolerance = config.numerical_atol + config.numerical_rtol * scale
            forward_residual_ratios.append(
                float(max(abs(residual) / np.maximum(tolerance, np.finfo(float).tiny)))
            )
        for name in sorted(selected_findings.get(i, ())):
            if (
                len(explanations) >= config.explanation_limit
                or name not in indicator_index
            ):
                continue
            j = indicator_index[name]
            contributions = direct.multiply(supply[None, :]).tocsr()
            row = contributions.getrow(j)
            ordered = sorted(zip(row.indices, row.data), key=lambda v: -abs(v[1]))[
                : config.contribution_limit
            ]
            first_by_provider = Counter()
            for provider, amount, exc in getattr(graph, "scoring_inputs", graph.inputs)[
                i
            ]:
                if provider != i:
                    first_by_provider[provider] += (
                        amount / graph.production[i] * all_scores[j, provider]
                    )
            first = sorted(first_by_provider.items(), key=lambda v: -abs(v[1]))[
                : config.contribution_limit
            ]
            explanations.append(
                dict(
                    activity=list(graph.keys[i]),
                    indicator=name,
                    total=float(totals[p, j]),
                    direct=float(local[p, j]),
                    extraction_processes=[
                        dict(activity=list(graph.keys[k]), contribution=float(v))
                        for k, v in ordered
                    ],
                    selected_process_residual=float(
                        totals[p, j] - sum(v for k, v in ordered)
                    ),
                    first_tier=[
                        dict(activity=list(graph.keys[k]), contribution=float(v))
                        for k, v in first
                    ],
                )
            )
        rhs[i] = 0.0
    # Append numerical findings emitted after the whole-inventory rule pass.
    findings.extend(book.findings[physical_finding_count:])
    if not np.all(np.isfinite(totals)):
        return blocked("Nonfinite sampled scores")
    metadata.update(
        scoring_status="completed",
        sampled_activities=len(indices),
        factorizations=1,
        solver=factor.backend,
        forward_solves=factor.forward,
        transpose_solves=factor.transpose,
        max_forward_adjoint_error=max_error.tolist(),
        max_scaled_forward_residual=max(forward_residuals, default=0),
        max_forward_residual_tolerance_ratio=max(forward_residual_ratios, default=0),
        max_scaled_adjoint_residual=scaled_adjoint_residual.tolist(),
        max_adjoint_residual_tolerance_ratio=adjoint_tolerance_ratio,
        sampled_panel_counts=dict(Counter(r["panel"] for r in sample)),
        elapsed_seconds=time.monotonic() - started,
    )
    if max(forward_residual_ratios, default=0) > 1 or adjoint_tolerance_ratio > 1:
        findings.append(
            AuditFinding(
                ValidationIssue(
                    "NUMERICAL.RESIDUAL",
                    "error",
                    "Linear-system residual exceeds the configured numerical tolerance",
                ),
                "numerical",
                "high",
            )
        )
    screen = []
    universe = (
        sorted(eligible) if eligible is not None else list(range(len(graph.nodes)))
    )
    for j, spec in enumerate(indicators):
        values = all_scores[j, universe]
        coverage[j].update(
            zero_product_scores=int(sum(values == 0)),
            negative_product_scores=int(sum(values < 0)),
        )
        for i in sorted(universe, key=lambda k: abs(all_scores[j, k]), reverse=True)[
            :5
        ]:
            screen.append(
                dict(
                    activity=list(graph.keys[i]),
                    indicator=spec.key,
                    value=float(all_scores[j, i]),
                    qualification="Magnitude screen only; heterogeneous reference units are not comparable",
                )
            )
    metadata["whole_graph_screen_activities"] = len(universe)
    flagged = {f.activity for f in findings if f.activity is not None}
    review_groups = defaultdict(list)
    review_rng = random.Random(config.seed + 2)
    for p, row in enumerate(sample):
        if tuple(row["activity"]) not in flagged:
            review_groups[row["stratum"]].append(p)
    for values in review_groups.values():
        review_rng.shuffle(values)
    selected_review = []
    review_count = min(
        config.manual_review_size, sum(len(v) for v in review_groups.values())
    )
    while len(selected_review) < review_count:
        for name in sorted(review_groups):
            if review_groups[name] and len(selected_review) < review_count:
                selected_review.append(review_groups[name].pop())
    review = []
    for p in selected_review:
        i = indices[p]
        observations = []
        for indicator in (
            "co2.fossil",
            "metals.Copper",
            "water.freshwater",
            "land.occupation",
        ):
            if indicator not in indicator_index:
                continue
            j = indicator_index[indicator]
            contributions_by_provider = Counter()
            for provider, amount, exc in getattr(graph, "scoring_inputs", graph.inputs)[
                i
            ]:
                contributions_by_provider[provider] += (
                    amount / graph.production[i] * all_scores[j, provider]
                )
            selected = sorted(
                contributions_by_provider.items(), key=lambda item: -abs(item[1])
            )[: config.contribution_limit]
            observations.append(
                dict(
                    indicator=indicator,
                    unit=indicators[j].unit,
                    total=float(totals[p, j]),
                    direct=float(local[p, j]),
                    providers=[
                        dict(activity=list(graph.keys[k]), contribution=float(v))
                        for k, v in selected
                    ],
                    residual=float(
                        totals[p, j] - local[p, j] - sum(v for k, v in selected)
                    ),
                )
            )
        review.append(
            dict(
                activity=sample[p]["activity"],
                stratum=sample[p]["stratum"],
                status="not_reviewed",
                observations=observations,
            )
        )
    metadata["manual_review_status"] = (
        "Queued unflagged controls; no human review implied"
    )
    metadata["manual_review_activities"] = len(review)
    notify("completed", len(indices), len(indices))
    for array in (totals, local, positive, negative):
        array.setflags(write=False)
    return AuditReport(
        metadata,
        tuple(s.to_dict() for s in indicators),
        tuple(sample),
        tuple(findings),
        tuple(coverage),
        book.coverage(),
        totals,
        local,
        positive,
        negative,
        tuple(explanations),
        tuple(screen),
        tuple(review),
    )


def compare_audits(before, after, *, threshold_percent=20.0):
    """Return comparison triggers, including new zeros, without declaring errors.

    Compare only exact functional identities and indicator units. Relative
    changes divide by absolute baseline. Zero baselines have no percentage.
    Unmatched identities and indicators are retained as explicit events.
    """
    if threshold_percent < 0 or not np.isfinite(threshold_percent):
        raise ValueError("Comparison threshold must be finite and nonnegative")
    a = {tuple(s["activity"]): i for i, s in enumerate(before.sample)}
    b = {tuple(s["activity"]): i for i, s in enumerate(after.sample)}
    ai = {s["key"]: (j, s["unit"]) for j, s in enumerate(before.indicators)}
    bi = {s["key"]: (j, s["unit"]) for j, s in enumerate(after.indicators)}
    rows = []
    for key in sorted(a.keys() | b.keys()):
        if key not in a or key not in b:
            rows.append(
                dict(
                    activity=list(key),
                    event="unmatched_activity",
                    stage="after" if key not in a else "before",
                )
            )
            continue
        for indicator in sorted(ai.keys() | bi.keys()):
            if indicator not in ai or indicator not in bi:
                rows.append(
                    dict(
                        activity=list(key),
                        indicator=indicator,
                        event="unmatched_indicator",
                    )
                )
                continue
            aj, au = ai[indicator]
            bj, bu = bi[indicator]
            if au != bu:
                raise ValueError(f"Incompatible indicator units: {indicator}")
            for field in ("flow_unit", "rules"):
                if before.indicators[aj].get(field) != after.indicators[bj].get(field):
                    raise ValueError(f"Incompatible indicator definitions: {indicator}")
            old, new = float(before.total[a[key], aj]), float(after.total[b[key], bj])
            percent = 100 * (new - old) / abs(old) if old != 0 else None
            if old == new:
                continue
            event = (
                "zero_to_nonzero"
                if old == 0
                else (
                    "nonzero_to_zero"
                    if new == 0
                    else "sign_change" if old * new < 0 else "relative_change"
                )
            )
            if event != "relative_change" or abs(percent) > threshold_percent:
                rows.append(
                    dict(
                        activity=list(key),
                        indicator=indicator,
                        unit=au,
                        before_score=old,
                        after_score=new,
                        absolute_change=new - old,
                        change_percent=percent,
                        event=event,
                    )
                )
    return rows
