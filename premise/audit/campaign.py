"""Sequential auditing of written graphs with a matched reference sample."""

from collections import defaultdict
from dataclasses import dataclass, field, replace
import json
import gc
from pathlib import Path
import random
import re

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
