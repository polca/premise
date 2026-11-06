"""Run ``python -m premise.audit`` against an explicit private Brightway project."""

import argparse
import json
import os
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    selection = p.add_mutually_exclusive_group(required=True)
    selection.add_argument("--database")
    selection.add_argument(
        "--campaign",
        type=Path,
        help="JSON list of label/database/config records; first case defines the discovery sample",
    )
    p.add_argument("--project", required=True)
    p.add_argument("--brightway-dir", type=Path)
    p.add_argument("--sample-size", type=int, default=5000)
    p.add_argument("--seed", type=int, default=20261101)
    p.add_argument("--system-model", choices=("cutoff", "consequential", "EN15804"))
    p.add_argument("--version")
    p.add_argument(
        "--budgets",
        type=Path,
        help="JSON exchange-ledger definitions with BudgetTerm records",
    )
    p.add_argument(
        "--contracts",
        type=Path,
        help="JSON list of independently sourced PhysicalContract definitions",
    )
    p.add_argument(
        "--native",
        action="store_true",
        help="Disable Premise-specific default contracts for a native source control",
    )
    p.add_argument("--crosscheck-size", type=int, default=250)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        p.error("Output directory already exists; choose a new audit directory")
    if args.brightway_dir:
        if not args.brightway_dir.is_dir():
            p.error("Brightway directory does not exist")
        os.environ["BRIGHTWAY2_DIR"] = str(args.brightway_dir.resolve())
    import bw2data as bd
    from . import (
        AuditConfig,
        PhysicalContract,
        BudgetTerm,
        ExchangeBudget,
        audit_brightway_database,
        AuditCase,
        audit_campaign,
    )

    if args.project not in bd.projects:
        p.error("Brightway project does not exist; audit will not create it")
    bd.projects.set_current(args.project)
    contracts = (
        tuple(PhysicalContract(**c) for c in json.loads(args.contracts.read_text()))
        if args.contracts
        else ()
    )
    budgets = (
        tuple(
            ExchangeBudget(**dict(b, terms=tuple(BudgetTerm(**t) for t in b["terms"])))
            for b in json.loads(args.budgets.read_text())
        )
        if args.budgets
        else ()
    )
    base = dict(
        sample_size=args.sample_size,
        seed=args.seed,
        version=args.version,
        system_model=args.system_model,
        contracts=contracts,
        budgets=budgets,
        include_default_contracts=not args.native,
    )
    if args.campaign:
        cases = []
        for entry in json.loads(args.campaign.read_text()):
            values = dict(base, **entry.get("config", {}))
            values["contracts"] = tuple(
                PhysicalContract(**c) if isinstance(c, dict) else c
                for c in values["contracts"]
            )
            values["budgets"] = tuple(
                (
                    ExchangeBudget(
                        **dict(b, terms=tuple(BudgetTerm(**t) for t in b["terms"]))
                    )
                    if isinstance(b, dict)
                    else b
                )
                for b in values["budgets"]
            )
            cases.append(
                AuditCase(entry["label"], entry["database"], AuditConfig(**values))
            )
        reports = audit_campaign(
            cases,
            output=args.output,
            crosscheck_size=args.crosscheck_size,
            progress=lambda event: print(json.dumps(event), flush=True),
        )
        print(
            json.dumps(
                dict(
                    output=str(args.output),
                    cases=list(reports),
                    errors=sum(
                        sum(f.issue.severity == "error" for f in r.findings)
                        for r in reports.values()
                    ),
                )
            )
        )
        return 1 if any(r.has_errors for r in reports.values()) else 0
    report = audit_brightway_database(
        args.database,
        config=AuditConfig(**base),
        crosscheck_size=args.crosscheck_size,
        progress=lambda event: print(json.dumps(event), flush=True),
    )
    report.write(args.output)
    print(
        json.dumps(
            dict(
                output=str(args.output),
                scoring_status=report.metadata["scoring_status"],
                sample_size=len(report.sample),
                findings=len(report.findings),
                errors=sum(f.issue.severity == "error" for f in report.findings),
            )
        )
    )
    return 1 if report.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
