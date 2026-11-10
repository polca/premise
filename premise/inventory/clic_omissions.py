"""Exact, approved CLIC omissions with owned original-exchange audit records."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from .clic_archive import ClicInventoryError, activity_identity, content_hash

REVIEW_PATH = Path(__file__).resolve().parents[1] / "data/clic/approved_omissions.json"


def _approval_error(activity, row, decision, target, differences):
    """Identify the approval and the values that need another review."""
    return ClicInventoryError(
        "CLIC omission approval does not cover the selected dataset or exchange.\n"
        f"Dataset: {activity['name']} ({row['slug']})\n"
        f"Approval: {decision['id']}\n"
        f"Exchange: {decision['exchange']['name']}\n"
        f"Target background: {json.dumps(target, sort_keys=True)}\n"
        + "\n".join(differences)
        + f"\nApproval file: {REVIEW_PATH}\n"
        "Conversion stopped; the changed inventory needs review before this "
        "exchange can be omitted."
    )


def prepare_approved_omissions(activities, versions, target):
    """Omit only an exact pinned record and exchange in its approved context.

    Changes in the source pin, quantity, uncertainty or foreground ownership
    require another review. Other consumers and destination contexts keep their
    exchanges and continue through the ordinary migration gates.
    """
    review = json.loads(REVIEW_PATH.read_text(encoding="utf-8"))
    if review.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported CLIC omission review schema.")
    rows = {row["activity_code"]: row for row in versions}
    records, audits = copy.deepcopy(activities), []
    for activity in records:
        row = rows[activity["code"]]
        original_indices = {
            id(exchange): index for index, exchange in enumerate(activity["exchanges"])
        }
        for decision in review["decisions"]:
            if decision["target"] != target or row.get("slug") != decision["slug"]:
                continue
            indices = [
                index
                for index, exchange in enumerate(activity["exchanges"])
                if activity_identity(exchange)
                == activity_identity(decision["exchange"])
            ]
            if not indices:
                continue
            differences = []
            for key, label in (
                ("version_id", "Version ID"),
                ("semantic_version", "Dataset version"),
                ("sha256", "Inventory hash (sha256)"),
            ):
                if row.get(key) != decision[key]:
                    differences.append(
                        f"{label}: approved {decision[key]!r}; received {row.get(key)!r}"
                    )
            if row["background"] != decision["source"]:
                differences.append(
                    f"Source background: approved {json.dumps(decision['source'], sort_keys=True)}; "
                    f"received {json.dumps(row['background'], sort_keys=True)}"
                )
            if len(indices) != 1:
                differences.append(
                    f"Matching exchanges: approved 1; received {len(indices)} "
                    f"at indices {indices}"
                )
            if differences:
                raise _approval_error(activity, row, decision, target, differences)
            index = indices[0]
            exchange = activity["exchanges"][index]
            if content_hash(exchange) != content_hash(decision["exchange"]):
                expected = decision["exchange"]
                differences = [
                    f"Exchange field {key!r}: approved {expected.get(key, '<missing>')!r}; "
                    f"received {exchange.get(key, '<missing>')!r}"
                    for key in sorted(expected.keys() | exchange.keys())
                    if key not in expected
                    or key not in exchange
                    or content_hash(expected[key]) != content_hash(exchange[key])
                ]
                raise _approval_error(activity, row, decision, target, differences)
            original_index = original_indices[id(exchange)]
            audit = {
                "severity": "warning",
                "code": "migration.technosphere_omission",
                "message": decision["message"],
                "path": f"datasets[{activity['code']}].exchanges[{original_index}]",
                "details": {
                    "decision_id": decision["id"],
                    "source_pin": {
                        key: row[key]
                        for key in ("slug", "version_id", "semantic_version", "sha256")
                    },
                    "source": copy.deepcopy(row["background"]),
                    "target": copy.deepcopy(target),
                    "source_exchange": copy.deepcopy(exchange),
                    "activity_code": activity["code"],
                    "approved_omission": True,
                    "original_exchange_index": original_index,
                    "screening_only": decision.get("screening_only", False),
                    "deferred_rule_id": decision.get("deferred_rule_id"),
                },
            }
            for key in ("resolution_status", "resolution_review"):
                if key in decision:
                    audit["details"][key] = copy.deepcopy(decision[key])
            audits.append(audit)
            del activity["exchanges"][index]
    return records, audits
