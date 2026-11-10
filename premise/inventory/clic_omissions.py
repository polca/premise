"""Identity-matched CLIC omissions with owned original-exchange audit records."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from .clic_archive import ClicInventoryError, activity_identity

REVIEW_PATH = Path(__file__).resolve().parents[1] / "data/clic/approved_omissions.json"


def _identity(record):
    return activity_identity(record)[:3]


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
    """Match dataset and exchange names, reference products and locations.

    Version pins and full-record hashes are historical approval evidence, not
    matching criteria. Audit the actual source exchange, including its quantity.
    Other consumers and destination contexts keep their exchanges.
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
            if (
                decision["target"] != target
                or not row.get("slug")
                or _identity(activity) != _identity(decision["activity"])
            ):
                continue
            indices = [
                index
                for index, exchange in enumerate(activity["exchanges"])
                if _identity(exchange) == _identity(decision["exchange"])
                and exchange.get("type") == decision["exchange"].get("type")
            ]
            if not indices:
                continue
            differences = []
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
            original_index = original_indices[id(exchange)]
            audit = {
                "severity": "warning",
                "code": "migration.technosphere_omission",
                "message": (
                    f"{decision['message']} Actual omitted quantity: "
                    f"{exchange.get('amount')} {exchange.get('unit')}."
                ),
                "path": f"datasets[{activity['code']}].exchanges[{original_index}]",
                "details": {
                    "decision_id": decision["id"],
                    "matching_basis": ["name", "reference product", "location"],
                    "approved_activity": copy.deepcopy(decision["activity"]),
                    "approved_exchange": copy.deepcopy(decision["exchange"]),
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
