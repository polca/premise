"""Exact, approved CLIC omissions with owned original-exchange audit records."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from .clic_archive import ClicInventoryError, activity_identity, content_hash

REVIEW_PATH = Path(__file__).resolve().parents[1] / "data/clic/approved_omissions.json"


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
            if (
                any(
                    row.get(key) != decision[key]
                    for key in ("version_id", "semantic_version", "sha256")
                )
                or row["background"] != decision["source"]
                or len(indices) != 1
            ):
                raise ClicInventoryError(
                    "CLIC omission approval does not cover this source pin or context."
                )
            index = indices[0]
            exchange = activity["exchanges"][index]
            if content_hash(exchange) != content_hash(decision["exchange"]):
                raise ClicInventoryError(
                    "CLIC omission approval does not cover this changed exchange."
                )
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
            audits.append(audit)
            del activity["exchanges"][index]
    return records, audits
