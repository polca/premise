"""Final unsupported GOES remains an exact, visible omission rather than a proxy."""

import copy
import hashlib
import json

import pytest

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_omissions import REVIEW_PATH, prepare_approved_omissions

DECISION = next(
    d
    for d in json.loads(REVIEW_PATH.read_text())["decisions"]
    if d.get("deferred_rule_id") == "bp-0baf74e5f1f09452"
)


def graph():
    row = {
        k: copy.deepcopy(DECISION[k])
        for k in ("slug", "version_id", "semantic_version", "sha256")
    }
    row.update(
        activity_code="transformer", background=copy.deepcopy(DECISION["source"])
    )
    activities = [
        {
            "code": row["activity_code"],
            "exchanges": [
                {"type": "production", "amount": 1},
                {"type": "technosphere", "name": "retained copper", "amount": 12},
                copy.deepcopy(DECISION["exchange"]),
            ],
        }
    ]
    return activities, [row], copy.deepcopy(DECISION["target"])


def test_final_goes_decision_keeps_exact_original_and_review_warning():
    activities, versions, target = graph()
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert activities == before and result[0]["exchanges"] == before[0]["exchanges"][:2]
    (audit,) = audits
    details = audit["details"]
    assert details["source_exchange"] == DECISION["exchange"]
    assert details["source_exchange"]["amount"] == 947.32
    assert details["resolution_status"] == "final_unsupported"
    assert details["resolution_review"] == DECISION["resolution_review"]
    assert details["screening_only"] and "FINAL UNSUPPORTED" in audit["message"]
    assert "unquantified" in audit["message"]
    review_path = REVIEW_PATH.parent / "uvek_final_goes_review.json"
    assert (
        hashlib.sha256(review_path.read_bytes()).hexdigest()
        == details["resolution_review"]["sha256"]
    )
    review = json.loads(review_path.read_text())
    assert review["producer_reference"]["technosphere_exchanges"] == 0
    assert review["producer_reference"]["biosphere_exchanges"] == 2747
    assert len(review["rejected_native_witnesses"]) == 4
    details["resolution_review"]["path"] = "changed"
    assert DECISION["resolution_review"]["path"] != "changed" and activities == before


@pytest.mark.parametrize("change", ["pin", "quantity", "sign", "formula", "ownership"])
def test_final_status_never_broadens_exact_scope(change):
    activities, versions, target = graph()
    if change == "pin":
        versions[0]["sha256"] = "0" * 64
    elif change == "quantity":
        activities[0]["exchanges"][-1]["amount"] += 1
    elif change == "sign":
        activities[0]["exchanges"][-1]["amount"] *= -1
    elif change == "formula":
        activities[0]["exchanges"][-1]["formula"] = "mass"
    else:
        activities[0]["exchanges"][-1]["input"] = ("__clic__", "owned")
    before = copy.deepcopy(activities)
    with pytest.raises(ClicInventoryError, match="omission approval does not cover"):
        prepare_approved_omissions(activities, versions, target)
    assert activities == before


def test_other_destination_keeps_original_steel():
    activities, versions, target = graph()
    target["technosphere"].update(family="ecoinvent", version="3.12")
    before = copy.deepcopy(activities)
    result, audits = prepare_approved_omissions(activities, versions, target)
    assert result == activities == before and not audits
