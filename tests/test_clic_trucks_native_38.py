"""Truck source3.8 permissions retain signs, warnings and older source approvals."""

import copy
import gzip
import json
import re
from pathlib import Path

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_trucks_native_38_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_35_native_inputs_keep_signed_quantities_and_review_on_cache_replay(
    tmp_path, amount
):
    resolved = graph(*(row["source"] for row in ROWS), version="3.8")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="truck_component_dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    original = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 3
        for edge, row in zip(result.activities[0]["exchanges"][1:], ROWS, strict=True):
            assert edge["amount"] == edge["loc"] == amount * row["conversion_factor"]
            expected_formula = (
                "truck_component_dose"
                if row["conversion_factor"] == 1
                else "(truck_component_dose) * (-1.0)"
            )
            assert edge["formula"] == expected_formula and edge["scale"] == 0.2
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == original


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_source38_decisions_refuse_it_and_keep_previous_source_contexts(
    tmp_path, monkeypatch, cached
):
    resolved = graph(*(row["source"] for row in ROWS), version="3.8")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        op for op in ledger["operations"] if op["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(
        ClicInventoryError, match="requires independent review"
    ) as raised:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    audit_path = re.search(r"Diagnostic: (.*\.audit\.json\.gz)", str(raised.value))
    assert audit_path is not None
    audit = json.loads(gzip.decompress(Path(audit_path.group(1)).read_bytes()))
    refused = {
        issue["details"]["rule_id"]
        for stage in audit["stages"]
        for issue in stage.get("issues", [])
        if issue["code"] == "migration.unapproved_correspondence"
    }
    assert refused == {row["rule_id"] for row in ROWS}
    by_id = {row["rule_id"]: row for row in ROWS}
    for version, ids in [
        (
            "3.9",
            [
                "bp-b84feb5869b37bc3",
                "bp-1104956dff1c701e",
                "bp-1a39c7b4c1f58b77",
                "bp-508c267844c98459",
            ],
        ),
        ("3.12", ["bp-9ff26f9cc56aec97"]),
    ]:
        prior = harmonizer.harmonize(
            graph(*(by_id[rid]["source"] for rid in ids), version=version),
            version="2026",
            family="uvek",
        )
        assert len(prior.activities[0]["exchanges"]) == len(ids) + 1
