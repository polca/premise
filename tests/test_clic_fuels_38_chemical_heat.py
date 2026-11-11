"""Strict CLIC retains MJ heat and all physical-water uncertainty through caching."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_fuels_38_chemical_heat_review.json").read_text()
)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_heat_and_stochastic_water_survive_fresh_and_cached_conversion(
    tmp_path, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    resolved = graph(REVIEW["original_rule"]["source"], version="3.8")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="chemical_heat_mj",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
            "scale": 0.3,
            "negative": amount < 0,
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == amount and edge["unit"] == "megajoule"
        assert edge["formula"] == "chemical_heat_mj" and edge["scale"] == 0.3
        assert edge["loc"] == incoming["loc"]
        if uncertainty == 2:
            assert edge["negative"] is (amount < 0)
        ds = next(a for a in result.activities if a["code"] == edge["input"][1])
        entries = [e for e in ds["exchanges"] if e["type"] != "production"]
        assert len(entries) == 10
        assert entries[0]["name"] == "Heat, unspecific, in chemical plant"
        assert entries[0]["amount"] == 1 and entries[0]["unit"] == "megajoule"
        for actual, retained in zip(
            entries[1:], REVIEW["retained_water_rows"], strict=True
        ):
            for field, value in retained["statistics"].items():
                assert actual[field] == value
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(REVIEW["limitations"])
            for d in result.diagnostics
        )
        assert not any(
            d["code"] == "migration.recipe_source_input_omission"
            for d in result.diagnostics
        )
    assert resolved.activities == before


def test_chemical_steam_energy_needs_exact_profile_review(tmp_path, monkeypatch):
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row for row in ledger["operations"] if row["rule_id"] != REVIEW["rule_id"]
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(REVIEW["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
