"""Restored material scopes work cold and cached without dropping caller inputs."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_deferred_materials_review.json").read_text()
)
CASES = [
    (r, p["source_profile"]["version"]) for r in REVIEW["rules"] for p in r["profiles"]
]


@pytest.mark.parametrize(
    "row,version", CASES, ids=[r["rule_id"] + "-" + v for r, v in CASES]
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_restored_material_is_not_omitted_and_retains_portable_limits(
    tmp_path, row, version, amount
):
    resolved = graph(row["source"], version=version)
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        actual = result.activities[0]["exchanges"][1]
        assert all(actual[k] == v for k, v in row["target"].items())
        assert all(
            actual[k] == exchange[k]
            for k in ("amount", "formula", "uncertainty type", "loc", "scale")
        )
        limits = [
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
        ]
        assert any(d["message"] == row["limitations"] for d in limits)
    assert resolved.activities == before


def test_restoration_ledger_preserves_original_pins_and_signed_exchanges():
    from premise.inventory.clic_omissions import REVIEW_PATH

    path = REVIEW_PATH.parent / "restored_screening_deferrals.json"
    ledger = json.loads(path.read_text())
    restoration = next(
        r
        for r in ledger["restorations"]
        if r["review_path"].endswith("uvek_deferred_materials_review.json")
    )
    restored = restoration["restored_decisions"]
    assert len(restored) == 21
    assert all(
        r["screening_only"] and r["sha256"] and "amount" in r["exchange"]
        for r in restored
    )
    current = json.loads(REVIEW_PATH.read_text())["decisions"]
    assert not {r["id"] for r in restored} & {r["id"] for r in current}
