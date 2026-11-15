"""Grid family uses the normal strict harmonizer and preserves earlier source scopes."""

import copy
import json
import math
import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph
from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_separator_grids_39_review.json").read_text()
)
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_all40_grid_aliases_signed_statistics_warnings_fresh_and_cache(
    tmp_path, amount, uncertainty
):
    resolved = graph(*(row["original_rule"]["source"] for row in ROWS), version="3.9")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="electricity_demand",
            **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
        )
        if uncertainty == 2:
            edge.update(loc=math.log(abs(amount)), negative=amount < 0)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in [False, True]:
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert (
            len(result.activities) == 1 and len(result.activities[0]["exchanges"]) == 41
        )
        for row, old, new in zip(
            ROWS,
            before[0]["exchanges"][1:],
            result.activities[0]["exchanges"][1:],
            strict=True,
        ):
            assert all(new[k] == v for k, v in row["compiled_rule"]["target"].items())
            for k in [
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
            ]:
                if k in old:
                    assert new[k] == old[k]
            assert all(
                limit in json.dumps(result.manifest) for limit in row["limitations"]
            )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rid",
    [
        ROWS[0]["rule_id"],
        next(
            r["rule_id"]
            for r in ROWS
            if r["boundary_checks"]["match_kind"] == "same_native_geography"
        ),
    ],
)
def test_new_scope_withdrawal_still_stops_the_matching_country(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for op in ledger["operations"]:
        if op["rule_id"] == rid:
            op["scopes"] = [s for s in op["scopes"] if s["source"]["version"] != "3.9"]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    row = next(r for r in ROWS if r["rule_id"] == rid)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(row["original_rule"]["source"], version="3.9"),
            version="2026",
            family="uvek",
        )


@pytest.mark.parametrize(
    "prior",
    REVIEW["preserved_existing_operations"],
    ids=lambda p: p["operation"]["rule_id"],
)
def test_all_five_prior_grid_permissions_remain_unchanged(tmp_path, prior):
    op = prior["operation"]
    assert approvals._approval_ledger()["operations"][prior["index"]] == op
    row = next(r for r in ROWS if r["rule_id"] == op["rule_id"])
    for scope in op["scopes"]:
        resolved = graph(
            row["original_rule"]["source"], version=scope["source"]["version"]
        )
        result = ClicHarmonizer(
            tmp_path, catalog_provider=PackageCatalogProvider()
        ).harmonize(resolved, version="2026", family="uvek")
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == -2 and all(
            edge[k] == v for k, v in row["original_rule"]["target"].items()
        )
