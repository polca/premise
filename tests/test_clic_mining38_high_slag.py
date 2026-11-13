"""High-slag composition and exact approval warnings survive the public cache."""

import copy
import gzip
import json
from pathlib import Path

import pytest

from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer


@pytest.mark.parametrize("version", ["3.8"])
def test_high_slag_signed_suppliers_and_matched_warnings_survive_cache(
    tmp_path, version
):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.reference_signs import select_profile_sign_rules
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_mining_38_high_slag_review.json").read_text()
    )
    rows = (
        review["records"]
        if version == "3.8"
        else [
            row
            for row in review["records"]
            if any(
                p["source_profile"]["version"] == version
                for p in row["original_rule"]
                .get("reference_flow_sign_review", {})
                .get("profiles", [])
            )
        ]
    )
    assert rows
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(),
        TechnosphereProfile("ecoinvent", version, "cutoff"),
    )
    rules = {r["id"]: r for r in selected["replace"]}
    resolved = graph(*(r["original_rule"]["source"] for r in rows), version=version)
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=-2.5,
            formula="signed_material",
            **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        cohort = result.manifest["cohorts"][0]
        assert cohort["cache_hit"] is cached
        for row, edge in zip(rows, result.activities[0]["exchanges"][1:], strict=True):
            factor = row["conversion_factor"]
            assert edge["amount"] == edge["loc"] == -2.5 * factor
            assert edge["scale"] == 0.3
            assert edge["formula"] == (
                "signed_material" if factor == 1 else "(signed_material) * (-1.0)"
            )
            target = rules[row["rule_id"]]["target"]
            if "recipe_ref" not in target:
                assert all(edge[key] == value for key, value in target.items())
        audit = json.loads(gzip.decompress(Path(cohort["audit_file"]).read_bytes()))
        matched = audit["premise_compatibility_reviews"]
        decisions = {r["rule_id"]: r for r in matched}
        for row in rows:
            if version == "3.8":
                assert decisions[row["rule_id"]]["limitations"] == row["limitations"]
            else:
                assert (
                    decisions[row["rule_id"]]["review_path"]
                    != "export/uvek_mining_38_high_slag_review.json"
                )
        warnings = [
            i
            for stage in audit["stages"]
            for i in stage.get("issues", [])
            if i["code"] == "clic.compatibility_proxy_limitations"
        ]
        assert {w["details"]["rule_id"] for w in warnings} == {
            r["rule_id"] for r in rows
        }
    assert resolved.activities == before


def test_actual_materialized_cement_helper_uses_shared_clinker_filter(tmp_path):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider
    from premise.inventory.clic_adapter import prepare_clic_for_premise
    from test_standard_uvek_cement import cement

    review = json.loads(
        (DATA_DIR / "export/uvek_mining_38_high_slag_review.json").read_text()
    )
    prepared = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(
        graph(review["records"][0]["original_rule"]["source"], version="3.8"),
        version="2026",
        family="uvek",
    )
    base = [
        {**row["identity"], "database": "native", "code": f"native-{i}"}
        for i, row in enumerate(review["native_2026_witnesses"])
    ]
    records, _ = prepare_clic_for_premise(
        prepared,
        base,
        version="2026",
        family="uvek",
        biosphere_name="ecoinvent-3.10-biosphere",
        biosphere_codes={("Heat, waste", ("air",), "megajoule"): "heat"},
        classifications={},
        catalog_provider=PackageCatalogProvider(),
    )
    obj = cement(records)
    (factory,) = obj._cement_production_datasets()
    assert (
        factory["brightpath_conversion"]["recipe_id"]
        == "mining-38-us-high-slag-cement-85"
    )
    edge = next(e for e in factory["exchanges"] if e["name"] == "Clinker, at plant")
    before = copy.deepcopy(edge)
    others = copy.deepcopy([e for e in factory["exchanges"] if e is not edge])
    obj.replace_clinker_production_with_markets()
    assert edge["name"] == "market for clinker" and edge["product"] == "clinker"
    assert edge["amount"] == 0.1425 and "input" not in edge
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "pedigree"):
        assert edge.get(key) == before.get(key)
    assert [e for e in factory["exchanges"] if e is not edge] == others
    regional = {**factory, "location": "EUR", "code": "regional"}
    wrong = {**factory, "reference product": "other cement"}
    assert cement([*records, regional, wrong])._cement_production_datasets() == [
        factory,
        regional,
    ]
