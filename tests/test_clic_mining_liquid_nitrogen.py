"""The inspected mining profile carries its exact scope and warnings."""

import copy
import json

from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer


def test_nitrogen_38_review_survives_fresh_and_cached_harmonization(tmp_path):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_mining_38_liquid_nitrogen_review.json").read_text()
    )
    resolved = graph(review["rule"]["source"], version="3.8")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=-2.5,
        formula="signed_nitrogen",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == -2.5
        assert edge["scale"] == 0.3
        assert edge["formula"] == "signed_nitrogen"
        assert all(
            edge[key] == value for key, value in review["rule"]["target"].items()
        )
        # Cache and fresh runs retain the same independently reviewed limitations.
        assert "uvek_mining_38_liquid_nitrogen_review.json" in json.dumps(
            result.manifest
        )
        assert all(
            message in json.dumps(result.manifest) for message in review["limitations"]
        )
    assert resolved.activities == before
