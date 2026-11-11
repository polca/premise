"""Reviewed construction proxies preserve signed caller demands and scope."""

import copy
import json

import pytest

from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("amount", [-2.0, 2.0])
def test_coal_plant_proxy_preserves_signed_demand_and_uncertainty(tmp_path, amount):
    from brightpath import DATA_DIR

    review = json.loads(
        (DATA_DIR / "export/uvek_heat_ccs_coal_plant_36_review.json").read_text()
    )["records"][0]
    resolved = graph(review["source"], version="3.6")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount, **{"uncertainty type": 3, "loc": amount, "scale": 0.2}
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(tmp_path, allow_documented_proxies=True).harmonize(
        resolved, version="2026", family="uvek", use_cache=False
    )
    output = result.activities[0]["exchanges"][1]
    for key, value in review["target"].items():
        assert output[key] == value
    for key in ("amount", "unit", "uncertainty type", "loc", "scale"):
        assert output[key] == before[0]["exchanges"][1][key]
    assert resolved.activities == before


def test_coal_plant_approval_is_limited_to_inspected_source_profile():
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource
    from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
    from premise.inventory.clic_target import resolve_inventory_target

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == "bp-4be6319700d5ef19")
    scopes = approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    for version in ("3.6", "3.7"):
        fingerprint = compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        assert (fingerprint in scopes) == (version == "3.6")
