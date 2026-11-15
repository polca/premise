"""Native PEM catalyst updates retain existing kilogram supply interfaces."""

from copy import deepcopy

import pytest

from premise.materials.metals_rules import ProviderSelector, load_material_rules
from test_pem_catalyst_reference import COATING_PT
from test_pem_iridium_provider import model

IRIDIUM = "Iridium production"
PLATINUM = "Platinum, at regional storage"


def boundary():
    providers = [
        {
            "name": name,
            "reference product": name,
            "location": location,
            "unit": "kilogram",
            "exchanges": [
                {
                    "type": "production",
                    "name": name,
                    "product": name,
                    "location": location,
                    "unit": "kilogram",
                    "amount": 1,
                }
            ],
        }
        for name, location in [(IRIDIUM, "GLO"), (PLATINUM, "RER")]
    ]
    stack = {
        "name": "Electrolyzer production, 1MWe, PEM, Stack",
        "reference product": "Electrolyzer production, 1MWe, PEM, Stack",
        "location": "RER",
        "unit": "unit",
        "exchanges": [
            {
                "type": "production",
                "amount": 1,
                "unit": "unit",
            },
            *[
                {
                    "type": "technosphere",
                    "name": p["name"],
                    "product": p["reference product"],
                    "location": p["location"],
                    "unit": "kilogram",
                    "amount": amount,
                }
                for p, amount in zip(providers, [0.5, 0.75], strict=True)
            ],
        ],
    }
    return stack, providers


def runtime(providers, version="2026"):
    result = model(providers, year=2050, version=version)
    result.material_rules_by_technology = {
        "PEMEL": [
            r
            for r in load_material_rules().enabled_rules
            if r.technology == "PEMEL" and r.design_group
        ]
    }
    return result


def test_complete_native_design_updates_quantities_without_new_suppliers_or_geography():
    stack, providers = boundary()
    original_providers = deepcopy(providers)
    result = runtime(providers)
    result.update_metal_use(stack, "PEMEL")
    assert providers == original_providers
    edges = {e["product"]: e for e in stack["exchanges"] if e["type"] == "technosphere"}
    assert set(edges) == {IRIDIUM, PLATINUM}
    assert edges[IRIDIUM]["amount"] == pytest.approx(0.4 / 3.8)
    assert edges[PLATINUM]["amount"] == pytest.approx(0.1 / 3.8 + COATING_PT)
    assert edges[IRIDIUM]["location"] == "GLO"
    assert edges[PLATINUM]["location"] == "RER"
    assert "opaque" in edges[IRIDIUM]["comment"]
    assert "no explicit iridium-resource" in edges[IRIDIUM]["comment"]
    assert "published unit" in edges[IRIDIUM]["comment"]
    assert "regional-storage" in edges[PLATINUM]["comment"]
    assert len(result.material_decisions) == 3
    assert all(d["status"] == "updated" for d in result.material_decisions)
    platinum = next(d for d in result.material_decisions if d["element"] == "Platinum")
    assert platinum["provider selection"] == "existing input fallback"
    assert result.material_update_diagnostics == []


def test_existing_input_fallback_never_substitutes_another_region():
    stack, providers = boundary()
    stack["exchanges"][2]["location"] = "CH"
    original = deepcopy(stack)
    result = runtime(providers)
    result.update_metal_use(stack, "PEMEL")
    assert stack == original
    assert len(result.material_decisions) == 3
    assert all(
        d["reason code"] == "metals.material_rule.design_incomplete"
        for d in result.material_decisions
    )


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.10", "3.11", "3.12"])
def test_native_provider_aliases_do_not_change_ecoinvent_interfaces(version):
    assert ProviderSelector("market for iridium", "iridium").for_version(
        version
    ) == ProviderSelector("market for iridium", "iridium")
    assert ProviderSelector("market for platinum", "platinum").for_version(
        version
    ) == ProviderSelector("market for platinum", "platinum")
    stack, providers = boundary()
    original = deepcopy(stack)
    result = runtime(providers, version=version)
    result.update_metal_use(stack, "PEMEL")
    assert stack == original
    assert all(d["status"] == "skipped" for d in result.material_decisions)
