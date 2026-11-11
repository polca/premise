"""Dedicated CSP heat aliases retain the reviewed legacy screening boundary."""

import pytest

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import act_fltr, get_mapping
from premise.inventory.clic_requirements import role_slugs


@pytest.mark.parametrize(
    "role",
    ["heat, secondary, from solar thermal"],
)
def test_solar_heat_filter_does_not_match_electricity_or_native_hot_water(role):
    definition = get_mapping(VARIABLES_DIR / "heat.yaml", "uvek_aliases")[role]
    heat = {
        "name": "heat production, at 110 MW concentrating solar power plant, with thermal energy storage",
        "reference product": "heat",
        "unit": "megajoule",
        "location": "CH",
    }
    electricity = {
        **heat,
        "name": heat["name"].replace("heat production", "electricity production"),
        "reference product": "electricity, high voltage",
        "unit": "kilowatt hour",
    }
    invalid_unit = {**heat, "unit": "kilowatt hour"}
    invalid_product = {**heat, "reference product": "electricity, high voltage"}
    pvt = {
        **heat,
        "name": "Heat, at 100m2 solar collector in PVT system, Al-Cu, slanted roof, hot water heat storage",
    }
    assert act_fltr(
        [heat, electricity, invalid_unit, invalid_product, pvt],
        fltr=definition["fltr"],
        exact=definition["exact"],
    ) == [heat]
    assert definition["use_legacy_templates"] is True
    assert definition["native_reference_amount"] == 1
    assert role_slugs("heat.yaml", role) == (
        "heat-production-at-110-mw-concentrating-solar-power-plant-with-thermal-energy-storage",
    )
    assert any(
        "electrical-energy-normalized" in note
        for note in definition["native_limitations"]
    )
    assert any("reuse credit" in note for note in definition["native_limitations"])
