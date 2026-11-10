"""Shared mapping lookups distinguish generated background helpers from publications."""

import pytest
from premise.inventory.activity_maps import InventorySet


@pytest.mark.parametrize("allowed", [False, True])
def test_generated_fuel_helpers_require_explicit_mapping_permission(allowed):
    name = (
        "market group for natural gas, high pressure, composed from resolved suppliers"
    )
    records = [
        {
            "name": name,
            "reference product": "natural gas, high pressure",
            "location": "RER",
            "unit": "cubic meter",
            "code": role,
            "exchanges": [],
            "clic provenance": {"role": role},
        }
        for role in ("generated helper", "root")
    ]
    definitions = {
        "gas fuel": {
            "allow_generated_helpers": allowed,
            "fltr": {
                "name": name,
                "reference product": "natural gas, high pressure",
                "unit": ["megajoule", "cubic meter"],
            },
            "exact": True,
        }
    }
    matched = InventorySet(records, family="uvek").generate_sets_from_filters(
        definitions
    )
    assert [a["code"] for a in matched["gas fuel"]] == (
        ["generated helper"] if allowed else []
    )


def test_shared_electricity_checks_count_native_products_and_mapped_hydro():
    from types import SimpleNamespace
    from unittest.mock import Mock
    import xarray as xr
    from premise.validation.validation import ElectricityValidation

    hydro = {
        "name": "Electricity, hydropower, at reservoir power plant",
        "product": "Electricity, hydropower",
        "unit": "kilowatt hour",
        "type": "technosphere",
        "amount": 0.4,
    }
    wind = {
        **hydro,
        "name": "Electricity, at wind farm",
        "product": "Electricity, wind",
        "amount": 0.6,
    }
    market = {
        "name": "market group for electricity, high voltage",
        "location": "EUR",
        "exchanges": [hydro, wind],
    }
    validator = object.__new__(ElectricityValidation)
    validator.database = [market]
    validator.technology_map = {"Hydro": [{"name": hydro["name"]}]}
    validator.regions = ["EUR"]
    validator.year = 2050
    validator.iam_data = SimpleNamespace(
        electricity_mix=xr.DataArray(
            [[[0.4], [0.6]]],
            dims=("region", "variables", "year"),
            coords={
                "region": ["EUR"],
                "variables": ["Hydro", "Wind Onshore"],
                "year": [2050],
            },
        )
    )
    validator.log_issue = Mock()
    validator.check_electricity_market_composition()
    validator.check_electricity_mix()
    validator.log_issue.assert_not_called()
    hydro["amount"] = 0
    validator.check_electricity_market_composition()
    validator.check_electricity_mix()
    assert validator.log_issue.call_count == 2


def test_product_aliases_preserve_links_quantities_and_biosphere_fields():
    from copy import deepcopy
    from premise.inventory.inventory_normalization import normalize_reference_products

    original = "Electricity, medium voltage, at grid"
    aliases = {
        original: {
            "source_product": original,
            "reference_product": "electricity, medium voltage",
            "unit": "kilowatt hour",
        }
    }
    edge = {
        "type": "technosphere",
        "name": original,
        "product": original,
        "unit": "kilowatt hour",
        "amount": -2,
        "input": ("native", "grid"),
        "formula": "demand",
        "uncertainty type": 3,
        "scale": 0.2,
    }
    bio = {**edge, "type": "biosphere"}
    activity = {
        "name": original,
        "reference product": original,
        "unit": "kilowatt hour",
        "exchanges": [edge, bio],
    }
    before = deepcopy(activity)
    assert normalize_reference_products([activity], aliases) == 1
    assert (
        activity["reference product"]
        == edge["product"]
        == "electricity, medium voltage"
    )
    assert activity["premise source labels"]["reference product"] == original
    for key in ("amount", "input", "formula", "uncertainty type", "scale"):
        assert edge[key] == before["exchanges"][0][key]
    assert bio == before["exchanges"][1]
    assert normalize_reference_products([activity], aliases) == 0


def test_standard_market_relinking_selects_native_mixes_exactly_not_photovoltaic_prefix():
    from copy import deepcopy
    from unittest.mock import Mock
    import yaml
    from premise.electricity.electricity import Electricity
    from premise.core.filesystem_constants import VARIABLES_DIR

    def record(name):
        return {
            "name": name,
            "reference product": "electricity, high voltage",
            "unit": "kilowatt hour",
            "location": "DE",
            "exchanges": [
                {"type": "production", "name": name, "amount": 1},
                {"type": "technosphere", "name": "Existing generator", "amount": 1},
            ],
        }

    native = record("Electricity, production mix")
    solar = record("Electricity, production mix photovoltaic, at plant")
    before = deepcopy(solar)
    owner = object.__new__(Electricity)
    owner.database = [native, solar]
    owner.market_mapping = yaml.safe_load(
        (VARIABLES_DIR / "electricity_markets.yaml").read_text()
    )["uvek"]
    owner.ecoinvent_to_iam_loc = {"DE": "EUR"}
    for method in (
        "write_log",
        "remove_from_index",
        "add_new_entry_to_cache",
        "create_new_markets_high_voltage",
        "create_new_markets_medium_voltage",
        "create_new_markets_low_voltage",
    ):
        setattr(owner, method, Mock())
    owner.update_electricity_markets()
    purchase = next(e for e in native["exchanges"] if e["type"] == "technosphere")
    assert purchase["name"] == "market group for electricity, high voltage"
    assert purchase["location"] == "EUR"
    assert solar == before
    owner.create_new_markets_high_voltage.assert_called_once()


@pytest.mark.parametrize(
    "fuel,product,amount,co2",
    [
        (
            "Hydrogen production, steam methane reforming of natural gas, 25 bar",
            "Hydrogen production, steam methane reforming of natural gas, 25 bar",
            1 / 120,
            0,
        ),
        ("Methanol, at plant", "Methanol, at plant", 0.05, 0.069),
    ],
)
def test_heat_checks_delivered_fuel_not_upstream_feedstock_or_capitalization(
    fuel, product, amount, co2
):
    from unittest.mock import Mock
    from premise.validation.validation import HeatValidation

    emission = {
        "name": "Carbon dioxide, fossil",
        "amount": co2,
        "unit": "kilogram",
        "type": "biosphere",
    }
    activity = {
        "name": "heat production, test boiler",
        "unit": "megajoule",
        "location": "EUR",
        "exchanges": [
            {
                "name": fuel,
                "product": product,
                "unit": "kilogram",
                "type": "technosphere",
                "amount": amount,
            },
            emission,
        ],
    }
    owner = object.__new__(HeatValidation)
    owner.database = [activity]
    owner.regions = ["EUR"]
    owner.log_issue = Mock()
    owner.check_heat_conversion_efficiency()
    owner.log_issue.assert_not_called()
    emission["amount"] = 0.5
    owner.check_heat_conversion_efficiency()
    owner.log_issue.assert_called_once()
