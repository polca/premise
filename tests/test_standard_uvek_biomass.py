"""The shared biomass transform uses mapped fuels and validates declared roles."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import xarray as xr

from premise.energy.biomass import Biomass
from premise.inventory.activity_maps import InventorySet
from premise.validation.validation import BiomassValidation


@pytest.mark.parametrize("amount", [-2.0, 2.0])
def test_standard_biomass_relinks_native_dry_fuel_without_rescaling(amount):
    name = "Wood chips, production mix, wet, measured as dry mass, at forest road & at sawmill"
    supplier = {
        "name": name,
        "reference product": name,
        "location": "CH",
        "unit": "kilogram",
        "code": "fuel",
        "exchanges": [],
    }
    fuel = {
        "name": name,
        "product": name,
        "location": "CH",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": amount,
        "input": ("source", "fuel"),
        "formula": "dose",
        "uncertainty type": 3,
        "scale": 0.2,
    }
    boiler = {
        "name": "Heat, residential, from wood chips",
        "reference product": "heat",
        "location": "CH",
        "unit": "megajoule",
        "code": "boiler",
        "exchanges": [deepcopy(fuel)],
    }
    material = {
        **deepcopy(boiler),
        "name": "Particle board",
        "unit": "kilogram",
        "code": "material",
    }
    owner = object.__new__(Biomass)
    owner.database = [supplier, boiler, material]
    owner.family = "uvek"
    owner.mapping = InventorySet(owner.database, family="uvek")
    owner.regions = ["EUR"]
    owner.ecoinvent_to_iam_loc = {"CH": "EUR"}
    owner.is_in_index = Mock(return_value=True)

    owner.replace_biomass_inputs()

    updated = boiler["exchanges"][0]
    assert updated["name"] == "market for lignocellulosic biomass, used as fuel"
    assert updated["location"] == "EUR"
    assert "input" not in updated
    for key in ("amount", "formula", "uncertainty type", "scale"):
        assert updated[key] == fuel[key]
    assert material["exchanges"] == [fuel]
    assert owner.biomass_consumers == {"boiler"}


def validator(database):
    owner = object.__new__(BiomassValidation)
    owner.database = database
    owner.iam_data = SimpleNamespace(
        biomass_mix=xr.DataArray(
            [[[0.8]]],
            dims=("region", "variables", "year"),
            coords={
                "region": ["EUR"],
                "variables": ["biomass - residual"],
                "year": [2050],
            },
        )
    )
    owner.regions = ["EUR"]
    owner.year = 2050
    owner.system_model = "cutoff"
    owner.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda _: "EUR")
    owner.log_issue = Mock()
    return owner


def test_standard_validation_detects_unlinked_native_consumer_not_indirect_wrapper():
    boiler = {
        "name": "Heat, wood",
        "unit": "megajoule",
        "location": "CH",
        "code": "boiler",
        "exchanges": [],
    }
    wrapper = {**boiler, "code": "wrapper"}
    owner = validator([boiler, wrapper])
    owner.expected_consumers = {"boiler"}

    owner.checking_linking()

    owner.log_issue.assert_called_once()
    assert owner.log_issue.call_args.args[0] is boiler


def test_standard_residual_share_check_uses_declared_technology_not_supplier_spelling():
    residual = {
        "type": "technosphere",
        "unit": "kilogram",
        "amount": 0.8,
        "name": "Native clean sawmill chips",
        "premise market technology": "biomass - residual",
    }
    forest = {
        **residual,
        "amount": 0.2,
        "name": "Native forest-road chips",
        "premise market technology": "biomass crops - purpose grown",
    }
    market = {
        "name": "market for lignocellulosic biomass, used as fuel",
        "location": "EUR",
        "exchanges": [residual, forest],
    }
    owner = validator([market])

    owner.check_residual_biomass_share()
    owner.log_issue.assert_not_called()
    residual["amount"] = 0.4
    owner.check_residual_biomass_share()
    owner.log_issue.assert_called_once()


def test_native_final_fuel_templates_are_not_regionalized_in_both_biomass_passes():
    import yaml
    from premise.core.filesystem_constants import DATA_DIR, VARIABLES_DIR

    procedures = yaml.safe_load(
        (DATA_DIR / "biomass/biomass_activities.yaml").read_text()
    )
    fuels = yaml.safe_load((VARIABLES_DIR / "biomass.yaml").read_text())

    def names(config):
        return {
            name
            for entry in config.values()
            for name in entry.get("uvek_aliases", {}).get("fltr", {}).get("name", [])
        }

    assert not names(procedures) & names(fuels)
