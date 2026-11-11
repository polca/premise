"""Source aliases can opt into the existing regional power-plant workflow."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import premise.electricity.electricity as module
from premise.electricity import Electricity


@pytest.mark.parametrize(
    "family,enabled", [("uvek", True), ("uvek", False), ("ecoinvent", True)]
)
def test_regional_copy_opt_in_preserves_legacy_selection_and_source_inventory(
    monkeypatch, family, enabled
):
    def plant(name):
        return {
            "name": name,
            "reference product": "electricity, high voltage",
            "unit": "kilowatt hour",
            "location": "RER",
            "code": name,
            "exchanges": [
                {
                    "name": name,
                    "product": "electricity, high voltage",
                    "type": "production",
                    "unit": "kilowatt hour",
                    "location": "RER",
                    "amount": 1,
                },
                {
                    "name": "synthetic fuel",
                    "product": "fuel",
                    "type": "technosphere",
                    "unit": "kilogram",
                    "location": "RER",
                    "amount": -2,
                    "uncertainty type": 3,
                    "loc": -2,
                    "scale": 0.2,
                    "minimum": -3,
                    "maximum": -1,
                    "formula": "-dose",
                },
            ],
        }

    legacy, opted, untouched = [
        plant(name)
        for name in ("legacy generator", "mapped generator", "ordinary generator")
    ]
    originals = deepcopy([legacy, opted, untouched])
    owner = object.__new__(Electricity)
    owner.family = family
    owner.regions = ["EUR", "USA"]
    owner.ecoinvent_to_iam_loc = {"RER": "EUR"}
    owner.database = [legacy, opted, untouched]
    owner.powerplant_map = {
        "Coal IGCC": [legacy],
        "Gas CC": [opted],
        "Hydro": [untouched],
    }
    owner.powerplant_map_rev = {
        dataset["name"]: technology
        for technology, datasets in owner.powerplant_map.items()
        for dataset in datasets
    }
    owner.mapping = SimpleNamespace(
        generate_powerplant_map=lambda: owner.powerplant_map
    )
    owner.is_in_index = Mock(return_value=False)
    owner.remove_from_index = Mock()
    owner.add_to_index = Mock()
    owner.write_log = Mock()
    owner.complete_consequential_powerplant_map = Mock()
    owner.relink_technosphere_exchanges = Mock(side_effect=lambda dataset: dataset)
    monkeypatch.setattr(
        module,
        "load_electricity_variables",
        lambda: {
            "Gas CC": {
                "uvek_aliases": {"regionalize": enabled},
                "ecoinvent_aliases": {},
            }
        },
    )

    owner.create_region_specific_power_plants()

    expected = {legacy["name"]}
    if family == "uvek" and enabled:
        expected.add(opted["name"])
    copies = [a for a in owner.database if a["location"] in owner.regions]
    assert {(a["name"], a["location"]) for a in copies} == {
        (name, region) for name in expected for region in owner.regions
    }
    assert len(copies) == len(expected) * len(owner.regions)
    for copy in copies:
        source = next(a for a in originals if a["name"] == copy["name"])
        assert copy["regionalized"] is True
        assert copy["exchanges"][1] == source["exchanges"][1]
        assert copy["exchanges"][0]["amount"] == 1
        assert copy["code"] != source["code"]
    assert [legacy, opted, untouched] == originals
    assert owner.relink_technosphere_exchanges.call_count == len(copies)
