"""GAINS reductions apply to all pollutant exchanges exactly once."""

import copy
import math

import numpy as np
import pytest
import xarray as xr

from premise.emissions import Emissions
from premise.inventory_store import CompactInventoryStore, LegacyInventoryStore


def make_updater():
    updater = object.__new__(Emissions)
    updater.year = 2050
    updater.ecoinvent_to_iam_loc = {"CAZ": "CAZ"}
    updater.rev_gains_map = {"test gasoline car": "cars"}
    updater.ei_pollutants = {
        "Nitrogen oxides": "NOx",
        "Methane, fossil": "CH4",
        "Methane, non-fossil": "CH4",
    }
    updater.gains_IAM = xr.DataArray(
        np.array([[[0.3, 0.6]]]),
        dims=("region", "sector", "pollutant"),
        coords={"region": ["CAZ"], "sector": ["cars"], "pollutant": ["NOx", "CH4"]},
    )
    updater.write_log = lambda dataset, status="updated": None
    return updater


def inventory(reverse, processed):
    exchanges = [
        {
            "name": "Nitrogen oxides",
            "type": "biosphere",
            "unit": "kilogram",
            "categories": ("air", compartment),
            "amount": amount,
            "uncertainty type": 2,
            "loc": math.log(amount),
            "scale": 0.2,
        }
        for compartment, amount in [
            ("urban air close to ground", 10.0),
            ("non-urban air or from high stacks", 5.0),
            ("low population density, long-term", 2.0),
        ]
    ]
    exchanges.extend(
        [
            {
                "name": "Methane, fossil",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air",),
                "amount": 4.0,
            },
            {
                "name": "Methane, non-fossil",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air",),
                "amount": 6.0,
            },
            {
                "name": "Nitrogen oxides",
                "type": "technosphere",
                "product": "chemical",
                "location": "CAZ",
                "unit": "kilogram",
                "amount": 9.0,
            },
            {
                "name": "Lead",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air",),
                "amount": 1.0,
            },
        ]
    )
    if reverse:
        exchanges.reverse()
    return {
        "name": "test gasoline car",
        "reference product": "transport",
        "unit": "kilometer",
        "location": "CAZ",
        "log parameters": {"NOx": 0.3} if processed else {},
        "exchanges": exchanges,
    }


@pytest.mark.parametrize("backend", ["dictionary", "legacy", "compact"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "processed", [False, True], ids=["fresh", "nox-already-processed"]
)
def test_all_compartments_and_pollutant_aliases_are_scaled_once(
    backend, reverse, processed
):
    original = inventory(reverse, processed)
    dataset = copy.deepcopy(original)
    updater = make_updater()
    if backend == "dictionary":
        updater.database = [dataset]
        updater.update_emissions_in_database()
        after = dataset
        snapshot = copy.deepcopy(after)
        updater.update_emissions_in_database()
        assert dataset == snapshot
    elif backend == "legacy":
        source = LegacyInventoryStore([dataset])
        updated = LegacyInventoryStore(
            updater.iter_updated_legacy_inventory(source), take_ownership=True
        )
        after = updated.materialize()[0]
        repeated = LegacyInventoryStore(
            updater.iter_updated_legacy_inventory(updated), take_ownership=True
        )
        assert repeated.materialize() == [after]
        assert source.materialize() == [original]
    else:
        updated = CompactInventoryStore([dataset])
        updater.update_emissions_in_store(updated)
        after = updated.materialize()[0]
        assert updater.update_emissions_in_store(updated) == frozenset()
        assert updated.materialize() == [after]

    for before, current in zip(original["exchanges"], after["exchanges"]):
        factor = 1.0
        if before["type"] == "biosphere":
            if before["name"] == "Nitrogen oxides" and not processed:
                factor = 0.3
            elif before["name"].startswith("Methane"):
                factor = 0.6
        assert current["amount"] == pytest.approx(before["amount"] * factor)
        if before.get("uncertainty type") == 2:
            assert current["loc"] == pytest.approx(before["loc"] + math.log(factor))
            assert current["scale"] == before["scale"]
    assert after["log parameters"]["NOx"] == 0.3
    assert after["log parameters"]["CH4"] == 0.6
