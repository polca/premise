import copy

import numpy as np
import pytest

xr = pytest.importorskip("xarray")

from premise.activity_maps import InventorySet
from premise.emissions import Emissions
from premise.inventory_store import CompactInventoryStore, LegacyInventoryStore


def test_legacy_emissions_replacement_preserves_source_and_nested_metadata(monkeypatch):
    from types import SimpleNamespace
    import premise.emissions as module

    original = [
        {
            "name": "emitter",
            "reference product": "service",
            "location": "World",
            "unit": "unit",
            "metadata": {"nested": [1]},
            "exchanges": [
                {"type": "biosphere", "name": "Nitrogen oxides", "amount": 10.0},
                {
                    "type": "technosphere",
                    "name": "unchanged provider",
                    "product": "thing",
                    "location": "World",
                    "unit": "unit",
                    "amount": 3.0,
                    "metadata": {"notes": [1]},
                },
            ],
        }
    ]
    source = LegacyInventoryStore(original)

    updater = object.__new__(Emissions)
    updater.year = 2030
    updater.ecoinvent_to_iam_loc = {"World": "World"}
    updater.gains_IAM = updater.prepare_data(
        _gains_data(np.array([[[[100.0], [50.0]]], [[[100.0], [50.0]]]]))
    )
    updater.rev_gains_map = {"emitter": "SEC"}
    updater.ei_pollutants = {"Nitrogen oxides": "NOx"}
    updater.write_log = lambda dataset, status="created": None
    monkeypatch.setattr(Emissions, "from_inventory_store", lambda **kwargs: updater)
    scenario = {
        "model": "image",
        "pathway": "SSP2-M",
        "year": 2030,
        "iam data": SimpleNamespace(gains_data_IAM=True),
        "_inventory_store": source,
    }
    module._update_emissions(scenario, "3.12", "cutoff", "CLE")
    updated = scenario["_inventory_store"].materialize()
    assert updated[0]["exchanges"][0]["amount"] == 5.0
    assert updated[0]["metadata"]["nested"] == [1]
    assert source.materialize() == original
    assert "_inventory_working_copy" not in scenario
    updated[0]["metadata"]["nested"].append(3)
    assert scenario["_inventory_store"].materialize()[0]["metadata"]["nested"] == [1]
    replacement = scenario["_inventory_store"]
    with replacement.transaction("change replacement") as transaction:
        transaction.patch_exchange(1, {"amount": 9.0, "metadata": {"notes": [2]}})
    assert source.materialize()[0]["exchanges"][1] == original[0]["exchanges"][1]
    with source.transaction("change source") as transaction:
        transaction.patch_exchange(1, {"amount": 7.0})
    assert replacement.materialize()[0]["exchanges"][1]["amount"] == 9.0


def _gains_data(values):
    return xr.DataArray(
        values,
        coords={
            "region": ["R1", "R2"],
            "sector": ["SEC"],
            "year": [2020, 2030],
            "pollutant": ["NOx"],
        },
        dims=("region", "sector", "year", "pollutant"),
    )


def test_prepare_data_adds_world_emissions_weighted_scaling():
    values = np.array(
        [
            [[[100.0], [50.0]]],
            [[[300.0], [60.0]]],
        ]
    )
    emissions = object.__new__(Emissions)
    emissions.year = 2030

    result = Emissions.prepare_data(emissions, _gains_data(values))

    assert result.sel(
        region="R1", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(0.5)
    assert result.sel(
        region="R2", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(0.2)
    assert result.sel(
        region="World", sector="SEC", pollutant="NOx"
    ).item() == pytest.approx(110 / 400)


def test_update_emissions_updates_world_dataset_with_world_scaling():
    values = np.array(
        [
            [[[100.0], [50.0]]],
            [[[300.0], [60.0]]],
        ]
    )
    dataset = {
        "name": "hot pollutant activity",
        "location": "World",
        "exchanges": [
            {
                "name": "Nitrogen oxides",
                "amount": 10.0,
                "type": "biosphere",
            }
        ],
    }
    emissions = object.__new__(Emissions)
    emissions.year = 2030
    emissions.database = [dataset]
    emissions.ecoinvent_to_iam_loc = {"World": "World"}
    emissions.gains_IAM = Emissions.prepare_data(emissions, _gains_data(values))
    emissions.rev_gains_map = {"hot pollutant activity": "SEC"}
    emissions.ei_pollutants = {"Nitrogen oxides": "NOx"}
    emissions.write_log = lambda dataset, status="created": None

    Emissions.update_emissions_in_database(emissions)

    assert dataset["exchanges"][0]["amount"] == pytest.approx(10.0 * 110 / 400)
    assert dataset["log parameters"]["NOx"] == pytest.approx(110 / 400)


@pytest.mark.parametrize("backend", [CompactInventoryStore, LegacyInventoryStore])
def test_store_native_emissions_matches_dictionary_path_exactly(backend):
    values = np.array(
        [
            [[[100.0], [50.0]]],
            [[[300.0], [60.0]]],
        ]
    )
    dataset = {
        "name": "hot pollutant activity",
        "reference product": "service",
        "location": "World",
        "unit": "unit",
        "exchanges": [
            {
                "name": "Nitrogen oxides",
                "amount": 10.0,
                "type": "biosphere",
                "uncertainty type": 2,
                "loc": 1.0,
                "scale": 0.2,
            },
            {
                "name": "Nitrogen oxides",
                "amount": 5.0,
                "type": "biosphere",
            },
        ],
    }

    def updater():
        emissions = object.__new__(Emissions)
        emissions.year = 2030
        emissions.ecoinvent_to_iam_loc = {"World": "World"}
        emissions.gains_IAM = Emissions.prepare_data(emissions, _gains_data(values))
        emissions.rev_gains_map = {"hot pollutant activity": "SEC"}
        emissions.ei_pollutants = {"Nitrogen oxides": "NOx"}
        emissions.write_log = lambda dataset, status="created": None
        return emissions

    expected = copy.deepcopy(dataset)
    legacy = updater()
    legacy.database = [expected]
    legacy.update_emissions_in_database()

    source = store = backend([dataset])
    native = updater()
    if backend is LegacyInventoryStore:
        store = LegacyInventoryStore(
            native.iter_updated_legacy_inventory(source), take_ownership=True
        )
        assert source.materialize() == [dataset]
    else:
        native.update_emissions_in_store(store)

    assert store.materialize() == [expected]
    assert store.generation == (1 if backend is CompactInventoryStore else 0)
    # Legacy semantics update only the first exchange for a repeated pollutant.
    assert store.materialize()[0]["exchanges"][1]["amount"] == 5.0


def test_store_gains_mapping_matches_legacy_filter_semantics():
    database = [
        {
            "name": "clinker production",
            "reference product": "clinker",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [],
        },
        {
            "name": "market for clinker production",
            "reference product": "clinker",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [],
        },
        {
            "name": "ammonia production, steam reforming, liquid",
            "reference product": "ammonia, liquid",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [],
        },
    ]
    legacy_mapping = InventorySet(copy.deepcopy(database)).generate_gains_mapping()
    expected = {
        activity["name"]: sector
        for sector, activities in legacy_mapping.items()
        for activity in activities
    }

    actual = Emissions._compile_store_gains_mapping(CompactInventoryStore(database))

    assert actual == expected
