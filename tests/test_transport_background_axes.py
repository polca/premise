"""Transport must use the same native product labels as the source baseline."""

from copy import deepcopy
from types import SimpleNamespace

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.core.runtime_cache import load_yaml_cached
from premise.inventory.inventory_normalization import normalize_reference_products
from premise.inventory.inventory_store import create_inventory_store
from premise.transport import Transport
from premise.validation.validation_framework import inventory_cycle_signatures


def native_grid_cycle():
    grid = {
        "name": "Electricity mix",
        "reference product": "Electricity mix",
        "location": "CH",
        "unit": "kilowatt hour",
    }
    plant = {
        "name": "Grid-support facility",
        "reference product": "grid support",
        "location": "CH",
        "unit": "unit",
    }
    return [
        {
            **grid,
            "exchanges": [
                {
                    "name": grid["name"],
                    "product": grid["reference product"],
                    "location": "CH",
                    "unit": "kilowatt hour",
                    "type": "production",
                    "amount": 1,
                },
                {
                    "name": plant["name"],
                    "product": plant["reference product"],
                    "location": "CH",
                    "unit": "unit",
                    "type": "technosphere",
                    "amount": 0.01,
                },
            ],
        },
        {
            **plant,
            "exchanges": [
                {
                    "name": plant["name"],
                    "product": plant["reference product"],
                    "location": "CH",
                    "unit": "unit",
                    "type": "production",
                    "amount": 1,
                },
                {
                    "name": grid["name"],
                    "product": grid["reference product"],
                    "location": "CH",
                    "unit": "kilowatt hour",
                    "type": "technosphere",
                    "amount": 1,
                },
            ],
        },
    ]


def cycles(database):
    return inventory_cycle_signatures(
        create_inventory_store(database, backend="compact")
    )


def test_uvek_transport_does_not_relabel_an_existing_grid_cycle_as_a_new_cycle():
    source = native_grid_cycle()
    baseline = deepcopy(source)
    aliases = load_yaml_cached(VARIABLES_DIR / "electricity_markets.yaml")["uvek"][
        "product_aliases"
    ]
    normalize_reference_products(baseline, aliases)
    expected = cycles(baseline)
    assert expected != cycles(
        source
    )  # The reproduced failure used different product faces.
    working = deepcopy(source)
    transformation = Transport(
        database=working,
        iam_data=SimpleNamespace(regions=["EUR"]),
        model="remind",
        pathway="test",
        year=2050,
        version="2026",
        system_model="cutoff",
        relink=False,
        vehicle_type="ship",
        has_fleet=False,
        family="uvek",
        biosphere_version="3.10",
    )
    assert transformation.family == "uvek"
    assert transformation.biosphere_version == "3.10"
    assert transformation.activity_mapping.family == "uvek"
    assert cycles(working) == expected
    assert [e["amount"] for a in working for e in a["exchanges"]] == [1, 0.01, 1, 1]
    assert source == native_grid_cycle()
