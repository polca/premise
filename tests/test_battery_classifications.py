"""Whole rechargeable cells/packs use CPC 46420; other products need separate review.

UN CPC 2.1 46420 (electric accumulators), correspondence to ISIC Rev.4 2720:
https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/EN/1074/46420
"""

import copy
import io
from contextlib import redirect_stdout
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

from premise.core.filesystem_constants import INVENTORY_DIR
from premise.inventory.inventory_imports import (
    DefaultInventory,
    get_classifications,
    get_classification_entry,
)

EXPECTED = {
    "ISIC rev.4 ecoinvent": "2720:Manufacture of batteries and accumulators",
    "CPC": "46420: Electric accumulators",
}


def whole_cell_or_pack(name, product):
    return name.lower().startswith(
        (
            "battery cell production,",
            "battery cell assembly,",
            "battery production,",
            "battery pack production,",
            "market for battery cell,",
            "market for battery,",
            "market for battery pack,",
        )
    ) and (
        product.lower() == "battery cell"
        or product.lower().startswith(
            (
                "battery cell,",
                "battery pack,",
                "battery,",
            )
        )
    )


def test_existing_rechargeable_cell_and_pack_entries_use_accumulator_code():
    classifications = get_classifications()
    reviewed = {
        key: value for key, value in classifications.items() if whole_cell_or_pack(*key)
    }
    assert len(reviewed) >= 97
    assert all(value == EXPECTED for value in reviewed.values())


def test_real_battery_imports_receive_classifications_without_inventory_changes(capsys):
    datasets = []
    for path in sorted(INVENTORY_DIR.glob("*.xlsx")):
        if not any(s in path.name.lower() for s in ("batter", "storage", "lfp")):
            continue
        with redirect_stdout(io.StringIO()):
            data = ExcelImporter(path).data
        datasets.extend(
            d for d in data if whole_cell_or_pack(d["name"], d["reference product"])
        )
    assert len(datasets) >= 77
    before = copy.deepcopy(datasets)
    importer = object.__new__(DefaultInventory)
    importer.import_db = SimpleNamespace(data=datasets)
    importer.classifications = get_classifications()
    importer.add_classifications()
    assert "missing classification" not in capsys.readouterr().out
    for original, classified in zip(before, datasets, strict=True):
        assert dict(classified["classifications"]) == EXPECTED
        assert {k: v for k, v in classified.items() if k != "classifications"} == {
            k: v for k, v in original.items() if k != "classifications"
        }


@pytest.mark.parametrize(
    "name,product",
    [
        (
            "battery cell production, lithium-ion, nmc811, silicon-graphite anode",
            "battery cell, Li-ion, NMC811, silicon-graphite anode",
        ),
        (
            "battery cell production, lithium-ion, nca, silicon-graphite anode",
            "battery cell, Li-ion, NCA, silicon-graphite anode",
        ),
        (
            "battery cell production, lithium-ion, nmc111, prismatic",
            "battery cell, Li-ion, NMC111, prismatic",
        ),
        (
            "market for battery cell, lithium-ion, nmc532",
            "battery cell, Li-ion, NMC532",
        ),
        (
            "market for battery cell, lithium-ion, nmc955, including end-of-life treatment",
            "battery cell, Li-ion, NMC955, including end-of-life treatment",
        ),
    ],
)
def test_prepared_battery_names_have_explicit_classification_entries(name, product):
    assert get_classification_entry(get_classifications(), name, product) == EXPECTED


@pytest.mark.parametrize(
    "name,product",
    [
        (
            "battery management system production, for Li-ion battery",
            "battery management system, for Li-ion battery",
        ),
        ("market for battery capacity, Li-ion, LFP", "electricity storage capacity"),
        ("anode paste, LFP", "anode paste, LFP"),
        (
            "treatment of used LFP battery cell, hydrometallurgical treatment",
            "used LFP battery cell",
        ),
        (
            "battery pack component production and end-of-life treatment, lithium-ion, nmc955, without cells and controls",
            "battery pack component provision, Li-ion, NMC955, without cells and controls",
        ),
    ],
)
def test_parts_services_chemicals_and_recycling_are_outside_accumulator_mapping(
    name, product
):
    assert not whole_cell_or_pack(name, product)
    entry = get_classification_entry(get_classifications(), name, product)
    assert entry is None or entry["CPC"] != EXPECTED["CPC"]
