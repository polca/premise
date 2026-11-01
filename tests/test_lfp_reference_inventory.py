"""The reviewed LFP pack follows the ordinary versioned-inventory route."""

from copy import deepcopy
from types import SimpleNamespace

from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell
from packaging.version import Version
import pytest

import premise.new_database as module
from premise.inventory_imports import migrate_import_db

PACK = "market for battery, Li-ion, LFP, curated reference"
CELL = "market for battery cell, Li-ion, LFP, curated supply"
OLD_CELL = "market for battery cell, Li-ion, LFP"


def extract(path):
    """Use the actual Excel importer without opening a Brightway database."""
    importer = ExcelImporter.__new__(ExcelImporter)
    data = importer.extractor.extract(path)
    data = [(name, rows) for name, rows in data if valid_first_cell(name, rows)]
    importer.db_name, importer.metadata = importer.get_database(data)
    return importer.process_activities(data)


@pytest.mark.parametrize(
    "version",
    [
        "3.5",
        "3.6",
        "3.7",
        "3.7.1",
        "3.8",
        "3.9",
        "3.9.1",
        "3.10",
        "3.10.1",
        "3.11",
        "3.12",
    ],
)
def test_lfp_source_selection_uses_migration_with_the_original_legacy_route(
    version, monkeypatch
):
    class SelectionComplete(Exception):
        pass

    selected = []

    def capture(filepaths):
        selected.extend(filepaths)
        raise SelectionComplete

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    builder = object.__new__(module.NewDatabase)
    builder.version = version
    with pytest.raises(SelectionComplete):
        builder._NewDatabase__import_inventories()
    if Version(version) >= Version("3.9"):
        assert (module.FILEPATH_LFP_REFERENCE, "3.12") in selected
        assert not any(p == module.FILEPATH_BATTERIES_NMC_NCA_LFP for p, _ in selected)
    else:
        assert not any(p == module.FILEPATH_LFP_REFERENCE for p, _ in selected)
        assert (module.FILEPATH_BATTERIES_NMC_NCA_LFP, "3.8") in selected
    assert (module.FILEPATH_BATTERIES_NMC955_LTO, "3.8") in selected
    assert (module.FILEPATH_LIS_BATTERY, "3.9") in selected


@pytest.mark.parametrize("target", ["3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"])
def test_reference_uses_existing_migrations_and_preserves_physical_quantities(target):
    data = extract(module.FILEPATH_LFP_REFERENCE)
    original = deepcopy(data)
    migrate_import_db(SimpleNamespace(data=data), "3.12", target)
    by_name = {activity["name"]: activity for activity in data}
    assert set(by_name) == {PACK, CELL, "battery cell production, Li-ion, LFP"}
    for old, new in zip(original, data):
        assert [e["amount"] for e in new["exchanges"]] == [
            e["amount"] for e in old["exchanges"]
        ]
    market = by_name[CELL]
    freight = [
        e
        for e in market["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "ton kilometer"
    ]
    assert sorted(e["amount"] for e in freight) == sorted(
        [0.0096, 0.0623, 0.0153, 0.31, 0.7368]
    )
    if Version(target) < Version("3.11"):
        assert {e["name"] for e in freight} == {
            "market group for transport, freight, light commercial vehicle",
            "market for transport, freight, aircraft, unspecified",
            "market group for transport, freight train",
            "market group for transport, freight, lorry, unspecified",
            "market for transport, freight, sea, container ship",
        }
    else:
        assert {e["name"] for e in freight} == {
            "market group for transport, freight, light commercial vehicle, fleet average",
            "market for transport, freight, aircraft, unspecified",
            "market group for transport, freight, train, fleet average",
            "market group for transport, freight, lorry, diesel, unspecified",
            "market for transport, freight, sea, container ship, heavy fuel oil",
        }
    wrapper_inputs = [
        e
        for e in by_name["battery cell production, Li-ion, LFP"]["exchanges"]
        if e["type"] == "technosphere"
    ]
    assert len(wrapper_inputs) == 1
    assert wrapper_inputs[0]["name"] == "battery cell production, Li-ion, LFP"
    assert wrapper_inputs[0]["location"] == "CN" and wrapper_inputs[0]["amount"] == 1


def test_pack_has_the_reviewed_mass_and_fixed_control_counts():
    data = extract(module.FILEPATH_LFP_REFERENCE)
    pack = next(a for a in data if a["name"] == PACK)
    inputs = [e for e in pack["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 4
    reference_kg = 203.4928
    expected = [196.85, 1, 1.12, 1.03]
    assert [e["amount"] * reference_kg for e in inputs] == pytest.approx(expected)
    assert inputs[1]["unit"] == "unit"
    assert inputs[2]["amount"] * reference_kg / 0.07 == pytest.approx(16)
    retained_kg = sum(
        e["amount"] * (4.4928 if e["unit"] == "unit" else 1) for e in inputs
    )
    assert retained_kg == pytest.approx(1)
    assert all(e.get("product") == e["reference product"] for e in inputs)
    assert not any(
        "used" in e["name"] or "management system, for Li-ion" in e["name"]
        for e in inputs
    )


def test_legacy_alias_preserves_the_original_cell_market_and_other_buyers():
    data = extract(module.FILEPATH_BATTERIES_NMC_NCA_LFP)
    by_name = {a["name"]: a for a in data}
    assert PACK in by_name and OLD_CELL in by_name and CELL in by_name
    old_inputs = [
        e for e in by_name[OLD_CELL]["exchanges"] if e["type"] == "technosphere"
    ]
    assert len(old_inputs) == 6
    assert len([e for e in old_inputs if e["unit"] == "ton kilometer"]) == 5
    alias_inputs = [
        e for e in by_name[CELL]["exchanges"] if e["type"] == "technosphere"
    ]
    assert len(alias_inputs) == 1 and alias_inputs[0]["name"] == OLD_CELL
    assert alias_inputs[0]["amount"] == 1
    producer = by_name["battery cell production, Li-ion, LFP"]
    assert len([e for e in producer["exchanges"] if e["type"] == "technosphere"]) == 10
