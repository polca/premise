"""Check LiOH equivalents and incremental hydrate water through battery import."""

from copy import deepcopy

from bw2io import ExcelImporter
import numpy as np
import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

COMMON = "lci-batteries-NMC111-811-NCA-LFP.xlsx"
LTO = "lci-batteries-NMC955-LTO.xlsx"
REAGENT = "market for lithium hydroxide, battery grade"
CASES = [
    (COMMON, "NCA oxide", 0.25),
    (COMMON, "NMC811 oxide", 0.246),
    (LTO, "NCA oxide", 0.25),
]


@pytest.fixture(scope="module")
def inventories():
    return {name: ExcelImporter(INVENTORY_DIR / name).data for name in (COMMON, LTO)}


def activity(data, name):
    (result,) = [d for d in data if d["name"] == name]
    return result


def exchange(ds, name):
    (result,) = [e for e in ds["exchanges"] if e["name"] == name]
    return result


def assert_hydration_balance(ds, source_dose):
    reagent, water = exchange(ds, REAGENT), exchange(ds, "Water")
    assert reagent["type"] == "technosphere"
    assert reagent["unit"] == "kilogram"
    assert (
        reagent.get("product") or reagent.get("reference product")
    ) == "lithium hydroxide, battery grade"
    assert reagent["location"] == "GLO"
    assert water["type"] == "biosphere" and water["unit"] == "cubic meter"
    assert water["categories"] in {
        "air::non-urban air or from high stacks",
        ("air", "non-urban air or from high stacks"),
    }
    # Independently count formula units from the imported reagent mass.
    # LiOH.H2O has Li/H/O counts 1/3/2; anhydrous LiOH has 1/1/1.
    # The water increment must contain precisely the added H and O, with no Li.
    source_moles = source_dose / 0.023947
    hydrate_moles = reagent["amount"] / 0.041962
    extra_water_kg = (water["amount"] - 0.00019562) * 1000
    water_moles = extra_water_kg / 0.018015
    assert hydrate_moles == pytest.approx(source_moles, rel=1e-12)
    np.testing.assert_allclose(
        hydrate_moles * np.array([1, 3, 2]) - source_moles * np.array([1, 1, 1]),
        water_moles * np.array([0, 2, 1]),
        atol=1e-12,
    )
    assert reagent["amount"] - source_dose == pytest.approx(extra_water_kg, abs=1e-14)
    # Check against declared oxide lithium content independently of the dose
    # conversion. Original rounded source doses agree within 0.4%, not exactly.
    if ds["name"] == "NCA oxide":
        oxide_molar = (
            6.94 + 0.8 * 58.6934 + 0.15 * 58.933194 + 0.05 * 26.9815385 + 2 * 15.999
        )
    else:
        oxide_molar = (
            6.94 + 0.8 * 58.6934 + 0.1 * 58.933194 + 0.1 * 54.938044 + 2 * 15.999
        )
    li_kg = hydrate_moles * 0.00694
    assert li_kg == pytest.approx(6.94 / oxide_molar, rel=0.004)
    assert "Hydrate-basis correction" in reagent["comment"]
    assert "hydration increment only" in water["comment"]
    assert not any(k.startswith("replacement ") for k in reagent)
    assert ds["unit"] == "kilogram"
    assert exchange(ds, ds["name"])["amount"] == 1
    assert (
        exchange(ds, "market group for electricity, medium voltage")["amount"] == 7.26
    )


@pytest.mark.parametrize("workbook,name,dose", CASES)
def test_source_oxides_preserve_lithium_and_release_bound_water(
    inventories, workbook, name, dose
):
    assert_hydration_balance(activity(inventories[workbook], name), dose)


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.10", "3.11", "3.12"])
@pytest.mark.parametrize("workbook,name,dose", CASES)
def test_hydration_basis_survives_inventory_migration(
    inventories, monkeypatch, version, workbook, name, dose
):
    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    selected = ExcelImporter(INVENTORY_DIR / workbook)
    selected.data = [deepcopy(activity(inventories[workbook], name))]
    importer = DefaultInventory(
        [],
        "3.8",
        version,
        INVENTORY_DIR / workbook,
        "cutoff",
        False,
        preloaded_importer=selected,
    )
    importer.prepare_inventory()
    assert_hydration_balance(activity(importer.import_db.data, name), dose)


def test_both_nca_copies_have_identical_exchanges(inventories):
    assert (
        activity(inventories[COMMON], "NCA oxide")["exchanges"]
        == activity(inventories[LTO], "NCA oxide")["exchanges"]
    )


def test_lto_cell_back_calculates_lithium_equivalents_and_bound_water(inventories):
    data = inventories[LTO]
    cell = activity(data, "battery cell production, Li-ion, LTO")
    cathode = activity(data, "cathode, NCA, for Li-ion battery")
    oxide = activity(data, "NCA oxide")
    cell_cathode = exchange(cell, cathode["name"])["amount"]
    cathode_oxide = exchange(cathode, oxide["name"])["amount"]
    hydrate = exchange(oxide, REAGENT)["amount"]
    # Direct selected path: 1 kg cell -> cathode -> oxide -> hydrate.
    matrix = np.eye(4)
    matrix[1, 0], matrix[2, 1], matrix[3, 2] = -cell_cathode, -cathode_oxide, -hydrate
    supply = np.linalg.solve(matrix, [1, 0, 0, 0])
    expected_oxide = cell_cathode * cathode_oxide
    assert supply[2] == pytest.approx(expected_oxide)
    assert supply[3] / 0.041962 == pytest.approx(expected_oxide * 0.25 / 0.023947)
    added_water = (
        expected_oxide * (exchange(oxide, "Water")["amount"] - 0.00019562) * 1000
    )
    assert supply[3] - expected_oxide * 0.25 == pytest.approx(added_water)
    assert_hydration_balance(oxide, 0.25)


@pytest.mark.parametrize(
    "constant", ["FILEPATH_BATTERIES_NMC_NCA_LFP", "FILEPATH_BATTERIES_NMC955_LTO"]
)
def test_changed_cathode_workbook_invalidates_import_cache(
    constant, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    path = tmp_path / "cathode.xlsx"
    path.write_bytes(b"old reagent basis")
    monkeypatch.setattr(module, constant, path)
    source = obj._database_cache_path("test")
    previous = obj._database_cache_path("test", inventories=True)
    path.write_bytes(b"corrected hydrate basis")
    assert obj._database_cache_path("test", inventories=True) != previous
    assert obj._database_cache_path("test") == source
