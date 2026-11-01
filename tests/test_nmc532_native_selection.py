"""Native NMC532 selection: version boundaries, quantities and cache behaviour.

Use real Excel extraction and importer preparation with supplier metadata.
These checks do not construct a licensed native database or run LCIA.
"""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

from bw2io import ExcelImporter
import pytest

import premise.new_database as module
from premise.inventory_imports import DefaultInventory, migrate_import_db

PRIVATE = "battery cell production, lithium-ion, nmc532, prismatic"
PACK = "battery pack production, lithium-ion, nmc532, without electronics"
LIFECYCLE = (
    "market for battery cell, lithium-ion, nmc532, including end-of-life treatment"
)
VERSIONS = ("3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")
MODELS = ("cutoff", "consequential")


@pytest.fixture(scope="module")
def legacy():
    return ExcelImporter(module.FILEPATH_BATTERIES_NMC955_LTO).data


@pytest.fixture(scope="module")
def interface():
    (record,) = ExcelImporter(module.FILEPATH_NMC532_NATIVE_312).data
    return record


def activity(records, name):
    (record,) = [r for r in records if r["name"] == name]
    return record


def worker(version, model):
    instance = object.__new__(module.NewDatabase)
    instance.version = version
    instance.system_model = model
    instance._database = []
    instance.keep_imports_uncertainty = False
    instance.keep_source_db_uncertainty = False
    return instance


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("model", MODELS)
def test_live_import_path_selection_is_scoped(version, model, monkeypatch):
    class Captured(Exception):
        pass

    selected = []

    def capture(paths):
        selected.extend(paths)
        raise Captured

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    with pytest.raises(Captured):
        worker(version, model)._NewDatabase__import_inventories()
    native = (module.FILEPATH_NMC532_NATIVE_312, "3.12")
    source = (module.FILEPATH_BATTERIES_NMC955_LTO, "3.8")
    assert (native in selected) == ((version, model) == ("3.12", "cutoff"))
    assert selected.count(source) == 1
    if native in selected:
        assert selected.count(native) == 1
        assert selected.index(native) < selected.index(source)
    nmc622 = (module.FILEPATH_BATTERIES_NMC622_532, "3.8")
    assert (nmc622 in selected) == (version not in ("3.11", "3.12"))


def test_selection_changes_only_two_supplier_edges_and_one_private_producer(
    legacy, interface
):
    original = deepcopy(legacy)
    importer = SimpleNamespace(data=deepcopy(original))
    module._select_nmc532_native_312(importer, [interface])
    expected = [r for r in deepcopy(original) if r["name"] != PRIVATE]
    for name in (PACK, LIFECYCLE):
        record = activity(expected, name)
        (exchange,) = [e for e in record["exchanges"] if e["name"] == PRIVATE]
        exchange.update(name=interface["name"], product=interface["reference product"])
        exchange["reference product"] = interface["reference product"]
    assert importer.data == expected
    assert legacy == original
    retirement = [
        e
        for e in activity(importer.data, LIFECYCLE)["exchanges"]
        if e["name"] == "market for used Li-ion battery"
    ]
    assert len(retirement) == 1 and retirement[0]["amount"] == -1


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("model", MODELS)
def test_live_caller_loads_reference_before_selecting_legacy_consumers(
    version, model, monkeypatch, legacy, interface
):
    def extract(paths):
        return [
            SimpleNamespace(
                data=deepcopy(legacy)
                if path == module.FILEPATH_BATTERIES_NMC955_LTO
                else [deepcopy(interface)]
                if path == module.FILEPATH_NMC532_NATIVE_312
                else []
            )
            for path, _ in paths
        ]

    observed = []

    class Inventory:
        def __init__(self, **kwargs):
            self.data = kwargs["preloaded_importer"].data
            self.list_unlinked = []
            observed.append((kwargs["path"], kwargs["version_in"]))

        def merge_inventory(self):
            return self.data

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "DefaultInventory", Inventory)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    # This source-isolated caller supplies no pipeline coating importers.
    monkeypatch.setattr(module, "use_native_pipeline_coating", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    # This caller fixture isolates NMC532 selection from other chemistry selectors.
    monkeypatch.setattr(module, "_select_nmc955_modern_references", lambda *args: None)
    actual = worker(version, model)._NewDatabase__import_inventories()
    if (version, model) != ("3.12", "cutoff"):
        assert actual == legacy
        return
    selected = SimpleNamespace(data=deepcopy(legacy))
    module._select_nmc532_native_312(selected, [interface])
    assert actual == [interface, *selected.data]
    assert observed.index((module.FILEPATH_NMC532_NATIVE_312, "3.12")) < (
        observed.index((module.FILEPATH_BATTERIES_NMC955_LTO, "3.8"))
    )


def test_current_pack_dose_and_future_uncertainty_survive_reordered_input(
    legacy, interface
):
    importer = SimpleNamespace(data=deepcopy(legacy))
    importer.data[0]["comment"] = "Unrelated source correction"
    pack = activity(importer.data, PACK)
    index = next(i for i, e in enumerate(pack["exchanges"]) if e["name"] == PRIVATE)
    exchange = pack["exchanges"].pop(index)
    exchange.update(
        amount=0.75,
        loc=0.75,
        minimum=0.7,
        maximum=0.8,
        input=("old", "private-cell"),
        **{"uncertainty type": 5},
    )
    pack["exchanges"].append(exchange)
    expected = deepcopy(exchange)
    expected.pop("input")
    expected.update(name=interface["name"], product=interface["reference product"])
    expected["reference product"] = interface["reference product"]
    module._select_nmc532_native_312(importer, [interface])
    assert activity(importer.data, PACK)["exchanges"][-1] == expected
    assert importer.data[0]["comment"] == "Unrelated source correction"


@pytest.mark.parametrize("failure", ("unit", "retirement", "missing_reference"))
def test_invalid_affected_contract_fails_before_mutation(legacy, interface, failure):
    importer = SimpleNamespace(data=deepcopy(legacy))
    database = [interface]
    if failure == "unit":
        pack = activity(importer.data, PACK)
        next(e for e in pack["exchanges"] if e["name"] == PRIVATE)["unit"] = "unit"
    elif failure == "retirement":
        lifecycle = activity(importer.data, LIFECYCLE)
        next(
            e
            for e in lifecycle["exchanges"]
            if e["name"] == "market for used Li-ion battery"
        )["amount"] = -2
    else:
        database = []
    unchanged = deepcopy(importer.data)
    with pytest.raises(ValueError):
        module._select_nmc532_native_312(importer, database)
    assert importer.data == unchanged


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("model", MODELS)
def test_companion_bytes_change_only_cutoff312_import_cache(
    version, model, monkeypatch, tmp_path
):
    reference = tmp_path / module.FILEPATH_NMC532_NATIVE_312.name
    reference.write_bytes(module.FILEPATH_NMC532_NATIVE_312.read_bytes())
    monkeypatch.setattr(module, "FILEPATH_NMC532_NATIVE_312", reference)
    instance = worker(version, model)
    before = instance._database_cache_path("source", inventories=True)
    native_before = instance._database_cache_path("source")
    reference.write_bytes(reference.read_bytes() + b"changed reference")
    after = instance._database_cache_path("source", inventories=True)
    assert (before != after) == ((version, model) == ("3.12", "cutoff"))
    assert instance._database_cache_path("source") == native_before


def test_companion_and_selected_consumers_prepare_with_explicit_products(
    legacy, interface
):
    # Supplier identities only: this checks preparation, not native availability.
    native_exchanges = []
    for record in [interface, activity(legacy, PACK), activity(legacy, LIFECYCLE)]:
        for exchange in record["exchanges"]:
            if exchange["type"] != "technosphere" or exchange["name"] == PRIVATE:
                continue
            native_exchanges.append(deepcopy(exchange))
    # These identities are authored in 3.8; supply metadata must use the same
    # target nomenclature as the real preparation call below.
    boundary = SimpleNamespace(
        data=[
            {
                "name": "supplier identity metadata",
                "reference product": "supplier identity metadata",
                "location": "GLO",
                "unit": "kilogram",
                "exchanges": native_exchanges,
            }
        ]
    )
    migrate_import_db(boundary, "3.8", "3.12")
    database = [
        {
            "name": exchange["name"],
            "reference product": exchange["reference product"],
            "location": exchange["location"],
            "unit": exchange["unit"],
            "exchanges": [],
        }
        for exchange in boundary.data[0]["exchanges"]
    ]
    bridge = DefaultInventory(
        database,
        "3.12",
        "3.12",
        module.FILEPATH_NMC532_NATIVE_312,
        "cutoff",
        False,
    )
    bridge.prepare_inventory()
    assert bridge.list_unlinked == []
    database.extend(bridge.import_db.data)
    source = SimpleNamespace(data=deepcopy(legacy))
    module._select_nmc532_native_312(source, database)
    source.data = [r for r in source.data if r["name"] in (PACK, LIFECYCLE)]
    selected = DefaultInventory(
        database,
        "3.8",
        "3.12",
        Path(module.FILEPATH_BATTERIES_NMC955_LTO),
        "cutoff",
        False,
        preloaded_importer=source,
    )
    selected.prepare_inventory()
    assert selected.list_unlinked == []
    for record in selected.import_db.data:
        (edge,) = [e for e in record["exchanges"] if e["name"] == interface["name"]]
        assert edge["product"] == interface["reference product"]
