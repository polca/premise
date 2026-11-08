"""User inventories add their own roots and keep quantities, versions and background choices."""

import copy
from types import SimpleNamespace

import pytest

from premise.inventory import clic_user as module
from premise.inventory.clic_archive import ClicInventoryError, ClicRecipe
from test_clic_archive import catalog, native


@pytest.fixture
def inputs(monkeypatch, tmp_path):
    path = tmp_path / "user.xlsx"
    path.write_bytes(b"parser fixture")
    raw = {
        "name": "user output",
        "reference product": "user output",
        "unit": "kilogram",
        "location": "GLO",
        "database": "user",
        "code": "user-root",
        "exchanges": [
            {
                "type": "production",
                "name": "user output",
                "product": "user output",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1,
            },
            {
                "type": "technosphere",
                "name": "component",
                "product": "component",
                "location": "GLO",
                "unit": "kilogram",
                "amount": -3,
                "uncertainty type": 3,
                "scale": 0.2,
            },
        ],
    }
    from premise.inventory import inventory_imports

    monkeypatch.setattr(
        inventory_imports,
        "ExcelImporter",
        lambda path: SimpleNamespace(data=[copy.deepcopy(raw)], db_name="user"),
    )
    monkeypatch.setattr(module, "registry", lambda: {"hybrid_sources": {"nodes": []}})
    specification = [{"filepath": path, "ecoinvent version": "3.12"}]
    return specification, raw


def target():
    return {
        "name": "component [CLIC dataset-1]",
        "reference product": "component",
        "location": "GLO",
        "unit": "kilogram",
        "database": "premise-CLIC",
        "code": "clic-component",
        "exchanges": [],
        "clic provenance": {
            "slug": "dataset-1",
            "name": "component",
            "reference_product": "component",
            "location": "GLO",
            "unit": "kilogram",
        },
    }


def test_user_purchase_adds_only_its_root_and_links_through_namespace(inputs):
    specification, raw = inputs
    user = module.ClicUserInventories(
        specification, [], version="3.12", system_model="cutoff"
    )
    assert user.required_roots(
        catalog(native(1, 1, "component")), ClicRecipe.read(None), []
    ) == {"dataset-1"}
    clic = target()
    records, metadata = user.prepare([clic], [clic])
    edge = records[0]["exchanges"][1]
    assert edge["name"] == clic["name"]
    assert edge["input"] == ("premise-CLIC", "clic-component")
    assert edge["amount"] == -3 and edge["scale"] == 0.2
    assert raw["exchanges"][1]["name"] == "component"
    assert metadata[0]["file"] == "user.xlsx"
    pins = {metadata[0]["recipe_key"]: metadata[0]["sha256"]}
    module.ClicUserInventories(
        specification, [], version="3.12", system_model="cutoff", pins=pins
    )
    with pytest.raises(ClicInventoryError, match="locked recipe"):
        module.ClicUserInventories(
            specification, [], version="3.12", system_model="cutoff", pins={}
        )


def test_existing_background_purchase_is_not_replaced_by_coincident_public_dataset(
    inputs,
):
    specification, _ = inputs
    background = {
        **target(),
        "name": "component",
        "database": "ecoinvent",
        "code": "native",
    }
    background.pop("clic provenance")
    user = module.ClicUserInventories(
        specification, [background], version="3.12", system_model="cutoff"
    )
    assert (
        user.required_roots(
            catalog(native(1, 1, "component")), ClicRecipe.read(None), [background]
        )
        == set()
    )
    records, _ = user.prepare([background, target()], [target()])
    assert records[0]["exchanges"][1]["name"] == "component"


def test_replacement_source_metadata_also_adds_its_public_root(inputs):
    specification, raw = inputs
    raw["exchanges"][1].update(
        {
            "replacement name": "replacement supplier",
            "replacement product": "replacement supplier",
            "replacement location": "GLO",
        }
    )
    user = module.ClicUserInventories(
        specification, [], version="3.12", system_model="cutoff"
    )
    roots = user.required_roots(
        catalog(native(1, 1, "replacement supplier")), ClicRecipe.read(None), []
    )
    assert roots == {"dataset-1"}


def test_recipe_requires_all_user_files_even_when_no_additional_files_supplied():
    with pytest.raises(ClicInventoryError, match="Supply all user inventories"):
        module.ClicUserInventories(
            [],
            [],
            version="3.12",
            system_model="cutoff",
            pins={"1-user.xlsx": "a" * 64},
        )


def test_user_purchase_can_require_a_retained_workbook_outside_requested_sector(
    inputs, monkeypatch
):
    specification, _ = inputs
    node = {
        "id": 0,
        "file": "retained.xlsx",
        "clic_slug": None,
        "science_hash": "reviewed",
        "identity": ["component", "component", "GLO", "kilogram"],
    }
    monkeypatch.setattr(
        module, "registry", lambda: {"hybrid_sources": {"nodes": [node]}}
    )
    user = module.ClicUserInventories(
        specification, [], version="3.12", system_model="cutoff"
    )
    roots = user.required_roots(
        catalog(native(1, 1, "unrelated")),
        ClicRecipe.read(None),
        [],
        source_files=[
            (specification[0]["filepath"].with_name("retained.xlsx"), "3.12")
        ],
    )
    assert roots == set()
    assert user.required_nodes == {0}
