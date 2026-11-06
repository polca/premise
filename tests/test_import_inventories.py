# content of test_activity_maps.py
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import (
    BaseInventoryImport,
    DefaultInventory,
    apply_migration_step,
    check_for_datasets_compliance_with_consequential_database,
    get_biosphere_code,
    get_classification_entry,
    get_classifications,
    get_consequential_blacklist,
    get_correspondence_bio_flows,
    migrate_import_db,
)

FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xlsx"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xlsx"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xlsx"
FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen.xlsx"
FILEPATH_SYNFUEL_INVENTORIES = INVENTORY_DIR / "lci-synfuel.xlsx"
FILEPATH_SYNGAS_INVENTORIES = INVENTORY_DIR / "lci-syngas.xlsx"
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx"
)


@pytest.mark.parametrize("ambiguous", [False, True])
def test_removed_case_alias_keeps_consumers_linked_to_unique_native_provider(ambiguous):
    from bw2io import ExcelImporter

    obj = object.__new__(DefaultInventory)
    obj.path = INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx"
    obj.import_db = ExcelImporter(obj.path)
    alias = "market for battery cell, li-ion, limn2o4"
    (duplicate,) = [d for d in obj.import_db.data if d["name"] == alias]
    native = {
        **deepcopy(duplicate),
        "name": "market for battery cell, Li-ion, LiMn2O4",
    }
    obj.database = [native, deepcopy(native)] if ambiguous else [native]
    original_native = deepcopy(obj.database)
    obj.db_names = [
        (native["name"].lower(), native["reference product"].lower(), "GLO")
    ]
    consumers = [
        e
        for d in obj.import_db.data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and e["name"] == alias
    ]
    assert len(consumers) == 2
    before = deepcopy(consumers)
    obj.check_for_already_existing_datasets()
    assert not any(d["name"] == alias for d in obj.import_db.data)
    for exchange, original in zip(consumers, before, strict=True):
        assert exchange == {
            **original,
            **(
                {}
                if ambiguous
                else {
                    "name": native["name"],
                    "product": native["reference product"],
                }
            ),
        }
    assert obj.database == original_native


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.11", "3.12"])
def test_consequential_assb_hdpe_uses_native_market_without_changing_amount(version):
    from bw2io import ExcelImporter

    importer = ExcelImporter(INVENTORY_DIR / "lci-batteries-ASSB.xlsx")
    migrate_import_db(importer, "3.10", version)
    selected = [
        e
        for ds in importer.data
        for e in ds["exchanges"]
        if e["type"] == "technosphere"
        and e.get("reference product") == "polyethylene, high density, granulate"
        and ", recycled to generic market" in e["name"]
    ]
    assert len(selected) == 1
    before = deepcopy(selected[0])
    check_for_datasets_compliance_with_consequential_database(
        importer.data, get_consequential_blacklist(), version
    )
    assert selected[0] == {
        **before,
        "name": "market for polyethylene, high density, granulate",
        "reference product": "polyethylene, high density, granulate",
        "product": "polyethylene, high density, granulate",
        "location": "RER" if version == "3.12" else "GLO",
    }


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.12"])
@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
@pytest.mark.parametrize(
    "filename,source_version,source_name,target_name,target_product,target_location",
    [
        (
            "lci-batteries-ASSB.xlsx",
            "3.10",
            "market for lithium iron phosphate",
            "Lithium iron phosphate [LiFePO4]",
            "Lithium iron phosphate [LiFePO4]",
            "JP",
        ),
        (
            "lci-iridium-economic-proxy.xlsx",
            "3.12",
            "market for sodium ethyl xanthate",
            "market for chemical, organic",
            "chemical, organic",
            "GLO",
        ),
    ],
)
def test_reviewed_chemical_proxies_are_confined_to_38_and_preserve_quantities(
    version,
    system_model,
    filename,
    source_version,
    source_name,
    target_name,
    target_product,
    target_location,
):
    from bw2io import ExcelImporter

    obj = object.__new__(DefaultInventory)
    obj.path = INVENTORY_DIR / filename
    obj.version_out = version
    obj.system_model = system_model
    obj.import_db = ExcelImporter(obj.path)
    migrate_import_db(obj.import_db, source_version, version)
    (selected,) = [
        e
        for ds in obj.import_db.data
        for e in ds["exchanges"]
        if e["name"] == source_name
    ]
    before = deepcopy(selected)
    obj.adapt_reviewed_38_chemical_proxies()
    if version != "3.8":
        assert selected == before
        return
    assert selected == {
        **before,
        "name": target_name,
        "reference product": target_product,
        "product": target_product,
        "location": target_location,
        "comment": selected["comment"],
    }
    assert "original kg quantity retained" in selected["comment"]
    once = deepcopy(selected)
    obj.adapt_reviewed_38_chemical_proxies()
    assert selected == once
    if filename == "lci-batteries-ASSB.xlsx":
        # The replacement is a recipe already selected for 3.8 imports.
        legacy = ExcelImporter(INVENTORY_DIR / "lci-batteries-NMC111-811-NCA-LFP.xlsx")
        assert any(
            (d["name"], d["reference product"], d["location"], d["unit"])
            == (target_name, target_product, target_location, "kilogram")
            for d in legacy.data
        )


@pytest.fixture(
    scope="module",
    params=[
        "lci-two_wheelers.xlsx",
        "lci-trucks.xlsx",
        "lci-electric-car-mechanical-powertrain.xlsx",
    ],
)
def vehicle_metal_scrap_handoffs(request):
    from bw2io import ExcelImporter

    products = {
        "aluminium scrap, new",
        "aluminium scrap, post-consumer",
        "copper scrap, sorted, pressed",
    }
    data = ExcelImporter(INVENTORY_DIR / request.param).data
    exchanges = [
        e
        for ds in data
        for e in ds["exchanges"]
        if e["type"] == "technosphere" and e.get("reference product") in products
    ]
    assert exchanges, request.param
    assert all(e["amount"] < 0 for e in exchanges)
    assert all(
        e["name"] == f'{e["reference product"]}, Recycled Content cut-off'
        for e in exchanges
    )
    return exchanges


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.11", "3.12"])
def test_consequential_vehicle_scrap_preserves_signed_quantities_and_uncertainty(
    vehicle_metal_scrap_handoffs, version
):
    original = deepcopy(vehicle_metal_scrap_handoffs)
    # Read the actual workbook exchanges so every existing uncertainty field
    # and any repeated material output is checked, rather than a synthetic row.
    consumer = {
        "name": "vehicle equipment",
        "reference product": "vehicle equipment",
        "unit": "kilogram",
        "exchanges": deepcopy(original),
    }
    (mapped,) = check_for_datasets_compliance_with_consequential_database(
        [consumer], get_consequential_blacklist(), version
    )
    assert len(mapped["exchanges"]) == len(original)
    for before, after in zip(original, mapped["exchanges"]):
        product = before["reference product"]
        assert after == {
            **before,
            "name": f"market for {product}",
            "product": product,
            "reference product": product,
            "location": "RoW" if product == "aluminium scrap, new" else "GLO",
        }
    assert vehicle_metal_scrap_handoffs == original


@pytest.mark.parametrize(
    ("version", "expected_name"),
    [("3.10", "Copper ion"), ("3.11", "Copper"), ("3.12", "Copper")],
)
@pytest.mark.parametrize("subcategory", ["surface water", "unspecified"])
def test_water_copper_alias_preserves_existing_flows(
    version, expected_name, subcategory
):
    original = {
        "name": "Copper",
        "type": "biosphere",
        "categories": ("water", subcategory),
        "unit": "kilogram",
        "amount": 0.002,
        "uncertainty type": 2,
        "loc": -6.214608098422191,
        "scale": 0.1,
    }
    importer = object.__new__(BaseInventoryImport)
    importer.path = Path("test.xlsx")
    importer.biosphere_dict = get_biosphere_code(version)
    importer.correspondence_bio_flows = get_correspondence_bio_flows()
    importer.import_db = SimpleNamespace(data=[{"exchanges": [original.copy()]}])
    importer.add_biosphere_links()
    expected_code = importer.biosphere_dict[
        (expected_name, "water", subcategory, "kilogram")
    ]
    assert importer.import_db.data[0]["exchanges"] == [
        {**original, "name": expected_name, "input": ("biosphere3", expected_code)}
    ]


@pytest.mark.parametrize("version", ["3.9", "3.10", "3.11", "3.12"])
@pytest.mark.parametrize(
    ("name", "old_name", "compartment", "subcategory"),
    [
        ("Isopropanol", "2-Propanol", "air", "urban air close to ground"),
        ("Calcium II", "Calcium", "air", "non-urban air or from high stacks"),
        ("Calcium II", "Calcium", "air", "urban air close to ground"),
        ("Calcium II", "Calcium", "soil", "agricultural"),
        ("Sodium I", "Sodium", "air", "non-urban air or from high stacks"),
        (
            "1,1-Difluoroethane",
            "Ethane, 1,1-difluoro-, HFC-152a",
            "air",
            "urban air close to ground",
        ),
        (
            "Trifluoromethane",
            "Methane, trifluoro-, HFC-23",
            "air",
            "urban air close to ground",
        ),
    ],
)
def test_pv_biosphere_aliases_preserve_flow_identity(
    version, name, old_name, compartment, subcategory
):
    original = {
        "name": name,
        "type": "biosphere",
        "categories": (compartment, subcategory),
        "unit": "kilogram",
        "amount": 0.002,
        "uncertainty type": 2,
        "loc": -6.214608098422191,
        "scale": 0.1,
    }
    importer = object.__new__(BaseInventoryImport)
    importer.path = Path("test.xlsx")
    importer.biosphere_dict = get_biosphere_code(version)
    importer.correspondence_bio_flows = get_correspondence_bio_flows()
    importer.import_db = SimpleNamespace(data=[{"exchanges": [original.copy()]}])
    importer.add_biosphere_links()
    expected_code = get_biosphere_code("3.9")[
        (old_name, compartment, subcategory, "kilogram")
    ]
    assert importer.import_db.data[0]["exchanges"] == [
        {
            **original,
            "name": old_name if version == "3.9" else name,
            "input": ("biosphere3", expected_code),
        }
    ]


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.10", "3.11", "3.12"])
@pytest.mark.parametrize(
    ("name", "old_name", "subcategory"),
    [
        ("Tin ion", "Tin", "urban air close to ground"),
        ("Tin ion", "Tin", "unspecified"),
        ("Aluminium III", "Aluminium", "urban air close to ground"),
        ("Aluminium III", "Aluminium", "non-urban air or from high stacks"),
        ("Potassium I", "Potassium", "non-urban air or from high stacks"),
    ],
)
def test_pv_38_biosphere_aliases_preserve_flow_identity(
    version, name, old_name, subcategory
):
    original = {
        "name": name,
        "type": "biosphere",
        "categories": ("air", subcategory),
        "unit": "kilogram",
        "amount": 0.002,
        "uncertainty type": 2,
        "loc": -6.214608098422191,
        "scale": 0.1,
    }
    importer = object.__new__(BaseInventoryImport)
    importer.path = Path("test.xlsx")
    importer.biosphere_dict = get_biosphere_code(version)
    importer.correspondence_bio_flows = get_correspondence_bio_flows()
    importer.import_db = SimpleNamespace(data=[{"exchanges": [original.copy()]}])
    importer.add_biosphere_links()
    expected_code = get_biosphere_code("3.8")[
        (old_name, "air", subcategory, "kilogram")
    ]
    assert importer.import_db.data[0]["exchanges"] == [
        {
            **original,
            "name": old_name if version == "3.8" else name,
            "input": ("biosphere3", expected_code),
        }
    ]


def get_db():
    db = [
        {
            "code": "argsthyfujgyftdgr",
            "name": "fake activity",
            "reference product": "fake product",
            "location": "IAI Area, Africa",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "fake activity",
                    "product": "fake product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_db", "6543541"),
                },
                {
                    "name": "1,4-Butanediol",
                    "categories": ("air", "urban air close to ground"),
                    "amount": 1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio", "123"),
                },
            ],
        }
    ]
    version = "3.5"
    return db, version


class DummyInventoryImport(BaseInventoryImport):
    def load_inventory(self):
        return SimpleNamespace(data=[])


def get_replacement_source_db():
    return [
        {
            "code": "source",
            "name": "source process",
            "reference product": "source product",
            "location": "RoW",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "source process",
                    "product": "source product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "location": "RoW",
                },
                {
                    "name": "market for heat",
                    "product": "heat",
                    "amount": 3,
                    "type": "technosphere",
                    "unit": "megajoule",
                    "location": "RoW",
                },
                {
                    "name": "market for heat",
                    "product": "heat",
                    "amount": 0.5,
                    "type": "technosphere",
                    "unit": "megajoule",
                    "location": "Europe without Switzerland",
                },
                {
                    "name": "market group for electricity",
                    "product": "electricity, high voltage",
                    "amount": 2.5,
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                    "location": "RER",
                },
                {
                    "name": "market group for electricity",
                    "product": "electricity, high voltage",
                    "amount": 9,
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                    "location": "RoW",
                },
            ],
        }
    ]


def get_base_inventory_import(tmp_path, database):
    testpath = tmp_path / "testfile"
    testpath.write_text("")
    return DummyInventoryImport(
        database,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )


def test_file_exists():
    db, version = get_db()
    with pytest.raises(FileNotFoundError) as wrapped_error:
        BaseInventoryImport(
            db,
            version_in=version,
            version_out="3.8",
            path="testfile",
            system_model="cutoff",
        )
    assert wrapped_error.type == FileNotFoundError


def test_biosphere_dict():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(
        db, version_in=version, version_out="3.8", path=testpath, system_model="cutoff"
    )
    assert (
        dbc.biosphere_dict[
            ("1,4-Butanediol", "air", "urban air close to ground", "kilogram")
        ]
        == "38a622c6-f086-4763-a952-7c6b3b1c42ba"
    )

    testpath.unlink()


def test_biosphere_dict_2():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(
        db, version_in=version, version_out="3.8", path=testpath, system_model="cutoff"
    )

    for act in dbc.database:
        for exc in act["exchanges"]:
            if exc["type"] == "biosphere":
                assert (
                    dbc.biosphere_dict[
                        (
                            exc["name"],
                            exc["categories"][0],
                            exc["categories"][1],
                            exc["unit"],
                        )
                    ]
                    == "38a622c6-f086-4763-a952-7c6b3b1c42ba"
                )

    testpath.unlink()


def test_fill_data_gaps_matches_technosphere_exchange_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for heat",
        "product": "heat",
        "location": "RoW",
        "amount": 0,
        "type": "technosphere",
        "unit": "megajoule",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 3
    assert "replacement name" not in exchange
    assert "replacement product" not in exchange
    assert "replacement location" not in exchange


def test_fill_data_gaps_sums_technosphere_exchanges_without_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for heat",
        "product": "heat",
        "amount": 0,
        "type": "technosphere",
        "unit": "megajoule",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 3.5


def test_fill_data_gaps_market_group_fallback_matches_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for electricity",
        "product": "electricity, high voltage",
        "location": "RER",
        "amount": 0,
        "type": "technosphere",
        "unit": "kilowatt hour",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 2.5


def test_load_carma():
    db, version = get_db()
    carma = DefaultInventory(
        db,
        version_in="3.6",
        version_out="3.8",
        path=FILEPATH_CARMA_INVENTORIES,
        system_model="cutoff",
        keep_uncertainty_data=False,
    )
    assert len(carma.import_db.data) >= 81


def test_load_biofuel():
    db, version = get_db()
    bio = DefaultInventory(
        db,
        version_in="3.7",
        version_out="3.8",
        path=FILEPATH_BIOFUEL_INVENTORIES,
        system_model="cutoff",
        keep_uncertainty_data=False,
    )
    assert len(bio.import_db.data) >= 150


def test_get_classifications_repairs_mojibake(tmp_path, monkeypatch):
    filepath = tmp_path / "classifications.csv"
    name = (
        "market for sawlog and veneer log, paran√° pine, "
        "measured as solid wood under bark"
    )
    product = "sawlog and veneer log, paran√° pine, measured as solid wood under bark"
    filepath.write_text(
        "\n".join(
            [
                "name,product,ISIC rev.4 ecoinvent,CPC",
                f'"{name}","{product}",0220:Logging,"3110: Wood"',
            ]
        ),
        encoding="utf-8",
    )

    get_classifications.cache_clear()
    monkeypatch.setattr("premise.inventory_imports.FILEPATH_CLASSIFICATIONS", filepath)

    classifications = get_classifications()
    classification = get_classification_entry(
        classifications,
        (
            "market for sawlog and veneer log, parana\u0301 pine, "
            "measured as solid wood under bark"
        ),
        "sawlog and veneer log, paraná pine, measured as solid wood under bark",
    )

    assert classification == {
        "ISIC rev.4 ecoinvent": "0220:Logging",
        "CPC": "3110: Wood",
    }

    get_classifications.cache_clear()


def test_fill_data_gaps_prefers_matching_supplier_location(tmp_path):
    testpath = tmp_path / "dummy.xlsx"
    testpath.write_text("")

    reference_db = [
        {
            "name": "pig iron production",
            "reference product": "pig iron",
            "location": "RER",
            "exchanges": [
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "GLO",
                    "amount": 9.72,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "RoW",
                    "amount": 9.68,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
            ],
        }
    ]

    importer = DummyInventoryImport(
        reference_db,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )

    exchange = {
        "name": "market for coke",
        "product": "coke",
        "location": "GLO",
        "unit": "megajoule",
        "type": "technosphere",
        "replacement name": "pig iron production",
        "replacement product": "pig iron",
        "replacement location": "RER",
    }

    importer.fill_data_gaps(exchange)

    assert exchange["amount"] == pytest.approx(9.72)
    assert "replacement name" not in exchange
    assert "replacement product" not in exchange
    assert "replacement location" not in exchange


def test_toggle_market_name_only_changes_market_prefix():
    embedded_market_name = (
        "heat, from municipal waste incineration to generic market for heat district "
        "or industrial, other than natural gas"
    )

    assert (
        BaseInventoryImport._toggle_market_name("market for hard coal")
        == "market group for hard coal"
    )
    assert (
        BaseInventoryImport._toggle_market_name("market group for hard coal")
        == "market for hard coal"
    )
    assert BaseInventoryImport._toggle_market_name(embedded_market_name) is None


def test_fill_dataset_data_gaps_replaces_migrated_split_markets(tmp_path):
    testpath = tmp_path / "dummy.xlsx"
    testpath.write_text("")

    reference_db = [
        {
            "name": "pig iron production",
            "reference product": "pig iron",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "RoW",
                    "amount": 9.724,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
                {
                    "name": "market group for hard coal",
                    "product": "hard coal",
                    "location": "RER",
                    "amount": 0.15,
                    "type": "technosphere",
                    "unit": "kilogram",
                },
            ],
        }
    ]

    importer = DummyInventoryImport(
        reference_db,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )

    dataset = {
        "name": "pig iron production, blast furnace, with carbon capture and storage",
        "reference product": "pig iron",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "pig iron production, blast furnace, with carbon capture and storage",
                "product": "pig iron",
                "amount": 1,
                "type": "production",
                "unit": "kilogram",
            },
            {
                "name": "market for coke",
                "product": "coke",
                "location": "CN",
                "unit": "megajoule",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for coke",
                "product": "coke",
                "location": "RoW",
                "unit": "megajoule",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for hard coal",
                "product": "hard coal",
                "location": "DE",
                "unit": "kilogram",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for hard coal",
                "product": "hard coal",
                "location": "FR",
                "unit": "kilogram",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
        ],
    }

    importer.fill_dataset_data_gaps(dataset)

    techno = [exc for exc in dataset["exchanges"] if exc["type"] == "technosphere"]

    assert techno == [
        {
            "name": "market for coke",
            "product": "coke",
            "location": "RoW",
            "unit": "megajoule",
            "amount": pytest.approx(9.724),
            "type": "technosphere",
        },
        {
            "name": "market group for hard coal",
            "product": "hard coal",
            "location": "RER",
            "unit": "kilogram",
            "amount": pytest.approx(0.15),
            "type": "technosphere",
        },
    ]


def test_forward_migration_applies_in_memory_without_bw2io_datastore():
    class DummyImporter:
        data = [
            {
                "name": "old dataset",
                "reference product": "old product",
                "location": "GLO",
                "unit": "kilogram",
                "exchanges": [
                    {
                        "name": "old exchange",
                        "reference product": "old exchange product",
                        "location": "RER",
                        "unit": "megajoule",
                        "type": "technosphere",
                        "input": ("test", "old"),
                    }
                ],
            }
        ]

        def migrate(self, migration_name):
            raise AssertionError("bw2io Migration datastore should not be used")

    available = {
        ("3.11", "3.12"): {
            "replace": [
                {
                    "source": {
                        "name": "old dataset",
                        "reference product": "old product",
                        "location": "GLO",
                        "unit": "kilogram",
                    },
                    "target": {
                        "name": "new dataset",
                        "reference product": "new product",
                        "location": "RER",
                        "unit": "ton kilometer",
                    },
                },
                {
                    "source": {
                        "name": "old exchange",
                        "reference product": "old exchange product",
                        "location": "RER",
                        "unit": "megajoule",
                    },
                    "target": {
                        "name": "new exchange",
                        "reference product": "new exchange product",
                        "location": "CH",
                        "unit": "kilowatt hour",
                    },
                },
            ],
            "disaggregate": [],
        }
    }

    importer = DummyImporter()

    apply_migration_step(importer, "3.11", "3.12", "forward", available)

    assert importer.data[0]["name"] == "new dataset"
    assert importer.data[0]["reference product"] == "new product"
    assert importer.data[0]["location"] == "RER"
    assert importer.data[0]["unit"] == "kilogram"

    exchange = importer.data[0]["exchanges"][0]
    assert exchange["name"] == "new exchange"
    assert exchange["reference product"] == "new exchange product"
    assert exchange["location"] == "CH"
    assert exchange["unit"] == "megajoule"
    assert "input" not in exchange


def test_correct_product_field_uses_indexed_reference_product(tmp_path):
    testpath = tmp_path / "dummy.xlsx"
    testpath.write_text("")

    reference_db = [
        {
            "name": "shared supplier",
            "reference product": "first product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        },
        {
            "name": "shared supplier",
            "reference product": "second product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        },
    ]

    importer = DummyInventoryImport(
        reference_db,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )

    assert (
        importer.correct_product_field(
            ("shared supplier", "GLO", "kilogram", "second product")
        )
        == "second product"
    )
    assert (
        importer.correct_product_field(("shared supplier", "GLO", "kilogram", None))
        == "first product"
    )

    BaseInventoryImport.correct_product_field.cache_clear()


def test_legacy_hydrogen_market_exchange_is_restored_for_old_versions(tmp_path):
    importer = get_base_inventory_import(tmp_path, [])
    importer.import_db.data = [
        {
            "name": "consumer",
            "reference product": "consumer product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for hydrogen, gaseous, low pressure",
                    "reference product": "hydrogen, gaseous, low pressure",
                    "product": "hydrogen, gaseous, low pressure",
                    "location": "RER",
                    "unit": "kilogram",
                    "amount": 1,
                    "type": "technosphere",
                }
            ],
        }
    ]

    importer.adapt_hydrogen_market_exchanges_for_legacy_versions()

    exchange = importer.import_db.data[0]["exchanges"][0]
    assert exchange["name"] == "market for hydrogen, gaseous"
    assert exchange["reference product"] == "hydrogen, gaseous"
    assert exchange["product"] == "hydrogen, gaseous"
    assert exchange["location"] == "GLO"
