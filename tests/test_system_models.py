"""EN15804 imports retain native allocation and signed physical quantities."""

import ast
import runpy
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest
from openpyxl import Workbook

from premise.data_collection import IAMDataCollection
from premise.inventory_imports import AdditionalInventory, DefaultInventory
from premise.new_database import NewDatabase, check_system_model
from premise.system_models import adapt_en15804_scrap_exchanges

# These are native 3.12 EN15804 reference outputs, not inferred from names.
NATIVE_TARGETS = (
    ("aluminium scrap, new", "market for aluminium scrap, new", "RoW", 1),
    (
        "aluminium scrap, post-consumer",
        "market for aluminium scrap, post-consumer",
        "GLO",
        -1,
    ),
    ("copper scrap, sorted, pressed", "burden-free production", "GLO", 1),
    ("iron scrap, unsorted", "market for iron scrap, unsorted", "GLO", -1),
)


@pytest.fixture
def scrap_case():
    providers, exchanges = [], []
    for i, (product, name, location, output) in enumerate(NATIVE_TARGETS, 1):
        provider = {
            "name": name,
            "reference product": product,
            "location": location,
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": name,
                    "product": product,
                    "location": location,
                    "unit": "kilogram",
                    "type": "production",
                    "amount": output,
                }
            ],
        }
        providers.append(provider)
        exchanges.append(
            {
                "name": f"{product}, Recycled Content cut-off",
                "reference product": product,
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": -i / 10,
                "uncertainty type": 3,
                "loc": -i / 10,
                "scale": 0.01,
                "comment": "Source handoff.",
            }
        )
    consumer = {
        "name": "equipment production",
        "reference product": "equipment",
        "unit": "kilogram",
        "location": "GLO",
        "exchanges": [
            {
                "name": "equipment production",
                "product": "equipment",
                "unit": "kilogram",
                "location": "GLO",
                "amount": 1,
                "type": "production",
            },
            *exchanges,
        ],
    }
    return [consumer], providers


def test_en15804_native_allocation_and_uncertainty_are_preserved(scrap_case):
    data, providers = scrap_case
    before, native_before = deepcopy(data), deepcopy(providers)
    assert adapt_en15804_scrap_exchanges(data, providers) == 4
    matrix = np.diag([1.0, *[row[3] for row in NATIVE_TARGETS]])
    for i, (product, name, location, _) in enumerate(NATIVE_TARGETS, 1):
        after = data[0]["exchanges"][i]
        original = before[0]["exchanges"][i]
        assert (
            after["name"],
            after["product"],
            after["reference product"],
            after["location"],
        ) == (name, product, product, location)
        for field in ("amount", "unit", "type", "loc", "scale", "uncertainty type"):
            assert after[field] == original[field]
        matrix[i, 0] = -after["amount"]
    # Negative supply credits the positive-output new-Al market. Negative
    # reference outputs instead request positive treatment activity.
    supply = np.linalg.solve(matrix, [1, 0, 0, 0, 0])
    np.testing.assert_allclose(supply, [1, -0.1, 0.2, -0.3, 0.4])
    assert providers == native_before
    first = deepcopy(data)
    assert adapt_en15804_scrap_exchanges(data, providers) == 0
    assert data == first


@pytest.mark.parametrize(
    "problem",
    [
        "missing",
        "duplicate",
        "sign",
        "copper_burden",
        "positive_handoff",
        "conflicting_product",
    ],
)
def test_en15804_invalid_native_contract_fails_before_mutation(scrap_case, problem):
    data, providers = scrap_case
    if problem == "missing":
        providers.pop()
    elif problem == "duplicate":
        providers.append(deepcopy(providers[-1]))
    elif problem == "sign":
        providers[-1]["exchanges"][0]["amount"] = 1
    elif problem == "copper_burden":
        providers[2]["exchanges"].append({"type": "biosphere", "amount": 0.1})
    elif problem == "positive_handoff":
        data[0]["exchanges"][-1]["amount"] = 0.4
    else:
        data[0]["exchanges"][-1]["product"] = "different scrap"
    before = deepcopy(data)
    with pytest.raises(ValueError, match="EN15804"):
        adapt_en15804_scrap_exchanges(data, providers)
    assert data == before


@pytest.mark.parametrize("importer_class", [DefaultInventory, AdditionalInventory])
@pytest.mark.parametrize("system_model", ["cutoff", "EN15804"])
def test_excel_import_links_scrap_under_the_selected_model(
    tmp_path, scrap_case, importer_class, system_model
):
    data, providers = scrap_case
    if system_model == "cutoff":
        providers = [
            {
                "name": e["name"],
                "reference product": e["reference product"],
                "unit": e["unit"],
                "location": e["location"],
                "exchanges": [{**e, "type": "production", "amount": 1}],
            }
            for e in data[0]["exchanges"][1:]
        ]
    workbook = Workbook()
    sheet = workbook.active
    for row in [
        ["database", "equipment"],
        [],
        ["Activity", "equipment production"],
        ["reference product", "equipment"],
        ["location", "GLO"],
        ["unit", "kilogram"],
        ["production amount", 1],
        ["Exchanges"],
        ["name", "amount", "location", "unit", "type", "reference product"],
    ]:
        sheet.append(row)
    for e in data[0]["exchanges"]:
        sheet.append(
            [
                e["name"],
                e["amount"],
                e["location"],
                e["unit"],
                e["type"],
                e.get("reference product", e.get("product")),
            ]
        )
    path = tmp_path / "equipment.xlsx"
    workbook.save(path)
    workbook.close()
    if importer_class is DefaultInventory:
        importer = importer_class(providers, "3.12", "3.12", path, system_model, False)
    else:
        importer = importer_class(providers, "3.12", "3.12", path, system_model)
    importer.prepare_inventory()
    assert importer.list_unlinked == []
    (consumer,) = importer.import_db.data
    inputs = [e for e in consumer["exchanges"] if e["type"] == "technosphere"]
    assert [e["amount"] for e in inputs] == [-0.1, -0.2, -0.3, -0.4]
    assert [e["name"] for e in inputs] == [p["name"] for p in providers]


@pytest.mark.parametrize("model", ["cutoff", "consequential", "EN15804"])
def test_system_model_accepts_the_three_explicit_models(model):
    assert check_system_model(model) == model


def test_system_model_rejects_unknown_values():
    with pytest.raises(ValueError):
        check_system_model("unknown")
    with pytest.raises(TypeError):
        check_system_model(None)


def test_en15804_cache_is_separate_from_cutoff_with_the_same_database_name():
    builder = object.__new__(NewDatabase)
    builder.version = "3.12"
    builder.source_type = "brightway"
    builder.keep_imports_uncertainty = builder.keep_source_db_uncertainty = False
    builder.system_model = "cutoff"
    source = builder._database_cache_path("same source")
    imports = builder._database_cache_path("same source", inventories=True)
    builder.system_model = "EN15804"
    assert builder._database_cache_path("same source") != source
    assert builder._database_cache_path("same source", inventories=True) != imports


def test_en15804_iam_market_shares_match_attributional_normalization():
    import xarray as xr

    iam = object.__new__(IAMDataCollection)
    iam.year = 2050
    values = xr.DataArray(
        [[[80.0, 60.0]], [[20.0, 40.0]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["output A", "output B"],
            "region": ["EUR"],
            "year": [2030, 2050],
        },
    )
    fetch = iam._IAMDataCollection__fetch_market_data
    cutoff = fetch(values.copy(deep=True), {"A": "output A", "B": "output B"}, "cutoff")
    en15804 = fetch(
        values.copy(deep=True), {"A": "output A", "B": "output B"}, "EN15804"
    )
    xr.testing.assert_identical(en15804, cutoff)
    assert en15804.sel(variables="A", region="EUR", year=2050).item() == 0.6
    assert en15804.sel(variables="B", region="EUR", year=2050).item() == 0.4


def test_documentation_inventory_selection_keeps_en15804_attributional_sources():
    root = Path(__file__).parents[1]
    generator = runpy.run_path(
        str(root / "docs/_tools/generate_documentation_reference.py")
    )
    tree = ast.parse((root / "premise/database/new_database.py").read_text())
    select = generator["inventory_selection"]
    assert select(tree, "3.12", "EN15804") == select(tree, "3.12", "cutoff")
