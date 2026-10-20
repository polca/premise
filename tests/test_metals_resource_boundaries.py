from copy import deepcopy

import pytest

import premise.metals as metals_module
from premise.metals import Metals
from premise.metals_resource_boundaries import build_resource_boundary_plan


def dataset(name, product, exchanges=(), location="GLO"):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": "kilogram",
        "exchanges": list(exchanges),
    }


def feed(ds, amount=1):
    return {
        "name": ds["name"],
        "product": ds["reference product"],
        "location": ds["location"],
        "type": "technosphere",
        "unit": "kilogram",
        "amount": amount,
    }


def resource(element, amount):
    return {
        "name": element,
        "amount": amount,
        "type": "biosphere",
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
    }


def correct(database, monkeypatch, system="cutoff"):
    metals = object.__new__(Metals)
    metals.database = database
    metals.version = "3.12"
    metals.system_model = system
    metals._validation_targets = {}
    metals.get_mining_share_dataset_ids = lambda: set()
    metals.get_mapped_metal_dataset_ids = lambda: set()
    monkeypatch.setattr(
        metals_module, "record_change_event", lambda *args, **kwargs: None
    )
    metals.post_allocation_correction()
    return metals


@pytest.mark.parametrize(
    "product,element", [("copper, cathode", "Copper"), ("silver", "Silver")]
)
def test_primary_coproduct_reagents_do_not_replace_extraction(
    product, element, monkeypatch
):
    ore = dataset("market for zinc concentrate", "zinc concentrate")
    reagent = dataset(f"market for {product}", product, [resource(element, 0.5)])
    refinery = dataset(
        "primary zinc production from concentrate",
        product,
        [feed(ore, 2), feed(reagent, 0.0002)],
        "RoW",
    )
    before = deepcopy(refinery["exchanges"])
    database = [refinery, reagent, ore]
    correct(database, monkeypatch)
    assert refinery["exchanges"][:-1] == before
    assert refinery["exchanges"][-1]["name"] == element
    assert refinery["exchanges"][-1]["amount"] == 1
    first = deepcopy(database)
    correct(database, monkeypatch)
    assert database == first


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "duplicate",
        "large_reagent",
        "no_feed",
        "unsupported_unit",
        "missing_flow",
    ],
)
def test_coproduct_boundary_preserves_unsupported_or_unresolved_cases(
    failure, monkeypatch
):
    ore = dataset("market for zinc concentrate", "zinc concentrate")
    reagent = dataset("market for silver", "silver")
    refinery = dataset(
        "primary zinc production from concentrate",
        "silver",
        [feed(ore, 2), feed(reagent, 0.001)],
    )
    database = [refinery, ore, reagent]
    if failure == "missing":
        database.remove(ore)
    elif failure == "duplicate":
        database.append(deepcopy(reagent))
    elif failure == "large_reagent":
        refinery["exchanges"][1]["amount"] = 1
    elif failure == "no_feed":
        refinery["exchanges"].pop(0)
    elif failure == "unsupported_unit":
        refinery["unit"] = "unit"
    plan = build_resource_boundary_plan(
        database, set() if failure == "missing_flow" else {"Silver"}
    )
    assert not plan.assignments
    assert id(refinery) in plan.reserved_ids
    if failure != "missing_flow":
        before = deepcopy(refinery)
        correct(database, monkeypatch)
        assert refinery == before


def test_scoped_resource_boundaries_preserve_consequential(monkeypatch):
    ore = dataset("market for zinc concentrate", "zinc concentrate")
    refinery = dataset(
        "primary zinc production from concentrate", "silver", [feed(ore, 2)]
    )
    database = [ore, refinery]
    before = deepcopy(database)
    correct(database, monkeypatch, system="consequential")
    assert database == before


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "source,product",
    [
        ("from concentrated brine", "lithium brine, 6.7 % Li"),
        ("from spodumene", "spodumene"),
    ],
)
def test_primary_carbonate_counts_li_once_upstream_of_refining(
    source, product, reverse, monkeypatch
):
    mineral = dataset(
        (
            "lithium brine inspissation"
            if "brine" in product
            else "spodumene mine operation"
        ),
        product,
    )
    if product == "spodumene":
        mineral["exchanges"].append(resource("Spodumene", 1))
    market = dataset(f"market for {product}", product, [feed(mineral)])
    carbonate = dataset(
        f"lithium carbonate production, {source}",
        "lithium carbonate",
        [feed(market, 5)],
    )
    chloride = dataset(
        "lithium chloride production", "lithium chloride", [feed(carbonate, 0.88)]
    )
    metal = dataset(
        "lithium production, lithium chloride electrolysis",
        "lithium",
        [feed(chloride, 6.62)],
    )
    database = [mineral, market, carbonate, chloride, metal]
    if reverse:
        database.reverse()
    before = {id(d): deepcopy(d["exchanges"]) for d in database}
    correct(database, monkeypatch)
    assert carbonate["exchanges"][-1]["amount"] == pytest.approx(0.1878518839324383)
    for ds in (mineral, market, chloride, metal):
        assert ds["exchanges"] == before[id(ds)]
    assert carbonate["exchanges"][:-1] == before[id(carbonate)]
    first = deepcopy(database)
    correct(database, monkeypatch)
    assert database == first


@pytest.mark.parametrize(
    "problem", ["cycle", "missing", "secondary", "existing_extraction"]
)
def test_lithium_boundary_requires_proven_primary_mineral_chain(problem):
    mineral = dataset("spodumene mine operation", "spodumene")
    market = dataset("market for spodumene", "spodumene", [feed(mineral)])
    carbonate = dataset(
        "lithium carbonate production, from spodumene",
        "lithium carbonate",
        [feed(market, 8)],
    )
    database = [mineral, market, carbonate]
    if problem == "cycle":
        market["exchanges"] = [feed(market)]
    elif problem == "missing":
        database.remove(mineral)
    elif problem == "secondary":
        mineral["name"] = "treatment of waste spodumene"
        market["exchanges"] = [feed(mineral)]
    else:
        mineral["exchanges"].append(resource("Lithium", 0.01))
    plan = build_resource_boundary_plan(database, {"Lithium"})
    assert not plan.assignments
    assert id(carbonate) in plan.reserved_ids


def test_arsenide_resource_preserves_zinc_proxy_and_purification_inputs(monkeypatch):
    zinc = dataset("market for zinc", "zinc", [resource("Zinc", 1)])
    arsenide = dataset(
        "sodium arsenide production from Imperial smelting furnace",
        "sodium arsenide",
        [feed(zinc, 0.52083)],
    )
    arsine = dataset("arsine production", "arsine", [feed(arsenide, 1.94)])
    arsenic = dataset(
        "arsenic production, high purity, from arsine",
        "arsenic, high purity, from arsine",
        [feed(arsine, 1.529944)],
    )
    database = [zinc, arsenide, arsine, arsenic]
    before = deepcopy(database)
    correct(database, monkeypatch)
    assert arsenide["exchanges"][-1]["amount"] == pytest.approx(0.5206833338401479)
    assert arsenide["exchanges"][:-1] == before[1]["exchanges"]
    for index in (0, 2, 3):
        assert database[index] == before[index]
    first = deepcopy(database)
    correct(database, monkeypatch)
    assert database == first


@pytest.mark.parametrize(
    "problem", ["missing_zinc", "arsenic_feed", "unrelated_synthesis"]
)
def test_arsenide_boundary_excludes_other_routes(problem):
    zinc = dataset("market for zinc", "zinc")
    arsenic = dataset("market for arsenic", "arsenic")
    arsenide = dataset(
        "sodium arsenide production from Imperial smelting furnace",
        "sodium arsenide",
        [feed(zinc, 0.52083)],
    )
    database = [zinc, arsenic, arsenide]
    if problem == "missing_zinc":
        database.remove(zinc)
    elif problem == "arsenic_feed":
        arsenide["exchanges"].append(feed(arsenic))
    else:
        arsenide["name"] = "sodium arsenide synthesis"
    assert not build_resource_boundary_plan(database, {"Arsenic"}).assignments
