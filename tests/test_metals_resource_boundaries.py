from copy import deepcopy

import pytest

import premise.metals as metals_module
from premise.metals import Metals
from premise.new_database import config as premise_config
from premise.metals_resource_boundaries import build_resource_boundary_plan
from premise.metals_resource_boundaries import (
    RARE_EARTH_CARBONATE,
    RARE_EARTH_CARRIER_NAME,
    RARE_EARTH_OXIDE_NAME,
)


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


def correct(database, monkeypatch, system="cutoff", version="3.12"):
    metals = object.__new__(Metals)
    metals.database = database
    metals.version = version
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


@pytest.mark.parametrize(
    "name",
    [
        "silicon production, metallurgical grade",
        "silicon production, metallurgical grade, photovoltaic supply chain",
    ],
)
def test_silicon_boundary_preserves_sand_and_downstream_purification(name, monkeypatch):
    sand = dataset("silica sand production", "silica sand")
    primary = dataset(name, "silicon, metallurgical grade", [feed(sand, 2.7)])
    refining = dataset(
        "silicon production, electronic grade",
        "silicon, electronic grade",
        [feed(primary, 1.1)],
    )
    database = [sand, primary, refining]
    before = deepcopy(database)
    correct(database, monkeypatch)
    assert primary["exchanges"][-1] == resource("Silicon", 1) | {"uncertainty type": 0}
    assert primary["exchanges"][:-1] == before[1]["exchanges"]
    assert sand == before[0] and refining == before[2]
    first = deepcopy(database)
    correct(database, monkeypatch)
    assert database == first


@pytest.mark.parametrize(
    "problem", ["missing", "duplicate", "silicon_feed", "no_silica", "downstream"]
)
def test_silicon_boundary_requires_primary_reduction_recipe(problem):
    sand = dataset("market for silica sand", "silica sand")
    silicon = dataset(
        "market for silicon, metallurgical grade", "silicon, metallurgical grade"
    )
    primary = dataset(
        "silicon production, metallurgical grade",
        "silicon, metallurgical grade",
        [feed(sand, 2.7)],
    )
    database = [sand, silicon, primary]
    if problem == "missing":
        database.remove(sand)
    elif problem == "duplicate":
        database.append(deepcopy(sand))
    elif problem == "silicon_feed":
        primary["exchanges"].append(feed(silicon, 1))
    elif problem == "no_silica":
        primary["exchanges"] = []
    else:
        primary["name"] = "silicon production, electronic grade"
    assert not build_resource_boundary_plan(database, {"Silicon"}).assignments


def carbonate_branch():
    carrier = dataset(
        RARE_EARTH_CARRIER_NAME,
        RARE_EARTH_CARBONATE,
        [
            resource("Neodymium", 0.0993),
            resource("Dysprosium", 0.0158),
            resource("Clay, unspecified", 2500),
        ],
        "CN",
    )
    market = dataset(
        f"market for {RARE_EARTH_CARBONATE}", RARE_EARTH_CARBONATE, [feed(carrier)]
    )
    nd = dataset(
        RARE_EARTH_OXIDE_NAME, "neodymium oxide", [feed(market, 1.4147346)], "CN"
    )
    dy = dataset(
        RARE_EARTH_OXIDE_NAME, "dysprosium oxide", [feed(market, 14.388964)], "CN"
    )
    metal = dataset(
        "dysprosium production, calcium reduction", "dysprosium", [feed(dy, 1.366517)]
    )
    return [carrier, market, nd, dy, metal]


@pytest.mark.parametrize("reverse", [False, True])
def test_complete_rare_earth_transfer_removes_shared_basket_once(reverse, monkeypatch):
    database = carbonate_branch()
    carrier, market, nd, dy, metal = database
    before = deepcopy(database)
    unrelated = dataset(
        "rare earth element mine operation and beneficiation, bastnaesite ore",
        "rare earth oxide concentrate, 70% REO",
        [resource("Neodymium", 0.12), resource("Dysprosium", 0.002)],
    )
    database.append(unrelated)
    other_before = deepcopy(unrelated)
    if reverse:
        database.reverse()
    correct(database, monkeypatch)
    assert [e["amount"] for e in carrier["exchanges"]] == [0, 0, 2500]
    assert nd["exchanges"][-1]["amount"] == pytest.approx(0.857355987410879)
    assert dy["exchanges"][-1]["amount"] == pytest.approx(0.8713206808633849)
    assert nd["exchanges"][:-1] == before[2]["exchanges"]
    assert dy["exchanges"][:-1] == before[3]["exchanges"]
    assert market == before[1] and metal == before[4] and unrelated == other_before
    first = deepcopy(database)
    correct(database, monkeypatch)
    assert database == first


@pytest.mark.parametrize(
    "failure",
    [
        "missing_oxide",
        "missing_flow",
        "missing_supplier",
        "cycle",
        "unknown_consumer",
        "unknown_product",
        "duplicate_resource",
    ],
)
def test_incomplete_rare_earth_branch_is_preserved_atomically(failure, monkeypatch):
    database = carbonate_branch()
    carrier, market, nd, dy, metal = database
    if failure == "missing_oxide":
        database.remove(nd)
    elif failure == "missing_supplier":
        database.remove(carrier)
    elif failure == "cycle":
        market["exchanges"] = [feed(market)]
    elif failure == "unknown_consumer":
        database.append(
            dataset("other carbonate processing", "other product", [feed(market)])
        )
    elif failure == "unknown_product":
        nd["reference product"] = "mixed rare earth oxide"
    elif failure == "duplicate_resource":
        carrier["exchanges"].append(resource("Neodymium", 0.1))
    flows = {"Neodymium", "Dysprosium"} - (
        {"Neodymium"} if failure == "missing_flow" else set()
    )
    plan = build_resource_boundary_plan(database, flows)
    assert not plan.assignments
    assert plan.diagnostics
    before = deepcopy(database)
    if failure != "missing_flow":
        correct(database, monkeypatch)
        assert database == before


def test_rare_earth_boundaries_preserve_consequential(monkeypatch):
    database = carbonate_branch()
    before = deepcopy(database)
    correct(database, monkeypatch, system="consequential")
    assert database == before


@pytest.mark.parametrize("kind", ["lithium", "rare_earth"])
def test_unlabelled_material_market_branch_preserves_boundaries(kind):
    if kind == "rare_earth":
        database = carbonate_branch()
        market = database[1]
        flows = {"Neodymium", "Dysprosium"}
    else:
        mineral = dataset("spodumene mine operation", "spodumene")
        market = dataset("market for spodumene", "spodumene", [feed(mineral)])
        carbonate = dataset(
            "lithium carbonate production, from spodumene",
            "lithium carbonate",
            [feed(market, 8)],
        )
        database, flows = [mineral, market, carbonate], {"Lithium"}
    unknown = feed(market, 0.1)
    unknown.pop("product")
    market["exchanges"].append(unknown)
    assert not build_resource_boundary_plan(database, flows).assignments


@pytest.mark.parametrize("version", premise_config["SUPPORTED_EI_VERSIONS"])
@pytest.mark.parametrize("system", ["cutoff", "consequential"])
def test_boundary_correction_preserves_quantities_and_model_scope_across_versions(
    version, system, monkeypatch
):
    database = carbonate_branch()
    ore = dataset("market for zinc concentrate", "zinc concentrate")
    zinc = dataset("market for zinc", "zinc")
    sand = dataset("market for silica sand", "silica sand")
    brine = dataset("lithium brine inspissation", "lithium brine, 6.7 % Li")
    boundaries = [
        dataset(
            "primary zinc production from concentrate",
            "copper, cathode",
            [feed(ore, 2)],
        ),
        dataset("primary zinc production from concentrate", "silver", [feed(ore, 2)]),
        dataset(
            "sodium arsenide production from Imperial smelting furnace",
            "sodium arsenide",
            [feed(zinc, 0.521)],
        ),
        dataset(
            "silicon production, metallurgical grade",
            "silicon, metallurgical grade",
            [feed(sand, 2.7)],
        ),
        dataset(
            "lithium carbonate production, from concentrated brine",
            "lithium carbonate",
            [feed(brine, 4.19)],
        ),
    ]
    database.extend([ore, zinc, sand, brine, *boundaries])
    before = deepcopy(database)
    metals = correct(database, monkeypatch, system=system, version=version)
    for old, new in zip(before, database):
        assert [e for e in old["exchanges"] if e["type"] == "technosphere"] == [
            e for e in new["exchanges"] if e["type"] == "technosphere"
        ]
    if system == "consequential":
        assert database == before
    else:
        available = metals.get_biosphere_in_ground_resource_flow_names()
        for boundary, element in zip(
            boundaries, ("Copper", "Silver", "Arsenic", "Silicon", "Lithium")
        ):
            resources = [e for e in boundary["exchanges"] if e["type"] == "biosphere"]
            if element in available:
                assert (
                    len(resources) == 1
                    and resources[0]["name"] == element
                    and resources[0]["amount"] > 0
                )
            else:
                assert not resources
                assert any(
                    d["name"] == boundary["name"] and d["reason"].endswith("unresolved")
                    for d in metals.resource_diagnostics
                )
        first = deepcopy(database)
        correct(database, monkeypatch, version=version)
        assert database == first
