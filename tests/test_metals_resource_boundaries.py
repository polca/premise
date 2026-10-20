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
