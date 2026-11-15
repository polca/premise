"""Retain primary aggregate extraction and trace declared supplier units."""

from collections import defaultdict
from copy import deepcopy
import json

import pytest

from premise.materials.metals import correct_metal_resource_exchanges
import premise.materials.metals_rules as rules_module
from premise.materials.metals_resource_coverage import (
    classify_preserved_resource,
    resource_diagnostic_is_informational,
)


def dataset(name, product, unit="kilogram", inputs=()):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": unit,
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": "GLO",
                "unit": unit,
                "type": "production",
                "amount": 1,
            },
            *inputs,
        ],
    }


def resource(element, amount):
    return {
        "name": element,
        "amount": amount,
        "unit": "kilogram",
        "type": "biosphere",
        "categories": ("natural resource", "in ground"),
    }


def feed(ds, amount=1):
    return {
        "name": ds["name"],
        "product": ds["reference product"],
        "location": ds["location"],
        "unit": ds["unit"],
        "type": "technosphere",
        "amount": amount,
    }


def index(database):
    result = defaultdict(list)
    for ds in database:
        result[ds["name"], ds["reference product"], ds["location"]].append(ds)
    return result


def test_cobalt_copper_aggregate_retains_source_amount_and_uncertainty():
    copper = {
        **resource("Copper", 0.37),
        "uncertainty type": 2,
        "loc": -0.994252273,
        "scale": 0.2,
    }
    ds = dataset(
        "cobalt production",
        "copper, anode",
        inputs=[copper, resource("Cobalt", 0.04), resource("Gold", 0.00001)],
    )
    before = deepcopy(copper)
    assert correct_metal_resource_exchanges(ds)
    assert (
        copper == before
    )  # Source allocation is retained, not a one-kg/purity assumption.
    assert [e["amount"] for e in ds["exchanges"][2:]] == [0, 0]
    once = deepcopy(ds)
    assert correct_metal_resource_exchanges(ds)
    assert ds == once


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "zero",
        "duplicate",
        "nan",
        "copper-feed",
        "volume-feed",
        "production",
        "multiple-outputs",
    ],
)
def test_aggregate_retention_requires_a_supported_primary_context(failure):
    ds = dataset(
        "cobalt production",
        "copper, anode",
        inputs=[resource("Copper", 0.37), resource("Cobalt", 0.04)],
    )
    if failure == "missing":
        ds["exchanges"].pop(1)
    elif failure == "zero":
        ds["exchanges"][1]["amount"] = 0
    elif failure == "duplicate":
        ds["exchanges"].append(resource("Copper", 0.1))
    elif failure == "nan":
        ds["exchanges"][1]["amount"] = float("nan")
    elif failure in {"copper-feed", "volume-feed"}:
        ds["exchanges"].append(
            feed(
                dataset(
                    "supplier",
                    "copper",
                    "cubic meter" if failure == "volume-feed" else "kilogram",
                )
            )
        )
    elif failure == "production":
        ds["exchanges"][0]["amount"] = 2
    else:
        ds["exchanges"].append(deepcopy(ds["exchanges"][0]))
    original = list(ds["exchanges"])
    original_amounts = [e["amount"] for e in original]
    assert not correct_metal_resource_exchanges(ds)
    assert ds["exchanges"] == original
    for e, amount in zip(original, original_amounts):
        assert e["amount"] is amount or e["amount"] == amount


def test_reviewed_anode_carrier_traversal_checks_every_primary_branch():
    mine = dataset("mine", "copper concentrate", inputs=[resource("Copper", 0.25)])
    smelter = dataset("smelting", "copper, anode", inputs=[feed(mine, 4)])
    cobalt = dataset(
        "cobalt production", "copper, anode", inputs=[resource("Copper", 0.37)]
    )
    market = dataset(
        "market for copper, anode",
        "copper, anode",
        inputs=[feed(smelter, 0.97), feed(cobalt, 0.03)],
    )
    refinery = dataset(
        "electrorefining of copper, anode", "copper, cathode", inputs=[feed(market)]
    )
    database = [mine, smelter, cobalt, market, refinery]
    before = deepcopy(database)
    result = classify_preserved_resource(refinery, index(database), "cutoff")
    assert resource_diagnostic_is_informational(result)
    assert len(result["evidence"]["extraction boundaries"]) == 2
    assert database == before
    cobalt["exchanges"].pop()
    assert not resource_diagnostic_is_informational(
        classify_preserved_resource(refinery, index(database), "cutoff")
    )


@pytest.mark.parametrize("matching_unit", [True, False])
def test_nickel_volume_route_needs_matching_reference_units_not_density(matching_unit):
    electrolyte = dataset(
        "cobalt production",
        "electrolyte, nickel-rich",
        "cubic meter",
        [resource("Nickel", 11.5)],
    )
    rename = dataset(
        "electrolyte conversion",
        "nickel-rich materials",
        inputs=[feed(electrolyte, 0.012986)],
    )
    refinery = dataset(
        "processing of nickel-rich materials", "nickel", inputs=[feed(rename, 1.0741)]
    )
    database = [electrolyte, rename, refinery]
    if not matching_unit:
        rename["exchanges"][-1]["unit"] = "kilogram"
    before = deepcopy(database)
    result = classify_preserved_resource(refinery, index(database), "cutoff")
    assert resource_diagnostic_is_informational(result) is matching_unit
    assert database == before


@pytest.mark.parametrize("failure", [None, "primary", "missing", "cycle"])
def test_negative_recyclable_output_requires_verified_cutoff_suppliers(failure):
    mine = dataset("mine", "copper concentrate", inputs=[resource("Copper", 0.25)])
    scrap = dataset("copper scrap, Recycled Content cut-off", "copper scrap")
    market = dataset("market for copper scrap", "copper scrap", inputs=[feed(scrap)])
    refinery = dataset(
        "refinery", "copper", inputs=[feed(mine, 4), feed(market, -0.01)]
    )
    database = [mine, scrap, market, refinery]
    if failure == "primary":
        scrap["exchanges"].append(resource("Copper", 0.1))
    elif failure == "missing":
        database.remove(scrap)
    elif failure == "cycle":
        market["exchanges"][-1] = feed(market)
    before = deepcopy(database)
    result = classify_preserved_resource(refinery, index(database), "cutoff")
    assert resource_diagnostic_is_informational(result) is (failure is None)
    assert database == before


def test_electronics_scrap_recovery_is_an_explicit_secondary_boundary():
    recovered = dataset(
        "treatment of electronics scrap, metals recovery in copper smelter",
        "metal part of electronics scrap, in copper, anode",
    )
    refinery = dataset("electrorefining", "copper", inputs=[feed(recovered)])
    result = classify_preserved_resource(
        refinery, index([recovered, refinery]), "cutoff"
    )
    assert resource_diagnostic_is_informational(result)


@pytest.mark.parametrize(
    "name",
    [
        "treatment of used cable",
        "treatment of non-Fe-Co-metals, from used Li-ion battery, hydrometallurgical processing",
        "treatment of non-Fe-Co-metals, from used Li-ion battery, pyrometallurgical processing",
    ],
)
@pytest.mark.parametrize("model", ["cutoff", "consequential"])
def test_reviewed_secondary_copper_outputs_are_system_scoped(name, model):
    recovered = dataset(name, "copper, cathode")
    refinery = dataset("refinery", "copper", inputs=[feed(recovered)])
    result = classify_preserved_resource(refinery, index([refinery, recovered]), model)
    assert resource_diagnostic_is_informational(result) is (model == "cutoff")


@pytest.mark.parametrize("failure", ["duplicate", "unit", "basis", "traversal"])
def test_retained_carrier_configuration_requires_source_qualified_units(
    failure, tmp_path, monkeypatch
):
    data = json.loads(rules_module.RESOURCE_POLICY_PATH.read_text())
    entry = data["source_retained_carrier_targets"][0]
    if failure == "duplicate":
        data["source_retained_carrier_targets"].append(deepcopy(entry))
    elif failure == "unit":
        entry["unit"] = "cubic meter"
    elif failure == "basis":
        entry.pop("basis")
    else:
        entry["traverse_carrier_product"] = "yes"
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(data))
    monkeypatch.setattr(rules_module, "RESOURCE_POLICY_PATH", path)
    with pytest.raises(rules_module.MetalsConfigError, match="source-retained"):
        rules_module.load_resource_policy()
