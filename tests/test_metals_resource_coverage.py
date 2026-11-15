"""Preserved inventories need verified scope evidence before warnings clear."""

from collections import defaultdict
from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.materials.metals import Metals
from premise.materials.metals_resource_coverage import (
    OUTSIDE_SCOPE,
    UPSTREAM_SUPPLY,
    classify_preserved_resource,
    resource_diagnostic_is_informational,
    upstream_resource_coverage,
)
from premise.reporting.provenance import ProvenanceCollector
from premise.validation.validation import MetalsValidation


def dataset(name, product="cobalt", exchanges=()):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": list(exchanges),
    }


def feed(ds, amount=1):
    return {
        "name": ds["name"],
        "product": ds["reference product"],
        "location": ds["location"],
        "type": "technosphere",
        "unit": ds["unit"],
        "amount": amount,
    }


def resource(name="Cobalt", amount=1):
    return {
        "name": name,
        "amount": amount,
        "type": "biosphere",
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
    }


def providers(database):
    result = defaultdict(list)
    for ds in database:
        result[ds["name"], ds["reference product"], ds["location"]].append(ds)
    return result


@pytest.mark.parametrize(
    "failure",
    ["missing", "ambiguous", "empty", "negative", "nan", "volume", "isolated-cycle"],
)
def test_one_known_extraction_path_cannot_hide_an_unknown_branch(failure):
    ore = dataset("mine", exchanges=[resource()])
    other = dataset("other")
    refinery = dataset("refinery", exchanges=[feed(ore), feed(other)])
    database = [ore, refinery]
    if failure != "missing":
        database.append(other)
    if failure == "ambiguous":
        database.append(deepcopy(other))
    if failure == "negative":
        refinery["exchanges"][-1]["amount"] = -1
    if failure == "nan":
        refinery["exchanges"][-1]["amount"] = float("nan")
    if failure == "volume":
        refinery["exchanges"][-1]["unit"] = "cubic meter"
    if failure == "isolated-cycle":
        other["exchanges"].append(feed(other))
    result = classify_preserved_resource(refinery, providers(database), "cutoff")
    assert not resource_diagnostic_is_informational(result)
    assert not result["evidence"]["all material branches accounted for"]


def test_connected_trade_cycle_preserves_inventory_and_proves_boundary_coverage():
    ore = dataset("mine", exchanges=[resource()])
    market = dataset("market for cobalt")
    market["exchanges"] = [feed(market, 0.1), feed(ore, 0.9)]
    refinery = dataset("refinery", exchanges=[feed(market)])
    database = [ore, market, refinery]
    before = deepcopy(database)
    result = classify_preserved_resource(refinery, providers(database), "cutoff")
    assert result["reason"] == UPSTREAM_SUPPLY
    assert resource_diagnostic_is_informational(result)
    assert len(result["evidence"]["extraction boundaries"]) == 1
    assert database == before
    assert not upstream_resource_coverage(
        refinery, "Cobalt", providers(database), "cutoff", max_depth=0
    )["all material branches accounted for"]


@pytest.mark.parametrize("system", ["cutoff", "consequential"])
@pytest.mark.parametrize(
    "name",
    [
        "aluminium scrap, new, Recycled Content cut-off",
        "treatment of used beverage cans, for recycling, unsorted, sorting",
    ],
)
def test_explicit_secondary_boundaries_are_system_scoped(system, name):
    ore = dataset("mine", "aluminium oxide", [resource("Aluminium")])
    secondary = dataset(name, "aluminium scrap, prepared for melting")
    refinery = dataset(
        "aluminium production", "aluminium", [feed(ore), feed(secondary)]
    )
    result = classify_preserved_resource(
        refinery, providers([ore, secondary, refinery]), system
    )
    assert resource_diagnostic_is_informational(result) is (system == "cutoff")


def test_zero_burden_or_recycling_name_without_secondary_product_is_not_enough():
    secondary = dataset("treatment of cobalt, for recycling", "cobalt")
    refinery = dataset("refinery", exchanges=[feed(secondary)])
    result = classify_preserved_resource(
        refinery, providers([secondary, refinery]), "cutoff"
    )
    assert not resource_diagnostic_is_informational(result)


def test_use_labels_do_not_turn_carbon_electrodes_into_aluminium_feed():
    ore = dataset("mine", "aluminium oxide", [resource("Aluminium")])
    carbon = dataset("anode production", "anode, prebake, for aluminium electrolysis")
    refinery = dataset("aluminium production", "aluminium", [feed(ore), feed(carbon)])
    database = [ore, carbon, refinery]
    result = classify_preserved_resource(refinery, providers(database), "cutoff")
    assert resource_diagnostic_is_informational(result)
    assert (
        result["evidence"]["excluded exchanges"][0]["product"]
        == carbon["reference product"]
    )
    # The same syntax must not discard real target metal in the product core.
    refinery["exchanges"][-1]["product"] = "aluminium concentrate, from copper mining"
    result = classify_preserved_resource(refinery, providers(database), "cutoff")
    assert not resource_diagnostic_is_informational(result)


def test_unassigned_carrier_boundary_is_not_proof_of_extraction():
    carrier = dataset("market for lead concentrate", "lead concentrate")
    refinery = dataset("lead refinery", "lead", [feed(carrier)])
    result = classify_preserved_resource(
        refinery, providers([carrier, refinery]), "cutoff"
    )
    assert not resource_diagnostic_is_informational(result)
    assert "carrier" in result["evidence"]["unresolved links"][0]["reason"]


@pytest.mark.parametrize("product", ["sand", "dolomite", "bentonite", "cobalt"])
def test_nonmetal_scope_requires_both_flows_and_product_to_be_outside_scope(product):
    ds = dataset("mineral production", product, [resource("Sand, unspecified")])
    before = deepcopy(ds)
    result = classify_preserved_resource(ds, providers([ds]), "cutoff")
    assert resource_diagnostic_is_informational(result) is (product != "cobalt")
    if product != "cobalt":
        assert result["reason"] == OUTSIDE_SCOPE
    assert ds == before


def test_mixed_ore_content_remains_a_warning_with_unchanged_flows():
    ds = dataset(
        "gold-silver mine",
        "gold-silver, ingot",
        [resource("Gold", 0.2), resource("Silver", 1.3)],
    )
    before = deepcopy(ds)
    result = classify_preserved_resource(ds, providers([ds]), "cutoff")
    assert not resource_diagnostic_is_informational(result)
    assert result["reason"] == "preserved_target_content_unverified"
    assert ds == before


def test_scope_decisions_are_persisted_as_provenance_without_inventory_changes():
    ore = dataset("mine", exchanges=[resource()])
    refinery = dataset("refinery", exchanges=[feed(ore)])
    metals = object.__new__(Metals)
    metals.database = [ore, refinery]
    metals.system_model = "cutoff"
    metals.resource_diagnostics = [
        {
            "name": refinery["name"],
            "product": "cobalt",
            "location": "GLO",
            "reason": "preserved_unresolved",
        }
    ]
    before = deepcopy(metals.database)
    collector = ProvenanceCollector("test")
    scenario = ("image", "path", 2050, ())
    with collector.session(scenario, "metals"):
        metals.classify_resource_diagnostics()
        metals.classify_resource_diagnostics()  # Same evidence is not recorded twice.
    assert metals.database == before
    assert resource_diagnostic_is_informational(metals.resource_diagnostics[0])
    events = collector.payload_for(scenario)["events"]
    assert len(events) == 1
    assert events[0]["reason_code"] == "metals.resource_correction.applicability"
    ore["exchanges"] = []
    metals.classify_resource_diagnostics()
    assert not resource_diagnostic_is_informational(metals.resource_diagnostics[0])


def test_validation_counts_scope_decisions_and_retains_unresolved_warnings():
    validator = MetalsValidation(
        "image", "path", 2050, ["WEU"], [], SimpleNamespace(), "cutoff", "3.12"
    )
    good = dataset("sand quarry", "sand", [resource("Sand, unspecified")])
    unknown = dataset("gold-silver mine", "gold-silver, ingot", [resource("Gold")])
    validator.resource_diagnostics = [
        dict(
            classify_preserved_resource(ds, providers([ds]), "cutoff"),
            name=ds["name"],
            product=ds["reference product"],
            location="GLO",
            **{"preservation reason": "preserved_extraction"},
        )
        for ds in (good, unknown)
    ]
    validator.metals_list = []
    report = validator.run_metals_checks()
    assert len(report.warnings) == 1
    assert "gold-silver" in report.warnings[0].message
    phase = report.get_phase("sector:metals:resource_scope")
    assert phase.rule_results[0].checked_object_count == 2
    assert phase.rule_results[0].actual == {
        OUTSIDE_SCOPE: 1,
        "preserved_target_content_unverified": 1,
    }


def test_unverified_scope_label_cannot_silence_a_warning():
    assert not resource_diagnostic_is_informational(
        {"reason": UPSTREAM_SUPPLY, "applicability": "not_applicable"}
    )


@pytest.mark.parametrize("reference", [-1, 1, None])
def test_outgoing_slag_is_verified_from_provider_reference_sign(reference):
    ore = dataset("mine", exchanges=[resource()])
    waste = dataset(
        "market for cobalt slag",
        "cobalt slag",
        [
            {
                "name": "market for cobalt slag",
                "product": "cobalt slag",
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": reference,
            }
        ],
    )
    refinery = dataset("refinery", exchanges=[feed(ore), feed(waste, -0.2)])
    database = [ore, refinery] if reference is None else [ore, waste, refinery]
    result = classify_preserved_resource(refinery, providers(database), "cutoff")
    assert resource_diagnostic_is_informational(result) is (
        reference is not None and reference < 0
    )


@pytest.mark.parametrize("equipment", ["factory", "furnace"])
def test_unit_based_factory_is_not_a_material_feed(equipment):
    ore = dataset("mine", exchanges=[resource()])
    factory = dataset(f"market for cobalt {equipment}", f"cobalt {equipment}")
    factory["unit"] = "unit"
    refinery = dataset("refinery", exchanges=[feed(ore), feed(factory, 0.00001)])
    result = classify_preserved_resource(
        refinery, providers([ore, factory, refinery]), "cutoff"
    )
    assert resource_diagnostic_is_informational(result)
    assert (
        result["evidence"]["excluded exchanges"][0]["reason"]
        == "capital equipment, not material feed"
    )
