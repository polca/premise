"""Independent physical audits: analytical scores, real faults and valid controls."""

from copy import deepcopy
import json
import re
from itertools import permutations

import numpy as np
import pytest

import premise
from premise.audit import (
    AuditConfig,
    PhysicalContract,
    audit_database,
    compare_audits,
    default_indicators,
    BudgetTerm,
    ExchangeBudget,
    AuditReport,
)
from premise.audit.graph import identity
from premise.inventory_store import CompactInventoryStore, LegacyInventoryStore


@pytest.mark.parametrize("order", list(permutations(range(3))))
def test_ill_scaled_recycling_cycle_preserves_forward_and_adjoint_units(order):
    from scipy.sparse import csc_matrix

    from premise.audit.graph import Factorization

    # A high-throughput cycle with a 2**-20 loss has an analytical solution.
    # The unit conversion spans 2**40 without changing its physical yield.
    conversion, loss = 2.0**40, 2.0**-20
    matrix = csc_matrix(
        [[1, -conversion, 0], [0, 1, -1 / conversion], [-(1 - loss), 0, 1]]
    )
    demand = np.array([0.0, 1.0, 0.0])
    resource = np.array([2.0**-60, 0.0, 0.0])
    supply = np.array([2.0**60, 2.0**20, 2.0**60 - 2.0**40])
    scores = np.array([2.0**-40, 1.0, 2.0**-40])
    order = np.array(order)
    permuted = matrix[order, :][:, order].tocsc()
    original = permuted.copy()
    factor = Factorization(permuted)
    forward = factor.solve(demand[order])
    adjoint = factor.solve(resource[order], transpose=True)
    # The near-unit cycle amplifies roundoff by its inverse loss, even after
    # correcting the unit scales. Bound that intrinsic conditioning explicitly.
    precision = 4 * np.finfo(float).eps / loss
    np.testing.assert_allclose(forward, supply[order], rtol=precision, atol=0)
    np.testing.assert_allclose(adjoint, scores[order], rtol=precision, atol=0)
    assert resource[order] @ forward == pytest.approx(1.0, rel=precision)
    assert demand[order] @ adjoint == pytest.approx(1.0, rel=precision)
    assert (permuted != original).nnz == 0


def process(
    name, product=None, location="GLO", unit="kilogram", production=1, exchanges=()
):
    product = product or name
    return dict(
        name=name,
        **{"reference product": product},
        location=location,
        unit=unit,
        database="audit-test",
        code=name + location,
        exchanges=[
            dict(
                name=name,
                product=product,
                location=location,
                unit=unit,
                type="production",
                amount=production,
            ),
            *exchanges,
        ],
    )


def flow(
    name, amount, categories=("natural resource", "in ground"), unit="kilogram", **extra
):
    return dict(
        name=name,
        amount=amount,
        categories=categories,
        unit=unit,
        type="biosphere",
        **extra,
    )


def feed(node, amount=1, **extra):
    return dict(
        name=node["name"],
        product=node["reference product"],
        location=node["location"],
        unit=node["unit"],
        type="technosphere",
        amount=amount,
        input=(node["database"], node["code"]),
        **extra,
    )


def test_lignocellulosic_igcc_includes_main_fuel_alongside_fossil_auxiliaries():
    biomass = process("biomass production", "lignocellulosic biomass")
    gas = process(
        "natural gas extraction",
        "natural gas, high pressure",
        exchanges=[flow("Gas, natural", 1, unit="cubic meter")],
    )
    plant = process(
        "electricity production, at biomass-fired IGCC power plant",
        "electricity, high voltage",
        unit="kilowatt hour",
        exchanges=[
            feed(biomass, 1),
            feed(gas, 0.01),
            flow("Carbon dioxide, non-fossil", 0.5 * 44 / 12, ("air",)),
            flow("Carbon dioxide, fossil", 0.01 * 0.75 * 44 / 12, ("air",)),
        ],
    )
    report = audit_database([biomass, gas, plant])
    assert "CARBON.ORIGIN" not in ids(report)
    assert not report.has_errors


def carbon_graph(biogenic_share=0, corrupt=False):
    fossil = process(
        "kerosene production, petroleum refinery operation",
        "kerosene",
        exchanges=[flow("Carbon dioxide, fossil", 0.2, ("air",))],
    )
    biomass = process("kerosene production, from biomass", "kerosene", location="RER")
    market = process(
        "market for kerosene",
        "kerosene",
        exchanges=[feed(fossil, 1 - biogenic_share), feed(biomass, biogenic_share)],
    )
    plane = process(
        "transport, freight, aircraft, dedicated freight, long haul",
        "transport, freight, aircraft",
        unit="ton kilometer",
        exchanges=[
            feed(market),
            flow(
                "Carbon dioxide, fossil",
                0 if corrupt else 3.15 * (1 - biogenic_share),
                ("air", "lower stratosphere + upper troposphere"),
            ),
            flow(
                "Carbon dioxide, non-fossil",
                3.15 if corrupt else 3.15 * biogenic_share,
                ("air",),
            ),
        ],
    )
    return [fossil, biomass, market, plane]


def iridium(amount=1, production=1):
    return process(
        "iridium production, economic allocation, proxy",
        "iridium",
        location="ZA",
        production=production,
        exchanges=[
            flow("Iridium", amount),
            flow("Carbon dioxide, fossil", 2, ("air",)),
        ],
    )


def ids(report):
    return {finding.rule_id for finding in report.findings}


def test_public_api_and_all_relics_indicators():
    assert premise.audit_database is audit_database
    assert premise.AuditConfig is AuditConfig
    specs = default_indicators()
    assert sum(s.family == "metals" for s in specs) == 65
    assert len({s.key for s in specs}) == len(specs)


@pytest.mark.parametrize("backend", [list, LegacyInventoryStore, CompactInventoryStore])
def test_analytical_scores_nonunit_output_readonly_and_one_factorization(backend):
    metal = iridium(2, production=2)
    market = process("market for iridium", "iridium", exchanges=[feed(metal, 3)])
    data = [metal, market]
    original = deepcopy(data)
    report = audit_database(backend(data))
    assert data == original
    assert not report.has_errors
    assert report.score(identity(metal), "metals.Iridium") == pytest.approx(1)
    assert report.score(identity(market), "metals.Iridium") == pytest.approx(3)
    assert report.score(identity(market), "metals.Iridium", scope="direct") == 0
    assert report.metadata["factorizations"] == 1
    assert report.metadata["forward_solves"] == 2
    assert report.metadata["transpose_solves"] == len(report.indicators)


def test_kerosene_mutation_detected_despite_identical_total_co2():
    before, after = audit_database(carbon_graph()), audit_database(
        carbon_graph(corrupt=True)
    )
    key = identity(carbon_graph()[-1])
    assert "CARBON.ORIGIN" not in ids(before)
    assert "CARBON.ORIGIN" in ids(after)
    assert sum(
        before.score(key, k) for k in ("co2.fossil", "co2.non_fossil")
    ) == pytest.approx(
        sum(after.score(key, k) for k in ("co2.fossil", "co2.non_fossil"))
    )
    issue = next(f for f in after.findings if f.rule_id == "CARBON.ORIGIN")
    assert issue.confidence == "high" and issue.issue.severity == "error"
    assert issue.issue.actual == 0
    assert issue.issue.expected == (1.0, 1.0)


@pytest.mark.parametrize(
    "emission_name,expected_fault",
    [("Carbon dioxide, fossil", False), ("Carbon dioxide, non-fossil", True)],
)
def test_marine_heavy_fuel_oil_carbon_origin_is_audited(emission_name, expected_fault):
    refinery = process(
        "heavy fuel oil production, petroleum refinery operation", "heavy fuel oil"
    )
    ship = process(
        "transport, freight, sea, container ship, heavy fuel oil",
        "transport, freight, sea",
        unit="ton kilometer",
        exchanges=[feed(refinery), flow(emission_name, 3.15, ("air",))],
    )
    report = audit_database([refinery, ship])
    assert ("CARBON.ORIGIN" in ids(report)) is expected_fault


@pytest.mark.parametrize("share", [0, 0.2, 0.5, 1])
def test_valid_fossil_biogenic_mixtures_are_not_errors(share):
    report = audit_database(carbon_graph(share))
    assert "CARBON.ORIGIN" not in ids(report)


def test_missing_iridium_fault_detected_without_changing_carbon_score():
    correct = iridium()
    missing = deepcopy(correct)
    missing["exchanges"] = [e for e in missing["exchanges"] if e["name"] != "Iridium"]
    before, after = audit_database([correct]), audit_database([missing])
    assert before.score(identity(correct), "co2.fossil") == after.score(
        identity(missing), "co2.fossil"
    )
    assert "CONTRACT.premise.iridium_content" in ids(after)
    assert "RESOURCE.PRIMARY_COVERAGE" in ids(after)
    assert after.score(identity(missing), "metals.Iridium") == 0
    assert (
        next(c for c in after.coverage if c["indicator"] == "metals.Iridium")[
            "matched_inventory_flows"
        ]
        == 0
    )


def test_duplicate_iridium_and_old_biosphere_alias():
    duplicate = iridium(2)
    assert "CONTRACT.premise.iridium_content" in ids(audit_database([duplicate]))
    old = iridium()
    old["exchanges"][1]["name"] = "Iridium, in ground"
    assert not audit_database([old]).has_errors


def test_upstream_only_resource_supply_is_valid():
    metal = iridium()
    wire = process("wire forming", "iridium wire", exchanges=[feed(metal, 0.5)])
    report = audit_database([metal, wire])
    assert not report.has_errors
    assert report.score(identity(wire), "metals.Iridium") == pytest.approx(0.5)


def test_unused_mine_does_not_hide_disconnected_resource():
    metal = iridium()
    empty = iridium(0)
    empty["code"] = "empty"
    empty["name"] = "iridium production, primary, disconnected"
    empty["exchanges"][0]["name"] = empty["name"]
    report = audit_database([metal, empty])
    finding = next(
        f for f in report.findings if f.rule_id == "RESOURCE.PRIMARY_COVERAGE"
    )
    assert finding.activity == identity(empty)


def test_negative_treatment_and_credit_cancellation_are_preserved():
    metal = iridium()
    burden = process("burden", "component", exchanges=[feed(metal)])
    credit = process("credit", "credit", exchanges=[feed(metal, -1)])
    combined = process("combined", "service", exchanges=[feed(burden), feed(credit)])
    treatment = process(
        "treatment of waste", "waste", production=-1, exchanges=[feed(metal)]
    )
    report = audit_database([metal, burden, credit, combined, treatment])
    assert report.score(identity(combined), "metals.Iridium") == pytest.approx(0)
    assert report.score(
        identity(combined), "metals.Iridium", scope="positive"
    ) == pytest.approx(1)
    assert report.score(
        identity(combined), "metals.Iridium", scope="negative"
    ) == pytest.approx(-1)
    assert report.score(identity(treatment), "metals.Iridium") == pytest.approx(-1)
    assert not report.has_errors


@pytest.mark.parametrize(
    "family,name,unit,categories",
    [
        (
            "water.freshwater",
            "Water, river",
            "cubic meter",
            ("natural resource", "in water"),
        ),
        (
            "land.occupation",
            "Occupation, industrial area",
            "square meter-year",
            ("natural resource", "land"),
        ),
    ],
)
def test_documented_water_land_budgets_detect_thousandfold_errors(
    family, name, unit, categories
):
    good = process(
        "documented manufacturing", exchanges=[flow(name, 1, categories, unit)]
    )
    contract = PhysicalContract(
        "budget",
        family,
        "Independent engineering reference",
        name_pattern=good["name"],
        minimum=1,
        maximum=1,
    )
    config = AuditConfig(contracts=(contract,))
    assert not audit_database([good], config=config).has_errors
    bad = deepcopy(good)
    bad["exchanges"][1]["amount"] *= 1000
    assert "CONTRACT.budget" in ids(audit_database([bad], config=config))


def test_wrong_biosphere_link_compartment_and_unit_detected():
    good = iridium()
    resource = good["exchanges"][1]
    resource["input"] = ("bio", "ir")
    good["exchanges"][2]["input"] = ("bio", "co2")
    catalog = {
        ("bio", "ir"): dict(
            name="Iridium",
            unit="kilogram",
            categories=("natural resource", "in ground"),
        ),
        ("bio", "co2"): dict(
            name="Carbon dioxide, fossil", unit="kilogram", categories=("air",)
        ),
    }
    assert not audit_database([good], biosphere=catalog).has_errors
    for field, value, rule in (
        ("unit", "cubic meter", "GRAPH.BIOSPHERE_UNIT"),
        ("categories", ("air",), "GRAPH.BIOSPHERE_IDENTITY"),
        ("input", ("bio", "missing"), "GRAPH.BIOSPHERE_LINK"),
    ):
        bad = deepcopy(good)
        bad["exchanges"][1][field] = value
        assert rule in ids(audit_database([bad], biosphere=catalog))


def test_missing_supplier_blocks_scores_instead_of_inventing_zero():
    a = process("a")
    b = process("b", exchanges=[feed(a)])
    report = audit_database([b])
    assert report.metadata["scoring_status"] == "blocked"
    assert report.has_errors and report.total.shape[0] == 0


def test_sampling_disjoint_reproducible_and_input_order_independent():
    data = [
        process(
            f"manufacturing {i:03d}",
            exchanges=[flow("Carbon dioxide, fossil", i, ("air",))],
        )
        for i in range(100)
    ]
    config = AuditConfig(sample_size=50, seed=123)
    before = audit_database(data, config=config)
    after = audit_database(list(reversed(data)), config=config)
    assert before.sample == after.sample
    assert len({tuple(r["activity"]) for r in before.sample}) == 50
    assert before.metadata["sampled_panel_counts"] == dict(
        uniform=30, stratified=15, risk=5
    )
    np.testing.assert_allclose(before.total, after.total)


def test_unknown_synthetic_carbon_is_not_assumed_biogenic():
    synthetic = process(
        "kerosene production, synthetic, unknown carbon source", "kerosene"
    )
    consumer = process(
        "kerosene, burned in turbine",
        exchanges=[feed(synthetic), flow("Carbon dioxide, non-fossil", 3.15, ("air",))],
    )
    report = audit_database([synthetic, consumer])
    rule = next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")
    assert rule["unresolved"] == 1
    assert "CARBON.ORIGIN" not in ids(report)


def test_cyclic_fuel_markets_terminate_with_unknown_branch_visible():
    a = process("market for kerosene", "kerosene")
    b = process("market for kerosene", "kerosene", location="RER")
    a["exchanges"].append(feed(b, 0.5))
    b["exchanges"].append(feed(a, 0.5))
    consumer = process(
        "kerosene, burned in turbine",
        exchanges=[feed(a), flow("Carbon dioxide, fossil", 3.15, ("air",))],
    )
    report = audit_database([a, b, consumer])
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")[
            "unresolved"
        ]
        == 1
    )


def test_comparison_retains_zero_events_and_qualifies_native_controls():
    old = audit_database(
        [iridium(0)], config=AuditConfig(include_default_contracts=False)
    )
    new = audit_database([iridium()])
    rows = compare_audits(old, new)
    row = next(r for r in rows if r.get("indicator") == "metals.Iridium")
    assert row["event"] == "zero_to_nonzero" and row["change_percent"] is None
    assert "CONTRACT.premise.iridium_content" not in ids(old)


def test_report_exports_are_json_valid_searchable_and_do_not_overwrite(tmp_path):
    node = iridium(0)
    report = audit_database([node])
    out = report.write(tmp_path / "audit")
    assert (
        json.loads((out / "audit.json").read_text())["metadata"]["factorizations"] == 1
    )
    assert "metals.Iridium" in (out / "scores.csv").read_text()
    assert "application/json" in (out / "report.html").read_text()
    restored = AuditReport.read(out)
    np.testing.assert_array_equal(report.total, restored.total)
    assert ids(restored) == ids(report)
    with pytest.raises(FileExistsError):
        report.write(out)


def test_report_html_escapes_activity_markup(tmp_path):
    report = audit_database([process("</script><script>alert(1)</script>")])
    page = (report.write(tmp_path / "safe") / "report.html").read_text()
    assert "</script><script>alert(1)</script>" not in page


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(sample_size=0),
        dict(sample_size=True),
        dict(seed=False),
        dict(numerical_rtol=-1),
        dict(carbon_fraction_tolerance=2),
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        AuditConfig(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(minimum=2, maximum=1),
        dict(minimum=float("nan")),
        dict(scope="guess"),
        dict(name_pattern="["),
    ],
)
def test_invalid_contract_is_rejected(kwargs):
    values = dict(minimum=0)
    values.update(kwargs)
    with pytest.raises((ValueError, re.error)):
        PhysicalContract("a", "co2.fossil", "source", **values)


def test_e5_name_does_not_make_the_entire_petrol_blend_biogenic():
    fossil = process("petrol production, petroleum refinery operation", "petrol")
    bio = process("ethanol production, from wheat", "ethanol")
    blend = process(
        "petrol production, 5% ethanol by volume from biomass",
        "petrol",
        exchanges=[feed(fossil, 0.95), feed(bio, 0.05)],
    )
    fossil_carbon = 0.95 * 0.85
    bio_carbon = 0.05 * 0.52
    consumer = process(
        "petrol, burned in engine",
        exchanges=[
            feed(blend),
            flow("Carbon dioxide, fossil", fossil_carbon * 44 / 12, ("air",)),
            flow("Carbon dioxide, non-fossil", bio_carbon * 44 / 12, ("air",)),
        ],
    )
    report = audit_database([fossil, bio, blend, consumer])
    assert "CARBON.ORIGIN" not in ids(report)


def test_unknown_market_component_is_retained_in_carbon_denominator():
    data = carbon_graph()
    unknown = process("unknown renewable component", "unrecognized jet component")
    data[2]["exchanges"] = [
        data[2]["exchanges"][0],
        feed(data[0], 0.5),
        feed(unknown, 0.5),
    ]
    report = audit_database([*data, unknown])
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")[
            "unresolved"
        ]
        == 1
    )


@pytest.mark.parametrize(
    "product",
    [
        "biomethane, from biogas upgrading",
        "methane, high pressure",
        "unrecognized gas component",
    ],
)
@pytest.mark.parametrize("unit", ["kilogram", "megajoule"])
def test_gas_market_keeps_alternative_carbon_across_names_and_units(product, unit):
    fossil = process(
        "natural gas production",
        "natural gas, high pressure",
        unit="cubic meter",
        exchanges=[flow("Gas, natural", 1, unit="cubic meter")],
    )
    alternative = process("alternative methane production", product, unit=unit)
    market = process(
        "market for natural gas, high pressure",
        "natural gas, high pressure",
        unit="cubic meter",
        exchanges=[
            feed(fossil, 0.5),
            feed(alternative, 0.39 if unit == "kilogram" else 0.39 * 48),
        ],
    )
    consumer = process(
        "electricity production, natural gas",
        unit="kilowatt hour",
        exchanges=[
            feed(market),
            flow("Carbon dioxide, fossil", 1, ("air",)),
            flow("Carbon dioxide, non-fossil", 1, ("air",)),
        ],
    )
    report = audit_database([fossil, alternative, market, consumer])
    assert "CARBON.ORIGIN" not in ids(report)
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")[
            "unresolved"
        ]
        == 1
    )


def test_native_fame_input_is_not_omitted_from_transport_carbon():
    data = carbon_graph()
    data[0]["name"] = "diesel production, petroleum refinery operation"
    for node in data[:3]:
        node["reference product"] = "diesel"
        node["exchanges"][0]["product"] = "diesel"
    data[2]["name"] = "market for diesel"
    data[2]["exchanges"] = [data[2]["exchanges"][0], feed(data[0])]
    fame = process(
        "fatty acid methyl ester production, from palm oil", "fatty acid methyl ester"
    )
    data[-1]["exchanges"] = [
        data[-1]["exchanges"][0],
        feed(data[2], 0.9),
        feed(fame, 0.1),
        flow("Carbon dioxide, fossil", 2.7, ("air",)),
        flow("Carbon dioxide, non-fossil", 0.3, ("air",)),
    ]
    report = audit_database([*data, fame])
    assert "CARBON.ORIGIN" not in ids(report)


def test_synthetic_methane_utility_does_not_establish_product_carbon_origin():
    fossil = process(
        "natural gas production",
        "natural gas",
        exchanges=[flow("Gas, natural", 1, unit="cubic meter")],
    )
    synthetic = process(
        "methane, from electrochemical methanation",
        "methane",
        exchanges=[feed(fossil, 0.01)],
    )
    consumer = process(
        "heat production, methane",
        exchanges=[feed(synthetic), flow("Carbon dioxide, non-fossil", 2.75, ("air",))],
    )
    report = audit_database([fossil, synthetic, consumer])
    assert "CARBON.ORIGIN" not in ids(report)
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")[
            "unresolved"
        ]
        == 1
    )


def test_forwarded_combustion_service_is_not_burned_twice():
    data = carbon_graph()
    data[-1]["name"] = "kerosene, burned in engine"
    data[-1]["reference product"] = "kerosene, burned in engine"
    data[-1]["exchanges"][0].update(
        name=data[-1]["name"], product=data[-1]["reference product"]
    )
    forwarded = process(
        "kerosene, burned in engine", location="RER", exchanges=[feed(data[-1])]
    )
    report = audit_database([*data, forwarded])
    assert not report.has_errors
    assert "CARBON.MASS_PLAUSIBILITY" not in ids(report)


def test_duplicated_component_and_treatment_sign_faults_require_explicit_contracts():
    metal = iridium()
    component = process("component assembly", exchanges=[feed(metal)])
    treatment = process(
        "treatment of component", production=-1, exchanges=[feed(metal)]
    )
    config = AuditConfig(
        contracts=(
            PhysicalContract(
                "component",
                "metals.Iridium",
                "Declared one-kg component",
                name_pattern="component assembly",
                scope="total",
                minimum=1,
                maximum=1,
            ),
            PhysicalContract(
                "retirement",
                "metals.Iridium",
                "Declared negative reference output",
                name_pattern="treatment of component",
                scope="total",
                minimum=-1,
                maximum=-1,
            ),
        )
    )
    assert not audit_database([metal, component, treatment], config=config).has_errors
    component["exchanges"].append(feed(metal))
    treatment["exchanges"][0]["amount"] = 1
    report = audit_database([metal, component, treatment], config=config)
    assert {"CONTRACT.component", "CONTRACT.retirement"} <= ids(report)
    assert report.score(
        identity(component), "metals.Iridium", scope="positive"
    ) == pytest.approx(2)


def test_complete_water_ledger_and_missing_return_fault():
    water = process(
        "water supply",
        "tap water",
        exchanges=[
            flow("Water, river", 0.001, ("natural resource", "in water"), "cubic meter")
        ],
    )
    product = process(
        "aqueous product",
        exchanges=[
            feed(water, 1000),
            flow("Water", 0.4, ("air",), "cubic meter"),
            flow("Water", 0.599, ("water",), "cubic meter"),
        ],
    )
    budget = ExchangeBudget(
        "water_ledger",
        "Independent complete process budget",
        (
            BudgetTerm("water supply", "technosphere", "kilogram", 0.001),
            BudgetTerm("Water", "biosphere", "cubic meter", -1, categories=("air",)),
            BudgetTerm("Water", "biosphere", "cubic meter", -1, categories=("water",)),
            BudgetTerm("aqueous product", "production", "kilogram", -0.001),
        ),
        name_pattern="aqueous product",
        unit="cubic meter",
    )
    config = AuditConfig(budgets=(budget,))
    assert not audit_database([water, product], config=config).has_errors
    product["exchanges"].pop()
    bad = audit_database([water, product], config=config)
    assert "BUDGET.water_ledger" in ids(bad)


def test_secondary_and_unrelated_coproducts_are_not_forced_to_one_kg():
    secondary = process("iridium production, secondary", "iridium")
    coproduct = process("zinc mine operation", "copper")
    report = audit_database([secondary, coproduct])
    assert not report.has_errors
    assert "RESOURCE.PRIMARY_COVERAGE" not in ids(report)
    assert (
        next(
            r for r in report.rule_coverage if r["rule"] == "RESOURCE.PRIMARY_COVERAGE"
        )["unresolved"]
        == 1
    )


def test_static_hydrogen_fault_when_scenario_supply_is_available():
    native = process("market for hydrogen, liquid", "hydrogen, liquid")
    scenario = process(
        "market for hydrogen, gaseous, low pressure", "hydrogen, gaseous, low pressure"
    )
    recipe = process(
        "ammonia production, with market-average hydrogen",
        "ammonia",
        exchanges=[feed(native, 0.176)],
    )
    assert "HYDROGEN.SCENARIO_SUPPLY" in ids(audit_database([native, scenario, recipe]))
    recipe["exchanges"][1] = feed(scenario, 0.176)
    assert "HYDROGEN.SCENARIO_SUPPLY" not in ids(
        audit_database([native, scenario, recipe])
    )


def test_unspecified_subcompartment_alias_does_not_create_false_identity_error():
    data = iridium()
    data["exchanges"][1]["input"] = ("bio", "ir")
    data["exchanges"][2].update(input=("bio", "co2"), categories=("air", "unspecified"))
    catalog = {
        ("bio", "ir"): dict(
            name="Iridium",
            unit="kilogram",
            categories=("natural resource", "in ground"),
        ),
        ("bio", "co2"): dict(
            name="Carbon dioxide, fossil", unit="kilogram", categories=("air",)
        ),
    }
    assert not audit_database([data], biosphere=catalog).has_errors


def test_sourced_contract_is_scoped_by_system_model():
    contract = PhysicalContract(
        "scoped",
        "metals.Iridium",
        "Attributional content convention",
        name_pattern="generic producer",
        minimum=1,
        system_models=("cutoff",),
    )
    report = audit_database(
        [process("generic producer")],
        config=AuditConfig(system_model="consequential", contracts=(contract,)),
    )
    assert "CONTRACT.scoped" not in ids(report)
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CONTRACT.scoped")[
            "not_applicable"
        ]
        == 1
    )


def test_unsupported_biomass_volume_does_not_disappear_from_fuel_balance():
    wood = process("wood chip production", "wood chips", unit="cubic meter")
    gas = process(
        "natural gas extraction", "natural gas", exchanges=[flow("Gas, natural", 1)]
    )
    plant = process(
        "electricity production, biomass IGCC",
        "electricity",
        unit="kilowatt hour",
        exchanges=[
            feed(wood, 1),
            feed(gas, 0.01),
            flow("Carbon dioxide, non-fossil", 1, ("air",)),
        ],
    )
    report = audit_database([wood, gas, plant])
    assert "CARBON.UNSUPPORTED_FUEL_BASIS" in ids(report)
    assert "CARBON.ORIGIN" not in ids(report)


def test_synthetic_gas_is_not_declared_fossil_from_its_name_alone():
    gas = process(
        "natural gas production, from hydrogen and carbon dioxide", "natural gas"
    )
    plant = process(
        "natural gas, burned in turbine",
        exchanges=[feed(gas), flow("Carbon dioxide, non-fossil", 2.7, ("air",))],
    )
    report = audit_database([gas, plant])
    assert (
        next(r for r in report.rule_coverage if r["rule"] == "CARBON.ORIGIN")[
            "unresolved"
        ]
        == 1
    )


def test_companion_review_queue_is_unflagged_and_not_claimed_reviewed():
    data = [process(f"component {i:03}") for i in range(140)]
    report = audit_database(data)
    assert len(report.review_sample) == 100
    assert all(r["status"] == "not_reviewed" for r in report.review_sample)
    assert len({tuple(r["activity"]) for r in report.review_sample}) == 100


def test_ambiguous_explicit_sample_is_rejected():
    data = [process("a")]
    with pytest.raises(ValueError):
        audit_database(
            data,
            config=AuditConfig(explicit_sample=(identity(data[0]), identity(data[0]))),
        )


def test_indicator_definition_changes_are_not_silently_compared():
    from dataclasses import replace

    indicator = default_indicators()[0]
    node = process("a", exchanges=[flow("Carbon dioxide, fossil", 1, ("air",))])
    old = audit_database([node], indicators=(indicator,))
    new = audit_database(
        [node],
        indicators=(
            replace(indicator, rules=((r"^Carbon dioxide, fossil$", ("air",), 2),)),
        ),
    )
    with pytest.raises(ValueError, match="definitions"):
        compare_audits(old, new)


def test_missing_activity_metadata_blocks_with_structural_finding():
    node = process("a")
    node["name"] = None
    report = audit_database([node])
    assert report.metadata["scoring_status"] == "blocked"
    assert "GRAPH.ACTIVITY_FIELDS" in ids(report)


def test_malformed_biosphere_metadata_is_reported_without_crashing():
    node = process("a", exchanges=[flow(None, 1, categories=(["air"],))])
    report = audit_database([node])
    assert "GRAPH.BIOSPHERE_IDENTITY" in ids(report)


def test_shared_suppliers_form_investigation_cluster_and_comparison_labels(tmp_path):
    import csv

    data = carbon_graph(corrupt=True)
    second = deepcopy(data[-1])
    second["name"] += ", second"
    second["code"] += "second"
    data.append(second)
    report = audit_database(data)
    report.write(tmp_path / "audit")
    with (tmp_path / "audit" / "root-causes.csv").open() as stream:
        clusters = list(csv.DictReader(stream))
    origin = next(r for r in clusters if r["rule"] == "CARBON.ORIGIN")
    assert origin["grouping_basis"] == "shared fuel suppliers"
    assert int(origin["finding_count"]) == 2
    assert "not proof" in origin["qualification"]
    before = audit_database(carbon_graph())
    changes = compare_audits(before, report)
    fossil = next(r for r in changes if r.get("indicator") == "co2.fossil")
    assert fossil["before_score"] > fossil["after_score"]
