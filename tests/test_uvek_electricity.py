"""Synthetic native-topology checks; no licensed coefficients or IAM key."""

import copy
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

import premise.core.transformation as transformation_module
import premise.electricity.electricity as electricity_module
from premise.electricity.uvek import (
    NATIVE_GRIDS,
    TOPOLOGY_LIMITATION,
    UVEKElectricity,
    identity,
    native_grid_losses,
)
from premise.reporting.provenance import ProvenanceCollector
from premise.inventory.inventory_store import (
    CompactInventoryStore,
    LegacyInventoryStore,
    get_scenario_inventory,
)


def activity(name, location="CH", unit="kilowatt hour", product=None):
    dataset = {
        "name": name,
        "reference product": product or name,
        "location": location,
        "unit": unit,
        "database": "synthetic-u",
        "code": f"{name}-{location}",
        "exchanges": [],
    }
    dataset["exchanges"] = [exchange(dataset, 1, kind="production")]
    dataset["exchanges"][0]["production volume"] = 0
    return dataset


def exchange(supplier, amount, kind="technosphere"):
    return {
        "name": supplier["name"],
        "product": supplier["reference product"],
        "location": supplier["location"],
        "unit": supplier["unit"],
        "type": kind,
        "amount": amount,
        "input": (supplier["database"], supplier["code"]),
    }


@pytest.fixture
def native(monkeypatch):
    wind1, wind2, pv = [
        activity(name) for name in ("Wind one", "Wind two", "Residential PV")
    ]
    busbar = activity("Electricity mix")
    busbar["exchanges"] += [
        exchange(wind1, 0.2),
        exchange(wind2, 0.3),
        exchange(pv, 0.5),
    ]
    production = activity("Electricity, production mix")
    production["exchanges"] += [exchange(busbar, 1)]
    network = activity("Native network", unit="kilometer")
    grids = []
    previous = busbar
    for voltage, amount in zip(NATIVE_GRIDS, [1.1, 1.2, 1.3]):
        grid = activity(NATIVE_GRIDS[voltage])
        wire = exchange(network, 0.0123)
        wire.update(
            {
                "uncertainty type": 2,
                "loc": -4.398,
                "scale": 0.2,
                "comment": "source infrastructure",
            }
        )
        grid["exchanges"] += [
            exchange(previous, amount),
            wire,
            {
                "name": "Sulfur hexafluoride",
                "type": "biosphere",
                "amount": 0.0007,
                "unit": "kilogram",
                "categories": ("air",),
                "input": ("bio310", "sf6"),
            },
        ]
        previous = grid
        grids.append(grid)
    consumer = activity("Electricity consumer", unit="unit")
    consumer["exchanges"].append(exchange(grids[-1], 12))
    database = [wind1, wind2, pv, busbar, production, network, *grids, consumer]
    mapping = {"Wind Onshore": [wind1, wind2], "Solar PV Residential": [pv]}
    seen = []

    class Mapping:
        def __init__(self, **kwargs):
            seen.append(kwargs)

        def generate_fuel_map(self):
            return {}

        def generate_powerplant_map(self):
            return mapping

        def generate_powerplant_fuels_map(self):
            return {}

        def generate_powerplant_efficiency_bounds(self):
            return {}, {}

    class Geo:
        def __init__(self, **kwargs):
            pass

        def ecoinvent_to_iam_location(self, location):
            return "R1"

        def iam_to_ecoinvent_location(self, region):
            return ["CH"]

    monkeypatch.setattr(electricity_module, "InventorySet", Mapping)
    monkeypatch.setattr(transformation_module, "Geomap", Geo)
    mix = xr.DataArray(
        [[[0.6, 0.6], [0.4, 0.4]]],
        dims=("region", "variables", "year"),
        coords={"region": ["R1"], "variables": list(mapping), "year": [2020, 2060]},
    )
    iam = SimpleNamespace(
        regions=["R1"], electricity_mix=mix, electricity_technology_efficiencies=None
    )
    kwargs = dict(
        database=database,
        iam_data=iam,
        model="remind",
        pathway="synthetic",
        year=2050,
        version="2026",
        system_model="cutoff",
        family="uvek",
        biosphere_version="3.10",
    )
    return SimpleNamespace(
        database=database,
        mapping=mapping,
        kwargs=kwargs,
        iam=iam,
        seen=seen,
        busbar=busbar,
        production=production,
        grids=grids,
        consumer=consumer,
        wind1=wind1,
        wind2=wind2,
        pv=pv,
    )


def test_native_losses_are_source_multipliers_without_volume_weights(native):
    losses = native_grid_losses(native.database)
    assert losses["CH"]["high"]["upstream kWh per kWh"] == 1.1
    assert losses["CH"]["medium"]["loss kWh per kWh"] == pytest.approx(0.2)
    assert losses["CH"]["low"]["upstream suppliers"] == [identity(native.grids[1])]


def test_native_constructor_keeps_technosphere_and_biosphere_axes(native, monkeypatch):
    versions = []
    monkeypatch.setattr(
        electricity_module,
        "biosphere_flows_dictionary",
        lambda version: versions.append(version) or {},
    )
    transform = UVEKElectricity(**native.kwargs)
    assert transform.version == "2026"
    assert transform.biosphere_version == "3.10"
    assert versions == ["3.10"]
    assert native.seen[0]["family"] == "uvek"
    assert transform.network_loss["CH"]["high"]["loss kWh per kWh"] == pytest.approx(
        0.1
    )


def test_native_entry_point_preserves_grids_codes_and_consumer_links(native):
    scenario = {
        "database": native.database,
        "iam data": native.iam,
        "model": "remind",
        "pathway": "synthetic",
        "year": 2050,
    }
    grids_before = copy.deepcopy(native.grids)
    consumer_before = copy.deepcopy(native.consumer)
    with pytest.warns(UserWarning, match="residential solar"):
        result = electricity_module._update_electricity(
            scenario, "2026", "cutoff", False, "uvek", "3.10"
        )
    assert result is scenario
    assert native.grids == grids_before
    assert native.consumer == consumer_before
    for dataset in (native.busbar, native.production):
        inputs = [e for e in dataset["exchanges"] if e["type"] == "technosphere"]
        assert [e["amount"] for e in inputs] == pytest.approx([0.24, 0.36, 0.4])
        assert [e["input"] for e in inputs] == [
            (p["database"], p["code"]) for p in (native.wind1, native.wind2, native.pv)
        ]
        assert all("premise electricity technology" not in e for e in inputs)
        assert (
            dataset["uvek electricity provenance"]["background context"][
                "biosphere_version"
            ]
            == "3.10"
        )
    assert result["electricity limitations"] == [TOPOLOGY_LIMITATION]
    assert (
        result["_validation_phase_results"][0]["phase_id"] == "sector:electricity:uvek"
    )


def test_aggregate_weights_expand_source_pure_kwh_mix(native):
    intermediate = activity("Native PV aggregate")
    intermediate["exchanges"].append(exchange(native.pv, 1))
    native.busbar["exchanges"][-1] = exchange(intermediate, 0.5)
    native.database.append(intermediate)
    transform = UVEKElectricity(**native.kwargs)
    assert transform.source_weights[identity(native.busbar)][identity(native.pv)] == 0.5
    assert [e["amount"] for e in transform.plan_mixes()[0][1]] == pytest.approx(
        [0.24, 0.36, 0.4]
    )


def test_source_trade_loop_uses_native_quantities(native):
    native.busbar["exchanges"].append(exchange(native.production, 0.1))
    transform = UVEKElectricity(**native.kwargs)
    assert transform.source_weights[identity(native.busbar)][
        identity(native.wind1)
    ] == pytest.approx(0.2 / 0.9)
    assert [e["amount"] for e in transform.plan_mixes()[0][1]] == pytest.approx(
        [0.24, 0.36, 0.4]
    )


def test_missing_source_weights_refuse_before_any_mix_changes(native):
    native.busbar["exchanges"] = native.busbar["exchanges"][:-1]
    another = activity("Another native PV generator", "FR")
    native.database.append(another)
    native.mapping["Solar PV Residential"].append(another)
    before = copy.deepcopy(native.database)
    transform = UVEKElectricity(**native.kwargs)
    with pytest.raises(
        ValueError, match="Required roles: Solar PV Residential"
    ) as caught:
        transform.plan_mixes()
    allocations = caught.value.diagnostics["missing supplier allocations"]
    assert len(allocations) == 2
    assert allocations[0]["candidates"][0]["identity"] == identity(native.pv)
    assert native.database == before


def test_unmapped_generator_auxiliary_grid_is_not_a_source_weight(native):
    unknown = activity("Unmapped generator")
    unknown["exchanges"] += [
        exchange(native.grids[0], 0.2),
        exchange(native.database[5], 0.1),
    ]
    native.busbar["exchanges"] = [native.busbar["exchanges"][0], exchange(unknown, 1)]
    native.database.append(unknown)
    transform = UVEKElectricity(**native.kwargs)
    assert transform.source_weights[identity(native.busbar)] == {}


def test_unique_clic_role_supplier_needs_no_invented_geographic_split(native):
    native.busbar["exchanges"] = native.busbar["exchanges"][:-1]
    native.pv["clic provenance"] = {
        "slug": "reviewed-pv",
        "role_bindings": ["electricity.yaml:Solar PV Residential"],
    }
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    assert (
        plans[0][-1]["Solar PV Residential"]["basis"]
        == "unique reviewed CLIC role supplier"
    )
    assert plans[0][1][-1]["amount"] == 0.4


def test_multiple_clic_suppliers_without_weights_do_not_get_equal_shares(native):
    native.busbar["exchanges"] = native.busbar["exchanges"][:-1]
    native.pv["clic provenance"] = {"slug": "reviewed-pv"}
    other = copy.deepcopy(native.pv)
    other.update(location="FR", code="other-pv")
    native.database.append(other)
    native.mapping["Solar PV Residential"].append(other)
    native.pv["location"] = "DE"
    transform = UVEKElectricity(**native.kwargs)
    with pytest.raises(
        ValueError, match="equal supplier/geographic shares are not assumed"
    ):
        transform.plan_mixes()


def test_native_validation_refuses_changed_grids_and_stale_provider_codes(native):
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    transform.update_native_mixes(plans)
    assert transform.validate_native_mixes(plans).valid
    native.busbar["exchanges"][1]["input"] = ("wrong-db", "wrong-code")
    with pytest.raises(ValueError, match="export supplier identity mismatch"):
        transform.validate_native_mixes(plans)
    native.busbar["exchanges"][1]["input"] = (
        native.wind1["database"],
        native.wind1["code"],
    )
    native.grids[-1]["exchanges"][1]["amount"] *= 2
    with pytest.raises(ValueError, match="voltage-grid quantities changed"):
        transform.validate_native_mixes(plans)


def test_native_weighting_and_topology_appear_in_structured_provenance(native):
    transform = UVEKElectricity(**native.kwargs)
    collector = ProvenanceCollector("native-test")
    with collector.session(("remind", "synthetic", 2050), "electricity"):
        transform.update_native_mixes(transform.plan_mixes())
    events = collector.payload_for(("remind", "synthetic", 2050))["events"]
    assert len(events) == 6
    event = next(
        e for e in events if e["reason_code"] == "electricity.uvek.native_busbar_mix"
    )
    assert event["reason_code"] == "electricity.uvek.native_busbar_mix"
    assert "residential solar" in event["explanation"]
    suppliers = event["computed_target_values"]["supplier weights"]["Wind Onshore"][
        "suppliers"
    ]
    assert [p["source coefficient"] for p in suppliers] == [0.2, 0.3]
    assert [p["share"] for p in suppliers] == [0.4, 0.6]


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", "2025"),
        ("biosphere_version", None),
        ("biosphere_version", "3.12"),
        ("system_model", "consequential"),
    ],
)
def test_unsupported_native_context_is_rejected(native, field, value):
    native.kwargs[field] = value
    with pytest.raises(ValueError, match="pilot requires"):
        UVEKElectricity(**native.kwargs)


def test_eci_default_skip_and_unknown_family_are_preserved():
    scenario = {"iam data": SimpleNamespace(electricity_mix=None)}
    assert (
        electricity_module._update_electricity(scenario, "3.10", "cutoff") is scenario
    )
    with pytest.raises(ValueError, match="Unsupported electricity background family"):
        electricity_module._update_electricity(
            scenario, "2026", "cutoff", family="other"
        )


def test_nuclear_efficiency_gap_is_reported_without_generic_lhv(native):
    native.mapping["Nuclear"] = [native.wind1]
    efficiencies = xr.DataArray(
        [[[1.2]]],
        dims=("region", "variables", "year"),
        coords={"region": ["R1"], "variables": ["Nuclear"], "year": [2050]},
    )
    native.iam.electricity_technology_efficiencies = efficiencies
    transform = UVEKElectricity(**native.kwargs)
    before = copy.deepcopy(native.wind1)
    with pytest.warns(UserWarning, match="nuclear fuel specifications"):
        transform.update_electricity_efficiency()
    assert native.wind1 == before
    assert "Nuclear efficiency unavailable" in transform.efficiency_limitations[0]
    assert "not inferred from a generic LHV" in transform.efficiency_limitations[0]
    report = transform.validate_native_mixes([])
    assert any(
        i.rule_id == "electricity.uvek.efficiency_unavailable" for i in report.warnings
    )


def test_signed_edges_are_aggregated_before_positive_net_weighting(native):
    native.busbar["exchanges"][1]["amount"] = 0.4
    imports = activity("Electricity imports")
    imports["exchanges"].append(exchange(native.wind1, -0.2))
    native.database.append(imports)
    native.busbar["exchanges"].append(exchange(imports, 1))
    before = copy.deepcopy(imports)
    transform = UVEKElectricity(**native.kwargs)
    assert transform.source_weights[identity(native.busbar)][
        identity(native.wind1)
    ] == pytest.approx(0.2)
    plans = transform.plan_mixes()
    assert [e["amount"] for e in plans[0][1]] == pytest.approx([0.24, 0.36, 0.4])
    transform.update_native_mixes(plans)
    assert imports == before
    # Signed source edges can leave an entirely nonnegative net busbar vector.
    # Its actual native weighting basis must still survive in provenance.
    assert all(
        value >= 0
        for value in transform.source_weights[identity(native.busbar)].values()
    )
    evidence = native.busbar["uvek electricity provenance"]["supplier weights"][
        "Wind Onshore"
    ]
    assert evidence["basis"] == "native positive source mix coefficients"
    assert [
        item["coefficient"] for item in evidence["signed net source coefficients"]
    ] == pytest.approx([0.2, 0.3])
    assert any(
        e["coefficient"] == -0.2
        for e in transform.source_weight_diagnostics["source exchange coefficients"]
    )


def test_negative_net_supplier_coefficient_stays_in_weighting_evidence(native):
    native.busbar["exchanges"][1]["amount"] = -0.2
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    assert [e["amount"] for e in plans[0][1]] == pytest.approx([0.6, 0.4])
    assert plans[0][-1]["Wind Onshore"]["signed net source coefficients"] == [
        {"identity": identity(native.wind1), "coefficient": -0.2},
        {"identity": identity(native.wind2), "coefficient": 0.3},
    ]
    transform.update_native_mixes(plans)
    assert (
        native.busbar["uvek electricity provenance"]["supplier weights"][
            "Wind Onshore"
        ]["signed net source coefficients"][0]["coefficient"]
        == -0.2
    )


def test_signed_policy_is_a_persistent_limitation_warning(native):
    native.busbar["exchanges"][1]["amount"] = -0.2
    scenario = {
        "database": native.database,
        "iam data": native.iam,
        "model": "remind",
        "pathway": "synthetic",
        "year": 2050,
    }
    with pytest.warns(UserWarning) as caught:
        electricity_module._update_electricity(
            scenario, "2026", "cutoff", family="uvek", biosphere_version="3.10"
        )
    assert any("No individual edge is clipped" in str(w.message) for w in caught)
    assert any(
        "positive net coefficients" in m for m in scenario["electricity limitations"]
    )


def test_native_efficiency_uses_exact_mj_combustion_carrier(native):
    fuel = activity("Native combustion", unit="megajoule")
    other = activity(
        "Native combustion", unit="megajoule", product="Another physical boundary"
    )
    other["code"] = "other-combustion"
    native.wind1["exchanges"] += [exchange(fuel, 9), exchange(other, 100)]
    native.database += [fuel, other]
    native.mapping["Coal PC"] = [native.wind1]
    native.iam.electricity_technology_efficiencies = xr.DataArray(
        [[[1.25]]],
        dims=("region", "variables", "year"),
        coords={"region": ["R1"], "variables": ["Coal PC"], "year": [2050]},
    )
    transform = UVEKElectricity(**native.kwargs)
    transform.mapping.generate_powerplant_fuels_map = lambda: {"Coal PC": [fuel]}
    transform.update_electricity_efficiency()
    assert native.wind1["parameters"]["efficiency"] == pytest.approx(0.5)
    assert native.wind1["exchanges"][1]["amount"] == pytest.approx(7.2)
    assert not transform.efficiency_limitations


def test_non_electricity_unknown_geography_is_not_relabelled(native):
    unknown = activity("Unrelated native activity", "CPA", unit="kilogram")
    native.database.append(unknown)
    before = copy.deepcopy(unknown)
    transform = UVEKElectricity(**native.kwargs)
    assert "CPA" not in transform.ecoinvent_to_iam_loc
    assert unknown == before


@pytest.mark.parametrize("store_type", [CompactInventoryStore, LegacyInventoryStore])
def test_native_entry_point_works_with_inventory_store_backends(native, store_type):
    before = copy.deepcopy(native.database)
    scenario = {
        "_inventory_store": store_type(copy.deepcopy(native.database)),
        "iam data": native.iam,
        "model": "remind",
        "pathway": "synthetic",
        "year": 2050,
    }
    with pytest.warns(UserWarning, match="residential solar"):
        electricity_module._update_electricity(
            scenario, "2026", "cutoff", family="uvek", biosphere_version="3.10"
        )
    rows = {identity(d): d for d in get_scenario_inventory(scenario)}
    assert [
        e["amount"]
        for e in rows[identity(native.busbar)]["exchanges"]
        if e["type"] == "technosphere"
    ] == pytest.approx([0.24, 0.36, 0.4])
    assert native.database == before
    for grid in native.grids:
        assert list(rows[identity(grid)]["exchanges"]) == grid["exchanges"]


def test_native_context_mismatch_is_rejected_before_mix_changes(native):
    before = copy.deepcopy(native.database)
    scenario = {
        "database": native.database,
        "iam data": native.iam,
        "model": "remind",
        "pathway": "synthetic",
        "year": 2050,
        "background context": {"family": "ecoinvent"},
    }
    with pytest.raises(ValueError, match="disagree with scenario background context"):
        electricity_module._update_electricity(
            scenario, "2026", "cutoff", family="uvek", biosphere_version="3.10"
        )
    assert native.database == before


def test_diagnostic_plan_collects_all_missing_roles_and_mixes_without_changes(native):
    native.busbar["exchanges"] = native.busbar["exchanges"][:1]
    another = activity("Another native PV generator", "FR")
    native.database.append(another)
    native.mapping["Solar PV Residential"].append(another)
    before = copy.deepcopy(native.database)
    transform = UVEKElectricity(**native.kwargs)
    with pytest.raises(ValueError, match="4 unresolved supplier allocations") as caught:
        transform.plan_mixes()
    allocations = caught.value.diagnostics["missing supplier allocations"]
    assert len(allocations) == 4
    assert {item["technology"] for item in allocations} == set(native.mapping)
    assert {item["mix"] for item in allocations} == {
        identity(native.busbar),
        identity(native.production),
    }
    assert {c["identity"] for item in allocations for c in item["candidates"]} == {
        identity(d) for rows in native.mapping.values() for d in rows
    }
    assert native.database == before


def test_complete_pv_inventory_is_a_generation_leaf_with_direct_flows(native):
    complete = activity("Electricity, production mix photovoltaic, at plant")
    complete["exchanges"].extend(
        [
            exchange(native.consumer, 0.0002),
            {
                "type": "biosphere",
                "name": "Heat, waste",
                "unit": "megajoule",
                "categories": ("air",),
                "amount": 0.8,
            },
        ]
    )
    native.database.append(complete)
    native.mapping["Solar PV Residential"] = [complete]
    native.busbar["exchanges"][-1] = exchange(complete, 0.5)
    before = copy.deepcopy(complete)
    transform = UVEKElectricity(**native.kwargs)
    assert transform.source_weights[identity(native.busbar)][identity(complete)] == 0.5
    plans = transform.plan_mixes()
    transform.update_native_mixes(plans)
    assert complete == before
    assert any(
        e.get("input") == (complete["database"], complete["code"])
        for e in native.busbar["exchanges"]
    )


def test_global_same_technology_portfolio_is_an_explicit_geographic_proxy(native):
    global_mix = activity("Electricity mix", "GLO")
    global_mix["exchanges"] += [
        exchange(native.wind1, 0.3),
        exchange(native.wind2, 0.1),
        exchange(native.pv, 0.6),
    ]
    native.database.append(global_mix)
    native.busbar["exchanges"] = native.busbar["exchanges"][:1] + [
        exchange(native.pv, 1)
    ]
    before = copy.deepcopy(native.grids)
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    target = next(plan for plan in plans if plan[0] is native.busbar)
    assert [e["amount"] for e in target[1]] == pytest.approx([0.45, 0.15, 0.4])
    assert "geographic proxy" in target[-1]["Wind Onshore"]["basis"]
    assert target[-1]["Wind Onshore"]["source portfolio"][2] == "GLO"
    transform.update_native_mixes(plans)
    assert native.grids == before
    assert any(
        i.rule_id == "electricity.uvek.supplier_proxy"
        for i in transform.validate_native_mixes(plans).warnings
    )


def test_sole_native_provider_has_recorded_proxy_without_invented_shares(native):
    native.busbar["exchanges"] = native.busbar["exchanges"][:-1]
    before = copy.deepcopy(native.pv)
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    assert (
        plans[0][-1]["Solar PV Residential"]["basis"]
        == "unique mapped native generator (geographic proxy)"
    )
    assert plans[0][-1]["Solar PV Residential"]["unique native supplier"] == identity(
        native.pv
    )
    assert native.pv == before
    assert plans[0][1][-1]["amount"] == 0.4


def igcc_fixture(native, technology):
    from premise.electricity.uvek import IGCC_FUEL_SLUGS, NATIVE_COAL_FUELS

    coal = [activity(name) for name in NATIVE_COAL_FUELS]
    native.database.extend(coal)
    native.mapping["Coal PC"] = coal
    global_mix = activity("Electricity mix", "GLO")
    imports = activity("Electricity imports", "GLO")
    imports["exchanges"].append(exchange(coal[0], -0.1))
    global_mix["exchanges"] += [
        exchange(coal[0], 0.4),
        exchange(coal[1], 0.1),
        exchange(imports, 1),
    ]
    native.database.extend([global_mix, imports])
    generators = []
    for fuel, slug in IGCC_FUEL_SLUGS[technology].items():
        generator = activity(f"Synthetic {technology} {fuel}", "RER")
        generator["clic provenance"] = {
            "slug": slug,
            "role_bindings": [f"electricity.yaml:{technology}"],
        }
        generators.append(generator)
    native.database.extend(generators)
    native.mapping[technology] = generators
    native.iam.electricity_mix = xr.DataArray(
        [[[1.0]]],
        dims=("region", "variables", "year"),
        coords={"region": ["R1"], "variables": [technology], "year": [2050]},
    )
    return generators, imports


@pytest.mark.parametrize("technology", ["Coal IGCC", "Coal IGCC CCS"])
def test_igcc_uses_signed_global_coal_fuel_split_and_preserves_complete_templates(
    native, technology
):
    generators, imports = igcc_fixture(native, technology)
    before = copy.deepcopy([generators, imports, native.grids])
    transform = UVEKElectricity(**native.kwargs)
    plans = transform.plan_mixes()
    assert [e["amount"] for e in plans[0][1]] == pytest.approx([0.75, 0.25])
    assert plans[0][-1][technology]["baseline fuel shares"] == pytest.approx(
        {"hard coal": 0.75, "lignite": 0.25}
    )
    transform.update_native_mixes(plans)
    assert [generators, imports, native.grids] == before
    assert (
        native.busbar["uvek electricity provenance"]["supplier weights"][technology][
            "source portfolio"
        ][2]
        == "GLO"
    )
    assert any("IGCC" in message for message in transform.supplier_proxy_limitations)


@pytest.mark.parametrize(
    "problem", ["extra", "missing", "wrong binding", "missing baseline fuel"]
)
def test_igcc_proxy_refuses_unapproved_variants_or_incomplete_basis(native, problem):
    generators, _ = igcc_fixture(native, "Coal IGCC")
    if problem == "extra":
        other = copy.deepcopy(generators[0])
        other.update(name="unreviewed IGCC", code="unreviewed")
        other["clic provenance"]["slug"] = "different-capture-configuration"
        native.database.append(other)
        native.mapping["Coal IGCC"].append(other)
    elif problem == "missing":
        native.mapping["Coal IGCC"].pop()
    elif problem == "wrong binding":
        generators[0]["clic provenance"]["role_bindings"] = [
            "electricity.yaml:Coal IGCC CCS"
        ]
    else:
        global_mix = next(
            d
            for d in native.database
            if d["name"] == "Electricity mix" and d["location"] == "GLO"
        )
        global_mix["exchanges"] = [
            e for e in global_mix["exchanges"] if "lignite" not in e["name"]
        ]
    before = copy.deepcopy(native.database)
    transform = UVEKElectricity(**native.kwargs)
    with pytest.raises(ValueError, match="Coal IGCC"):
        transform.plan_mixes()
    assert native.database == before


def test_public_update_reports_approved_geographic_proxy(native):
    from premise.electricity.uvek import GEOGRAPHIC_SUPPLIER_LIMITATION

    native.busbar["exchanges"] = native.busbar["exchanges"][:-1]
    before = copy.deepcopy(native.pv)
    scenario = {
        "database": native.database,
        "iam data": native.iam,
        "model": "remind",
        "pathway": "synthetic",
        "year": 2050,
    }
    with pytest.warns(UserWarning) as warnings:
        result = electricity_module._update_electricity(
            scenario, "2026", "cutoff", False, "uvek", "3.10"
        )
    assert any("geographic screening proxy" in str(item.message) for item in warnings)
    assert GEOGRAPHIC_SUPPLIER_LIMITATION in result["electricity limitations"]
    assert native.pv == before
    assert (
        result["_validation_phase_results"][0]["phase_id"] == "sector:electricity:uvek"
    )
    assert native.busbar["uvek electricity provenance"]["supplier weights"][
        "Solar PV Residential"
    ]["unique native supplier"] == identity(native.pv)
