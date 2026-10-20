import pytest
import pandas as pd
import xarray as xr
import numpy as np
from copy import deepcopy
from dataclasses import replace
from premise.data_collection import get_metals_intensity_factors_data

import premise.metals as metals_module
import premise.metals_rules as metals_rules_module
import premise.validation as validation_module
from premise.activity_maps import InventorySet
from premise.filesystem_constants import DATA_DIR
from premise.metals import (
    Metals,
    PostAllocationCorrectionError,
    _load_mining_shares_mapping,
    build_transport_lookup,
    correct_metal_resource_exchanges,
    extract_exact_filter_values,
    extract_reference_products_from_filter,
    is_secondary_metal_supply_exchange,
    matches_filter_query,
    update_exchanges,
    select_transport_provider,
    mining_shares_for_year,
)
from premise.metals_rules import (
    MetalsConfigError,
    load_material_rules,
    load_technology_conversions,
)
from premise.new_database import config as premise_config
from premise.metal_input_comparison import (
    compare_direct_metal_inputs,
    configured_metal_products,
)


def test_yaml_material_configuration_is_complete_and_valid():
    config = load_material_rules()
    conversions = load_technology_conversions()

    assert len(config.rules) == 308
    assert len(config.enabled_rules) == 281
    assert len(conversions) == 62
    by_name = {item.activity_name: item for item in conversions}
    frame = by_name["frame, blanks and saddle, for lorry"]
    assert frame.ecoinvent_unit == "kilogram"
    assert frame.factor == 1

    aluminium = [
        rule
        for rule in config.enabled_rules
        if rule.technology == "Nuclear" and rule.element == "Aluminium"
    ]
    assert len(aluminium) == 2
    assert sum(rule.exchange_amount_factor for rule in aluminium) == pytest.approx(1)
    assert {rule.provider.reference_product for rule in aluminium} == {
        "aluminium, wrought alloy",
        "aluminium, cast alloy",
    }


def test_material_rule_loader_rejects_unknown_fields(tmp_path, monkeypatch):
    path = tmp_path / "metal_products.yaml"
    path.write_text(
        "schema_version: 1\nrules: []\nactivity_policies: []\nunknown: true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(metals_rules_module, "MATERIAL_RULES_PATH", path)
    load_material_rules.cache_clear()
    try:
        with pytest.raises(MetalsConfigError, match="unknown fields"):
            load_material_rules()
    finally:
        load_material_rules.cache_clear()


def test_update_exchanges_only_replaces_matching_technosphere_exchange():
    activity = {
        "name": "consumer",
        "exchanges": [
            technosphere_exchange("old lead", "lead", "GLO", 2),
            {
                "name": "Lead",
                "product": "lead",
                "amount": 7,
                "unit": "kilogram",
                "type": "biosphere",
            },
        ],
    }
    provider = market_dataset("market for lead", "lead", "World", [])

    update_exchanges(activity, 5, provider, "Lead")

    technosphere = [e for e in activity["exchanges"] if e["type"] == "technosphere"]
    biosphere = [e for e in activity["exchanges"] if e["type"] == "biosphere"]
    assert [(e["product"], e["amount"]) for e in technosphere] == [("lead", 5)]
    assert [(e["product"], e["amount"]) for e in biosphere] == [("lead", 7)]


def _metals_for_material_rule_tests(technology, element, intensity):
    metals = object.__new__(Metals)
    metals.version = "3.12"
    metals.precomputed_medians = xr.DataArray(
        [[[intensity, intensity, intensity]]],
        coords={
            "origin_var": [technology],
            "metal": [element],
            "variable": ["median", "min", "max"],
        },
        dims=("origin_var", "metal", "variable"),
    )
    metals.material_policies = load_material_rules().policies
    metals.material_update_diagnostics = []
    metals.material_decisions = []
    metals._validation_targets = {}
    return metals


@pytest.mark.parametrize(
    ("version", "grade"),
    [
        ("3.8", "semiconductor-grade"),
        ("3.9", "semiconductor-grade"),
        ("3.10", "semiconductor-grade"),
        ("3.11", "high-grade"),
        ("3.12", "high-grade"),
    ],
)
def test_gallium_rule_updates_versioned_provider_and_records_existing_amount(
    version, grade
):
    rule = next(
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == "CIGS" and rule.element == "Gallium"
    )
    metals = _metals_for_material_rule_tests("CIGS", "Gallium", 2)
    metals.version = version
    product = f"gallium, {grade}"
    provider = market_dataset(f"market for {product}", product, "GLO", [])
    metals.db_index = {provider["name"]: {product: [provider]}}
    metals.is_in_index = lambda _dataset: True
    dataset = {
        "name": "photovoltaic laminate production, CIS",
        "reference product": "photovoltaic laminate, CIS",
        "location": "DE",
        "unit": "square meter",
        "exchanges": [technosphere_exchange(provider["name"], product, "GLO", 0.01098)],
    }

    metals._apply_material_rule(
        dataset=dataset, technology="CIGS", rule=rule, conversion_factor=1
    )

    assert len(dataset["exchanges"]) == 1
    exchange = dataset["exchanges"][0]
    assert (exchange["name"], exchange["product"], exchange["amount"]) == (
        provider["name"],
        product,
        6,
    )
    assert metals.material_update_diagnostics == []
    (decision,) = metals.material_decisions
    assert decision["status"] == "updated"
    assert decision["provider product"] == product
    assert decision["old direct amount"] == 0.01098
    assert decision["target direct amount"] == 6
    assert (provider["name"], product) in configured_metal_products()


def test_unavailable_gallium_provider_still_records_warning():
    rule = next(
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == "CIGS" and rule.element == "Gallium"
    )
    metals = _metals_for_material_rule_tests("CIGS", "Gallium", 2)
    metals.db_index = {}
    dataset = {
        "name": "photovoltaic laminate production, CIS",
        "location": "DE",
        "exchanges": [],
    }

    metals._apply_material_rule(
        dataset=dataset, technology="CIGS", rule=rule, conversion_factor=1
    )

    assert dataset["exchanges"] == []
    (diagnostic,) = metals.material_update_diagnostics
    assert diagnostic["provider product"] == "gallium, high-grade"
    assert (
        metals.material_decisions[0]["reason code"]
        == "metals.material_rule.provider_missing"
    )


def test_epr_material_policy_preserves_source_exchanges():
    rule = next(
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == "Nuclear" and rule.element == "Lead"
    )
    metals = _metals_for_material_rule_tests("Nuclear", "Lead", 40)
    dataset = {
        "name": "EPR construction",
        "reference product": "EPR construction",
        "location": "CH",
        "unit": "unit",
        "exchanges": [technosphere_exchange("battery", "battery", "GLO", 1)],
    }
    original = [exchange.copy() for exchange in dataset["exchanges"]]

    metals._apply_material_rule(
        dataset=dataset,
        technology="Nuclear",
        rule=rule,
        conversion_factor=1500,
    )

    assert dataset["exchanges"] == original
    assert len(metals.material_decisions) == 1
    assert metals.material_decisions[0]["reason code"] == (
        "metals.material_rule.preserved_source"
    )
    assert "target direct amount" not in metals.material_decisions[0]


def test_both_nuclear_aluminium_rules_are_applied_once():
    rules = [
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == "Nuclear" and rule.element == "Aluminium"
    ]
    metals = _metals_for_material_rule_tests("Nuclear", "Aluminium", 165)
    metals.material_policies = ()
    providers = {
        rule.provider.reference_product: market_dataset(
            rule.provider.name, rule.provider.reference_product, "World", []
        )
        for rule in rules
    }
    metals.get_metal_market_dataset = lambda _name, reference_product: providers[
        reference_product
    ]
    dataset = {
        "name": "nuclear power plant construction, pressure water reactor, 1000MW",
        "reference product": "nuclear power plant",
        "location": "RER",
        "unit": "unit",
        "exchanges": [],
    }

    for rule in rules:
        metals._apply_material_rule(
            dataset=dataset,
            technology="Nuclear",
            rule=rule,
            conversion_factor=1000,
        )

    amounts = {
        exchange["product"]: exchange["amount"]
        for exchange in dataset["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert amounts == pytest.approx(
        {
            "aluminium, wrought alloy": 52_800,
            "aluminium, cast alloy": 112_200,
        }
    )
    assert len(metals.material_decisions) == 2


@pytest.mark.parametrize(
    ("technology", "name", "product", "unit", "factor"),
    [
        (
            "Wind-DDPM",
            "wind power plant construction, 800kW, moving parts, direct drive",
            "wind power plant, 800kW, moving parts",
            "unit",
            0.8,
        ),
        (
            "Wind-Gearbox",
            "wind power plant construction, 2MW, offshore, moving parts",
            "wind power plant, 2MW, offshore, moving parts",
            "unit",
            2.0,
        ),
        (
            "Wind-DDPM",
            "wind turbine construction, 750kW, onshore, direct drive",
            "wind turbine, 750kW, onshore",
            "unit",
            0.75,
        ),
        (
            "EV",
            "electric motor production, vehicle",
            "electric motor, vehicle",
            "kilogram",
            1.0,
        ),
        (
            "PEMFC",
            "fuel cell stack production, 1 kWe, proton exchange membrane (PEM)",
            "fuel cell stack, 1 kWe, proton exchange membrane (PEM)",
            "unit",
            0.001,
        ),
        (
            "AEL",
            "electrolyzer production, 1MWe, AEC, Stack",
            "electrolyzer, 1MWe, AEC, Stack",
            "unit",
            1.0,
        ),
        (
            "HTEL - Yttrium",
            "electrolyzer production, 1MWe, SOEC, Stack",
            "electrolyzer, 1MWe, SOEC, Stack",
            "unit",
            1.0,
        ),
    ],
)
@pytest.mark.parametrize("version", premise_config["SUPPORTED_EI_VERSIONS"])
def test_material_plan_excludes_scrap_coproducts(
    technology, name, product, unit, factor, version
):
    from copy import deepcopy

    equipment = {
        "name": name,
        "reference product": product,
        "location": "RER",
        "unit": unit,
        "exchanges": [],
    }
    scrap = {
        **equipment,
        "reference product": "aluminium scrap, new",
        "unit": "kilogram",
    }
    database = [scrap, equipment]
    before = deepcopy(database)
    metals = object.__new__(Metals)
    metals.activities_metals_map = InventorySet(
        database, version
    ).generate_metals_activities_map()
    rules = [
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == technology
    ]
    metals.material_rules_by_technology = {technology: rules}
    metals.material_policies = load_material_rules().policies
    metals.version = version
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    metals.db_index = {}

    plan = metals._compile_material_update_plan()

    assert metals.activities_metals_map[technology] == [equipment]
    assert plan
    assert all(item["dataset"] is equipment for item in plan)
    assert all(
        (
            item["conversion_factor"] is None
            if item["rule"].preserve_reason
            else item["conversion_factor"] == pytest.approx(factor)
        )
        for item in plan
    )
    assert database == before


def test_material_plan_deduplicates_dataset_rule_pairs():
    dataset = {
        "name": "nuclear power plant construction, pressure water reactor, 1000MW",
        "reference product": "nuclear power plant",
        "location": "RER",
        "unit": "unit",
        "exchanges": [],
    }
    rules = [
        rule
        for rule in load_material_rules().enabled_rules
        if rule.technology == "Nuclear" and rule.element == "Aluminium"
    ]
    metals = object.__new__(Metals)
    metals.activities_metals_map = {"Nuclear": [dataset, dataset]}
    metals.material_rules_by_technology = {"Nuclear": rules}
    metals.material_policies = load_material_rules().policies
    metals.version = "3.12"
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    metals.db_index = {}

    plan = metals._compile_material_update_plan()

    assert len(plan) == 2
    assert {item["rule"].id for item in plan} == {rule.id for rule in rules}


def test_material_update_executes_each_compiled_pair_once():
    metals = object.__new__(Metals)
    metals.material_update_plan = [
        {
            "dataset": {"name": "one"},
            "technology": "A",
            "rule": object(),
            "conversion_factor": 1,
        },
        {
            "dataset": {"name": "two"},
            "technology": "B",
            "rule": object(),
            "conversion_factor": 2,
        },
    ]
    metals.material_update_metrics = {
        "compiled pairs": 2,
        "executed pairs": 0,
        "provider index lookups": 0,
    }
    calls = []
    metals._apply_material_rule = lambda **kwargs: calls.append(kwargs)

    metals.update_metals_use_in_database()

    assert len(calls) == 2
    assert metals.material_update_metrics["executed pairs"] == 2


def test_metal_provider_location_preference_is_deterministic():
    row = market_dataset("market for lead", "lead", "RoW", [])
    global_ = market_dataset("market for lead", "lead", "GLO", [])
    world = market_dataset("market for lead", "lead", "World", [])
    metals = object.__new__(Metals)
    metals.db_index = {"market for lead": {"lead": [row, global_, world]}}
    metals.db_index_by_name = {"market for lead": [row, global_, world]}
    metals.is_in_index = lambda _dataset: True

    assert metals.get_metal_market_dataset("market for lead", "lead") is world


def test_dataset_wise_metal_comparison_justifies_expected_differences():
    epr_identity = {
        "name": "EPR construction",
        "reference product": "EPR construction",
        "location": "CH",
        "unit": "unit",
    }
    pwr_identity = {
        "name": "nuclear power plant construction, pressure water reactor, 1000MW",
        "reference product": "nuclear power plant",
        "location": "RER",
        "unit": "unit",
    }
    lead = technosphere_exchange("market for lead", "lead", "GLO", 60_000)
    cast = technosphere_exchange(
        "market for aluminium, cast alloy",
        "aluminium, cast alloy",
        "World",
        112_200,
    )
    source = [
        {**epr_identity, "exchanges": []},
        {**pwr_identity, "exchanges": []},
    ]
    before = [
        {**epr_identity, "exchanges": [lead]},
        {**pwr_identity, "exchanges": []},
    ]
    after = [
        {**epr_identity, "exchanges": []},
        {**pwr_identity, "exchanges": [cast]},
    ]
    decisions = [
        {
            "activity": epr_identity,
            "provider name": "market for lead",
            "provider product": "lead",
            "material rule id": "nuclear-lead-lead",
            "target direct amount": 60_000,
            "reason code": "metals.material_rule.preserved_source",
            "explanation": "Preserved EPR material structure.",
        },
        {
            "activity": pwr_identity,
            "provider name": "market for aluminium, cast alloy",
            "provider product": "aluminium, cast alloy",
            "material rule id": "nuclear-aluminium-cast",
            "target direct amount": 112_200,
            "reason code": "metals.material_rule.applied",
            "explanation": "Applied the missing cast-aluminium rule.",
        },
    ]

    rows = compare_direct_metal_inputs(source, before, after, decisions=decisions)

    changed = {row["provider product"]: row for row in rows}
    assert changed["lead"]["classification"] == "epr_preserve_source"
    assert changed["aluminium, cast alloy"]["classification"] == (
        "missing_rule_now_applied"
    )
    assert all(row["valid"] for row in rows)


def test_dataset_wise_comparison_groups_suppliers_by_product():
    identity = {
        "name": "nuclear power plant construction, pressure water reactor, 1000MW",
        "reference product": "nuclear power plant",
        "location": "RER",
        "unit": "unit",
    }
    alternative = technosphere_exchange(
        "aluminium supplier from the source inventory",
        "aluminium, cast alloy",
        "RER",
        1_000,
    )
    configured = technosphere_exchange(
        "market for aluminium, cast alloy",
        "aluminium, cast alloy",
        "World",
        112_200,
    )
    decision = {
        "activity": identity,
        "provider name": "market for aluminium, cast alloy",
        "provider product": "aluminium, cast alloy",
        "material rule id": "nuclear-aluminium-cast",
        "target direct amount": 112_200,
        "reason code": "metals.material_rule.applied",
        "explanation": "Applied the configured cast-aluminium rule.",
    }

    rows = compare_direct_metal_inputs(
        [{**identity, "exchanges": [alternative]}],
        [{**identity, "exchanges": [alternative]}],
        [{**identity, "exchanges": [configured]}],
        decisions=[decision],
    )

    changed = [row for row in rows if row["classification"] != "unchanged"]
    assert len(changed) == 2
    assert {row["classification"] for row in changed} == {
        "material_rule_output_corrected"
    }
    assert all(row["valid"] for row in changed)


def test_mining_share_loader_returns_unfiltered_source_values(monkeypatch):
    source = pd.DataFrame(
        {
            "Metal": ["Copper", "Copper"],
            "Year 2020": [0.005, 0.995],
            "Year 2030": [0.004, 0.996],
        }
    )
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: source.copy())
    _load_mining_shares_mapping.cache_clear()
    try:
        loaded = _load_mining_shares_mapping("3.12")
    finally:
        _load_mining_shares_mapping.cache_clear()

    assert loaded.columns.tolist() == ["Metal", "2020", "2030"]
    assert loaded["2020"].tolist() == [0.005, 0.995]
    assert loaded["2030"].tolist() == [0.004, 0.996]


def test_transport_lookup_preserves_all_modes_and_frame_metadata():
    dataframe = pd.DataFrame(
        {
            "country": ["CH", "CH", "DE"],
            "Metal": ["Copper", "Copper", "Copper"],
            "TransportMode Label": ["Railway", "Road", "Sea"],
        },
        index=[10, 20, 30],
    )
    metals = object.__new__(Metals)
    metals.metals_transport = dataframe
    metals.transport_lookup = build_transport_lookup(dataframe)
    metals.alt_names = {"copper": "Copper"}

    result = metals.get_weighted_average_distance("CH", "copper")

    pd.testing.assert_frame_equal(result, dataframe.iloc[[0, 1]])


def test_metals_validation_reuses_supplied_mining_shares(monkeypatch):
    validator = object.__new__(validation_module.MetalsValidation)
    validator.mining_shares_mapping = pd.DataFrame({"Metal": [], "Country": []})

    monkeypatch.setattr(
        validation_module,
        "_load_mining_shares_mapping_for_validation",
        lambda _version: pytest.fail("validation reopened the mining-share workbook"),
    )

    validator.check_excel_shares_preserved()


def biosphere_resource(name, amount):
    return {
        "name": name,
        "amount": amount,
        "unit": "kilogram",
        "type": "biosphere",
        "categories": ("natural resource", "in ground"),
    }


def market_dataset(name, product, location, exchanges):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": "kilogram",
        "exchanges": exchanges,
    }


def technosphere_exchange(name, product, location, amount, unit="kilogram"):
    return {
        "name": name,
        "product": product,
        "location": location,
        "amount": amount,
        "unit": unit,
        "type": "technosphere",
    }


def test_pv_gallium_intensity_is_assigned_to_cigs_not_crystalline_silicon():
    metals_db = pd.read_csv(DATA_DIR / "metals" / "metals_db.csv")
    gallium = metals_db[metals_db["metal"] == "Gallium"]

    assert gallium[gallium["origin_var"] == "c-Si"].empty

    cigs_2020 = gallium[(gallium["origin_var"] == "CIGS") & (gallium["year"] == 2020.0)]
    assert len(cigs_2020) == 1
    assert cigs_2020[["mean", "median", "min", "max"]].iloc[0].to_dict() == (
        pytest.approx(
            {
                "mean": 16.986969696969695,
                "median": 6.17,
                "min": 2.32,
                "max": 124.0,
            }
        )
    )


def test_extract_reference_products_from_filter_handles_either_expression():
    expression = (
        "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
        "{'equals': 'lithium carbonate'}]}"
    )

    assert extract_reference_products_from_filter(expression) == [
        "lithium carbonate, battery grade",
        "lithium carbonate",
    ]


@pytest.mark.parametrize(
    ("value", "query", "expected"),
    [
        ("copper mine operation", {"contains": "mine"}, True),
        ("copper mine operation", {"equals": "copper mine operation"}, True),
        ("copper mine operation", {"startswith": "copper"}, True),
        (
            "copper mine operation",
            {"all": [{"contains": "copper"}, {"contains": "mine"}]},
            True,
        ),
        (
            "copper mine operation",
            {"either": [{"equals": "other"}, {"contains": "mine"}]},
            True,
        ),
        ("copper mine operation", {"contains": "market"}, False),
    ],
)
def test_matches_filter_query(value, query, expected):
    assert matches_filter_query(value, query) is expected


def test_extract_exact_filter_values_handles_boolean_expressions():
    assert extract_exact_filter_values(
        {"either": [{"equals": "copper"}, {"equals": "zinc"}]}
    ) == {"copper", "zinc"}
    assert extract_exact_filter_values({"contains": "copper"}) is None


def test_metal_filter_lookup_uses_exact_activity_index():
    copper = market_dataset("copper mine", "copper", "GLO", [])
    zinc = market_dataset("zinc mine", "zinc", "GLO", [])
    metals = object.__new__(Metals)
    metals.database = [zinc, copper]
    metals.build_db_indexes()

    assert metals.get_datasets_matching_filters(
        {"equals": "copper mine"}, {"equals": "copper"}
    ) == [copper]
    assert metals.get_datasets_matching_filters(
        {"contains": "copper"}, {"equals": "copper"}
    ) == [copper]
    assert metals.get_datasets_matching_filters(
        {"either": [{"equals": "copper mine"}, {"equals": "zinc mine"}]},
        {"either": [{"equals": "copper"}, {"equals": "zinc"}]},
    ) == [zinc, copper]


def test_create_metal_markets_refreshes_exact_index_before_correction(monkeypatch):
    original = market_dataset("vanadium mine", "vanadium ore", "GLO", [])
    regional_proxy = market_dataset("vanadium mine", "vanadium ore", "BR", [])
    metals = object.__new__(Metals)
    metals.database = [original]
    metals.country_codes = {}
    metals.version = "3.12"
    metals.build_db_indexes()

    dataframe = pd.DataFrame(
        {
            "Work done": ["Yes"],
            "Country": ["Brazil"],
            "Metal": ["Vanadium"],
        }
    )
    monkeypatch.setattr(
        "premise.metals.load_mining_shares_mapping", lambda _: dataframe.copy()
    )

    def create_market(_metal, _dataframe):
        metals.database.append(regional_proxy)
        return None

    metals.create_market = create_market
    matched = []

    def post_allocation_correction():
        matched.extend(
            metals.get_datasets_matching_filters(
                {"equals": "vanadium mine"}, {"equals": "vanadium ore"}
            )
        )

    metals.post_allocation_correction = post_allocation_correction
    metals.create_metal_markets()

    assert matched == [original, regional_proxy]


def test_mining_share_dataset_membership_is_cached_but_returned_as_a_copy(
    monkeypatch,
):
    dataset = {
        "name": "copper mine operation",
        "reference product": "copper",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Copper", 1.0)],
    }
    dataframe = pd.DataFrame(
        {
            "Work done": ["Yes"],
            "Process": ["{'contains': 'copper mine'}"],
            "Reference product": ["{'equals': 'copper'}"],
        }
    )
    calls = 0

    def load_mapping(_):
        nonlocal calls
        calls += 1
        return dataframe.copy()

    monkeypatch.setattr("premise.metals.load_mining_shares_mapping", load_mapping)
    metals = object.__new__(Metals)
    metals.database = [dataset]
    metals.version = "3.12"

    first = metals.get_mining_share_dataset_ids()
    first.clear()
    second = metals.get_mining_share_dataset_ids()

    assert second == {id(dataset)}
    assert calls == 1


def test_in_ground_resource_exchange_index_scans_each_dataset_once(monkeypatch):
    resource_exchange = biosphere_resource("Copper", 1.0)
    copper = market_dataset(
        "copper mine operation", "copper", "GLO", [resource_exchange]
    )
    empty = market_dataset("market for copper", "copper", "GLO", [])
    original = metals_module.get_in_ground_resource_exchanges
    scanned = []

    def record_scan(dataset):
        scanned.append(id(dataset))
        return original(dataset)

    monkeypatch.setattr(metals_module, "get_in_ground_resource_exchanges", record_scan)
    metals = object.__new__(Metals)
    metals.database = [copper, empty]

    metals.build_in_ground_resource_exchange_index()

    first = metals._get_in_ground_resource_exchanges(copper)
    repeated = metals._get_in_ground_resource_exchanges(copper)
    no_resources = metals._get_in_ground_resource_exchanges(empty)

    assert scanned == [id(copper), id(empty)]
    assert first is repeated
    assert first == [resource_exchange]
    assert no_resources == []


def test_is_secondary_metal_supply_exchange_matches_recovery_terms():
    assert is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "treatment of copper scrap by electrolytic refining",
            "copper, cathode",
            "RoW",
            0.2,
        ),
        "copper, cathode",
    )
    assert is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "metalliferous hydroxide sludge to market for zinc concentrate",
            "zinc concentrate",
            "GLO",
            0.0001,
        ),
        "zinc concentrate",
    )
    assert not is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "market for transport, freight, lorry",
            "transport, freight, lorry",
            "GLO",
            1.0,
            unit="ton kilometer",
        )
    )


def test_existing_market_lookup_uses_fallback_reference_product():
    metals = object.__new__(Metals)
    lithium_market = market_dataset(
        "market for lithium carbonate",
        "lithium carbonate",
        "GLO",
        [],
    )
    metals.database = [lithium_market]

    df_metal = pd.DataFrame(
        {
            "Reference product": [
                "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
                "{'equals': 'lithium carbonate'}]}"
            ]
        }
    )

    assert metals.get_existing_metal_market(df_metal) is lithium_market


def test_secondary_extraction_tries_next_old_market_candidate():
    metals = object.__new__(Metals)
    metals.system_model = "cutoff"
    lithium_battery_grade_market = market_dataset(
        "market for lithium carbonate, battery grade",
        "lithium carbonate, battery grade",
        "GLO",
        [
            technosphere_exchange(
                "lithium carbonate production, from brine",
                "lithium carbonate, battery grade",
                "CL",
                1.0,
            )
        ],
    )
    lithium_market = market_dataset(
        "market for lithium carbonate",
        "lithium carbonate",
        "GLO",
        [
            technosphere_exchange(
                "lithium carbonate production, from brine",
                "lithium carbonate",
                "CL",
                0.99,
            ),
            technosphere_exchange(
                "treatment of used Li-ion battery, hydrometallurgical treatment",
                "lithium carbonate",
                "GLO",
                0.01,
            ),
        ],
    )
    metals.database = [lithium_battery_grade_market, lithium_market]

    df_metal = pd.DataFrame(
        {
            "Reference product": [
                "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
                "{'equals': 'lithium carbonate'}]}"
            ]
        }
    )

    exchanges = metals.build_secondary_market_exchanges_from_existing_market(df_metal)

    assert exchanges == [
        technosphere_exchange(
            "treatment of used Li-ion battery, hydrometallurgical treatment",
            "lithium carbonate",
            "GLO",
            0.01,
        )
    ]


def test_create_market_uses_secondary_share_from_existing_market_instead_of_yaml():
    metals = object.__new__(Metals)
    metals.system_model = "cutoff"
    metals.prim_sec_split = {
        "copper": {
            "name": "market for copper",
            "reference product": "copper",
            "shares": {
                "primary": {2020: 0.1},
                "secondary": {2020: 0.9},
            },
        }
    }
    metals.database = [
        market_dataset(
            "market for copper",
            "copper",
            "GLO",
            [
                technosphere_exchange(
                    "primary copper production",
                    "copper",
                    "GLO",
                    0.75,
                ),
                technosphere_exchange(
                    "treatment of copper scrap by electrolytic refining",
                    "copper",
                    "RoW",
                    0.25,
                ),
            ],
        ),
        market_dataset("primary copper production", "copper", "GLO", []),
        market_dataset(
            "treatment of copper scrap by electrolytic refining",
            "copper",
            "RoW",
            [],
        ),
    ]
    metals.create_region_specific_markets = lambda df: [
        technosphere_exchange("primary copper production", "copper", "GLO", 1.0)
    ]
    metals.add_transport_to_market = lambda dataset, metal: []
    metals.remove_from_index = lambda dataset: None
    metals.is_in_index = lambda dataset: False

    df_metal = pd.DataFrame(
        {
            "Process": ["{'equals': 'primary copper production'}"],
            "Reference product": ["{'equals': 'copper'}"],
        }
    )

    market = metals.create_market("copper", df_metal)
    exchanges = {
        (exc["name"], exc.get("location")): exc["amount"]
        for exc in market["exchanges"]
        if exc["type"] == "technosphere"
    }

    assert exchanges[("primary copper production", "GLO")] == pytest.approx(0.75)
    assert exchanges[
        ("treatment of copper scrap by electrolytic refining", "RoW")
    ] == pytest.approx(0.25)


def test_create_market_preserves_existing_marginal_market_for_consequential():
    metals = object.__new__(Metals)
    metals.system_model = "consequential"
    metals.prim_sec_split = {}
    metals.database = [
        market_dataset(
            "market for copper",
            "copper",
            "GLO",
            [
                technosphere_exchange(
                    "treatment of copper scrap by electrolytic refining",
                    "copper",
                    "RoW",
                    0.25,
                )
            ],
        ),
        market_dataset("primary copper production", "copper", "GLO", []),
        market_dataset(
            "treatment of copper scrap by electrolytic refining",
            "copper",
            "RoW",
            [],
        ),
    ]
    metals.create_region_specific_markets = lambda df: [
        technosphere_exchange("primary copper production", "copper", "GLO", 1.0)
    ]
    metals.add_transport_to_market = lambda dataset, metal: []
    metals.remove_from_index = lambda dataset: None
    metals.is_in_index = lambda dataset: False

    df_metal = pd.DataFrame(
        {
            "Process": ["{'equals': 'primary copper production'}"],
            "Reference product": ["{'equals': 'copper'}"],
        }
    )

    before = deepcopy(metals.database)
    assert metals.create_market("copper", df_metal) is None
    assert metals.database == before


def test_post_allocation_correction_sets_target_to_one_and_others_to_zero():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.42),
            biosphere_resource("Gold", 0.001),
            {
                "name": "Carbon dioxide, fossil",
                "amount": 2.0,
                "unit": "kilogram",
                "type": "biosphere",
                "categories": ("air", "urban air close to ground"),
            },
        ],
    }

    assert correct_metal_resource_exchanges(dataset) is True

    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Copper"] == 1.0
    assert amounts["Gold"] == 0.0
    assert amounts["Carbon dioxide, fossil"] == 2.0


def test_post_allocation_correction_uses_metal_content_for_hydroxides():
    dataset = {
        "name": "beryllium hydroxide production",
        "reference product": "beryllium hydroxide",
        "location": "US",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Beryllium", 1.0),
            biosphere_resource("Gold", 0.001),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Beryllium"] == pytest.approx(0.20945869667541112)
    assert amounts["Gold"] == 0.0


def test_post_allocation_correction_handles_pure_metal_production_coproducts():
    dataset = {
        "name": "cobalt production",
        "reference product": "nickel, class 1",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.067),
            biosphere_resource("Nickel", 0.772),
            biosphere_resource("Sulfur", 10.5),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Cobalt"] == 0.0
    assert amounts["Nickel"] == 1.0
    assert amounts["Sulfur"] == 0.0


def test_post_allocation_correction_skips_unresolved_non_extraction_compounds():
    dataset = {
        "name": "cobalt production",
        "reference product": "nickel sulfate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.02),
            biosphere_resource("Nickel", 0.2),
            biosphere_resource("Sulfur", 2.7),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.02, 0.2, 2.7]


def test_post_allocation_correction_skips_market_datasets():
    dataset = {
        "name": "market for copper concentrate, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.42),
            biosphere_resource("Gold", 0.001),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.42, 0.001]


def test_post_allocation_correction_raises_when_strict_target_is_missing():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gold", 0.001)],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Could not find"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_skips_non_strict_missing_target():
    dataset = {
        "name": "gravel and sand quarry operation",
        "reference product": "sand",
        "location": "CH",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gravel", 1.0)],
    }

    assert correct_metal_resource_exchanges(dataset, strict=False) is False
    assert dataset["exchanges"][0]["amount"] == 1.0


def test_post_allocation_correction_skips_out_of_scope_resource_matches():
    dataset = {
        "name": "hard coal mine operation and hard coal preparation",
        "reference product": "hard coal",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Coal, hard", 1.32)],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert dataset["exchanges"][0]["amount"] == 1.32


def test_post_allocation_correction_does_not_treat_forest_as_ore_context():
    dataset = {
        "name": "clear-cutting, primary forest to arable land, annual crop",
        "reference product": (
            "land tenure, arable land, measured as carbon net primary "
            "productivity, annual crop"
        ),
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource(
                "Carbon, organic, decrease in soil or biomass stock", 4.2
            )
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert dataset["exchanges"][0]["amount"] == 4.2


def test_post_allocation_correction_raises_on_ambiguous_target():
    dataset = {
        "name": "zinc mine operation",
        "reference product": "bulk lead-zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Lead", 0.5),
            biosphere_resource("Zinc", 0.5),
        ],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Ambiguous"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_skips_non_strict_ambiguous_target():
    dataset = {
        "name": "sodium percarbonate production, powder",
        "reference product": "sodium percarbonate, powder",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Sodium", 0.1),
            biosphere_resource("Sodium chloride", 0.2),
            biosphere_resource("Sodium nitrate", 0.3),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=False) is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.1, 0.2, 0.3]


def test_post_allocation_correction_raises_on_duplicate_target_flow():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.2),
            biosphere_resource("Copper", 0.3),
        ],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Duplicate"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_uses_mineral_aliases():
    dataset = {
        "name": "molybdenite mine operation",
        "reference product": "molybdenite",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Molybdenum", 0.3),
            biosphere_resource("Copper", 0.1),
        ],
    }

    assert correct_metal_resource_exchanges(dataset) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Molybdenum"] == pytest.approx(0.54545591456969)
    assert amounts["Copper"] == 0.0


def test_post_allocation_correction_uses_resource_flow_aliases():
    dataset = {
        "name": "graphite production",
        "reference product": "graphite",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource(
                "Metamorphous rock, graphite containing, in ground", 1.7
            ),
            biosphere_resource("Gold", 0.1),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Metamorphous rock, graphite containing, in ground"] == 1.0
    assert amounts["Gold"] == 0.0


def test_post_allocation_correction_uses_zircon_alias():
    dataset = {
        "name": "heavy mineral sand quarry operation and titania slag production",
        "reference product": "zircon",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Zirconium", 0.2),
            biosphere_resource("Titanium", 0.8),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Zirconium"] == 0.2
    assert amounts["Titanium"] == 0.0


def test_post_allocation_correction_uses_stibnite_alias():
    dataset = {
        "name": "stibnite mine operation and beneficiation",
        "reference product": "stibnite concentrate",
        "location": "CA-QC",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Antimony", 0.4),
            biosphere_resource("Gangue", 0.6),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Antimony"] == pytest.approx(0.501807477185752)
    assert amounts["Gangue"] == 0.6


def test_stibnite_content_preserves_different_refining_yields():
    factor, _ = metals_module.get_metal_bearing_product_content_factor(
        "stibnite concentrate", "Antimony"
    )
    assert factor * 1.99279613291 == pytest.approx(1)
    # The legacy lead-smelting proxy's larger feed is not normalized away.
    assert factor * 4.009 == pytest.approx(2.011746176)


def test_post_allocation_correction_uses_phosphate_alias():
    dataset = {
        "name": "phosphate rock beneficiation",
        "reference product": "phosphate rock, beneficiated",
        "location": "US",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Phosphorus", 0.2),
            biosphere_resource("Fluorine", 0.8),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Phosphorus"] == 0.2
    assert amounts["Fluorine"] == 0.8


def test_post_allocation_correction_clears_downstream_attributed_carriers():
    carrier_datasets = [
        {
            "name": "lead mine operation",
            "reference product": "lead concentrate",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Lead", 0.1),
                biosphere_resource("Zinc", 0.2),
            ],
        },
        {
            "name": "zinc mine operation",
            "reference product": "zinc concentrate",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Zinc", 0.5),
                biosphere_resource("Lead", 0.1),
            ],
        },
        {
            "name": "smelting of copper concentrate, sulfide ore",
            "reference product": "copper, anode",
            "location": "RoW",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Copper", 0.9),
                biosphere_resource("Gold", 0.001),
            ],
        },
    ]

    for dataset in carrier_datasets:
        corrected = correct_metal_resource_exchanges(dataset, strict=True)

        assert corrected is True
        assert all(
            exc["amount"] == 0.0
            for exc in dataset["exchanges"]
            if exc["type"] == "biosphere"
        )


def test_post_allocation_correction_clears_copper_cobalt_ore_carrier():
    dataset = {
        "name": "copper-cobalt mining, industrial, economic allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.0047),
            biosphere_resource("Copper", 0.024),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Cobalt"] == 0.0
    assert amounts["Copper"] == 0.0


def test_post_allocation_correction_preserves_unresolved_concentrate_content():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "CL",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.271842),
            biosphere_resource("Gold", 0.000003),
            biosphere_resource("Gangue", 30.58),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Copper"] == 0.271842
    assert amounts["Gold"] == 0.0
    assert amounts["Gangue"] == 30.58


def test_post_allocation_correction_clears_generic_pgm_carrier():
    dataset = {
        "name": "platinum group metal, mine and concentration operations",
        "reference product": "platinum group metal concentrate",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.038),
            biosphere_resource("Platinum", 0.000253),
            biosphere_resource("Rhodium", 0.000008),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    assert {exc["name"]: exc["amount"] for exc in dataset["exchanges"]} == {
        "Copper": 0.0,
        "Platinum": 0.0,
        "Rhodium": 0.0,
    }


def test_post_allocation_correction_clears_non_extraction_carriers():
    dataset = {
        "name": "sodium chloride production, powder",
        "reference product": "sodium chloride, powder",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Sodium chloride", 1.0),
            biosphere_resource("Sodium sulfate", 0.02),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is True
    assert {exc["name"]: exc["amount"] for exc in dataset["exchanges"]} == {
        "Sodium chloride": 1.0,
        "Sodium sulfate": 0.02,
    }


def test_post_allocation_correction_adds_missing_target_for_mapped_pure_metal():
    dataset = {
        "name": "indium production",
        "reference product": "indium",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for indium rich leaching residues, from zinc production",
                "product": "indium rich leaching residues, from zinc production",
                "amount": 10000,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(
        dataset, strict=True, add_missing_target_resource=True
    )

    assert corrected is True
    target_exchange = dataset["exchanges"][-1]
    assert target_exchange == {
        "amount": 1.0,
        "type": "biosphere",
        "name": "Indium",
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
        "uncertainty type": 0,
    }


@pytest.mark.parametrize(
    ("reference_product", "expected_flow"),
    [
        ("gallium, high-grade", "Gallium"),
        ("hafnium sponge", "Hafnium"),
        ("tellurium, semiconductor-grade", "Tellurium"),
        ("titanium sponge", "Titanium"),
        ("aluminium, primary, liquid", "Aluminium"),
        ("nickel, class 1", "Nickel"),
        ("7N Gallium", "Gallium"),
    ],
)
def test_post_allocation_correction_adds_missing_target_for_pure_qualified_metals(
    reference_product, expected_flow
):
    dataset = {
        "name": f"{reference_product} production",
        "reference product": reference_product,
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for upstream intermediate",
                "product": "upstream intermediate",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(
        dataset, strict=True, add_missing_target_resource=True
    )

    assert corrected is True
    assert dataset["exchanges"][-1] == {
        "amount": 1.0,
        "type": "biosphere",
        "name": expected_flow,
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
        "uncertainty type": 0,
    }


def test_post_allocation_correction_does_not_add_missing_target_by_default():
    dataset = {
        "name": "tin production",
        "reference product": "tin",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for tin concentrate",
                "product": "tin concentrate",
                "amount": 2.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is False
    assert all(exc["type"] != "biosphere" for exc in dataset["exchanges"])


def test_missing_target_detection_ignores_downstream_attributed_carriers():
    pgm_concentrate = {
        "name": "market for platinum group metal concentrate",
        "reference product": "platinum group metal concentrate",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Platinum", 0.000103)],
    }
    platinum_refinery = {
        "name": "platinum group metal, extraction and refinery operations",
        "reference product": "platinum",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for platinum group metal concentrate",
                "product": "platinum group metal concentrate",
                "location": "World",
                "amount": 1000,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    tin_concentrate = {
        "name": "market for tin concentrate",
        "reference product": "tin concentrate",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Tin", 0.48)],
    }
    tin_production = {
        "name": "tin production",
        "reference product": "tin",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for tin concentrate",
                "product": "tin concentrate",
                "location": "World",
                "amount": 1 / 0.48379101906039534,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    zinc_concentrate = {
        "name": "market for zinc concentrate",
        "reference product": "zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "zinc mine operation",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    zinc_mine = {
        "name": "zinc mine operation",
        "reference product": "zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cadmium", 0.0001),
            biosphere_resource("Zinc", 0.5),
        ],
    }
    cadmium_production = {
        "name": "primary zinc production from concentrate",
        "reference product": "cadmium",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for zinc concentrate",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 10.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    silver_production = {
        "name": "primary zinc production from concentrate",
        "reference product": "silver",
        "location": "MX",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for zinc concentrate",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 300.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
            {
                "name": "market for silver",
                "product": "silver",
                "location": "World",
                "amount": 0.0001,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }
    silver_market = {
        "name": "market for silver",
        "reference product": "silver",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Silver", 1.0)],
    }
    gold_unrefined_market = {
        "name": "market for gold, unrefined",
        "reference product": "gold, unrefined",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gold", 0.8)],
    }
    gold_refinery = {
        "name": "gold refinery operation",
        "reference product": "gold",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for gold, unrefined",
                "product": "gold, unrefined",
                "location": "GLO",
                "amount": 1.2,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    catalyst_treatment = {
        "name": "treatment of automobile catalyst",
        "reference product": "palladium",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [],
    }
    lithium_brine = {
        "name": "market for lithium brine, 6.7 % Li",
        "reference product": "lithium brine, 6.7 % Li",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Lithium", 0.0667)],
    }
    lithium_production = {
        "name": "lithium production, lithium chloride electrolysis",
        "reference product": "lithium",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for lithium brine, 6.7 % Li",
                "product": "lithium brine, 6.7 % Li",
                "location": "GLO",
                "amount": 8.3,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    copper_cobalt_ore = {
        "name": "copper-cobalt mining, industrial, mass allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Cobalt", 0.0047)],
    }
    cobalt_production = {
        "name": "cobalt metal production, from copper mining, via electrolysis",
        "reference product": "cobalt",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "copper-cobalt mining, industrial, mass allocation",
                "product": "copper-cobalt ore",
                "location": "CD",
                "amount": 200.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    economic_cobalt_ore = {
        "name": "copper-cobalt mining, industrial, economic allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Cobalt", 0.0047)],
    }
    economic_cobalt_production = {
        "name": "cobalt metal production, from copper mining, via electrolysis",
        "reference product": "cobalt",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "copper-cobalt mining, industrial, economic allocation",
                "product": "copper-cobalt ore",
                "location": "CD",
                "amount": 200.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    metals = object.__new__(Metals)
    metals.database = [
        pgm_concentrate,
        platinum_refinery,
        tin_concentrate,
        tin_production,
        zinc_concentrate,
        zinc_mine,
        cadmium_production,
        silver_production,
        silver_market,
        gold_unrefined_market,
        gold_refinery,
        catalyst_treatment,
        lithium_brine,
        lithium_production,
        copper_cobalt_ore,
        cobalt_production,
        economic_cobalt_ore,
        economic_cobalt_production,
    ]

    missing_target_ids = metals.get_missing_target_resource_dataset_ids(
        {
            id(platinum_refinery),
            id(tin_production),
            id(cadmium_production),
            id(silver_production),
            id(gold_refinery),
            id(catalyst_treatment),
            id(lithium_production),
            id(cobalt_production),
            id(economic_cobalt_production),
        }
    )

    assert id(platinum_refinery) in missing_target_ids
    assert id(tin_production) not in missing_target_ids
    assert id(cadmium_production) in missing_target_ids
    assert id(silver_production) not in missing_target_ids
    assert id(gold_refinery) not in missing_target_ids
    assert id(catalyst_treatment) not in missing_target_ids
    assert id(lithium_production) in missing_target_ids
    assert id(cobalt_production) in missing_target_ids
    assert id(economic_cobalt_production) in missing_target_ids


@pytest.mark.parametrize("unit", ["kilogram", "unit"])
def test_material_plan_rejects_missing_or_incompatible_conversion_before_updates(unit):
    from copy import deepcopy

    valid = {
        "name": "photovoltaic cell production, single-Si wafer",
        "reference product": "photovoltaic cell",
        "location": "CN",
        "unit": "square meter",
        "exchanges": [],
    }
    invalid = {**valid, "unit": unit}
    if unit == "unit":
        invalid["name"] = "new unconfigured photovoltaic cell production"
    metals = object.__new__(Metals)
    metals.material_policies = load_material_rules().policies
    metals.version = "3.12"
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    metals.activities_metals_map = {"c-Si": [valid, invalid]}
    metals.material_rules_by_technology = {
        "c-Si": [
            r for r in load_material_rules().enabled_rules if r.technology == "c-Si"
        ]
    }
    metals.db_index = {}
    before = deepcopy([valid, invalid])
    with pytest.raises(MetalsConfigError, match="conversion"):
        metals._compile_material_update_plan()
    assert [valid, invalid] == before


@pytest.mark.parametrize(
    "name,unit,expected",
    [
        ("photovoltaic cell production, single-Si wafer", "square meter", 0.000224),
        (
            "wind turbine construction, small-scale, 6kW, onshore, direct drive",
            "unit",
            0.006,
        ),
        (
            "wind turbine construction, 2.3MW, precast concrete tower, onshore, direct drive",
            "unit",
            2.3,
        ),
        ("wind turbine construction, 750kW, onshore, direct drive", "unit", 0.75),
        ("frame, blanks and saddle, for lorry", "kilogram", 1.0),
    ],
)
def test_material_conversion_is_explicit_and_unit_checked(name, unit, expected):
    metals = object.__new__(Metals)
    metals.version = "3.12"
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    assert (
        metals._technology_conversion_factor({"name": name, "unit": unit}) == expected
    )


def test_redox_flow_battery_stacks_do_not_match_sofc_material_rules():
    from premise.activity_maps import InventorySet

    names = [
        "power subsystem production, cell stack, for hybrid redox flow battery (HFB), 5kW/ 40kWh",
        "power subsystem production, cell stack, for organic redox flow battery (OFB), 5kW/ 40kWh",
    ]
    db = [
        {
            "name": n,
            "reference product": n,
            "location": "GLO",
            "unit": "unit",
            "exchanges": [],
        }
        for n in names
    ]
    assert not InventorySet(db, "3.12").generate_metals_activities_map().get("SOFC - Y")


def test_intensity_data_preserves_units_and_distinguishes_missing_from_zero():
    factors = get_metals_intensity_factors_data()
    assert factors.intensity_unit.sel(origin_var="EV").item() == "kg metal/kg motor"
    assert factors.sel(
        metal="Aluminium", origin_var="EV", year=2020, variable="median"
    ).item() == pytest.approx(0.2252697267412005)
    assert (
        factors.sel(
            metal="Platinum", origin_var="SOFC - Y", year=2020, variable="median"
        ).item()
        == 0
    )
    assert np.isnan(
        factors.sel(
            metal="Scandium", origin_var="SOFC - Y", year=2020, variable="median"
        ).item()
    )


def test_ev_intensity_applies_per_kg_motor_without_capacity_conversion():
    metals = _metals_for_material_rule_tests("EV", "Aluminium", 1)
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    rule = next(
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "EV" and r.element == "Aluminium"
    )
    provider = market_dataset(
        rule.provider.name, rule.provider.reference_product, "GLO", []
    )
    metals.get_metal_market_dataset = lambda *args: provider
    dataset = market_dataset(
        "electric motor production, vehicle", "electric motor, vehicle", "RER", []
    )
    conversion = metals._technology_conversion_factor(dataset, "EV")
    metals._apply_material_rule(
        dataset=dataset, technology="EV", rule=rule, conversion_factor=conversion
    )
    assert dataset["exchanges"][0]["amount"] == pytest.approx(
        0.2252697267412005 * rule.exchange_amount_factor
    )
    assert conversion == 1


@pytest.mark.parametrize("version", premise_config["SUPPORTED_EI_VERSIONS"])
def test_ev_material_plan_excludes_rail_traction_and_industrial_motors(version):
    road_motors = [
        market_dataset(name, product, location, [])
        for name, product, location in [
            ("electric motor production, vehicle", "electric motor, vehicle", "RER"),
            ("electric motor production, vehicle", "electric motor, vehicle", "RoW"),
            (
                "electric motor production, vehicle (electric powertrain)",
                "electric motor, electric passenger car",
                "GLO",
            ),
            (
                "electric motor production, for electric scooter",
                "electric motor, for electric scooter",
                "GLO",
            ),
        ]
    ]
    other_motors = [
        market_dataset(
            name,
            product,
            "GLO",
            [
                technosphere_exchange(
                    "market for copper, cathode", "copper, cathode", "GLO", 0.09995002
                )
            ],
        )
        for name, product in [
            (
                "electric motor production, asynchronous, traction",
                "electric motor, asynchronous, traction",
            ),
            ("electric motor production, industrial", "electric motor, industrial"),
        ]
    ]
    database = [*other_motors, *road_motors]
    before = deepcopy(database)
    metals = _metals_for_material_rule_tests("EV", "Aluminium", 1)
    metals.activities_metals_map = InventorySet(
        database, version
    ).generate_metals_activities_map()
    metals.material_rules_by_technology = {
        "EV": [r for r in load_material_rules().enabled_rules if r.technology == "EV"]
    }
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    metals.db_index = {}
    metals.version = version

    plan = metals._compile_material_update_plan()

    assert metals.activities_metals_map["EV"] == road_motors
    assert {id(item["dataset"]) for item in plan} == {id(d) for d in road_motors}
    assert all(
        (
            item["conversion_factor"] is None
            if item["rule"].preserve_reason
            else item["conversion_factor"] == 1
        )
        for item in plan
    )
    assert database == before


@pytest.mark.parametrize(
    "technology,stack_name,stack_product,factor",
    [
        (
            "PEMFC",
            "fuel cell stack production, 1 kWe, proton exchange membrane (PEM)",
            "fuel cell stack, 1 kWe, proton exchange membrane (PEM)",
            0.001,
        ),
        (
            "AEL",
            "electrolyzer production, 1MWe, AEC, Stack",
            "electrolyzer, 1MWe, AEC, Stack",
            1.0,
        ),
        (
            "HTEL - Yttrium",
            "electrolyzer production, 1MWe, SOEC, Stack",
            "electrolyzer, 1MWe, SOEC, Stack",
            1.0,
        ),
    ],
)
@pytest.mark.parametrize("version", premise_config["SUPPORTED_EI_VERSIONS"])
def test_stack_material_plan_excludes_materials_and_other_equipment(
    technology, stack_name, stack_product, factor, version
):
    stack = market_dataset(
        stack_name,
        stack_product,
        "GLO",
        [],
    )
    stack["unit"] = "unit"
    other_datasets = [
        market_dataset(name, product, "GLO", [])
        for name, product in [
            (
                "polyethylene terephthalate adhesive film production, for fuel cells",
                "polyethylene terephthalate adhesive film, for fuel cells",
            ),
            ("membrane production, PEM", "membrane, PEM"),
            (
                "fuel cell system assembly, 1 kWe, proton exchange membrane (PEM)",
                "fuel cell system, 1 kWe, proton exchange membrane (PEM)",
            ),
            (
                "fuel cell production, stack solid oxide, 125kW electrical, future",
                "fuel cell, stack solid oxide, 125kW electrical, future",
            ),
            (
                "electrolyzer production, 1MWe, PEM, Stack",
                "electrolyzer, 1MWe, PEM, Stack",
            ),
            (
                "motor drive assembly production, electrolysis, AEC",
                "motor drive assembly, electrolysis, AEC",
            ),
            (
                "motor drive assembly production, electrolysis, SOEC",
                "motor drive assembly, electrolysis, SOEC",
            ),
            (
                "glass seal production, SOEC, barium alumino-borosilicate",
                "glass seal, SOEC, barium alumino-borosilicate",
            ),
            (
                "electrolyzer production, 1MWe, AEC, Balance of Plant",
                "electrolyzer, 1MWe, AEC, Balance of Plant",
            ),
            (
                "electrolyzer production, 1MWe, SOEC, Balance of Plant",
                "electrolyzer, 1MWe, SOEC, Balance of Plant",
            ),
        ]
    ]
    for dataset in other_datasets:
        if "motor drive" in dataset["name"] or "electrolyzer" in dataset["name"]:
            dataset["unit"] = "unit"
    database = [*other_datasets, stack]
    before = deepcopy(database)
    metals = object.__new__(Metals)
    metals.activities_metals_map = InventorySet(
        database, version
    ).generate_metals_activities_map()
    config = load_material_rules()
    metals.material_rules_by_technology = {
        technology: [r for r in config.enabled_rules if r.technology == technology]
    }
    metals.material_policies = config.policies
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    metals.version = version
    metals.db_index = {}

    plan = metals._compile_material_update_plan()

    assert metals.activities_metals_map[technology] == [stack]
    assert len(plan) == len(metals.material_rules_by_technology[technology])
    assert all(item["dataset"] is stack for item in plan)
    assert all(item["conversion_factor"] == factor for item in plan)
    assert database == before


def test_unit_mismatch_and_overlap_fail_before_mutation():
    metals = _metals_for_material_rule_tests("EV", "Aluminium", 1)
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    metals.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    dataset = market_dataset(
        "electric motor production, vehicle", "electric motor, vehicle", "RER", []
    )
    rule = next(
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "EV" and r.element == "Aluminium"
    )
    metals.activities_metals_map = {"EV": [dataset]}
    metals.material_rules_by_technology = {
        "EV": [rule, replace(rule, id="conflicting-aluminium")]
    }
    with pytest.raises(MetalsConfigError, match="Overlapping"):
        metals._compile_material_update_plan()
    metals.material_rules_by_technology = {"EV": [rule]}
    conversion = metals.technology_conversions_by_name[dataset["name"]]
    metals.technology_conversions_by_name[dataset["name"]] = replace(
        conversion, intensity_unit="kg/MW"
    )
    with pytest.raises(MetalsConfigError, match="unit mismatch"):
        metals._compile_material_update_plan()
    assert dataset["exchanges"] == []


@pytest.mark.parametrize(
    "intensity,remaining,reason",
    [(0, 0, "zero_intensity"), (np.nan, 1, "intensity_missing")],
)
def test_zero_and_missing_intensities_have_distinct_recorded_outcomes(
    intensity, remaining, reason
):
    rule = next(
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "SOFC - Y" and r.element == "Platinum"
    )
    metals = _metals_for_material_rule_tests("SOFC - Y", "Platinum", intensity)
    dataset = market_dataset(
        "fuel cell production",
        "fuel cell",
        "GLO",
        [technosphere_exchange("market for platinum", "platinum", "GLO", 1)],
    )
    metals._apply_material_rule(
        dataset=dataset, technology="SOFC - Y", rule=rule, conversion_factor=1
    )
    assert len(dataset["exchanges"]) == remaining
    assert len(metals.material_decisions) == 1
    assert metals.material_decisions[0]["reason code"].endswith(reason)


def test_shared_icev_engine_has_explicit_preservation_decisions():
    metals = _metals_for_material_rule_tests("ICEV", "Platinum", 1)
    dataset = market_dataset(
        "internal combustion engine production, passenger car",
        "internal combustion engine, passenger car",
        "GLO",
        [technosphere_exchange("market for platinum", "platinum", "GLO", 0.00001)],
    )
    before = deepcopy(dataset)
    rules = [
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "ICEV" and r.element == "Platinum"
    ]
    for rule in rules:
        metals._apply_material_rule(
            dataset=dataset, technology="ICEV", rule=rule, conversion_factor=None
        )
    assert len(rules) == len(metals.material_decisions) == 2
    assert dataset == before
    assert all(
        d["reason code"].endswith("preserved_source") for d in metals.material_decisions
    )


@pytest.mark.parametrize(
    "name",
    [
        "treatment of automobile catalyst",
        "palladium recovery from scrap",
        "recycling of palladium",
    ],
)
def test_recycling_never_acquires_or_scales_virgin_resource(name):
    for exchanges in ([], [biosphere_resource("Palladium", 0.002)]):
        dataset = market_dataset(name, "palladium", "RoW", exchanges)
        before = deepcopy(dataset)
        assert not correct_metal_resource_exchanges(
            dataset, strict=True, add_missing_target_resource=True
        )
        assert dataset == before


def test_resource_uncertainty_is_rescaled_or_removed_and_gangue_preserved():
    copper = {
        **biosphere_resource("Copper", 0.5),
        "uncertainty type": 2,
        "loc": np.log(0.5),
        "scale": 0.2,
    }
    gold = {
        **biosphere_resource("Gold", 0.1),
        "uncertainty type": 2,
        "loc": np.log(0.1),
        "scale": 0.3,
        "negative": False,
    }
    gangue = biosphere_resource("Gangue", 3)
    dataset = market_dataset(
        "copper mine operation", "copper", "CL", [copper, gold, gangue]
    )
    correct_metal_resource_exchanges(dataset, strict=True)
    assert copper["amount"] == 1
    assert np.exp(copper["loc"]) == pytest.approx(1)
    assert copper["scale"] == 0.2
    assert gold["amount"] == 0 and gold["uncertainty type"] == 0
    assert not {"loc", "scale", "negative"} & gold.keys()
    assert gangue["amount"] == 3


def test_resource_search_traverses_markets_and_treats_missing_links_as_unknown():
    ore = market_dataset(
        "gold mine", "gold, unrefined", "GLO", [biosphere_resource("Gold", 0.8)]
    )
    market = market_dataset(
        "market for gold",
        "gold",
        "GLO",
        [technosphere_exchange(ore["name"], ore["reference product"], "GLO", 1.25)],
    )
    refinery = market_dataset(
        "gold refining",
        "gold",
        "GLO",
        [technosphere_exchange(market["name"], "gold", "GLO", 1)],
    )
    metals = object.__new__(Metals)
    metals.database = [ore, market, refinery]
    assert (
        metals.target_resource_is_supplied_upstream(
            refinery, "Gold", metals.build_exchange_provider_index(), {}
        )
        is True
    )
    metals.database = [refinery]
    assert (
        metals.target_resource_is_supplied_upstream(
            refinery, "Gold", metals.build_exchange_provider_index(), {}
        )
        is None
    )
    assert metals.get_missing_target_resource_dataset_ids({id(refinery)}) == set()


def test_transport_sums_all_modes_and_routes_with_one_primary_weight():
    metals = object.__new__(Metals)
    metals.alt_names = {"copper": "Copper"}
    metals.metals_transport = pd.DataFrame(
        {
            "country": ["CL"] * 4,
            "Metal": ["Copper"] * 4,
            "TransportMode Label": ["Air", "Sea", "Road", "Railway"],
            "Weighted Distance (km)": [100, 500, 200, 200],
        }
    )
    metals.transport_lookup = build_transport_lookup(metals.metals_transport)
    metals.metals_transport_activities = {
        mode.lower(): {"name": mode, "reference product": mode, "unit": "ton kilometer"}
        for mode in ["Air", "Sea", "Road", "Railway"]
    }
    dataset = {
        "exchanges": [
            technosphere_exchange("mine A", "copper", "CL", 0.3),
            technosphere_exchange("mine B", "copper", "CL", 0.45),
        ]
    }
    exchanges = metals.add_transport_to_market(dataset, "copper")
    assert len(exchanges) == 4
    assert sum(e["amount"] for e in exchanges) == pytest.approx(0.75)
    assert {e["name"]: e["amount"] for e in exchanges}["Sea"] == pytest.approx(0.375)


def test_mining_shares_keep_small_producers_and_sum_duplicate_country_routes():
    metals = object.__new__(Metals)
    metals.year = 2030
    df = pd.DataFrame(
        {
            "Country": ["Chile", "Chile", "Peru"],
            "2020": [0.5, 0.495, 0.005],
            "2030": [0.4, 0.595, 0.005],
        }
    )
    shares = metals.get_shares(df, {"Chile": "CL", "Peru": "PE"}, "mine", "copper")
    assert shares[("mine", "copper", "PE")] == pytest.approx(0.005)
    assert shares[("mine", "copper", "CL")] == pytest.approx(0.995)


def test_serialized_reference_product_selector_relinks_grade_fallback():
    metals = object.__new__(Metals)
    old = market_dataset("market for lithium carbonate", "lithium carbonate", "GLO", [])
    new = market_dataset(
        "market for lithium carbonate, battery grade",
        "lithium carbonate, battery grade",
        "World",
        [],
    )
    consumer = market_dataset(
        "consumer",
        "consumer",
        "GLO",
        [
            {
                **technosphere_exchange(
                    old["name"], old["reference product"], "GLO", 2
                ),
                "input": ("old", "code"),
            }
        ],
    )
    metals.database = [old, new, consumer]
    metals.write_log = lambda *args: None
    metals.substitute_old_markets(
        new, pd.DataFrame({"Reference product": ["{'equals': 'lithium carbonate'}"]})
    )
    assert consumer["exchanges"][0]["product"] == new["reference product"]
    assert consumer["exchanges"][0]["location"] == "World"
    assert "input" not in consumer["exchanges"][0]


def test_two_missing_resource_flows_are_filled_only_at_extraction_boundary():
    ore = market_dataset("gold mine", "gold", "GLO", [])
    refinery = market_dataset(
        "gold refinery",
        "gold",
        "CH",
        [technosphere_exchange("gold mine", "gold", "GLO", 1)],
    )
    metals = object.__new__(Metals)
    metals.database = [refinery, ore]  # downstream first must not affect the result
    assert metals.get_missing_target_resource_dataset_ids({id(ore), id(refinery)}) == {
        id(ore)
    }


def test_resource_search_cycles_are_unresolved_not_absence():
    a = market_dataset(
        "gold A", "gold", "GLO", [technosphere_exchange("gold B", "gold", "GLO", 1)]
    )
    b = market_dataset(
        "gold B", "gold", "GLO", [technosphere_exchange("gold A", "gold", "GLO", 1)]
    )
    metals = object.__new__(Metals)
    metals.database = [a, b]
    assert (
        metals.target_resource_is_supplied_upstream(
            a, "Gold", metals.build_exchange_provider_index(), {}
        )
        is None
    )
    assert metals.get_missing_target_resource_dataset_ids({id(a), id(b)}) == set()


def test_market_missing_one_route_preserves_source_instead_of_renormalizing():
    metals = object.__new__(Metals)
    metals.database = [market_dataset("mine A", "copper", "CL", [])]
    metals.create_region_specific_markets = lambda _: pytest.fail(
        "mutated suppliers before checking missing route"
    )
    df = pd.DataFrame(
        {
            "Process": ["{'equals': 'mine A'}", "{'equals': 'absent mine'}"],
            "Reference product": ["{'equals': 'copper'}"] * 2,
        }
    )
    assert metals.create_market("copper", df) is None
    assert metals.market_diagnostics[0]["reason"] == "market_preserved_supplier_missing"


def test_market_transport_uses_primary_fraction_once():
    metals = object.__new__(Metals)
    metals.database = []
    metals.create_region_specific_markets = lambda _: [
        technosphere_exchange("mine", "copper", "CL", 1)
    ]
    metals.build_secondary_market_exchanges_from_existing_market = lambda _: [
        technosphere_exchange("recycling", "copper", "GLO", 0.25)
    ]
    observed = []

    def transport(dataset, _):
        observed.extend(deepcopy(dataset["exchanges"][1:]))
        return [
            {
                **technosphere_exchange(
                    "transport", "transport", "GLO", sum(e["amount"] for e in observed)
                ),
                "unit": "ton kilometer",
            }
        ]

    metals.add_transport_to_market = transport
    metals.activity_exists = lambda _: True
    market = metals.create_market(
        "copper", pd.DataFrame(columns=["Process", "Reference product"])
    )
    assert sum(e["amount"] for e in observed) == pytest.approx(0.75)
    assert next(
        e["amount"] for e in market["exchanges"] if e["unit"] == "ton kilometer"
    ) == pytest.approx(0.75)


def test_validation_uses_applied_suppliers_and_catches_even_small_share_loss():
    validator = object.__new__(validation_module.MetalsValidation)
    primary = technosphere_exchange("mine", "copper", "CL", 0.75)
    secondary = technosphere_exchange("recycling", "copper", "GLO", 0.25)
    validator.database = [
        market_dataset(
            "market for copper",
            "copper",
            "World",
            [deepcopy(primary), deepcopy(secondary)],
        )
    ]
    validator.system_model = "cutoff"
    validator.applied_market_shares = {
        "copper": {
            "primary share": 0.75,
            "secondary share": 0.25,
            "primary suppliers": [primary],
            "secondary suppliers": [secondary],
        }
    }
    validator.prim_sec_split = {"copper": {"shares": {"primary": {2020: 0.1}}}}
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_excel_shares_preserved()
    assert issues == []
    validator.database[0]["exchanges"][0]["amount"] -= 0.001
    validator.check_excel_shares_preserved()
    assert len(issues) == 1 and issues[0][1]["issue_type"] == "major"


@pytest.mark.parametrize("count", [0, 2])
def test_validation_reports_missing_or_duplicate_market(count):
    validator = object.__new__(validation_module.MetalsValidation)
    validator.metals_list = ["copper"]
    validator.database = [
        market_dataset("market for copper", "copper", "World", []) for _ in range(count)
    ]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_market_balance()
    assert len(issues) == 1 and issues[0][1]["issue_type"] == "major"


def test_air_transport_selects_freight_service_not_aircraft_manufacture():
    aircraft = {
        "name": "market for aircraft, belly freight, long haul",
        "reference product": "aircraft, belly freight, long haul",
        "unit": "unit",
        "location": "GLO",
    }
    freight = {
        "name": "market for transport, freight, aircraft, long haul",
        "reference product": "transport, freight, aircraft, long haul",
        "unit": "ton kilometer",
        "location": "GLO",
    }
    assert select_transport_provider([aircraft, freight], "air") is freight
    with pytest.raises(metals_module.ws.NoResults):
        select_transport_provider([aircraft], "air")


@pytest.mark.parametrize(
    "name",
    [
        "market group for transport, freight train",
        "market group for transport, freight, train, fleet average",
    ],
)
def test_freight_selector_accepts_supported_rail_market_names(name):
    provider = {"name": name, "unit": "ton kilometer", "location": "GLO"}
    assert select_transport_provider([provider], "railway") is provider


def test_partial_proxy_mapping_falls_back_for_each_missing_country():
    metals = object.__new__(Metals)
    source = market_dataset("mine", "copper", "GLO", [])
    metals.db_index = {"mine": {"copper": [source]}}
    metals.is_in_index = lambda ds: ds["location"] == "CL"
    captured = {}

    def proxies(**kwargs):
        captured.update(kwargs)
        return {}

    metals.fetch_proxies = proxies
    metals.create_new_mining_activity(
        "mine",
        "copper",
        {"Chile": "CL", "Peru": "PE", "Zambia": "ZM"},
        {"PE": "GLO", "ZM": "missing"},
    )
    assert captured["geo_mapping"] == {"PE": source, "ZM": source}
    assert captured["unlist"] is False
    assert metals.market_diagnostics[0]["country"] == "ZM"


@pytest.mark.parametrize("share", [np.nan, -0.1, np.inf])
def test_invalid_individual_mining_share_cannot_be_hidden_by_aggregation(share):
    metals = object.__new__(Metals)
    metals.year = 2035
    df = pd.DataFrame(
        {"Country": ["Chile", "Chile"], "2020": [share, 1], "2030": [share, 1]}
    )
    with pytest.raises(MetalsConfigError, match="Invalid mining"):
        metals.get_shares(df, {"Chile": "CL"}, "mine", "copper")


def test_mine_reagent_does_not_displace_virgin_extraction(monkeypatch):
    mine = market_dataset(
        "copper mine operation",
        "copper",
        "CL",
        [
            biosphere_resource("Copper", 0.5),
            technosphere_exchange(
                "market for copper sulfate", "copper sulfate", "GLO", 0.000001
            ),
        ],
    )
    reagent = market_dataset(
        "market for copper sulfate",
        "copper sulfate",
        "GLO",
        [biosphere_resource("Copper", 0.2)],
    )
    metals = object.__new__(Metals)
    metals.database = [mine, reagent]
    metals.version = "3.12"
    metals._validation_targets = {}
    metals.get_mining_share_dataset_ids = lambda: {id(mine)}
    metals.get_mapped_metal_dataset_ids = lambda: {id(mine)}
    monkeypatch.setattr(
        metals_module, "record_change_event", lambda *args, **kwargs: None
    )
    metals.post_allocation_correction()
    assert mine["exchanges"][0]["amount"] == 1


def test_refinery_resource_is_counted_once_with_full_upstream_feed(monkeypatch):
    mine = market_dataset(
        "gold mine operation", "gold", "GLO", [biosphere_resource("Gold", 0.5)]
    )
    refinery = market_dataset(
        "gold refining",
        "gold",
        "CH",
        [
            biosphere_resource("Gold", 0.5),
            technosphere_exchange("gold mine operation", "gold", "GLO", 1),
        ],
    )
    metals = object.__new__(Metals)
    metals.database = [refinery, mine]
    metals.version = "3.12"
    metals._validation_targets = {}
    metals.get_mining_share_dataset_ids = lambda: {id(mine), id(refinery)}
    metals.get_mapped_metal_dataset_ids = lambda: {id(mine), id(refinery)}
    monkeypatch.setattr(
        metals_module, "record_change_event", lambda *args, **kwargs: None
    )
    metals.post_allocation_correction()
    assert mine["exchanges"][0]["amount"] == 1
    assert refinery["exchanges"][0]["amount"] == 0


@pytest.mark.parametrize(
    "name,product,unit,expected",
    [
        (
            "market for electricity, cobalt industry",
            "electricity, cobalt industry",
            "kilowatt hour",
            False,
        ),
        ("heat for cobalt industry", "heat, cobalt industry", "megajoule", False),
        ("cobalt production", "sulfuric acid", "kilogram", False),
        ("cobalt sulfate production", "cobalt sulfate", "kilogram", True),
        ("copper-cobalt mining", "copper-cobalt ore", "kilogram", True),
        ("market for cobalt", None, "kilogram", True),
    ],
)
def test_resource_path_uses_material_unit_and_product(name, product, unit, expected):
    exchange = technosphere_exchange(name, product, "GLO", 1)
    exchange["unit"] = unit
    assert (
        metals_module.exchange_may_carry_target_resource(exchange, "Cobalt") is expected
    )


def run_resource_correction(database, primary, monkeypatch):
    metals = object.__new__(Metals)
    metals.database = database
    metals.version = "3.12"
    metals._validation_targets = {}
    metals.get_mining_share_dataset_ids = lambda: {id(ds) for ds in primary}
    metals.get_mapped_metal_dataset_ids = lambda: {id(ds) for ds in primary}
    monkeypatch.setattr(
        metals_module, "record_change_event", lambda *args, **kwargs: None
    )
    metals.post_allocation_correction()
    return metals


def test_cobalt_attribution_survives_industry_electricity_cycle(monkeypatch):
    ore = market_dataset(
        "copper-cobalt mining",
        "copper-cobalt ore",
        "CD",
        [biosphere_resource("Cobalt", 0.01)],
    )
    electricity = market_dataset(
        "market for electricity, cobalt industry",
        "electricity, cobalt industry",
        "GLO",
        [],
    )
    electricity["unit"] = "kilowatt hour"
    power = technosphere_exchange(
        electricity["name"], electricity["reference product"], "GLO", 3
    )
    power["unit"] = "kilowatt hour"
    electricity["exchanges"] = [{**power, "amount": 0.0046}]
    intermediate = market_dataset(
        "cobalt hydroxide production",
        "cobalt hydroxide",
        "CD",
        [technosphere_exchange(ore["name"], "copper-cobalt ore", "CD", 100), power],
    )
    cobalt = market_dataset(
        "cobalt metal production, from copper mining, via electrolysis, economic allocation",
        "cobalt",
        "CN",
        [technosphere_exchange(intermediate["name"], "cobalt hydroxide", "CD", 2)],
    )
    database = [cobalt, intermediate, ore, electricity]
    before_inputs = [
        deepcopy(ds["exchanges"]) for ds in (cobalt, intermediate, electricity)
    ]
    metals = run_resource_correction(database, [cobalt, ore], monkeypatch)
    assert metals.dataset_has_target_resource_exchange(cobalt, "Cobalt")
    assert not metals.dataset_has_target_resource_exchange(ore, "Cobalt")
    assert not any(
        d["reason"] == "upstream_unresolved"
        for d in getattr(metals, "resource_diagnostics", [])
    )
    for ds, before in zip((cobalt, intermediate, electricity), before_inputs):
        assert [e for e in ds["exchanges"] if e["type"] == "technosphere"] == before
    first = deepcopy(database)
    metals.post_allocation_correction()
    assert database == first


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("canada_has_resource", [False, True])
def test_zinc_primary_attribution_survives_auxiliary_cycles(
    monkeypatch, reverse, canada_has_resource
):
    concentrate = market_dataset(
        "market for zinc concentrate",
        "zinc concentrate",
        "GLO",
        [],
    )
    name = "primary zinc production from concentrate"
    feed = technosphere_exchange(concentrate["name"], "zinc concentrate", "GLO", 1.5)
    china = market_dataset(
        name,
        "zinc",
        "CN",
        [
            deepcopy(feed),
            technosphere_exchange("market for zinc", "zinc", "World", 0.003),
            technosphere_exchange("zinc oxide production", "zinc oxide", "GLO", 0.04),
        ],
    )
    canada = market_dataset(name, "zinc", "CA", [deepcopy(feed)])
    if canada_has_resource:
        canada["exchanges"].append(biosphere_resource("Zinc", 1))
    market = market_dataset(
        "market for zinc",
        "zinc",
        "World",
        [
            technosphere_exchange(name, "zinc", "CN", 0.95),
            technosphere_exchange(name, "zinc", "CA", 0.05),
        ],
    )
    oxide = market_dataset(
        "zinc oxide production",
        "zinc oxide",
        "GLO",
        [
            technosphere_exchange(market["name"], "zinc", "World", 0.8),
        ],
    )
    refinery = market_dataset(
        "zinc refining",
        "zinc",
        "CH",
        [
            technosphere_exchange(market["name"], "zinc", "World", 1),
        ],
    )
    secondary = market_dataset("zinc recycling", "zinc", "GLO", [deepcopy(feed)])
    database = [refinery, china, market, oxide, canada, concentrate, secondary]
    if reverse:
        database.reverse()
    before_inputs = {
        id(ds): deepcopy([e for e in ds["exchanges"] if e["type"] == "technosphere"])
        for ds in database
    }
    metals = run_resource_correction(
        database, [china, canada, refinery, secondary], monkeypatch
    )
    for ds in (china, canada):
        resources = metals_module.get_in_ground_resource_exchanges(ds)
        assert len(resources) == 1 and resources[0]["amount"] == 1
    for ds in (refinery, secondary, concentrate):
        assert not metals.dataset_has_target_resource_exchange(ds, "Zinc")
    for ds in database:
        assert [
            e for e in ds["exchanges"] if e["type"] == "technosphere"
        ] == before_inputs[id(ds)]
    first = deepcopy(database)
    metals.post_allocation_correction()
    assert database == first


@pytest.mark.parametrize(
    "failure",
    ["missing", "ambiguous", "missing_auxiliary", "insufficient", "full_metal_feed"],
)
def test_primary_zinc_boundary_requires_supported_feed(failure):
    concentrate = market_dataset(
        "market for zinc concentrate", "zinc concentrate", "GLO", []
    )
    zinc = market_dataset(
        "primary zinc production from concentrate",
        "zinc",
        "CN",
        [technosphere_exchange(concentrate["name"], "zinc concentrate", "GLO", 1.5)],
    )
    database = [zinc, concentrate]
    if failure == "missing":
        database.remove(concentrate)
    elif failure == "ambiguous":
        database.append(deepcopy(concentrate))
    elif failure == "insufficient":
        zinc["exchanges"][0]["amount"] = 0.01
    elif failure == "missing_auxiliary":
        zinc["exchanges"].append(
            technosphere_exchange("missing zinc", "zinc", "GLO", 0.01)
        )
    else:
        supplier = market_dataset(
            "zinc supplier", "zinc", "GLO", [biosphere_resource("Zinc", 1)]
        )
        zinc["exchanges"].append(
            technosphere_exchange(supplier["name"], "zinc", "GLO", 1)
        )
        database.append(supplier)
    metals = object.__new__(Metals)
    metals.database = database
    index = metals.build_exchange_provider_index()
    assert not metals.has_primary_resource_attribution_feed(zinc, "Zinc", index)
    assert metals.get_missing_target_resource_dataset_ids({id(zinc)}) == set()
    if failure in {"missing", "ambiguous", "missing_auxiliary"}:
        assert (
            metals.target_resource_is_supplied_upstream(zinc, "Zinc", index, {}) is None
        )


@pytest.mark.parametrize("element", ["Lithium", "Platinum", "Neodymium", "Boron"])
def test_unverified_ev_component_scope_preserves_existing_inputs(element):
    rule = next(
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "EV" and r.element == element
    )
    metals = _metals_for_material_rule_tests("EV", element, 100)
    dataset = market_dataset(
        "electric motor production, vehicle",
        "electric motor, vehicle",
        "GLO",
        [technosphere_exchange("existing component", "existing component", "GLO", 0.4)],
    )
    before = deepcopy(dataset)
    metals.get_metal_market_dataset = lambda *args: pytest.fail(
        "provider lookup for unverified scope"
    )
    metals._apply_material_rule(
        dataset=dataset, technology="EV", rule=rule, conversion_factor=None
    )
    assert dataset == before
    assert metals.material_decisions[0]["reason code"].endswith(
        "component_scope_unverified"
    )


@pytest.mark.parametrize(
    "year,applied", [(2020, True), (2025, False), (2050, False), (2019, False)]
)
def test_material_year_support_is_per_pair_not_global(year, applied):
    metals = _metals_for_material_rule_tests("EV", "Aluminium", 1)
    metals.year = year
    metals.precomputed_medians = get_metals_intensity_factors_data().sel(year=2020)
    rule = next(
        r
        for r in load_material_rules().enabled_rules
        if r.technology == "EV" and r.element == "Aluminium"
    )
    dataset = market_dataset("motor", "motor", "GLO", [])
    provider = market_dataset(
        rule.provider.name, rule.provider.reference_product, "GLO", []
    )
    metals.get_metal_market_dataset = lambda *args: provider
    metals._apply_material_rule(
        dataset=dataset, technology="EV", rule=rule, conversion_factor=1
    )
    assert bool(dataset["exchanges"]) is applied
    assert metals.material_decisions[0]["source year maximum"] == 2020
    if not applied:
        assert metals.material_decisions[0]["reason code"].endswith(
            "year_outside_support"
        )


def test_intensity_loader_does_not_extrapolate_other_series_endpoints(
    tmp_path, monkeypatch
):
    import premise.data_collection as collection

    directory = tmp_path / "metals"
    directory.mkdir()
    rows = [
        ["Gold", "kg/MW", 2020, 4, 4, 3, 5, "A"],
        ["Gold", "kg/MW", 2030, 2, 2, 1, 3, "A"],
        ["Silver", "kg/MW", 2025, 9, 9, 8, 10, "B"],
        ["Silver", "kg/MW", 2050, 9, 9, 8, 10, "B"],
    ]
    pd.DataFrame(
        rows,
        columns=["metal", "unit", "year", "mean", "median", "min", "max", "origin_var"],
    ).to_csv(directory / "metals_db.csv", index=False)
    monkeypatch.setattr(collection, "DATA_DIR", tmp_path)
    array = collection.get_metals_intensity_factors_data.__wrapped__()
    pair = array.sel(metal="Gold", origin_var="A", variable="median")
    assert pair.sel(year=2025).item() == 3
    assert pair.sel(year=2050).item() == 2  # selection fill, not a permitted forecast
    assert pair.source_year_max.item() == 2030
    assert np.isnan(array.sel(metal="Gold", origin_var="B")).all()


def test_consequential_correction_preserves_substitution_and_all_resource_flows():
    dataset = market_dataset(
        "joint mine",
        "molybdenite",
        "GLO",
        [
            technosphere_exchange("copper market", "copper, cathode", "GLO", -2),
            {
                "name": "Copper",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("natural resource", "in ground"),
                "amount": 3,
            },
        ],
    )
    metals = object.__new__(Metals)
    metals.system_model = "consequential"
    metals.database = [dataset]
    before = deepcopy(dataset)
    metals.build_in_ground_resource_exchange_index = lambda: pytest.fail(
        "economic correction entered"
    )
    metals.post_allocation_correction()
    assert dataset == before
    assert (
        metals.resource_diagnostics[0]["reason"]
        == "preserved_consequential_substitution"
    )


def test_consequential_market_preserved_before_any_supplier_changes():
    metals = object.__new__(Metals)
    metals.system_model = "consequential"
    metals.create_region_specific_markets = lambda _: pytest.fail(
        "changed marginal supply"
    )
    assert metals.create_market("copper", pd.DataFrame()) is None
    assert metals.market_diagnostics[0]["reason"] == "market_preserved_consequential"


def test_mining_share_interpolation_and_no_endpoint_forecast():
    frame = pd.DataFrame(
        {"Country": ["A", "B"], "2020": [0.4, 0.6], "2030": [0.8, 0.2]}
    )
    assert mining_shares_for_year(frame, 2025).tolist() == pytest.approx([0.6, 0.4])
    assert mining_shares_for_year(frame, 2050) is None
    assert mining_shares_for_year(frame, 2019) is None
    frame.loc[0, "2030"] = -1
    with pytest.raises(MetalsConfigError, match="nonnegative"):
        mining_shares_for_year(frame, 2025)


@pytest.mark.parametrize(
    "year,reason", [(2050, "year_outside_support"), (2020, "transport_missing")]
)
def test_market_gaps_preserve_source_before_mutating_proxies(year, reason):
    metals = object.__new__(Metals)
    metals.system_model = "cutoff"
    metals.year = year
    metals.country_codes = {"Chile": "CL", "Peru": "PE"}
    metals.get_weighted_average_distance = lambda country, _: (
        pd.DataFrame({"distance": [2]}) if country == "CL" else pd.DataFrame()
    )
    metals.create_region_specific_markets = lambda _: pytest.fail(
        "modified supplier before coverage check"
    )
    frame = pd.DataFrame(
        {"Country": ["Chile", "Peru"], "2020": [0.75, 0.25], "2030": [0.75, 0.25]}
    )
    before = frame.copy(deep=True)
    assert metals.create_market("copper", frame) is None
    pd.testing.assert_frame_equal(frame, before)
    diagnostic = metals.market_diagnostics[0]
    assert diagnostic["reason"] == "market_preserved_" + reason
    if year == 2020:
        assert diagnostic["missing_primary_fraction"] == 0.25
        assert diagnostic["missing_origins"] == {"PE": 0.25}


def test_zero_freight_distance_is_coverage_and_invalid_distances_fail(
    tmp_path, monkeypatch
):
    directory = tmp_path / "metals"
    directory.mkdir()
    path = directory / "transport_markets_data.csv"
    data = pd.DataFrame(
        {
            "Country": ["CL"],
            "Metal": ["Copper"],
            "TransportMode Label": ["Road"],
            "Weighted Distance (km)": [0],
        }
    )
    data.to_csv(path, index=False)
    monkeypatch.setattr(metals_module, "DATA_DIR", tmp_path)
    assert len(metals_module.load_metals_transport()) == 1
    data["Weighted Distance (km)"] = -1
    data.to_csv(path, index=False)
    with pytest.raises(MetalsConfigError, match="nonnegative"):
        metals_module.load_metals_transport()
