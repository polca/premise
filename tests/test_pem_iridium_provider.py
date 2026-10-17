"""Apply configured kg/MW to the existing PEM iridium supply boundary."""

from copy import deepcopy
from dataclasses import replace
from collections import defaultdict

from bw2io import ExcelImporter
import numpy as np
import pytest
import yaml

from premise.data_collection import get_metals_intensity_factors_data
from premise.filesystem_constants import INVENTORY_DIR
from premise.metals import Metals
from premise.metals_rules import (
    MetalsConfigError,
    load_material_rules,
    load_technology_conversions,
)
import premise.metals_rules as rules_module
from premise.metal_input_comparison import configured_metal_products

STACK = "electrolyzer production, 1MWe, PEM, Stack"
REFINERY = "platinum group metal, extraction and refinery operations"


@pytest.fixture(scope="module")
def source():
    stack = next(
        d
        for d in ExcelImporter(INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx").data
        if d["name"] == STACK
    )
    providers = [
        d
        for d in ExcelImporter(INVENTORY_DIR / "lci-PGM.xlsx").data
        if d["reference product"] == "iridium"
    ]
    for d in [stack, *providers]:
        for exc in d["exchanges"]:
            if "reference product" in exc:
                exc["product"] = exc.pop("reference product")
    return stack, providers


def rule():
    return next(
        r
        for r in load_material_rules().enabled_rules
        if r.id == "pemel-iridium-iridium"
    )


def model(providers, year=2050, version="3.12", material_rule=None):
    m = object.__new__(Metals)
    m.version, m.year = version, year
    factors = get_metals_intensity_factors_data()
    selected = min(2050, max(2020, year))
    m.precomputed_medians = (
        factors.sel(year=selected)
        if selected in factors.year
        else factors.interp(year=selected)
    )
    m.material_policies = load_material_rules().policies
    # Isolate supplier-fallback behavior here; full matched-design tests are separate.
    selected_rule = material_rule or rule()
    m.material_rules_by_technology = {
        "PEMEL": [replace(selected_rule, design_group=None)]
    }
    m.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    m.material_update_diagnostics, m.material_decisions = [], []
    m._validation_targets = {}
    m.db_index = defaultdict(lambda: defaultdict(list))
    for provider in providers:
        m.db_index[provider["name"]][provider["reference product"]].append(provider)
    m.is_in_index = lambda provider: True
    return m


def iridium(ds):
    return [
        e
        for e in ds["exchanges"]
        if e["type"] == "technosphere" and e.get("product") == "iridium"
    ]


@pytest.mark.parametrize(
    "version", ["3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]
)
@pytest.mark.parametrize(
    "year,expected",
    [
        (2020, 2 / (2 * 1.9)),
        (2035, 2 / (2 * 1.9)),
        (2050, 2 / (2 * 1.9)),
    ],
)
def test_real_stack_uses_exact_existing_refinery_and_kg_per_MW(
    source, version, year, expected
):
    original, providers = source
    ds = deepcopy(original)
    before = deepcopy(providers)
    m = model(providers, year, version)
    m.update_metal_use(ds, "PEMEL")
    (old,), (new,) = iridium(original), iridium(ds)
    assert old["amount"] == 0.8
    assert new["amount"] == pytest.approx(expected)
    assert (new["name"], new["product"], new["location"], new["unit"]) == (
        REFINERY,
        "iridium",
        "ZA",
        "kilogram",
    )
    assert [e for e in ds["exchanges"] if e not in iridium(ds)] == [
        e for e in original["exchanges"] if e not in iridium(original)
    ]
    assert providers == before
    assert new["uncertainty type"] == 0
    assert "minimum" not in new and "maximum" not in new
    assert old["comment"] in new["comment"]
    (decision,) = m.material_decisions
    assert decision["status"] == "updated"
    assert decision["requested provider name"] == "market for iridium"
    assert decision["provider name"] == REFINERY
    assert decision["provider location"] == "ZA"
    assert decision["provider selection"] == "existing input fallback"
    assert decision["provider fallback rank"] is None
    assert decision["technology conversion factor"] == 1
    assert m.material_update_diagnostics == []
    assert (REFINERY, "iridium") in configured_metal_products()
    m.update_metal_use(ds, "PEMEL")
    assert iridium(ds) == [new]


def test_original_missing_market_failure_is_reproduced_without_opt_in(source):
    ds = deepcopy(source[0])
    m = model(source[1], material_rule=replace(rule(), existing_provider_fallback=None))
    before = deepcopy(ds)
    m.update_metal_use(ds, "PEMEL")
    assert ds == before
    assert (
        m.material_decisions[0]["reason code"]
        == "metals.material_rule.provider_missing"
    )


def test_present_configured_market_is_preferred(source):
    ds = deepcopy(source[0])
    market = dict(
        name="market for iridium",
        **{"reference product": "iridium"},
        location="GLO",
        unit="kilogram",
        exchanges=[],
    )
    m = model([*source[1], market])
    m.update_metal_use(ds, "PEMEL")
    (new,) = iridium(ds)
    assert new["name"] == "market for iridium"
    assert new["location"] == "GLO"
    assert m.material_decisions[0]["provider selection"] == "configured market"
    assert m.material_decisions[0]["provider fallback rank"] == 1


@pytest.mark.parametrize(
    "change",
    [
        "no input",
        "two inputs",
        "different supplier",
        "compound",
        "input unit",
        "input formula",
        "negative input",
        "missing supplier",
        "different location",
        "two providers",
        "inactive provider",
        "supplier unit",
        "output unit",
        "output amount",
        "output product",
        "two outputs",
    ],
)
def test_unrecognized_existing_boundary_is_preserved_and_diagnosed(source, change):
    ds, providers = deepcopy(source)
    (exc,) = iridium(ds)
    za = next(p for p in providers if p["location"] == "ZA")
    if change == "no input":
        ds["exchanges"].remove(exc)
    elif change == "two inputs":
        ds["exchanges"].append(deepcopy(exc))
    elif change == "different supplier":
        exc["name"] = "unrecognized refinery"
    elif change == "compound":
        exc["product"] = "iridium oxide"
    elif change == "input unit":
        exc["unit"] = "gram"
    elif change == "input formula":
        exc["formula"] = "custom_parameter"
    elif change == "negative input":
        exc["amount"] = -0.8
    elif change == "missing supplier":
        providers.remove(za)
    elif change == "different location":
        za["location"] = "RU-other"
    elif change == "two providers":
        providers.append(deepcopy(za))
    elif change == "supplier unit":
        za["unit"] = "gram"
    else:
        (output,) = [e for e in za["exchanges"] if e["type"] == "production"]
        if change == "output unit":
            output["unit"] = "gram"
        elif change == "output amount":
            output["amount"] = 2
        elif change == "output product":
            output["product"] = "platinum"
        elif change == "two outputs":
            za["exchanges"].append(deepcopy(output))
    m = model(providers)
    if change == "inactive provider":
        m.is_in_index = lambda p: False
    before = deepcopy(ds)
    m.update_metal_use(ds, "PEMEL")
    assert ds == before
    assert m.material_decisions[0]["status"] == "skipped"
    assert (
        m.material_decisions[0]["reason code"]
        == "metals.material_rule.provider_missing"
    )
    assert (
        "Existing-input fallback unavailable"
        in m.material_update_diagnostics[0]["error"]
    )


@pytest.mark.parametrize("year", [2019, 2051])
def test_out_of_support_is_not_rescued_by_fallback(source, year):
    ds = deepcopy(source[0])
    m = model(source[1], year)
    before = deepcopy(ds)
    m.update_metal_use(ds, "PEMEL")
    assert ds == before
    assert (
        m.material_decisions[0]["reason code"]
        == "metals.material_rule.year_outside_support"
    )


def test_stack_activity_unit_is_checked_before_override(source):
    ds = deepcopy(source[0])
    ds["unit"] = "kilogram"
    before = deepcopy(ds)
    with pytest.raises(MetalsConfigError, match="unit mismatch"):
        model(source[1]).update_metal_use(ds, "PEMEL")
    assert ds == before


def test_wrong_market_unit_is_not_used(source):
    ds = deepcopy(source[0])
    before = deepcopy(ds)
    market = dict(
        name="market for iridium",
        **{"reference product": "iridium"},
        location="GLO",
        unit="gram",
        exchanges=[],
    )
    with pytest.raises(MetalsConfigError, match="kilogram provider"):
        model([*source[1], market]).update_metal_use(ds, "PEMEL")
    assert ds == before


@pytest.mark.parametrize(
    "fallback",
    [
        True,
        {},
        {"name": REFINERY, "reference_product": "platinum"},
        {"name": REFINERY, "reference_product": "iridium", "unknown": True},
    ],
)
def test_fallback_configuration_is_explicit_and_same_product(
    tmp_path, monkeypatch, fallback
):
    payload = yaml.safe_load(rules_module.MATERIAL_RULES_PATH.read_text())
    target = next(r for r in payload["rules"] if r["id"] == "pemel-iridium-iridium")
    target["existing_provider_fallback"] = fallback
    path = tmp_path / "metals.yaml"
    path.write_text(yaml.safe_dump(payload))
    monkeypatch.setattr(rules_module, "MATERIAL_RULES_PATH", path)
    load_material_rules.cache_clear()
    try:
        with pytest.raises(MetalsConfigError):
            load_material_rules()
    finally:
        load_material_rules.cache_clear()


def test_fallback_is_opted_in_only_for_pem_iridium():
    assert [
        r.id for r in load_material_rules().rules if r.existing_provider_fallback
    ] == ["pemel-iridium-iridium"]


def test_iridium_back_calculates_through_real_hydrogen_lifetime_allocation(source):
    from openpyxl import load_workbook

    path = INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx"
    data = ExcelImporter(path).data
    hydrogen = next(
        d
        for d in data
        if d["name"]
        == "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity"
    )
    specs = load_workbook(path, data_only=True)["Final specifications"]
    years, replacements, productivity = (specs[f"C{row}"].value for row in (7, 8, 13))
    lifetime_hydrogen = years * 8000 * productivity
    stacks = 1 + replacements
    (feed,) = [e for e in hydrogen["exchanges"] if e["name"] == STACK]
    assert feed["amount"] == pytest.approx(stacks / lifetime_hydrogen)
    assert feed["unit"] == "unit"
    ds = deepcopy(source[0])
    model(source[1]).update_metal_use(ds, "PEMEL")
    (exc,) = iridium(ds)
    # Independent hydrogen-design denominator and original replacement count.
    # A = output minus input; background processes outside these paths are
    # intentionally omitted from this direct-input back-calculation.
    matrix = np.array(
        [[1.0, 0.0, 0.0], [-feed["amount"], 1.0, 0.0], [0.0, -exc["amount"], 1.0]]
    )
    activity = np.linalg.solve(matrix, [lifetime_hydrogen, 0.0, 0.0])
    assert activity[1] == pytest.approx(stacks)
    assert activity[2] == pytest.approx(stacks * 2 / (2 * 1.9))
    assert hydrogen == next(
        d for d in ExcelImporter(path).data if d["name"] == hydrogen["name"]
    )
