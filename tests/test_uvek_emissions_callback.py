"""Atomic native emissions dispatch, independent checks and exported provenance."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import numpy as np

import premise.emissions.uvek_emissions as native
from premise.core.utils import scenario_metadata
from premise.emissions.emissions import _update_emissions
from premise.emissions.uvek_combustion import (
    CALLER,
    VARIANT,
    plan_native_combustion_variants,
)
from premise.inventory.inventory_store import (
    create_inventory_store,
    get_scenario_inventory,
)
from premise.inventory.profiles import identity
from premise.reporting.provenance import ProvenanceCollector
from premise.validation.sector_validation import validate_sector_contract
from test_uvek_emissions_combustion import case  # noqa: F401


@pytest.fixture
def callback(case, monkeypatch):  # noqa: F811
    data, bindings, factors, context = case
    monkeypatch.setattr(native, "plan_native_gains_reductions", lambda *args: factors)

    def mapper(location):
        return "EUR"

    monkeypatch.setattr(
        native,
        "Geomap",
        lambda model: SimpleNamespace(ecoinvent_to_iam_location=mapper),
    )
    monkeypatch.setattr(
        native,
        "_contexts",
        lambda mapper: {
            (identity(row["caller"]), identity(row["supplier"])): (
                row["sector"],
                row["region"],
            )
            for row in bindings
        },
    )

    def planner(database, selected, scenario, mapper, *, biosphere_name):
        result = plan_native_combustion_variants(
            database, bindings, selected, scenario, biosphere_name=biosphere_name
        )
        result["diagnostics"].update(
            reviews=["synthetic review"],
            limitations=["Synthetic incomplete review; retain unreviewed emissions."],
        )
        return result

    monkeypatch.setattr(native.combustion, "plan_reviewed_native_combustion", planner)
    scenario = {
        **context,
        "database": data,
        "iam data": SimpleNamespace(gains_data_IAM=object()),
        "cache": {"stale": "provider"},
        "index": {"old": "index"},
    }
    return scenario, factors, planner


def update(scenario):
    return _update_emissions(scenario, "2026", "cutoff", "CLE", "uvek", "3.10")


def test_callback_declares_scope_audits_metadata_and_does_not_duplicate_events(
    callback,
):
    scenario, _, _ = callback
    source = scenario["database"]
    before = copy.deepcopy(source)
    collector = ProvenanceCollector("native-test")
    sid = ("remind", "test", 2050)
    with collector.session(sid, "emissions"), pytest.warns(UserWarning):
        update(scenario)
    assert source == before
    data = get_scenario_inventory(scenario)
    assert len(data) == 7 and sum(VARIANT in a for a in data) == 2
    assert scenario["emissions diagnostics"]["gaseous_air_changes"] == 4
    assert "index" not in scenario and "stale" not in scenario["cache"]
    assert validate_sector_contract(scenario, "emissions").valid
    assert len(scenario["_validation_intents"]["emissions"]["allowed_added_keys"]) == 2
    metadata = scenario_metadata(scenario, "2026", "cutoff")
    assert metadata["scientific_status"] == "screening_only"
    assert (
        metadata["native_sector_audits"]["emissions"]["full_sector_mapping_complete"]
        is False
    )
    assert len(collector.events_for(sid)) == 4
    with collector.session(sid, "emissions"), pytest.warns(UserWarning):
        update(scenario)
    assert len(collector.events_for(sid)) == 4
    assert len(get_scenario_inventory(scenario)) == 7


@pytest.mark.parametrize(
    "fault",
    [
        "caller_quantity",
        "air_quantity",
        "factor",
        "upstream",
        "sector",
        "omitted_caller",
        "empty_plan",
    ],
)
def test_independent_checks_refuse_corruption_before_inventory_or_metadata_commit(
    callback, monkeypatch, fault
):
    scenario, _, planner = callback
    source = scenario["database"]
    before = copy.deepcopy(source)

    def corrupt(*args, **kwargs):
        plan = planner(*args, **kwargs)
        if fault == "caller_quantity":
            plan["replacements"][0]["activity"]["exchanges"][1]["amount"] *= 2
        elif fault == "air_quantity":
            plan["activities"][0]["exchanges"][2]["amount"] *= 2
        elif fault == "factor":
            plan["activities"][0][VARIANT]["changes"][0]["factor"] *= 0.5
        elif fault == "upstream":
            plan["activities"][0]["exchanges"][1]["amount"] *= 2
        elif fault == "sector":
            plan["replacements"][0]["activity"][CALLER][0]["sector"] = "unreviewed"
        elif fault == "omitted_caller":
            plan["replacements"].pop()
        elif fault == "empty_plan":
            plan["replacements"] = []
            plan["activities"] = []
        return plan

    monkeypatch.setattr(native.combustion, "plan_reviewed_native_combustion", corrupt)
    with pytest.raises(ValueError):
        update(scenario)
    assert scenario["database"] is source and source == before
    assert scenario["cache"] == {"stale": "provider"}
    assert (
        "emissions diagnostics" not in scenario
        and "native sector audits" not in scenario
    )


@pytest.mark.parametrize("backend", ["legacy", "compact"])
def test_store_bridge_preserves_source_and_literal_originals(callback, backend):
    scenario, _, _ = callback
    source = create_inventory_store(scenario.pop("database"), backend=backend)
    before = source.materialize()
    scenario["_inventory_store"] = create_inventory_store(before, backend=backend)
    with pytest.warns(UserWarning):
        update(scenario)
    assert source.materialize() == before
    assert sum(CALLER in a for a in get_scenario_inventory(scenario)) == 2


def test_missing_GAINS_is_explicitly_not_applicable_without_inventory_access(
    callback, monkeypatch
):
    scenario, _, _ = callback
    scenario["iam data"].gains_data_IAM = None
    monkeypatch.setattr(
        native,
        "get_scenario_inventory",
        Mock(side_effect=AssertionError("unexpected inventory read")),
    )
    update(scenario)
    phase = validate_sector_contract(scenario, "emissions")
    assert phase.valid and phase.rule_results[0].applicability == "not_applicable"


def test_no_reductions_preserve_inventory_and_are_not_a_false_success(callback):
    scenario, factors, _ = callback
    source = scenario["database"]
    factors["status"][:] = "unchanged"
    factors["factors"][:] = float("nan")
    update(scenario)
    assert scenario["database"] is source
    assert scenario["emissions diagnostics"]["applicability"] == "not_applicable"
    assert (
        validate_sector_contract(scenario, "emissions").rule_results[0].applicability
        == "not_applicable"
    )


def test_wrong_background_refused_and_public_gate_stays_closed(callback):
    scenario, _, _ = callback
    scenario["background context"] = {"family": "ecoinvent"}
    with pytest.raises(ValueError, match="background context"):
        update(scenario)
    from premise import NewDatabase

    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    owner.prepare_inventories = Mock()
    with pytest.raises(ValueError, match="unsupported sectors"):
        owner.update("emissions")
    owner.prepare_inventories.assert_not_called()


def test_cached_numpy_quantities_and_audits_keep_their_exact_values(callback):
    scenario, _, _ = callback
    source = scenario["database"][1]["exchanges"][2]
    source["amount"] = np.float32(10.1)
    source["loc"] = np.float32(np.log(source["amount"]))
    original = source["amount"].item()
    with pytest.warns(UserWarning):
        update(scenario)
    variants = [a for a in get_scenario_inventory(scenario) if VARIANT in a]
    for variant in variants:
        factor = 0.5 if variant[VARIANT]["sector"] == "power" else 0.2
        assert variant["exchanges"][2]["amount"] == original * factor
        assert variant[VARIANT]["changes"][0]["source_exchange"]["amount"] == original
