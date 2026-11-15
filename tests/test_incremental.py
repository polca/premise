"""Incremental dispatch uses the normal sector definitions and cumulative stores."""

from copy import deepcopy

import premise.database.new_database as database_module
from premise.database.incremental import IncrementalDatabase
from premise.inventory.inventory_store import CompactInventoryStore


def test_shared_dispatch_preserves_cumulative_groups_alias_and_scenario_isolation(
    monkeypatch,
):
    builder = object.__new__(IncrementalDatabase)
    builder.version = "3.12"
    builder.system_model = "cutoff"
    builder.use_absolute_efficiency = False
    builder.gains_scenario = "CLE"
    builder.scenarios = [
        {"model": model, "pathway": "SSP2-Test", "year": 2050}
        for model in ("image", "remind")
    ]
    baseline = [
        {
            "name": "service",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "exchanges": [],
            "steps": [],
        }
    ]
    prepared = []
    outputs = []
    builder.prepare_inventories = prepared.extend
    builder._load_original_database = lambda: deepcopy(baseline)

    def persist(definition, scenario, *, persist):
        assert persist is True
        store = CompactInventoryStore(scenario["_inventory_working_copy"])
        outputs.append((definition["model"], store.materialize()[0]["steps"]))
        return store

    builder._store_updated_scenario = persist

    def updater(label, expected_args):
        def apply(scenario, *args):
            assert args == expected_args
            scenario["_inventory_working_copy"][0]["steps"].append(label)
            return scenario

        return apply

    for name, label in (
        ("_update_metals", "metals"),
        ("_update_mining", "mining"),
        ("_update_wind_turbines", "renewable"),
        ("_update_final_energy", "final energy"),
        ("_update_cdr", "cdr"),
    ):
        monkeypatch.setattr(database_module, name, updater(label, ("3.12", "cutoff")))
    monkeypatch.setattr(
        database_module,
        "_update_vehicles",
        updater("ships", ("ship", "3.12", "cutoff")),
    )
    builder.update(
        {
            "materials": ["metals", "mining"],
            "renewables": "renewable",
            "shipping": "ships",
            "demand": "final energy",
            "legacy carbon removal": "dac",
        }
    )

    expected = [
        ["metals", "mining"],
        ["metals", "mining", "renewable"],
        ["metals", "mining", "renewable", "ships"],
        ["metals", "mining", "renewable", "ships", "final energy"],
        ["metals", "mining", "renewable", "ships", "final energy", "cdr"],
    ]
    assert outputs == [
        (model, steps) for model in ("image", "remind") for steps in expected
    ]
    assert prepared == ["metals", "mining", "renewable", "ships", "final energy", "dac"]
    assert baseline[0]["steps"] == []
