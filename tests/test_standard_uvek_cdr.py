"""Standalone CDR uses shared family normalization and explicit support roles."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from premise.emissions import carbon_dioxide_removal as module
from premise.inventory.activity_maps import InventorySet
from premise.inventory.clic_requirements import bind_activity_roles, registry


def activity(name, product=None, unit="kilogram", **extra):
    return {
        "name": name,
        "reference product": product or name,
        "unit": unit,
        "location": "CH",
        "exchanges": [{"type": "production", "amount": 1, "unit": unit}],
        **extra,
    }


@pytest.mark.parametrize(
    "family,version,biosphere", [("ecoinvent", "3.12", None), ("uvek", "2026", "3.10")]
)
def test_real_shared_constructor_normalizes_only_the_selected_family(
    family, version, biosphere
):
    name = "Electricity mix"
    supplier = activity(name, unit="kilowatt hour")
    supplier["exchanges"][0].update(name=name, product=name)
    consumer = activity("A consumer")
    edge = {
        "name": name,
        "product": name,
        "reference product": name,
        "location": "CH",
        "unit": "kilowatt hour",
        "type": "technosphere",
        "amount": -0.25,
        "uncertainty type": 3,
        "loc": -0.25,
        "scale": 0.1,
        "formula": "demand",
        "input": ("source", "electricity"),
    }
    consumer["exchanges"].append(edge)
    before = deepcopy(edge)
    owner = module.CarbonDioxideRemoval(
        [supplier, consumer],
        SimpleNamespace(regions=["EUR"]),
        "remind",
        "test",
        2050,
        version,
        "cutoff",
        family=family,
        biosphere_version=biosphere,
    )
    assert owner.family == owner.mapping.family == family
    assert owner.biosphere_version == (biosphere or version)
    expected = "electricity, high voltage" if family == "uvek" else name
    assert supplier["reference product"] == expected
    assert supplier["exchanges"][0]["product"] == expected
    assert edge["reference product"] == edge["product"] == expected
    assert {
        k: edge[k] for k in before if k not in {"product", "reference product"}
    } == {k: before[k] for k in before if k not in {"product", "reference product"}}
    assert (name, expected) in owner.index


def published_support():
    data = registry()["hybrid_sources"]
    support = data["roles"]["cdr_activities.yaml"]
    slugs = {data["nodes"][i]["clic_slug"] for ids in support.values() for i in ids} - {
        None
    }
    records = []
    for slug in sorted(slugs):
        node = next(n for n in data["nodes"] if n["clic_slug"] == slug)
        records.append(
            activity(
                "Renamed support: " + slug,
                unit=node["identity"][3],
                **{"clic provenance": {"slug": slug}},
            )
        )
    sources = [
        (Path(name), row["source_versions"][0]) for name, row in data["files"].items()
    ]
    bind_activity_roles(records, sources, family="uvek")
    return records, support


def test_support_selection_preserves_reviewed_slugs_without_name_inheritance():
    records, expected = published_support()
    filters = module.fetch_mapping()
    unbound = deepcopy(records[0])
    unbound["clic provenance"]["role_bindings"] = []
    unbound["name"] = "carbon dioxide compression, transport and storage"
    database = [*records, unbound]
    before = deepcopy(database)
    selected = InventorySet(database, family="uvek").generate_sets_from_filters(filters)
    nodes = registry()["hybrid_sources"]["nodes"]
    for role, ids in expected.items():
        assert {a["clic provenance"]["slug"] for a in selected[role]} == {
            nodes[i]["clic_slug"] for i in ids if nodes[i]["clic_slug"]
        }
    assert not any(a is unbound for group in selected.values() for a in group)
    assert database == before


def test_support_registry_replays_the_existing_filters_and_keeps_units():
    from dev.build_clic_hybrid_registry import cdr_support_roles

    nodes = registry()["hybrid_sources"]["nodes"]
    roles = cdr_support_roles(nodes, module.fetch_mapping())
    assert roles == registry()["hybrid_sources"]["roles"]["cdr_activities.yaml"]
    assert (
        roles["land use"] == []
    )  # The historical filter itself has no retained template.
    for role, indices in roles.items():
        definition = module.fetch_mapping()[role]
        if definition.get("clic_slugs"):
            assert definition["clic_role_key"] == "cdr_activities.yaml:" + role
            assert set(definition["clic_slugs"]) == {
                nodes[i]["clic_slug"] for i in indices if nodes[i]["clic_slug"]
            }


@pytest.mark.parametrize("family", ["ecoinvent", "uvek"])
def test_dispatch_forwards_axes_before_inventory_preparation(monkeypatch, family):
    from premise import NewDatabase
    from premise.database import background

    owner = object.__new__(NewDatabase)
    owner.source_family = family
    owner.version = "2026" if family == "uvek" else "3.12"
    owner.biosphere_version = "3.10" if family == "uvek" else "3.12"
    owner.system_model = "cutoff"
    owner.use_absolute_efficiency = False
    owner.gains_scenario = "CLE"
    owner.prepare_inventories = Mock(
        side_effect=RuntimeError("stop before preparation")
    )
    monkeypatch.setattr(background, "validate_uvek_scope", lambda sectors: [sectors])
    with pytest.raises(RuntimeError, match="stop before preparation"):
        owner.update("cdr")
    expected = (owner.version, "cutoff") + (
        (family, "3.10") if family == "uvek" else ()
    )
    assert owner.sector_update_methods["cdr"]["args"] == expected


def test_no_cdr_iam_data_keeps_normal_skip(monkeypatch):
    constructor = Mock()
    monkeypatch.setattr(module, "CarbonDioxideRemoval", constructor)
    scenario = {"iam data": SimpleNamespace(cdr_technology_mix=None)}
    assert module._update_cdr(scenario, "2026", "cutoff", "uvek", "3.10") is scenario
    constructor.assert_not_called()
