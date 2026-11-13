"""Mining receives its explicit background and binds reviewed treatment roles."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from premise.inventory.activity_maps import InventorySet
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_requirements import bind_activity_roles, registry
from premise.materials import mining as module

NATIVE = "Disposal, sulfidic tailings, off-site"
FILE = "tailings_activities.yaml"
ROLE_SLUGS = {
    "sulfidic tailings - backfill": "treatment-of-sulfidic-tailings-generic-backfilling",
    "sulfidic tailings - flocculation": "treatment-of-sulfidic-tailings-flocculation-flotation-process",
    "sulfidic tailings - roasting": "treatment-of-sulfidic-tailings-roasting-and-leaching",
}


def activity(name, product=None, reference=1, **extra):
    return {
        "name": name,
        "reference product": product or name,
        "unit": "kilogram",
        "location": "GLO",
        "exchanges": [{"type": "production", "amount": reference, "unit": "kilogram"}],
        **extra,
    }


def published():
    records = [
        activity(
            f"Renamed published treatment {index}",
            product="sulfidic tailings, generic",
            reference=-1,
            **{"clic provenance": {"slug": slug}},
        )
        for index, slug in enumerate(ROLE_SLUGS.values())
    ]
    bind_activity_roles(
        records, [(Path("lci-sulfidic-tailings.xlsx"), "3.8")], family="uvek"
    )
    return records


def test_native_and_published_roles_remain_disjoint_and_keep_reference_signs():
    impoundment = activity(NATIVE)
    roots = published()
    wrong = [
        activity("Disposal, non-sulfidic tailings, off-site"),
        activity(NATIVE, product="another waste"),
        activity(NATIVE, unit="cubic meter"),
        activity(
            NATIVE, **{"clic provenance": {"slug": "unreviewed", "role_bindings": []}}
        ),
        activity(
            "treatment of sulfidic tailings, generic, backfilling",
            product="sulfidic tailings, generic",
            reference=-1,
        ),
    ]
    database = [impoundment, *roots, *wrong]
    before = deepcopy(database)
    mapping = InventorySet(
        database, family="uvek", version="2026"
    ).generate_mining_waste_map()
    assert mapping["sulfidic tailings - impoundment"] == [impoundment]
    for (role, _), record in zip(ROLE_SLUGS.items(), roots, strict=True):
        assert mapping[role] == [record]
        assert record["clic provenance"]["role_bindings"] == [f"{FILE}:{role}"]
        assert record["exchanges"][0]["amount"] == -1
    assert impoundment["exchanges"][0]["amount"] == 1
    assert database == before


def test_published_role_cannot_change_reference_unit():
    record = activity(
        "Renamed public record",
        **{"clic provenance": {"slug": next(iter(ROLE_SLUGS.values()))}},
    )
    record["unit"] = "cubic meter"
    with pytest.raises(ClicInventoryError, match="reference unit changed"):
        bind_activity_roles(
            [record], [(Path("lci-sulfidic-tailings.xlsx"), "3.8")], family="uvek"
        )


@pytest.mark.parametrize("family,biosphere", [("ecoinvent", None), ("uvek", "3.10")])
def test_update_hands_family_and_biosphere_to_shared_constructor(
    monkeypatch, family, biosphere
):
    database = [activity("unchanged source")]
    scenario = {
        "model": "remind",
        "pathway": "test",
        "iam data": SimpleNamespace(),
        "year": 2050,
    }
    owner = SimpleNamespace(
        database=database,
        cache={},
        index={},
        update_tailings_treatment=Mock(),
        prepare_market_templates=Mock(),
        forward_market_consumers=Mock(),
        relink_datasets=Mock(),
    )
    constructor = Mock(return_value=owner)
    monkeypatch.setattr(module, "Mining", constructor)
    monkeypatch.setattr(module, "get_scenario_inventory", lambda s: database)
    replacement = Mock()
    monkeypatch.setattr(module, "replace_scenario_inventory", replacement)
    if family == "ecoinvent":
        result = module._update_mining(scenario, "3.10", "cutoff")
    else:
        result = module._update_mining(scenario, "2026", "cutoff", family, biosphere)
    assert result is scenario
    assert constructor.call_args.kwargs["family"] == family
    assert constructor.call_args.kwargs["biosphere_version"] == biosphere
    owner.update_tailings_treatment.assert_called_once_with()
    owner.prepare_market_templates.assert_called_once_with()
    owner.forward_market_consumers.assert_called_once_with()
    owner.relink_datasets.assert_called_once_with()
    replacement.assert_called_once_with(scenario, database)


def test_constructor_passes_family_to_base_and_inventory_mapping(monkeypatch):
    calls = []

    def base(owner, *args, **kwargs):
        calls.append(kwargs)
        owner.family = kwargs["family"]

    monkeypatch.setattr(module.BaseTransformation, "__init__", base)
    monkeypatch.setattr(module, "Geomap", lambda model: SimpleNamespace())
    owner = module.Mining(
        [activity(NATIVE), *published()],
        SimpleNamespace(),
        "remind",
        "test",
        2050,
        "2026",
        "cutoff",
        family="uvek",
        biosphere_version="3.10",
    )
    assert calls == [{"family": "uvek", "biosphere_version": "3.10"}]
    assert len(owner.mining_map["sulfidic tailings - impoundment"]) == 1
    assert all(owner.mining_map[role] for role in ROLE_SLUGS)


def test_mining_role_ledger_matches_legacy_identity_filters():
    import yaml
    from dev.build_clic_role_registry import matches

    definitions = yaml.safe_load(module.TAILINGS_WASTE_SHARES.read_text())
    data = registry()["hybrid_sources"]
    expected = {}
    for role, definition in definitions.items():
        expected[role] = [
            node["id"]
            for node in data["nodes"]
            if matches(
                dict(
                    zip(
                        ("name", "reference product", "location", "unit"),
                        node["identity"],
                        strict=True,
                    )
                ),
                definition["ecoinvent_aliases"],
            )
        ]
    assert data["roles"][FILE] == expected
    assert expected["sulfidic tailings - impoundment"] == []
    assert all(len(expected[role]) == 1 for role in ROLE_SLUGS)


def market_owner(family="uvek"):
    """Small inventory exercising the real mixer with deterministic geography."""
    from premise.inventory.inventory_copy import clone_inventory_dataset

    owner = object.__new__(module.Mining)
    owner.family, owner.year, owner.regions = family, 2050, ["EUR", "World"]
    owner.tailings_shares = module.load_tailings_config("remind").sel(
        region=owner.regions
    )
    owner.ecoinvent_to_iam_loc = {"GLO": "World"}
    roots = published()
    native = activity(NATIVE)
    for record, amount in zip([native, *roots], [10, 1, 2, 3], strict=True):
        record["code"] = record["name"]
        record["exchanges"].append(
            {
                "type": "biosphere",
                "amount": amount,
                "uncertainty type": 2,
                "loc": 0.5,
                "scale": 0.2,
            }
        )
    owner.database = [native, *roots]
    owner.mining_map = {
        role: [record] for role, record in zip(ROLE_SLUGS, roots, strict=True)
    }
    owner.mining_map["sulfidic tailings - impoundment"] = [native]
    index = {}

    def key(record, location=None):
        return (
            record["name"],
            record["reference product"],
            location or record["location"],
        )

    owner.add_to_index = lambda record: index.update({key(record): record})
    owner.remove_from_index = lambda record: index.pop(key(record), None)
    owner.is_in_index = lambda record, location=None: key(record, location) in index
    owner.write_log = Mock()

    def proxies(datasets, relink=True, regions=None):
        result = {}
        for region in regions or owner.regions:
            proto = next((a for a in datasets if a["location"] == region), datasets[0])
            record = clone_inventory_dataset(proto)
            record["location"] = region
            record["code"] = f"{record['name']}-{region}"
            record["regionalized"] = True
            for production in module.ws.production(record):
                production["location"] = region
            result[region] = record
        for record in datasets:
            owner.remove_from_index(record)
        return result

    owner.fetch_proxies = proxies
    for record in owner.database:
        owner.add_to_index(record)
    return owner


def test_market_bridge_keeps_one_inventory_and_correct_signed_treatment_demand():
    import numpy as np

    owner = market_owner()
    original = deepcopy(owner.database[0])
    owner.prepare_market_templates()
    owner.update_tailings_treatment()
    owner.forward_market_consumers()
    cfg = module.get_mapping(module.TAILINGS_WASTE_SHARES, "uvek_aliases")[
        "sulfidic tailings - impoundment"
    ]["market_template"]
    index = {
        (a["name"], a["reference product"], a["location"]): i
        for i, a in enumerate(owner.database)
    }
    assert len(index) == len(owner.database)
    matrix = np.zeros((len(index), len(index)))
    inventory = np.zeros(len(index))
    for j, record in enumerate(owner.database):
        for exchange in record["exchanges"]:
            if exchange["type"] == "production":
                matrix[j, j] += exchange["amount"]
            elif exchange["type"] == "biosphere":
                inventory[j] += exchange["amount"]
            else:
                provider = (exchange["name"], exchange["product"], exchange["location"])
                matrix[index[provider], j] -= exchange["amount"]
    role_burdens = {
        "sulfidic tailings - impoundment": 10,
        **dict(zip(ROLE_SLUGS, [1, 2, 3], strict=True)),
    }
    for region in owner.regions:
        shares = owner.tailings_shares.sel(region=region, year=2050)["mean"]
        expected = sum(
            shares.sel(technology=role).item() * value
            for role, value in role_burdens.items()
        )
        for sign in (1, -1):
            demand = np.zeros(len(index))
            demand[index[(NATIVE, NATIVE, region)]] = sign
            assert inventory @ np.linalg.solve(matrix, demand) == pytest.approx(
                sign * expected
            )
        producer = owner.database[
            index[
                (cfg["supplier"]["name"], cfg["supplier"]["reference product"], region)
            ]
        ]
        assert [e for e in producer["exchanges"] if e["type"] == "biosphere"] == [
            e for e in original["exchanges"] if e["type"] == "biosphere"
        ]
        assert list(module.ws.production(producer))[0]["amount"] == -1
        forwarder = owner.database[index[(NATIVE, NATIVE, region)]]
        assert not list(module.ws.biosphere(forwarder))
        assert list(module.ws.production(forwarder))[0]["amount"] == 1
    mapping = InventorySet(owner.database, family="uvek").generate_mining_waste_map()
    assert all(
        a.get("premise mining role") == "impoundment supplier"
        for a in mapping["sulfidic tailings - impoundment"]
    )


@pytest.mark.parametrize("amount", [-1, 0, 2])
def test_market_bridge_refuses_unreviewed_native_reference_before_mutation(amount):
    owner = market_owner()
    owner.database[0]["exchanges"][0]["amount"] = amount
    before = deepcopy(owner.database)
    with pytest.raises(ValueError, match="deterministic"):
        owner.prepare_market_templates()
    assert owner.database == before


def test_default_ecoinvent_does_not_add_native_market_bridges():
    owner = market_owner("ecoinvent")
    before = deepcopy(owner.database)
    owner.prepare_market_templates()
    owner.forward_market_consumers()
    assert owner.database == before and owner.market_template_bindings == []


@pytest.mark.parametrize("role", ["supplier", "market"])
def test_reused_template_cannot_change_reference_sign(role):
    owner = market_owner()
    owner.prepare_market_templates()
    tag = "impoundment supplier" if role == "supplier" else "market template"
    changed = next(a for a in owner.database if a.get("premise mining role") == tag)
    list(module.ws.production(changed))[0]["amount"] = 1
    before = deepcopy(owner.database)
    with pytest.raises(ValueError, match="deterministic -1"):
        owner.prepare_market_templates()
    assert owner.database == before


def test_existing_ecoinvent_negative_waste_market_uses_the_same_mixer():
    owner = market_owner("ecoinvent")
    for record in owner.database:
        list(module.ws.production(record))[0]["amount"] = -1
    market = activity(
        "market for sulfidic tailings, generic",
        product="sulfidic tailings, generic",
        reference=-1,
    )
    owner.database.append(market)
    owner.add_to_index(market)
    owner.prepare_market_templates()
    owner.update_tailings_treatment()
    owner.forward_market_consumers()
    for region in owner.regions:
        candidates = [
            a
            for a in owner.database
            if a["name"] == market["name"] and a["location"] == region
        ]
        assert len(candidates) == 1
        edges = [e for e in candidates[0]["exchanges"] if e["type"] == "technosphere"]
        assert len(edges) == 4
        assert sum(e["amount"] for e in edges) == pytest.approx(-1)
        for role, activities in owner.mining_map.items():
            expected = -owner.tailings_shares.sel(
                region=region, year=2050, technology=role
            )["mean"].item()
            assert (
                next(e for e in edges if e["name"] == activities[0]["name"])["amount"]
                == expected
            )
    assert not any(a.get("premise mining role") for a in owner.database)
