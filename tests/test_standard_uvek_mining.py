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
