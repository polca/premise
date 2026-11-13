"""Mapped native wind templates use the existing variant-copying workflow."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from premise.electricity import renewables as module
from premise.inventory.activity_maps import get_mapping


def config():
    return get_mapping(
        module.DATA_DIR / "renewables" / "wind_turbines.yaml", "uvek_aliases"
    )["direct drive"]


def owner(family="uvek"):
    result = object.__new__(module.WindTurbine)
    result.family = family
    result.variant_mapping = config() if family == "uvek" else None
    result.write_log = Mock()
    result.database = []
    for i, identity in enumerate(config()["exact_identities"]):
        result.database.append(
            {
                **identity,
                "location": "RER",
                "code": str(i),
                "comment": "Native documentation",
                "exchanges": [
                    {
                        "name": identity["name"],
                        "product": identity["reference product"],
                        "unit": identity["unit"],
                        "type": "production",
                        "amount": 1,
                        "input": ("source", str(i)),
                    },
                    {
                        "name": "Unchanged emission",
                        "type": "biosphere",
                        "amount": -0.75,
                        "uncertainty type": 2,
                        "loc": -0.3,
                        "scale": 0.2,
                        "input": ("bio", "flow"),
                    },
                ],
            }
        )
    return result


def purchase(node, amount=0.125):
    return {
        "type": "technosphere",
        "name": node["name"],
        "product": node["reference product"],
        "unit": node["unit"],
        "location": node["location"],
        "amount": amount,
        "uncertainty type": 2,
        "loc": -2.0,
        "scale": 0.4,
        "input": ("source", node["code"]),
    }


def test_exact_native_variants_preserve_inventory_and_relink_only_owned_components():
    obj = owner()
    generator = next(a for a in obj.database if a["unit"] == "kilowatt hour")
    component = next(a for a in obj.database if a["unit"] == "unit")
    other = {
        "name": "Substation, 200MW onshore wind farm",
        "reference product": "Substation, 200MW onshore wind farm",
        "location": "RER",
        "unit": "unit",
        "code": "shared",
    }
    generator["exchanges"] += [purchase(component), purchase(other, -0.25)]
    misleading = deepcopy(component)
    misleading.update(code="wrong", **{"reference product": "Another product"})
    published = deepcopy(component)
    published.update(code="published", **{"clic provenance": {"slug": "unbound"}})
    obj.database += [misleading, published]
    before = deepcopy(obj.database)
    with pytest.warns(UserWarning, match="preparatory direct-drive variants"):
        obj.create_direct_drive_turbines()
    assert obj.database[: len(before)] == before
    copies = obj.database[len(before) :]
    assert len(copies) == len(config()["exact_identities"])
    assert len({a["code"] for a in obj.database}) == len(obj.database)
    by_name = {a["name"]: a for a in copies}
    for original in before[:-2]:
        variant = by_name[original["name"] + ", direct drive"]
        assert variant["reference product"] == original["reference product"]
        assert list(module.ws.biosphere(variant)) == list(module.ws.biosphere(original))
        assert list(module.ws.production(variant))[0]["amount"] == 1
        assert "input" not in list(module.ws.production(variant))[0]
        assert "Native documentation" in variant["comment"]
        assert config()["limitations"] in variant["comment"]
    edges = list(module.ws.technosphere(by_name[generator["name"] + ", direct drive"]))
    expected = purchase(component)
    expected["name"] += ", direct drive"
    expected.pop("input")
    assert edges == [expected, purchase(other, -0.25)]
    assert component["name"] + ", direct drive" in by_name


@pytest.mark.parametrize("amount", [-1, 0, 2, True])
def test_unreviewed_reference_is_refused_before_any_copy(amount):
    obj = owner()
    obj.database[0]["exchanges"][0]["amount"] = amount
    before = deepcopy(obj.database)
    with pytest.raises(ValueError, match="deterministic"):
        obj.create_direct_drive_turbines()
    assert obj.database == before


def test_missing_component_template_is_refused_before_any_copy():
    obj = owner()
    obj.database.pop()
    before = deepcopy(obj.database)
    with pytest.raises(ValueError, match="Missing native wind variant template"):
        obj.create_direct_drive_turbines()
    assert obj.database == before


def test_original_ecoinvent_selection_and_unit_relinking_remain():
    obj = owner("ecoinvent")
    obj.database = obj.database[:1]
    record = obj.database[0]
    record["name"] = "electricity production, wind, 1-3MW turbine, onshore"
    record["exchanges"] += [
        {
            "type": "technosphere",
            "name": "wind turbine construction",
            "unit": "unit",
            "amount": 1,
        },
        {
            "type": "technosphere",
            "name": "network connection",
            "unit": "unit",
            "amount": 1,
        },
    ]
    before = deepcopy(record)
    obj.create_direct_drive_turbines()
    assert len(obj.database) == 2 and record == before
    copy = obj.database[1]
    assert copy["name"] == before["name"] + ", direct drive"
    assert [e["name"] for e in module.ws.technosphere(copy)] == [
        "wind turbine construction, direct drive",
        "network connection",
    ]
    assert "variant of the ecoinvent dataset" in copy["comment"]


@pytest.mark.parametrize("family,bio", [("ecoinvent", None), ("uvek", "3.10")])
def test_wrapper_forwards_background_to_shared_constructor(monkeypatch, family, bio):
    obj = SimpleNamespace(
        database=[], cache={}, index={}, create_direct_drive_turbines=Mock()
    )
    constructor = Mock(return_value=obj)
    monkeypatch.setattr(module, "WindTurbine", constructor)
    monkeypatch.setattr(module, "get_scenario_inventory", lambda s: [])
    monkeypatch.setattr(module, "replace_scenario_inventory", Mock())
    scenario = {
        "iam data": SimpleNamespace(),
        "model": "remind",
        "pathway": "test",
        "year": 2050,
    }
    assert (
        module._update_wind_turbines(scenario, "2026", "cutoff", family, bio)
        is scenario
    )
    assert constructor.call_args.kwargs["family"] == family
    assert constructor.call_args.kwargs["biosphere_version"] == bio
    obj.create_direct_drive_turbines.assert_called_once_with()


@pytest.mark.parametrize(
    "reference_patch",
    [
        {"uncertainty type": 2, "scale": 0.1},
        {"formula": "reference_parameter"},
        {"unit": "kilogram"},
    ],
)
def test_uncertain_formula_or_wrong_unit_reference_is_refused(reference_patch):
    obj = owner()
    obj.database[0]["exchanges"][0].update(reference_patch)
    before = deepcopy(obj.database)
    with pytest.raises(ValueError, match="deterministic"):
        obj.create_direct_drive_turbines()
    assert obj.database == before


def test_constructor_uses_explicit_family_for_base_and_inventory_filters(monkeypatch):
    calls = []

    def base(obj, *args, **kwargs):
        calls.append(kwargs)
        obj.family = kwargs["family"]

    monkeypatch.setattr(module.BaseTransformation, "__init__", base)
    obj = module.WindTurbine(
        owner().database,
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
    assert obj.variant_mapping == config()
    assert len(obj.powerplant_map["Wind Onshore"]) == 4
    assert len(obj.powerplant_map["Wind Offshore"]) == 3
