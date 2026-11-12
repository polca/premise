"""Keep complete distinct steel products outside the ordinary IAM mix."""

from copy import deepcopy

from premise.inventory.activity_maps import InventorySet, STEEL_TECHS, get_mapping
from premise.materials.steel import Steel
from test_steel_market_grades import activity


def native_other_producers():
    definitions = {
        role: definition
        for role, definition in get_mapping(STEEL_TECHS, "uvek_aliases").items()
        if definition.get("regionalize_as_other")
    }
    records = []
    for definition in definitions.values():
        name = definition["fltr"]["name"]
        locations = (
            ["DE", "IT", "PL", "FR"]
            if "import" in name
            else ["CH", "RER"] if name == "Reinforcing steel, at plant" else ["RER"]
        )
        for location in locations:
            record = activity(name, name, location)
            record["exchanges"].append(
                {
                    "name": "retained processing",
                    "product": "processing",
                    "unit": "kilogram",
                    "type": "technosphere",
                    "location": location,
                    "amount": -0.2,
                    "uncertainty type": 3,
                    "loc": -0.2,
                    "scale": 0.02,
                }
            )
            records.append(record)
    return definitions, records


def test_exact_grade_pairs_cannot_match_other_products_units_or_coproducts():
    definitions, records = native_other_producers()
    assert len(definitions) == 14 and len(records) == 21
    names = [d["fltr"]["name"] for d in definitions.values()]
    crossed = activity(names[0], names[1], "RER")
    wrong_unit = activity(names[0], names[0], "RER")
    wrong_unit["unit"] = "megajoule"
    excluded = [
        crossed,
        wrong_unit,
        activity(
            "Steel, pyrometallurgical processing Li-ion batteries, at plant",
            "Steel, pyrometallurgical processing Li-ion batteries, at plant",
        ),
        activity("Steel, low-alloyed, at plant", "Steel, low-alloyed, at plant"),
        activity(
            "Steel, electric, low-alloyed, at plant",
            "Steel, electric, low-alloyed, at plant",
        ),
    ]
    published = deepcopy(records[0])
    published["clic provenance"] = {"slug": "unreviewed-published-record"}
    inv = InventorySet(
        [*records, *excluded, published], "2026", "remind", family="uvek"
    )
    selected = inv.generate_map(definitions)
    assert {id(a) for group in selected.values() for a in group} == {
        id(a) for a in records
    }
    ordinary = inv.generate_steel_map()
    assert not any(
        id(a) in {id(r) for r in records} for group in ordinary.values() for a in group
    )
    assert not any(
        d.get("regionalize_as_other")
        for d in get_mapping(STEEL_TECHS, "ecoinvent_aliases").values()
    )


def test_other_producers_use_existing_shared_regionalisation_without_average_efficiency():
    definitions, records = native_other_producers()
    obj = Steel.__new__(Steel)
    ordinary = activity(
        "steel production, blast furnace-basic oxygen furnace, low-alloyed",
        "steel, low-alloyed",
    )
    legacy_other = activity("steel production, other grade", "steel, special")
    obj.database = [*records, ordinary, legacy_other]
    obj.steel_map = {"steel - primary - BF/BOF": [ordinary]}
    obj.iam_data = type("IAM", (), {"production_volumes": None})()
    obj.other_steel_map = InventorySet(
        records, "2026", "remind", family="uvek"
    ).generate_map(definitions)
    calls = []
    obj.process_and_add_activities = lambda **kwargs: calls.append(kwargs)
    before = deepcopy(obj.database)
    obj.create_steel_production_activities()
    assert len(calls) == 2
    assert calls[0]["mapping"] is obj.steel_map
    assert calls[0]["efficiency_adjustment_fn"] == obj.adjust_process_efficiency
    assert set(calls[1]) == {"mapping"}
    assert {id(a) for a in calls[1]["mapping"]["other"]} == {
        id(a) for a in [*records, legacy_other]
    }
    assert obj.database == before
