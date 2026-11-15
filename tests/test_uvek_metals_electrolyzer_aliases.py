"""Native 1 MW stack aliases exclude complete systems and disposal services."""

import copy

import pytest

from premise.inventory.activity_maps import InventorySet
from premise.materials.metals_rules import load_technology_conversions


@pytest.mark.parametrize(
    "role,technology", [("PEMEL", "PEM"), ("AEL", "AEC"), ("HTEL - Yttrium", "SOEC")]
)
def test_one_native_stack_matches_with_the_existing_capacity_basis(role, technology):
    name = f"Electrolyzer production, 1MWe, {technology}, Stack"
    stack = {
        "name": name,
        "reference product": name,
        "location": "RER",
        "unit": "unit",
    }
    graph = [stack]
    for replacement in ["Balance of Plant", "assembly", "treatment"]:
        other = copy.deepcopy(stack)
        other["name"] = name.replace("Stack", replacement)
        graph.append(other)
    wrong_product = copy.deepcopy(stack)
    wrong_product["reference product"] = "complete hydrogen production service"
    wrong_unit = copy.deepcopy(stack)
    wrong_unit["unit"] = "kilowatt hour"
    graph.extend([wrong_product, wrong_unit])
    original = copy.deepcopy(graph)
    mapping = InventorySet(
        graph, "2026", "remind", family="uvek"
    ).generate_metals_activities_map()
    assert mapping[role] == [stack]
    assert all(not values for key, values in mapping.items() if key != role)
    assert graph == original
    (conversion,) = [
        row for row in load_technology_conversions() if row.activity_name == name
    ]
    assert conversion.ecoinvent_unit == "unit"
    assert conversion.intensity_unit == "kg/MW"
    assert conversion.factor == 1


def test_existing_ecoinvent_stack_conversions_are_preserved():
    conversions = {row.activity_name: row for row in load_technology_conversions()}
    for technology in ["PEM", "AEC", "SOEC"]:
        lower = f"electrolyzer production, 1MWe, {technology}, Stack"
        upper = lower[0].upper() + lower[1:]
        source, native = conversions[lower], conversions[upper]
        assert source.ecoinvent_unit == native.ecoinvent_unit == "unit"
        assert source.intensity_unit == native.intensity_unit == "kg/MW"
        assert source.factor == native.factor == 1
