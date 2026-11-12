"""Native petrol must enter shared regionalization without blended substitutes."""

import pytest

from premise.fuels.config import LIQUID_FUEL_SOURCES
from premise.fuels.utils import fetch_mapping
from premise.inventory.activity_maps import InventorySet
from premise.inventory.inventory_store import IndexedInventoryList


@pytest.mark.parametrize("container", [list, IndexedInventoryList])
@pytest.mark.parametrize(
    "fuel,role,key",
    [
        ("Petrol", "gasoline", "uvek refinery petrol"),
        ("Diesel", "diesel", "uvek refinery diesel"),
        ("Kerosene", "kerosene", "uvek refinery kerosene"),
    ],
)
def test_native_petrol_refinery_matches_both_fuel_and_regionalization_maps(
    container, fuel, role, key
):
    def activity(name=None, **fields):
        name = name or f"{fuel}, at refinery"
        return {
            "name": name,
            "reference product": name,
            "unit": "kilogram",
            "location": "RER",
            **fields,
        }

    suppliers = [activity(), activity(location="CH")]
    excluded = [
        activity("Petrol, at regional storage"),
        activity("Petrol, two-stroke blend, at regional storage"),
        activity(
            "Petrol, 4% vol. ETBE additive, with ethanol from biomass, at refinery"
        ),
        activity("Pyrolysis gasoline, production mix, at plant"),
        activity(unit="cubic meter"),
        activity(**{"reference product": "heat"}),
    ]
    inv = InventorySet(
        container(suppliers + excluded), family="uvek", version="2026", model="remind"
    )
    assert inv.generate_fuel_map(model="remind")[role] == suppliers
    copies = inv.generate_sets_from_filters(fetch_mapping(LIQUID_FUEL_SOURCES))
    assert copies[key] == suppliers
    assert not inv.generate_sets_from_filters(
        fetch_mapping(LIQUID_FUEL_SOURCES), database=[activity(unit="megajoule")]
    )[key]
    assert not InventorySet(container(suppliers), model="remind").generate_fuel_map(
        model="remind"
    )[role]


def test_native_wood_ethanol_requires_dry_basis_distillery_product_and_enters_wood_stage():
    from premise.fuels.config import BIOFUEL_SOURCES

    name = "Ethanol, 95% in H2O, from wood, at distillery"
    final = {
        "name": name,
        "reference product": name,
        "unit": "kilogram",
        "location": "SE",
    }
    excluded = [
        {**final, "name": "Ethanol, 99.7% in H2O, from wood, at distillation"},
        {**final, "name": "Ethanol, 99.7% in H2O, from biomass, at distillation"},
        {**final, "name": "Ethanol from ethylene, at plant"},
        {**final, "unit": "cubic meter"},
    ]
    inv = InventorySet(
        [final, *excluded], family="uvek", version="2026", model="remind"
    )
    assert inv.generate_fuel_map(model="remind")["bioethanol, from wood"] == [final]
    prefixes = fetch_mapping(BIOFUEL_SOURCES)["wood"]["forest residues"]
    assert any(name.startswith(prefix) for prefix in prefixes)


def test_native_soybean_ester_binds_oil_crop_role_and_shared_feedstock_stage():
    from premise.fuels.config import BIOFUEL_SOURCES

    name = "Soybean methyl ester, at esterification plant"
    suppliers = [
        {"name": name, "reference product": name, "unit": "kilogram", "location": loc}
        for loc in ["US", "BR"]
    ]
    excluded = [
        {**suppliers[0], "name": "Soybean oil, at oil mill"},
        {
            **suppliers[0],
            "name": "xx Glycerine, from soybean oil, at esterification plant",
        },
        {**suppliers[0], "unit": "megajoule"},
    ]
    inv = InventorySet(
        [*suppliers, *excluded], family="uvek", version="2026", model="remind"
    )
    assert (
        inv.generate_fuel_map(model="remind")["biodiesel, from oil crops"] == suppliers
    )
    assert name in fetch_mapping(BIOFUEL_SOURCES)["oil"]["soybean"]


@pytest.mark.parametrize("container", [list, IndexedInventoryList])
def test_gas_supply_uses_only_reviewed_carrier_identity_triples(
    container,
):
    name = "market group for natural gas, high pressure, reviewed native supply [BrightPath UVEK conversion 03a1d748] [CLIC brightpath-uvek-import-03a1d7481f35ec1a56c18cd5]"
    helper = {
        "name": name,
        "reference product": "natural gas, high pressure",
        "unit": "cubic meter",
        "location": "RoW",
        "clic provenance": {"role": "generated helper"},
        "brightpath_conversion": {"recipe_id": "fuels-312-8474b2c5b3efba4ba611"},
    }
    native_name = "Natural gas, high pressure, at consumer"
    native = {
        "name": native_name,
        "reference product": native_name,
        "unit": "megajoule",
        "location": "CH",
    }
    excluded = [
        {**helper, "unit": "megajoule"},
        {**native, "unit": "cubic meter"},
        {**helper, "reference product": native_name},
        {**helper, "name": name + " unreviewed"},
        {
            **helper,
            "clic provenance": {
                "slug": "unreviewed-published-process",
                "role_bindings": [],
            },
        },
    ]
    inv = InventorySet(
        container([helper, native, *excluded]),
        family="uvek",
        model="remind",
        version="2026",
    )
    classification = inv.generate_fuel_map(model="remind")["natural gas"]
    assert len(classification) == 2
    assert helper in classification and native in classification
    assert inv.generate_fuel_map(model="remind", for_markets=True)["natural gas"] == [
        helper
    ]
    # Purpose selection cannot mutate the cached default aliases used by heat.
    assert inv.generate_fuel_map()["natural gas"] == classification


def test_ecoinvent_supply_selection_retains_original_filters():
    name = "natural gas production"
    source = {
        "name": name,
        "reference product": "natural gas, high pressure",
        "unit": "cubic meter",
        "location": "RoW",
    }
    inv = InventorySet([source], model="remind")
    assert inv.generate_fuel_map(
        model="remind", for_markets=True
    ) == inv.generate_fuel_map(model="remind")
    assert inv.generate_fuel_map(model="remind", for_markets=True)["natural gas"] == [
        source
    ]
