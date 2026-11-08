"""Offline mapping contracts use native identity metadata, never coefficients."""

import pytest

from premise.inventory.activity_maps import (
    FUELS_TECHS,
    POWERPLANT_TECHS,
    InventorySet,
    act_fltr,
    get_mapping,
)
from premise.inventory.clic_requirements import role_slugs
from premise.inventory.inventory_store import IndexedInventoryList


def native(name, unit="kilowatt hour", **overrides):
    return {
        "name": name,
        "reference product": name,
        "unit": unit,
        "location": "CH",
        **overrides,
    }


@pytest.mark.parametrize("container", [list, IndexedInventoryList])
def test_native_uvek_generation_excludes_other_families_and_output_boundaries(
    container,
):
    coal = native("Electricity, hard coal, at power plant")
    excluded = [
        native("electricity production, hard coal"),
        native("Electricity, hard coal, at power plant, merged", "megajoule"),
        native("Electricity, hard coal, at coal mine power plant"),
        native("Electricity, hard coal, at power plant", "megajoule"),
        native(
            "Electricity, hard coal, at power plant", **{"reference product": "heat"}
        ),
        native("Electricity, high voltage, production from hard coal, at grid"),
        native("Electricity, hard coal, at power plant, unreviewed variant"),
    ]
    inv = InventorySet(container([coal, *excluded]), family="uvek", version="2026")

    assert inv.generate_powerplant_map()["Coal PC"] == [coal]


def test_default_ecoinvent_does_not_select_native_uvek():
    ecoinvent = native("electricity production, hard coal")
    uvek = native("Electricity, hard coal, at power plant")

    assert InventorySet([ecoinvent, uvek]).generate_powerplant_map()["Coal PC"] == [
        ecoinvent
    ]


@pytest.mark.parametrize("model", ["remind", "remind-eu"])
def test_aggregate_iam_pv_role_includes_complete_native_pv_inventory(model):
    small = native("Electricity, PV, at 3kWp slanted-roof, single-Si, panel, mounted")
    large = native("Electricity, PV, at 3.5 MWp open ground, multi-Si")
    complete = native("Electricity, production mix photovoltaic, at plant")
    cli = native(
        complete["name"],
        **{"clic provenance": {"slug": "unreviewed", "role_bindings": []}},
    )
    mapping = InventorySet(
        [small, large, complete, cli], family="uvek", model=model
    ).generate_powerplant_map()
    assert mapping["Solar PV Centralized"] == [large, complete]
    assert "Solar PV Residential" not in mapping
    # Cached YAML definitions are immutable across model-specific lookups.
    separate = InventorySet(
        [small, large, complete], family="uvek", model="image"
    ).generate_powerplant_map()
    assert separate["Solar PV Centralized"] == [large]
    assert separate["Solar PV Residential"] == [small]


def test_native_role_inclusion_does_not_borrow_another_clic_role():
    role = "Solar PV Residential"
    source = native(
        "Published residential PV",
        **{
            "clic provenance": {
                "slug": role_slugs("electricity.yaml", role)[0],
                "role_bindings": [f"electricity.yaml:{role}"],
            }
        },
    )
    result = InventorySet(
        [source], family="uvek", model="remind"
    ).generate_powerplant_map()
    assert result["Solar PV Centralized"] == []


@pytest.mark.parametrize("model", [None, "image", "remind", "message", "tiam-ucl"])
def test_families_share_iam_roles_and_efficiency_metadata(model):
    ecoinvent = InventorySet([], model=model)
    uvek = InventorySet([], model=model, family="uvek", version="2026")

    assert set(uvek.generate_powerplant_map()) == set(
        ecoinvent.generate_powerplant_map()
    )
    assert (
        uvek.generate_powerplant_efficiency_bounds()
        == ecoinvent.generate_powerplant_efficiency_bounds()
    )
    assert uvek.powerplant_max_efficiency == ecoinvent.powerplant_max_efficiency
    assert uvek.powerplant_min_efficiency == ecoinvent.powerplant_min_efficiency


def test_missing_uvek_native_mapping_is_explicit_and_never_uses_eci_name():
    eci_name = "electricity production, deep geothermal"
    inv = InventorySet([native(eci_name)], family="uvek")

    assert inv.generate_powerplant_map()["Geothermal"] == []
    assert "Geothermal" in inv.missing_aliases["electricity.yaml:uvek_aliases"]


@pytest.mark.parametrize(
    "role,name",
    [
        ("Biomass CHP", "Electricity, at cogen ORC 1400kWth, wood, allocation exergy"),
        ("Coal CHP", "Electricity, hard coal, at power plant"),
        ("Coal CHP", "Electricity, lignite, at power plant"),
        ("Gas ST", "Electricity, natural gas, at power plant"),
    ],
)
def test_approved_native_proxies_match_exact_electricity_boundary(role, name):
    generator = native(name)
    alternatives = [
        native(name, "megajoule"),
        native(name, **{"reference product": "heat"}),
        native(name + ", unreviewed variant"),
    ]
    mapping = InventorySet(
        [generator, *alternatives], family="uvek", model="remind"
    ).generate_powerplant_map()
    assert mapping[role] == [generator]
    definition = get_mapping(POWERPLANT_TECHS, "uvek_aliases")[role]
    assert definition["native_reference_amount"] == 1
    assert definition["native_limitations"]


def test_uvek_clic_binding_requires_reviewed_slug_and_explicit_role_tag():
    role = "Solar PV Residential"
    tag = f"electricity.yaml:{role}"
    slug = role_slugs("electricity.yaml", role)[0]
    native_generator = native(
        "Electricity, PV, at 3kWp slanted-roof, single-Si, panel, mounted"
    )
    published = native(
        "Renamed published photovoltaic generator [CLIC]",
        **{"clic provenance": {"slug": slug, "role_bindings": [tag]}},
    )
    wrong_slug = {
        **native_generator,
        "clic provenance": {"slug": "unreviewed-boundary", "role_bindings": [tag]},
    }
    wrong_tag = {
        **native_generator,
        "clic provenance": {"slug": slug, "role_bindings": ["electricity.yaml:Gas CC"]},
    }
    upstream = {
        **native_generator,
        "clic provenance": {"slug": slug, "role_bindings": []},
    }
    unbound = {**native_generator, "clic provenance": {"slug": slug}}
    inv = InventorySet(
        [native_generator, published, wrong_slug, wrong_tag, upstream, unbound],
        family="uvek",
    )

    assert inv.generate_powerplant_map()[role] == [native_generator, published]
    assert get_mapping(POWERPLANT_TECHS, "uvek_aliases")[role]["clic_slugs"] == (
        get_mapping(POWERPLANT_TECHS, "ecoinvent_aliases")[role]["clic_slugs"]
    )


def test_clic_only_role_binds_renamed_inventory_without_native_alias():
    role = "Biomass IGCC CCS"
    slug = role_slugs("electricity.yaml", role)[0]
    published = native(
        "Published biomass CCS generator with revised scientific name",
        **{
            "clic provenance": {
                "slug": slug,
                "role_bindings": [f"electricity.yaml:{role}"],
            }
        },
    )
    inv = InventorySet([published], family="uvek")

    assert inv.generate_powerplant_map()[role] == [published]
    assert role in inv.missing_aliases["electricity.yaml:uvek_aliases"]


def test_csp_complete_proxy_is_bound_only_to_the_approved_family_role():
    slug = "electricity-production-at-110-mw-concentrating-solar-power-plant-with-thermal-energy-storage"
    published = native(
        "Complete renamed CSP inventory",
        **{
            "clic provenance": {
                "slug": slug,
                "role_bindings": ["electricity.yaml:Solar CSP"],
            }
        },
    )
    assert InventorySet(
        [published], family="uvek", model="remind"
    ).generate_powerplant_map()["Solar CSP"] == [published]
    assert (
        InventorySet(
            [published], family="ecoinvent", model="remind"
        ).generate_powerplant_map()["Solar CSP"]
        == []
    )
    definition = get_mapping(POWERPLANT_TECHS, "uvek_aliases")["Solar CSP"]
    assert definition["clic_reference_amount"] == 1
    assert definition["clic_unit"] == "kilowatt hour"
    assert "nine hours" in definition["clic_limitations"][0]


def test_undeclared_family_alias_keeps_role_but_has_no_native_fallback(tmp_path):
    path = tmp_path / "mapping.yaml"
    path.write_text(
        "role:\n  iam_aliases:\n    image: shared IAM role\n"
        "  ecoinvent_aliases:\n    fltr: ecoinvent name\n",
        encoding="utf-8",
    )
    inv = InventorySet([native("ecoinvent name")], family="uvek", model="image")
    filters = inv._get_aliases(path, model="image")

    assert inv.generate_sets_from_filters(filters) == {"role": []}
    assert inv.missing_aliases == {"mapping.yaml:uvek_aliases": ("role",)}
    assert get_mapping(path, "uvek_aliases", model="other") == {}


def test_powerplant_fuel_selection_includes_actual_uvek_combustion_layer():
    coal = native("Hard coal, burned in power plant", "megajoule")
    gas = native(
        "Natural gas, burned in combined cycle plant, best technology", "megajoule"
    )
    excluded = [
        native("market for hard coal", "kilogram"),
        native("market for natural gas, high pressure", "cubic meter"),
        native("Hard coal, burned in power plant, unreviewed variant", "megajoule"),
        native("Heat, hard coal, at industrial furnace 1-10MW", "megajoule"),
        native(
            coal["name"],
            "megajoule",
            **{"clic provenance": {"slug": "unreviewed-fuel", "role_bindings": []}},
        ),
    ]
    inv = InventorySet([coal, gas, *excluded], family="uvek")
    mapping = inv.generate_powerplant_fuels_map()

    assert mapping["Coal PC"] == [coal]
    assert mapping["Gas CC"] == [gas]
    assert mapping["Gas CC CCS"] == [gas]


def test_fuel_specs_match_uvek_products_and_keep_shared_lhv():
    coal = native("Hard coal, at regional storage", "kilogram")
    combustion = native("Hard coal, burned in power plant", "megajoule")
    gas = native("Natural gas, high pressure, at consumer", "megajoule")
    eci_fuel = native("market for hard coal", "kilogram")
    inv = InventorySet([coal, combustion, gas, eci_fuel], family="uvek")
    fuels = inv.generate_fuel_map()

    assert fuels["hard coal"] == [coal, combustion]
    assert fuels["natural gas"] == [gas]
    assert coal["lhv"] == get_mapping(FUELS_TECHS, "lhv")["hard coal"]
    assert combustion["unit"] == "megajoule"


def test_nuclear_fuels_require_reviewed_enrichment_instead_of_generic_lhv_proxy():
    assembly = native(
        "Fuel elements BWR, UO2 4.0% & MOX, at nuclear fuel fabrication plant",
        "kilogram",
    )
    inv = InventorySet([assembly], family="uvek")

    assert (
        inv.generate_fuel_map()["nuclear fuel element, for boiling water reactor"] == []
    )
    assert inv.generate_powerplant_fuels_map()["Nuclear"] == []
    assert (
        "nuclear fuel element, for boiling water reactor"
        in inv.missing_aliases["fuels.yaml:uvek_aliases"]
    )


def test_pv_generators_exclude_aggregate_mix_and_grid_busbar_nodes():
    residential = native(
        "Electricity, PV, at 3kWp slanted-roof, single-Si, panel, mounted"
    )
    centralized = native("Electricity, PV, at 3.5 MWp open ground, multi-Si")
    aggregates = [
        native("Electricity, production mix photovoltaic, at plant"),
        native("Electricity, photovoltaic mix slanted-roof, at plant"),
        native("Electricity, low voltage, production from photovoltaic, at grid"),
        native("Electricity, medium voltage, production from photovoltaic, at grid"),
        native("Electricity, high voltage, production from photovoltaic, at grid"),
    ]
    mapping = InventorySet(
        [residential, centralized, *aggregates], family="uvek"
    ).generate_powerplant_map()

    assert mapping["Solar PV Residential"] == [residential]
    assert mapping["Solar PV Centralized"] == [centralized]
    assert all(
        activity not in values for activity in aggregates for values in mapping.values()
    )


def test_exact_inclusive_filters_preserve_existing_any_mask_semantics():
    wanted = native("generator")
    other_location = {**wanted, "location": "DE"}
    other_name = native("generator with another boundary")
    assert act_fltr(
        [wanted, other_location, other_name],
        fltr={"name": ["generator"], "unit": "kilowatt hour"},
        mask={"location": ["DE", "FR"]},
        exact=True,
    ) == [wanted]
