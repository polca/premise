"""Select native ordinary steel routes without rolling or specialty suppliers."""

from types import SimpleNamespace

from premise.inventory.activity_maps import InventorySet
from premise.materials.steel import Steel
from premise.core.transformation import BaseTransformation
from test_steel_market_grades import activity

PRIMARY = [
    "Steel, converter, low-alloyed, at plant",
    "Steel, converter, unalloyed, at plant",
]
SECONDARY = [
    "Steel, electric, unalloyed, at plant",
    "Steel, electric, low-alloyed, at plant",
    "Steel, electric, un- and low-alloyed, at plant",
]


def test_native_steel_aliases_exclude_processing_and_specialties():
    excluded = [
        "Steel, low-alloyed, at plant",
        "Steel, electric, chromium steel 18/8, at plant",
        "Steel, converter, chromium steel 18/8, at plant",
        "Steel, electric, alloyed, 42CrMoS4, at plant",
        "Steel, electric, low-alloyed, at plant, best plants (min. values)",
        "Steel, electric, low-alloyed, at plant, worst plants (max. values)",
        "Steel, electric, un- and low-alloyed, for reinforcing steel, import to CH, at plant",
        "Hot rolling, electric steel",
        "Reinforcing steel, electric, at plant",
        "Disposal, slag, unalloyed electr. steel, 0% water, to residual material landfill",
    ]
    database = [activity(name, name) for name in PRIMARY + SECONDARY + excluded]
    wrong_unit = activity(SECONDARY[0], SECONDARY[0])
    wrong_unit["unit"] = "megajoule"
    database.append(wrong_unit)
    mapping = InventorySet(
        database, "2026", "remind", family="uvek"
    ).generate_steel_map()
    assert [ds["name"] for ds in mapping["steel - primary - BF/BOF"]] == PRIMARY
    assert [ds["name"] for ds in mapping["steel - secondary"]] == SECONDARY
    legacy_mapping = InventorySet(database, "3.10", "remind").generate_steel_map()
    assert not any(legacy_mapping.values())


def test_steel_constructor_passes_background_family_to_shared_maps(monkeypatch):
    def initialize(
        self,
        database,
        iam_data,
        model,
        pathway,
        year,
        version,
        system_model,
        cache,
        index,
        *,
        family,
        biosphere_version,
    ):
        self.database = database
        self.model = model
        self.family = family
        self.biosphere_version = biosphere_version

    monkeypatch.setattr(BaseTransformation, "__init__", initialize)
    database = [activity(name, name) for name in PRIMARY + SECONDARY]
    steel = Steel(
        database,
        SimpleNamespace(),
        "remind",
        "SSP2-NPi",
        2050,
        "2026",
        "cutoff",
        family="uvek",
        biosphere_version="3.10",
    )
    assert steel.inv.family == "uvek"
    assert steel.biosphere_version == "3.10"
    assert len(steel.steel_map["steel - secondary"]) == 3


def test_pig_iron_auxiliary_alias_is_exact_and_not_an_iam_steel_technology():
    from premise.inventory.activity_maps import STEEL_TECHS, get_mapping

    name = "Pig iron, at plant"
    native = activity(name, name, "GLO")
    wrong = activity(name, "cast iron", "GLO")
    supply = activity("market for pig iron", "pig iron", "GLO")
    inv = InventorySet([native, wrong, supply], "2026", "remind", family="uvek")
    definition = get_mapping(STEEL_TECHS, "uvek_aliases")["pig iron"]
    assert inv.generate_map({"pig iron": definition})["pig iron"] == [native]
    assert "pig iron" not in inv.generate_steel_map()
    assert "pig iron" not in get_mapping(STEEL_TECHS, "ecoinvent_aliases")


def test_all_reviewed_retained_steel_routes_have_distinct_native_target_bindings():
    from premise.inventory.activity_maps import STEEL_TECHS, get_mapping

    definitions = get_mapping(STEEL_TECHS, "uvek_aliases")
    roles = [
        r
        for r in definitions
        if r.startswith("steel -")
        and r not in {"steel - primary - BF/BOF", "steel - secondary"}
    ]
    records = []
    for role in roles:
        for name in definitions[role]["fltr"]["name"]:
            product = (
                "steel, low-alloyed"
                if name.endswith("low-alloyed")
                else "steel, unalloyed"
            )
            record = activity(name, product, "GLO")
            record["inventory provenance"] = {"source": "retained workbook"}
            records.append(record)
    assert len(roles) == 7 and len(records) == 14
    mapping = InventorySet(records, "2026", "remind", family="uvek").generate_map(
        {r: definitions[r] for r in roles}
    )
    for role in roles:
        assert {a["name"] for a in mapping[role]} == set(
            definitions[role]["fltr"]["name"]
        )
        assert len(mapping[role]) == 2
    assert not any(
        a in mapping["steel - primary - DRI"]
        for a in mapping["steel - primary - DRI CCS"]
    )
