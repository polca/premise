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
