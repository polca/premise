import pytest

from premise.metals import (
    Metals,
    PostAllocationCorrectionError,
    correct_metal_resource_exchanges,
)


def biosphere_resource(name, amount):
    return {
        "name": name,
        "amount": amount,
        "unit": "kilogram",
        "type": "biosphere",
        "categories": ("natural resource", "in ground"),
    }


def test_post_allocation_correction_sets_target_to_one_and_others_to_zero():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.42),
            biosphere_resource("Gold", 0.001),
            {
                "name": "Carbon dioxide, fossil",
                "amount": 2.0,
                "unit": "kilogram",
                "type": "biosphere",
                "categories": ("air", "urban air close to ground"),
            },
        ],
    }

    assert correct_metal_resource_exchanges(dataset) is True

    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Copper"] == 1.0
    assert amounts["Gold"] == 0.0
    assert amounts["Carbon dioxide, fossil"] == 2.0


def test_post_allocation_correction_uses_metal_content_for_hydroxides():
    dataset = {
        "name": "beryllium hydroxide production",
        "reference product": "beryllium hydroxide",
        "location": "US",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Beryllium", 1.0),
            biosphere_resource("Gold", 0.001),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Beryllium"] == pytest.approx(0.20945869667541112)
    assert amounts["Gold"] == 0.0


def test_post_allocation_correction_handles_pure_metal_production_coproducts():
    dataset = {
        "name": "cobalt production",
        "reference product": "nickel, class 1",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.067),
            biosphere_resource("Nickel", 0.772),
            biosphere_resource("Sulfur", 10.5),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Cobalt"] == 0.0
    assert amounts["Nickel"] == 1.0
    assert amounts["Sulfur"] == 0.0


def test_post_allocation_correction_skips_unresolved_non_extraction_compounds():
    dataset = {
        "name": "cobalt production",
        "reference product": "nickel sulfate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.02),
            biosphere_resource("Nickel", 0.2),
            biosphere_resource("Sulfur", 2.7),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.02, 0.2, 2.7]


def test_post_allocation_correction_skips_market_datasets():
    dataset = {
        "name": "market for copper concentrate, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.42),
            biosphere_resource("Gold", 0.001),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.42, 0.001]


def test_post_allocation_correction_raises_when_strict_target_is_missing():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gold", 0.001)],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Could not find"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_skips_non_strict_missing_target():
    dataset = {
        "name": "gravel and sand quarry operation",
        "reference product": "sand",
        "location": "CH",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gravel", 1.0)],
    }

    assert correct_metal_resource_exchanges(dataset, strict=False) is False
    assert dataset["exchanges"][0]["amount"] == 1.0


def test_post_allocation_correction_skips_out_of_scope_resource_matches():
    dataset = {
        "name": "hard coal mine operation and hard coal preparation",
        "reference product": "hard coal",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Coal, hard", 1.32)],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert dataset["exchanges"][0]["amount"] == 1.32


def test_post_allocation_correction_does_not_treat_forest_as_ore_context():
    dataset = {
        "name": "clear-cutting, primary forest to arable land, annual crop",
        "reference product": (
            "land tenure, arable land, measured as carbon net primary "
            "productivity, annual crop"
        ),
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource(
                "Carbon, organic, decrease in soil or biomass stock", 4.2
            )
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is False
    assert dataset["exchanges"][0]["amount"] == 4.2


def test_post_allocation_correction_raises_on_ambiguous_target():
    dataset = {
        "name": "zinc mine operation",
        "reference product": "bulk lead-zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Lead", 0.5),
            biosphere_resource("Zinc", 0.5),
        ],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Ambiguous"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_skips_non_strict_ambiguous_target():
    dataset = {
        "name": "sodium percarbonate production, powder",
        "reference product": "sodium percarbonate, powder",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Sodium", 0.1),
            biosphere_resource("Sodium chloride", 0.2),
            biosphere_resource("Sodium nitrate", 0.3),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=False) is False
    assert [exc["amount"] for exc in dataset["exchanges"]] == [0.1, 0.2, 0.3]


def test_post_allocation_correction_raises_on_duplicate_target_flow():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.2),
            biosphere_resource("Copper", 0.3),
        ],
    }

    with pytest.raises(PostAllocationCorrectionError, match="Duplicate"):
        correct_metal_resource_exchanges(dataset, strict=True)


def test_post_allocation_correction_uses_mineral_aliases():
    dataset = {
        "name": "molybdenite mine operation",
        "reference product": "molybdenite",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Molybdenum", 0.3),
            biosphere_resource("Copper", 0.1),
        ],
    }

    assert correct_metal_resource_exchanges(dataset) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Molybdenum"] == pytest.approx(0.54545591456969)
    assert amounts["Copper"] == 0.0


def test_post_allocation_correction_uses_resource_flow_aliases():
    dataset = {
        "name": "graphite production",
        "reference product": "graphite",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource(
                "Metamorphous rock, graphite containing, in ground", 1.7
            ),
            biosphere_resource("Gold", 0.1),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Metamorphous rock, graphite containing, in ground"] == 1.0
    assert amounts["Gold"] == 0.0


def test_post_allocation_correction_uses_zircon_alias():
    dataset = {
        "name": "heavy mineral sand quarry operation and titania slag production",
        "reference product": "zircon",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Zirconium", 0.2),
            biosphere_resource("Titanium", 0.8),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Zirconium"] == 0.2
    assert amounts["Titanium"] == 0.0


def test_post_allocation_correction_uses_stibnite_alias():
    dataset = {
        "name": "stibnite mine operation and beneficiation",
        "reference product": "stibnite concentrate",
        "location": "CA-QC",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Antimony", 0.4),
            biosphere_resource("Gangue", 0.6),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Antimony"] == pytest.approx(0.24943856834512806)
    assert amounts["Gangue"] == 0.0


def test_post_allocation_correction_uses_phosphate_alias():
    dataset = {
        "name": "phosphate rock beneficiation",
        "reference product": "phosphate rock, beneficiated",
        "location": "US",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Phosphorus", 0.2),
            biosphere_resource("Fluorine", 0.8),
        ],
    }

    assert correct_metal_resource_exchanges(dataset, strict=True) is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Phosphorus"] == 0.2
    assert amounts["Fluorine"] == 0.0


def test_post_allocation_correction_clears_downstream_attributed_carriers():
    carrier_datasets = [
        {
            "name": "lead mine operation",
            "reference product": "lead concentrate",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Lead", 0.1),
                biosphere_resource("Zinc", 0.2),
            ],
        },
        {
            "name": "zinc mine operation",
            "reference product": "zinc concentrate",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Zinc", 0.5),
                biosphere_resource("Lead", 0.1),
            ],
        },
        {
            "name": "smelting of copper concentrate, sulfide ore",
            "reference product": "copper, anode",
            "location": "RoW",
            "unit": "kilogram",
            "exchanges": [
                biosphere_resource("Copper", 0.9),
                biosphere_resource("Gold", 0.001),
            ],
        },
    ]

    for dataset in carrier_datasets:
        corrected = correct_metal_resource_exchanges(dataset, strict=True)

        assert corrected is True
        assert all(
            exc["amount"] == 0.0
            for exc in dataset["exchanges"]
            if exc["type"] == "biosphere"
        )


def test_post_allocation_correction_clears_copper_cobalt_ore_carrier():
    dataset = {
        "name": "copper-cobalt mining, industrial, economic allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cobalt", 0.0047),
            biosphere_resource("Copper", 0.024),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Cobalt"] == 0.0
    assert amounts["Copper"] == 0.0


def test_post_allocation_correction_preserves_unresolved_concentrate_content():
    dataset = {
        "name": "copper mine operation and beneficiation, sulfide ore",
        "reference product": "copper concentrate, sulfide ore",
        "location": "CL",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.271842),
            biosphere_resource("Gold", 0.000003),
            biosphere_resource("Gangue", 30.58),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    amounts = {exc["name"]: exc["amount"] for exc in dataset["exchanges"]}
    assert amounts["Copper"] == 0.271842
    assert amounts["Gold"] == 0.0
    assert amounts["Gangue"] == 0.0


def test_post_allocation_correction_clears_generic_pgm_carrier():
    dataset = {
        "name": "platinum group metal, mine and concentration operations",
        "reference product": "platinum group metal concentrate",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Copper", 0.038),
            biosphere_resource("Platinum", 0.000253),
            biosphere_resource("Rhodium", 0.000008),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is True
    assert {exc["name"]: exc["amount"] for exc in dataset["exchanges"]} == {
        "Copper": 0.0,
        "Platinum": 0.0,
        "Rhodium": 0.0,
    }


def test_post_allocation_correction_clears_non_extraction_carriers():
    dataset = {
        "name": "sodium chloride production, powder",
        "reference product": "sodium chloride, powder",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Sodium chloride", 1.0),
            biosphere_resource("Sodium sulfate", 0.02),
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=False)

    assert corrected is True
    assert {exc["name"]: exc["amount"] for exc in dataset["exchanges"]} == {
        "Sodium chloride": 0.0,
        "Sodium sulfate": 0.0,
    }


def test_post_allocation_correction_adds_missing_target_for_mapped_pure_metal():
    dataset = {
        "name": "indium production",
        "reference product": "indium",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for indium rich leaching residues, from zinc production",
                "product": "indium rich leaching residues, from zinc production",
                "amount": 10000,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(
        dataset, strict=True, add_missing_target_resource=True
    )

    assert corrected is True
    target_exchange = dataset["exchanges"][-1]
    assert target_exchange == {
        "amount": 1.0,
        "type": "biosphere",
        "name": "Indium",
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
        "uncertainty type": 0,
    }


@pytest.mark.parametrize(
    ("reference_product", "expected_flow"),
    [
        ("gallium, high-grade", "Gallium"),
        ("hafnium sponge", "Hafnium"),
        ("tellurium, semiconductor-grade", "Tellurium"),
        ("titanium sponge", "Titanium"),
        ("aluminium, primary, liquid", "Aluminium"),
        ("nickel, class 1", "Nickel"),
        ("7N Gallium", "Gallium"),
    ],
)
def test_post_allocation_correction_adds_missing_target_for_pure_qualified_metals(
    reference_product, expected_flow
):
    dataset = {
        "name": f"{reference_product} production",
        "reference product": reference_product,
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for upstream intermediate",
                "product": "upstream intermediate",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(
        dataset, strict=True, add_missing_target_resource=True
    )

    assert corrected is True
    assert dataset["exchanges"][-1] == {
        "amount": 1.0,
        "type": "biosphere",
        "name": expected_flow,
        "unit": "kilogram",
        "categories": ("natural resource", "in ground"),
        "uncertainty type": 0,
    }


def test_post_allocation_correction_does_not_add_missing_target_by_default():
    dataset = {
        "name": "tin production",
        "reference product": "tin",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for tin concentrate",
                "product": "tin concentrate",
                "amount": 2.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }

    corrected = correct_metal_resource_exchanges(dataset, strict=True)

    assert corrected is False
    assert all(exc["type"] != "biosphere" for exc in dataset["exchanges"])


def test_missing_target_detection_ignores_downstream_attributed_carriers():
    pgm_concentrate = {
        "name": "market for platinum group metal concentrate",
        "reference product": "platinum group metal concentrate",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Platinum", 0.000103)],
    }
    platinum_refinery = {
        "name": "platinum group metal, extraction and refinery operations",
        "reference product": "platinum",
        "location": "ZA",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for platinum group metal concentrate",
                "product": "platinum group metal concentrate",
                "location": "World",
                "amount": 1000,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    tin_concentrate = {
        "name": "market for tin concentrate",
        "reference product": "tin concentrate",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Tin", 0.48)],
    }
    tin_production = {
        "name": "tin production",
        "reference product": "tin",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for tin concentrate",
                "product": "tin concentrate",
                "location": "World",
                "amount": 1 / 0.48379101906039534,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    zinc_concentrate = {
        "name": "market for zinc concentrate",
        "reference product": "zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "zinc mine operation",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 1.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    zinc_mine = {
        "name": "zinc mine operation",
        "reference product": "zinc concentrate",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            biosphere_resource("Cadmium", 0.0001),
            biosphere_resource("Zinc", 0.5),
        ],
    }
    cadmium_production = {
        "name": "primary zinc production from concentrate",
        "reference product": "cadmium",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for zinc concentrate",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 10.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    silver_production = {
        "name": "primary zinc production from concentrate",
        "reference product": "silver",
        "location": "MX",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for zinc concentrate",
                "product": "zinc concentrate",
                "location": "GLO",
                "amount": 300.0,
                "unit": "kilogram",
                "type": "technosphere",
            },
            {
                "name": "market for silver",
                "product": "silver",
                "location": "World",
                "amount": 0.0001,
                "unit": "kilogram",
                "type": "technosphere",
            },
        ],
    }
    silver_market = {
        "name": "market for silver",
        "reference product": "silver",
        "location": "World",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Silver", 1.0)],
    }
    gold_unrefined_market = {
        "name": "market for gold, unrefined",
        "reference product": "gold, unrefined",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Gold", 0.8)],
    }
    gold_refinery = {
        "name": "gold refinery operation",
        "reference product": "gold",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for gold, unrefined",
                "product": "gold, unrefined",
                "location": "GLO",
                "amount": 1.2,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    catalyst_treatment = {
        "name": "treatment of automobile catalyst",
        "reference product": "palladium",
        "location": "RoW",
        "unit": "kilogram",
        "exchanges": [],
    }
    lithium_brine = {
        "name": "market for lithium brine, 6.7 % Li",
        "reference product": "lithium brine, 6.7 % Li",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Lithium", 0.0667)],
    }
    lithium_production = {
        "name": "lithium production, lithium chloride electrolysis",
        "reference product": "lithium",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "market for lithium brine, 6.7 % Li",
                "product": "lithium brine, 6.7 % Li",
                "location": "GLO",
                "amount": 8.3,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    copper_cobalt_ore = {
        "name": "copper-cobalt mining, industrial, mass allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Cobalt", 0.0047)],
    }
    cobalt_production = {
        "name": "cobalt metal production, from copper mining, via electrolysis",
        "reference product": "cobalt",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "copper-cobalt mining, industrial, mass allocation",
                "product": "copper-cobalt ore",
                "location": "CD",
                "amount": 200.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    economic_cobalt_ore = {
        "name": "copper-cobalt mining, industrial, economic allocation",
        "reference product": "copper-cobalt ore",
        "location": "CD",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Cobalt", 0.0047)],
    }
    economic_cobalt_production = {
        "name": "cobalt metal production, from copper mining, via electrolysis",
        "reference product": "cobalt",
        "location": "CN",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "copper-cobalt mining, industrial, economic allocation",
                "product": "copper-cobalt ore",
                "location": "CD",
                "amount": 200.0,
                "unit": "kilogram",
                "type": "technosphere",
            }
        ],
    }
    metals = object.__new__(Metals)
    metals.database = [
        pgm_concentrate,
        platinum_refinery,
        tin_concentrate,
        tin_production,
        zinc_concentrate,
        zinc_mine,
        cadmium_production,
        silver_production,
        silver_market,
        gold_unrefined_market,
        gold_refinery,
        catalyst_treatment,
        lithium_brine,
        lithium_production,
        copper_cobalt_ore,
        cobalt_production,
        economic_cobalt_ore,
        economic_cobalt_production,
    ]

    missing_target_ids = metals.get_missing_target_resource_dataset_ids(
        {
            id(platinum_refinery),
            id(tin_production),
            id(cadmium_production),
            id(silver_production),
            id(gold_refinery),
            id(catalyst_treatment),
            id(lithium_production),
            id(cobalt_production),
            id(economic_cobalt_production),
        }
    )

    assert id(platinum_refinery) in missing_target_ids
    assert id(tin_production) not in missing_target_ids
    assert id(cadmium_production) in missing_target_ids
    assert id(silver_production) in missing_target_ids
    assert id(gold_refinery) not in missing_target_ids
    assert id(catalyst_treatment) in missing_target_ids
    assert id(lithium_production) in missing_target_ids
    assert id(cobalt_production) in missing_target_ids
    assert id(economic_cobalt_production) in missing_target_ids
