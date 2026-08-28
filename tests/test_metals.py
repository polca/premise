import pytest
import pandas as pd

import premise.metals as metals_module
import premise.validation as validation_module
from premise.filesystem_constants import DATA_DIR
from premise.metals import (
    Metals,
    PostAllocationCorrectionError,
    _load_mining_shares_mapping,
    build_transport_lookup,
    correct_metal_resource_exchanges,
    extract_exact_filter_values,
    extract_reference_products_from_filter,
    is_secondary_metal_supply_exchange,
    matches_filter_query,
)


def test_mining_share_loader_returns_unfiltered_source_values(monkeypatch):
    source = pd.DataFrame(
        {
            "Metal": ["Copper", "Copper"],
            "Year 2020": [0.005, 0.995],
            "Year 2030": [0.004, 0.996],
        }
    )
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: source.copy())
    _load_mining_shares_mapping.cache_clear()
    try:
        loaded = _load_mining_shares_mapping("3.12")
    finally:
        _load_mining_shares_mapping.cache_clear()

    assert loaded.columns.tolist() == ["Metal", "2020", "2030"]
    assert loaded["2020"].tolist() == [0.005, 0.995]
    assert loaded["2030"].tolist() == [0.004, 0.996]


def test_transport_lookup_preserves_last_duplicate_row_and_frame_metadata():
    dataframe = pd.DataFrame(
        {
            "country": ["CH", "CH", "DE"],
            "Metal": ["Copper", "Copper", "Copper"],
            "TransportMode Label": ["Railway", "Road", "Sea"],
        },
        index=[10, 20, 30],
    )
    metals = object.__new__(Metals)
    metals.metals_transport = dataframe
    metals.transport_lookup = build_transport_lookup(dataframe)
    metals.alt_names = {"copper": "Copper"}

    result = metals.get_weighted_average_distance("CH", "copper")

    pd.testing.assert_frame_equal(result, dataframe.iloc[[1]])


def test_metals_validation_reuses_supplied_mining_shares(monkeypatch):
    validator = object.__new__(validation_module.MetalsValidation)
    validator.mining_shares_mapping = pd.DataFrame({"Metal": [], "Country": []})

    monkeypatch.setattr(
        validation_module,
        "_load_mining_shares_mapping_for_validation",
        lambda _version: pytest.fail("validation reopened the mining-share workbook"),
    )

    validator.check_excel_shares_preserved()


def biosphere_resource(name, amount):
    return {
        "name": name,
        "amount": amount,
        "unit": "kilogram",
        "type": "biosphere",
        "categories": ("natural resource", "in ground"),
    }


def market_dataset(name, product, location, exchanges):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": "kilogram",
        "exchanges": exchanges,
    }


def technosphere_exchange(name, product, location, amount, unit="kilogram"):
    return {
        "name": name,
        "product": product,
        "location": location,
        "amount": amount,
        "unit": unit,
        "type": "technosphere",
    }


def test_pv_gallium_intensity_is_assigned_to_cigs_not_crystalline_silicon():
    metals_db = pd.read_csv(DATA_DIR / "metals" / "metals_db.csv")
    gallium = metals_db[metals_db["metal"] == "Gallium"]

    assert gallium[gallium["origin_var"] == "c-Si"].empty

    cigs_2020 = gallium[(gallium["origin_var"] == "CIGS") & (gallium["year"] == 2020.0)]
    assert len(cigs_2020) == 1
    assert cigs_2020[["mean", "median", "min", "max"]].iloc[0].to_dict() == (
        pytest.approx(
            {
                "mean": 16.986969696969695,
                "median": 6.17,
                "min": 2.32,
                "max": 124.0,
            }
        )
    )


def test_extract_reference_products_from_filter_handles_either_expression():
    expression = (
        "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
        "{'equals': 'lithium carbonate'}]}"
    )

    assert extract_reference_products_from_filter(expression) == [
        "lithium carbonate, battery grade",
        "lithium carbonate",
    ]


@pytest.mark.parametrize(
    ("value", "query", "expected"),
    [
        ("copper mine operation", {"contains": "mine"}, True),
        ("copper mine operation", {"equals": "copper mine operation"}, True),
        ("copper mine operation", {"startswith": "copper"}, True),
        (
            "copper mine operation",
            {"all": [{"contains": "copper"}, {"contains": "mine"}]},
            True,
        ),
        (
            "copper mine operation",
            {"either": [{"equals": "other"}, {"contains": "mine"}]},
            True,
        ),
        ("copper mine operation", {"contains": "market"}, False),
    ],
)
def test_matches_filter_query(value, query, expected):
    assert matches_filter_query(value, query) is expected


def test_extract_exact_filter_values_handles_boolean_expressions():
    assert extract_exact_filter_values(
        {"either": [{"equals": "copper"}, {"equals": "zinc"}]}
    ) == {"copper", "zinc"}
    assert extract_exact_filter_values({"contains": "copper"}) is None


def test_metal_filter_lookup_uses_exact_activity_index():
    copper = market_dataset("copper mine", "copper", "GLO", [])
    zinc = market_dataset("zinc mine", "zinc", "GLO", [])
    metals = object.__new__(Metals)
    metals.database = [zinc, copper]
    metals.build_db_indexes()

    assert metals.get_datasets_matching_filters(
        {"equals": "copper mine"}, {"equals": "copper"}
    ) == [copper]
    assert metals.get_datasets_matching_filters(
        {"contains": "copper"}, {"equals": "copper"}
    ) == [copper]
    assert metals.get_datasets_matching_filters(
        {"either": [{"equals": "copper mine"}, {"equals": "zinc mine"}]},
        {"either": [{"equals": "copper"}, {"equals": "zinc"}]},
    ) == [zinc, copper]


def test_create_metal_markets_refreshes_exact_index_before_correction(monkeypatch):
    original = market_dataset("vanadium mine", "vanadium ore", "GLO", [])
    regional_proxy = market_dataset("vanadium mine", "vanadium ore", "BR", [])
    metals = object.__new__(Metals)
    metals.database = [original]
    metals.country_codes = {}
    metals.version = "3.12"
    metals.build_db_indexes()

    dataframe = pd.DataFrame(
        {
            "Work done": ["Yes"],
            "Country": ["Brazil"],
            "Metal": ["Vanadium"],
        }
    )
    monkeypatch.setattr(
        "premise.metals.load_mining_shares_mapping", lambda _: dataframe.copy()
    )

    def create_market(_metal, _dataframe):
        metals.database.append(regional_proxy)
        return None

    metals.create_market = create_market
    matched = []

    def post_allocation_correction():
        matched.extend(
            metals.get_datasets_matching_filters(
                {"equals": "vanadium mine"}, {"equals": "vanadium ore"}
            )
        )

    metals.post_allocation_correction = post_allocation_correction
    metals.create_metal_markets()

    assert matched == [original, regional_proxy]


def test_mining_share_dataset_membership_is_cached_but_returned_as_a_copy(
    monkeypatch,
):
    dataset = {
        "name": "copper mine operation",
        "reference product": "copper",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [biosphere_resource("Copper", 1.0)],
    }
    dataframe = pd.DataFrame(
        {
            "Work done": ["Yes"],
            "Process": ["{'contains': 'copper mine'}"],
            "Reference product": ["{'equals': 'copper'}"],
        }
    )
    calls = 0

    def load_mapping(_):
        nonlocal calls
        calls += 1
        return dataframe.copy()

    monkeypatch.setattr("premise.metals.load_mining_shares_mapping", load_mapping)
    metals = object.__new__(Metals)
    metals.database = [dataset]
    metals.version = "3.12"

    first = metals.get_mining_share_dataset_ids()
    first.clear()
    second = metals.get_mining_share_dataset_ids()

    assert second == {id(dataset)}
    assert calls == 1


def test_in_ground_resource_exchange_index_scans_each_dataset_once(monkeypatch):
    resource_exchange = biosphere_resource("Copper", 1.0)
    copper = market_dataset(
        "copper mine operation", "copper", "GLO", [resource_exchange]
    )
    empty = market_dataset("market for copper", "copper", "GLO", [])
    original = metals_module.get_in_ground_resource_exchanges
    scanned = []

    def record_scan(dataset):
        scanned.append(id(dataset))
        return original(dataset)

    monkeypatch.setattr(metals_module, "get_in_ground_resource_exchanges", record_scan)
    metals = object.__new__(Metals)
    metals.database = [copper, empty]

    metals.build_in_ground_resource_exchange_index()

    first = metals._get_in_ground_resource_exchanges(copper)
    repeated = metals._get_in_ground_resource_exchanges(copper)
    no_resources = metals._get_in_ground_resource_exchanges(empty)

    assert scanned == [id(copper), id(empty)]
    assert first is repeated
    assert first == [resource_exchange]
    assert no_resources == []


def test_is_secondary_metal_supply_exchange_matches_recovery_terms():
    assert is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "treatment of copper scrap by electrolytic refining",
            "copper, cathode",
            "RoW",
            0.2,
        ),
        "copper, cathode",
    )
    assert is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "metalliferous hydroxide sludge to market for zinc concentrate",
            "zinc concentrate",
            "GLO",
            0.0001,
        ),
        "zinc concentrate",
    )
    assert not is_secondary_metal_supply_exchange(
        technosphere_exchange(
            "market for transport, freight, lorry",
            "transport, freight, lorry",
            "GLO",
            1.0,
            unit="ton kilometer",
        )
    )


def test_existing_market_lookup_uses_fallback_reference_product():
    metals = object.__new__(Metals)
    lithium_market = market_dataset(
        "market for lithium carbonate",
        "lithium carbonate",
        "GLO",
        [],
    )
    metals.database = [lithium_market]

    df_metal = pd.DataFrame(
        {
            "Reference product": [
                "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
                "{'equals': 'lithium carbonate'}]}"
            ]
        }
    )

    assert metals.get_existing_metal_market(df_metal) is lithium_market


def test_secondary_extraction_tries_next_old_market_candidate():
    metals = object.__new__(Metals)
    metals.system_model = "cutoff"
    lithium_battery_grade_market = market_dataset(
        "market for lithium carbonate, battery grade",
        "lithium carbonate, battery grade",
        "GLO",
        [
            technosphere_exchange(
                "lithium carbonate production, from brine",
                "lithium carbonate, battery grade",
                "CL",
                1.0,
            )
        ],
    )
    lithium_market = market_dataset(
        "market for lithium carbonate",
        "lithium carbonate",
        "GLO",
        [
            technosphere_exchange(
                "lithium carbonate production, from brine",
                "lithium carbonate",
                "CL",
                0.99,
            ),
            technosphere_exchange(
                "treatment of used Li-ion battery, hydrometallurgical treatment",
                "lithium carbonate",
                "GLO",
                0.01,
            ),
        ],
    )
    metals.database = [lithium_battery_grade_market, lithium_market]

    df_metal = pd.DataFrame(
        {
            "Reference product": [
                "{'either': [{'equals': 'lithium carbonate, battery grade'}, "
                "{'equals': 'lithium carbonate'}]}"
            ]
        }
    )

    exchanges = metals.build_secondary_market_exchanges_from_existing_market(df_metal)

    assert exchanges == [
        technosphere_exchange(
            "treatment of used Li-ion battery, hydrometallurgical treatment",
            "lithium carbonate",
            "GLO",
            0.01,
        )
    ]


def test_create_market_uses_secondary_share_from_existing_market_instead_of_yaml():
    metals = object.__new__(Metals)
    metals.system_model = "cutoff"
    metals.prim_sec_split = {
        "copper": {
            "name": "market for copper",
            "reference product": "copper",
            "shares": {
                "primary": {2020: 0.1},
                "secondary": {2020: 0.9},
            },
        }
    }
    metals.database = [
        market_dataset(
            "market for copper",
            "copper",
            "GLO",
            [
                technosphere_exchange(
                    "primary copper production",
                    "copper",
                    "GLO",
                    0.75,
                ),
                technosphere_exchange(
                    "treatment of copper scrap by electrolytic refining",
                    "copper",
                    "RoW",
                    0.25,
                ),
            ],
        ),
        market_dataset("primary copper production", "copper", "GLO", []),
        market_dataset(
            "treatment of copper scrap by electrolytic refining",
            "copper",
            "RoW",
            [],
        ),
    ]
    metals.create_region_specific_markets = lambda df: [
        technosphere_exchange("primary copper production", "copper", "GLO", 1.0)
    ]
    metals.add_transport_to_market = lambda dataset, metal: []
    metals.remove_from_index = lambda dataset: None
    metals.is_in_index = lambda dataset: False

    df_metal = pd.DataFrame({"Reference product": ["{'equals': 'copper'}"]})

    market = metals.create_market("copper", df_metal)
    exchanges = {
        (exc["name"], exc.get("location")): exc["amount"]
        for exc in market["exchanges"]
        if exc["type"] == "technosphere"
    }

    assert exchanges[("primary copper production", "GLO")] == pytest.approx(0.75)
    assert exchanges[
        ("treatment of copper scrap by electrolytic refining", "RoW")
    ] == pytest.approx(0.25)


def test_create_market_ignores_existing_secondary_market_for_consequential():
    metals = object.__new__(Metals)
    metals.system_model = "consequential"
    metals.prim_sec_split = {}
    metals.database = [
        market_dataset(
            "market for copper",
            "copper",
            "GLO",
            [
                technosphere_exchange(
                    "treatment of copper scrap by electrolytic refining",
                    "copper",
                    "RoW",
                    0.25,
                )
            ],
        ),
        market_dataset("primary copper production", "copper", "GLO", []),
        market_dataset(
            "treatment of copper scrap by electrolytic refining",
            "copper",
            "RoW",
            [],
        ),
    ]
    metals.create_region_specific_markets = lambda df: [
        technosphere_exchange("primary copper production", "copper", "GLO", 1.0)
    ]
    metals.add_transport_to_market = lambda dataset, metal: []
    metals.remove_from_index = lambda dataset: None
    metals.is_in_index = lambda dataset: False

    df_metal = pd.DataFrame({"Reference product": ["{'equals': 'copper'}"]})

    market = metals.create_market("copper", df_metal)
    exchanges = [exc for exc in market["exchanges"] if exc["type"] == "technosphere"]

    assert exchanges == [
        technosphere_exchange("primary copper production", "copper", "GLO", 1.0)
    ]


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
