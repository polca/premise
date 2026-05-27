import csv
from types import SimpleNamespace

import pytest

import premise.trails as trails
from premise.trails import TrailsDataPackage


TEMPORAL_HEADER = [
    "name",
    "reference product",
    "ISIC rev.4 ecoinvent",
    "CPC",
    "EcoSpold01Categories",
    "temporal_tag",
    "tag_confidence",
    "tag_notes",
    "lifetime",
    "age distribution type",
    "loc",
    "scale",
    "offsets",
    "weights",
    "minimum",
    "maximum",
    "param_confidence",
    "param_notes",
]


def test_load_temporal_specs_reads_long_term_biosphere_selectors(tmp_path):
    path = tmp_path / "temporal_distributions.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEMPORAL_HEADER)
        writer.writeheader()
        writer.writerow(
            {
                "name": "*",
                "reference product": "water|ground-, long-term|*",
                "temporal_tag": "long_term_emission",
                "lifetime": "1000",
                "age distribution type": "6",
                "param_notes": "profile:uniform_100_1000 priority:25",
            }
        )

    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    _, _, _, _, long_term_biosphere, _ = obj._load_temporal_specs_from_csv(path)

    params = long_term_biosphere[0]
    assert params["temporal_distribution"] == 6
    assert params["temporal_profile"] == "uniform_100_1000"
    assert params["temporal_offsets"][0] == 105.0
    assert params["temporal_offsets"][-1] == 975.0
    assert len(params["temporal_offsets"]) == 32
    assert len(params["temporal_weights"]) == 32
    assert sum(params["temporal_weights"]) == pytest.approx(1.0)
    assert params["priority"] == 25.0
    assert params["name"] == "*"
    assert params["compartment"] == "water"
    assert params["subcompartment"] == "ground-, long-term"
    assert params["unit"] == "*"


def _long_term_params(
    name="*",
    compartment="water",
    subcompartment="ground-, long-term",
    unit="*",
    profile="uniform_100_1000",
    priority=0,
    source_row=1,
):
    offsets, weights, profile = TrailsDataPackage._long_term_profile_distribution(
        profile
    )
    return {
        "name": name,
        "compartment": compartment,
        "subcompartment": subcompartment,
        "unit": unit,
        "priority": priority,
        "source_row": source_row,
        "temporal_profile": profile,
        "temporal_distribution": 6,
        "temporal_loc": None,
        "temporal_scale": None,
        "temporal_min": None,
        "temporal_max": None,
        "temporal_offsets": offsets,
        "temporal_weights": weights,
    }


def test_add_temporal_distributions_applies_long_term_biosphere_params(
    monkeypatch, tmp_path
):
    scenario = {
        "model": "model",
        "pathway": "pathway",
        "year": 2030,
        "database": [
            {
                "name": "dataset",
                "reference product": "product",
                "exchanges": [
                    {
                        "type": "biosphere",
                        "name": "Zinc II",
                        "categories": ("water", "ground-, long-term"),
                        "unit": "kilogram",
                    },
                    {
                        "type": "biosphere",
                        "name": "Lead",
                        "categories": ("soil", "agricultural, long-term"),
                        "unit": "kilogram",
                    },
                    {
                        "type": "biosphere",
                        "name": "Zinc II",
                        "categories": ("water", "ground-"),
                        "unit": "kilogram",
                    },
                ],
            }
        ],
    }

    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = set()
    obj.dataset_lifetimes = {}
    obj.long_term_biosphere_params = [
        _long_term_params(profile="uniform_100_1000", priority=0, source_row=1),
        _long_term_params(
            name="Zinc II", profile="mobile_metal", priority=100, source_row=2
        ),
        _long_term_params(
            compartment="soil",
            subcompartment="*long-term*",
            profile="uniform_100_1000",
            priority=0,
            source_row=3,
        ),
    ]
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    exchanges = scenario["database"][0]["exchanges"]
    assert exchanges[0]["temporal_distribution"] == 6
    assert exchanges[0]["temporal_offsets"][0] == 105.0
    assert len(exchanges[0]["temporal_offsets"]) == 32
    assert sum(exchanges[0]["temporal_weights"]) == pytest.approx(1.0)
    assert (
        exchanges[0]["temporal_weights"][0]
        > exchanges[1]["temporal_weights"][0]
    )
    assert exchanges[1]["temporal_distribution"] == 6
    assert "temporal_distribution" not in exchanges[2]

    audit_file = tmp_path / "trails_temp" / "long_term_biosphere_matches.csv"
    with audit_file.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert [row["exchange_name"] for row in rows] == ["Zinc II", "Lead"]
    assert rows[0]["temporal_profile"] == "mobile_metal"
    assert rows[0]["matched_source_row"] == "2"
    assert rows[0]["temporal_bin_count"] == "32"


def test_add_temporal_distributions_records_ambiguous_long_term_selectors(
    monkeypatch, tmp_path
):
    scenario = {
        "model": "model",
        "pathway": "pathway",
        "year": 2030,
        "database": [
            {
                "name": "dataset",
                "reference product": "product",
                "exchanges": [
                    {
                        "type": "biosphere",
                        "name": "Zinc II",
                        "categories": ("water", "ground-, long-term"),
                        "unit": "kilogram",
                    },
                ],
            }
        ],
    }

    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = set()
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = set()
    obj.dataset_lifetimes = {}
    obj.long_term_biosphere_params = [
        _long_term_params(
            name="Zinc II", profile="mobile_metal", priority=100, source_row=1
        ),
        _long_term_params(
            name="Zinc II", profile="sorbed_metal", priority=100, source_row=2
        ),
    ]
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    exchange = scenario["database"][0]["exchanges"][0]
    assert "temporal_distribution" not in exchange

    faulty_file = (
        tmp_path / "trails_temp" / "temporal_distribution_faulty_exchanges.csv"
    )
    with faulty_file.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert "Ambiguous long_term_emission selectors" in rows[0]["reason"]
