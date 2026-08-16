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

HYDROGEN_HEAT_DAC_ACTIVITIES = [
    (
        "carbon dioxide, captured, with a solvent-based direct air capture "
        "system, 1MtCO2, with hydrogen heat, and grid electricity"
    ),
    (
        "carbon dioxide, captured and stored, with a solvent-based direct air "
        "capture system, 1MtCO2, with hydrogen heat, and grid electricity"
    ),
    (
        "carbon dioxide, captured, with a sorbent-based direct air capture "
        "system, 100ktCO2, with hydrogen heat, and grid electricity"
    ),
    (
        "carbon dioxide, captured and stored, with a sorbent-based direct air "
        "capture system, 100ktCO2, with hydrogen heat, and grid electricity"
    ),
]


def test_hydrogen_heat_dac_activities_have_twenty_year_lifetimes():
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    *_, dataset_lifetimes = obj._load_temporal_specs_from_csv(
        trails.FILEPATH_TEMPORAL_PARAMETERS
    )

    for name in HYDROGEN_HEAT_DAC_ACTIVITIES:
        assert dataset_lifetimes[(name, "carbon dioxide, captured")] == 20


def test_nested_lifecycle_services_are_scheduled_in_the_caller_year(
    monkeypatch, tmp_path
):
    scenario = {
        "model": "model",
        "pathway": "pathway",
        "year": 2030,
        "database": [
            {
                "name": "asset operation",
                "reference product": "service",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "name": "asset treatment",
                        "product": "used asset",
                    }
                ],
            },
            {
                "name": "asset treatment",
                "reference product": "used asset",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "name": "component treatment",
                        "product": "used component",
                    }
                ],
            },
            {
                "name": "maintenance service",
                "reference product": "maintenance",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "name": "component maintenance",
                        "product": "component maintenance",
                    }
                ],
            },
        ],
    }

    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.stock_asset_params = {}
    obj.end_of_life_suppliers = {
        ("asset treatment", "used asset"),
        ("component treatment", "used component"),
    }
    obj.biomass_growth_params = {}
    obj.maintenance_suppliers = {
        ("maintenance service", "maintenance"),
        ("component maintenance", "component maintenance"),
    }
    obj.dataset_lifetimes = {("asset operation", "service"): 20}
    obj.long_term_biosphere_params = []
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    asset_treatment = scenario["database"][0]["exchanges"][0]
    nested_treatment = scenario["database"][1]["exchanges"][0]
    nested_maintenance = scenario["database"][2]["exchanges"][0]

    assert asset_treatment["temporal_distribution"] == 6
    assert asset_treatment["temporal_offsets"] == [21.0]
    assert nested_treatment["temporal_distribution"] == 6
    assert nested_treatment["temporal_offsets"] == [0.0]
    assert nested_maintenance["temporal_distribution"] == 6
    assert nested_maintenance["temporal_offsets"] == [0.0]
    faulty_file = (
        tmp_path / "trails_temp" / "temporal_distribution_faulty_exchanges.csv"
    )
    assert not faulty_file.exists()


def test_trails_default_years_follow_selected_iam_file(monkeypatch, tmp_path):
    iam_file = tmp_path / "image_custom.csv"
    iam_file.write_text(
        "Region,Variable,Unit,2020,2035,2110,foo\n" "World,variable,unit,1,2,3,4\n",
        encoding="utf-8",
    )

    captured = {}

    class DummyNewDatabase:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(trails, "NewDatabase", DummyNewDatabase)
    monkeypatch.setattr(trails.bw2data, "databases", {"biosphere3": object()})
    monkeypatch.setattr(trails, "get_classifications", lambda: {})
    monkeypatch.setattr(
        TrailsDataPackage,
        "_load_temporal_specs_from_csv",
        lambda self, path: ({}, set(), {}, set(), [], {}),
    )

    obj = TrailsDataPackage(
        scenario={
            "model": "image",
            "pathway": "custom",
            "filepath": str(tmp_path),
        },
        source_type="ecospold",
        source_file_path=".",
    )

    assert obj.years == [2020, 2035]
    assert [scenario["year"] for scenario in obj.scenarios] == [2020, 2035]
    assert captured["scenarios"] == obj.scenarios


def test_trails_explicit_years_skip_iam_year_inference(monkeypatch):
    captured = {}

    class DummyNewDatabase:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    def fail_inference(*args, **kwargs):
        raise AssertionError("explicit years should not inspect IAM files")

    monkeypatch.setattr(trails, "NewDatabase", DummyNewDatabase)
    monkeypatch.setattr(trails.bw2data, "databases", {"biosphere3": object()})
    monkeypatch.setattr(trails, "get_classifications", lambda: {})
    monkeypatch.setattr(TrailsDataPackage, "_infer_years_from_scenario", fail_inference)
    monkeypatch.setattr(
        TrailsDataPackage,
        "_load_temporal_specs_from_csv",
        lambda self, path: ({}, set(), {}, set(), [], {}),
    )

    obj = TrailsDataPackage(
        scenario={"model": "image", "pathway": "custom"},
        years=[2040],
        source_type="ecospold",
        source_file_path=".",
    )

    assert obj.years == [2040]
    assert [scenario["year"] for scenario in captured["scenarios"]] == [2040]


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
    assert exchanges[0]["temporal_weights"][0] > exchanges[1]["temporal_weights"][0]
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
