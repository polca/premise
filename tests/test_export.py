import pandas as pd
import pytest

from premise.clean_datasets import remove_uncertainty
from premise.export import *
from premise.export import _aggregate_duplicate_superstructure_rows


def test_simapro_units():
    simapro_units = get_simapro_units()
    assert simapro_units["cubic meter-year"] == "m3y"


def test_simapro_compartments():
    simapro_compartments = get_simapro_compartments()
    assert simapro_compartments["in water"] == "in water"


def test_simapro_exchange_categories():
    simapro_exchange_categories = get_simapro_category_of_exchange()
    agr = simapro_exchange_categories[
        ("2-butanol production by hydration of butene", "2-butanol")
    ]
    assert agr["category"] == "material"
    assert agr["sub_category"] == "Chemicals\Organic\Transformation"


def test_resolve_simapro_category_falls_back_on_blank_mapping():
    main_category, sub_category = resolve_simapro_category(
        "foo",
        "bar",
        {("foo", "bar"): {"category": "", "sub_category": ""}},
    )
    assert main_category == "material"
    assert sub_category == "Others\Transformation"


def test_simapro_biosphere_dict():
    s_bio = get_simapro_biosphere_dictionnary()
    assert s_bio["Propanol"] == "1-Propanol"


dummy_db = [
    {
        "name": "fake activity",
        "reference product": "fake product",
        "location": "FR",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "fake activity",
                "product": "fake product",
                "amount": 1,
                "type": "production",
                "unit": "kilogram",
                "input": ("dummy_db", "6543541"),
            },
            {
                "name": "1,4-Butanediol",
                "categories": ("air", "urban air close to ground"),
                "amount": 1,
                "type": "biosphere",
                "unit": "kilogram",
                "input": ("dummy_bio", "123"),
                "uncertainty type": 5,
                "loc": 1.1,
                "minimum": 0.8,
                "maximum": 1.35,
            },
        ],
    },
    {
        "name": "fake activity",
        "reference product": "fake product",
        "location": "FR",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "fake activity",
                "product": "fake product",
                "amount": 1,
                "type": "production",
                "unit": "kilogram",
                "input": ("dummy_db", "6543541"),
            },
            {
                "name": "1,4-Butanediol",
                "categories": ("air", "urban air close to ground"),
                "amount": 1,
                "type": "biosphere",
                "unit": "kilogram",
                "input": ("dummy_bio", "123"),
            },
        ],
    },
]


def test_remove_uncertainty():
    db = dummy_db.copy()
    db = remove_uncertainty(db)
    for ds in db:
        for exc in ds["exchanges"]:
            if "uncertainty_type" in exc:
                assert exc["uncertainty_type"] == 0


def test_aggregate_duplicate_superstructure_rows_sums_biosphere_collisions():
    df = pd.DataFrame(
        [
            {
                "from key": ("biosphere3", "bio-1"),
                "to key": ("super-db", "act-1"),
                "from activity name": "Acetaldehyde",
                "to activity name": "consumer",
                "flow type": "biosphere",
                "original": 0.1,
                "scenario a": 0.2,
            },
            {
                "from key": ("biosphere3", "bio-1"),
                "to key": ("super-db", "act-1"),
                "from activity name": "Acetaldehyde",
                "to activity name": "consumer",
                "flow type": "biosphere",
                "original": 0.3,
                "scenario a": 0.4,
            },
        ]
    )

    aggregated, exact_duplicates, duplicate_collisions = (
        _aggregate_duplicate_superstructure_rows(
            df=df,
            scenario_columns=["original", "scenario a"],
        )
    )

    assert exact_duplicates == 0
    assert duplicate_collisions == 1
    assert len(aggregated) == 1
    assert aggregated.loc[0, "flow type"] == "biosphere"
    assert aggregated.loc[0, "original"] == pytest.approx(0.4)
    assert aggregated.loc[0, "scenario a"] == pytest.approx(0.6)


def test_aggregate_duplicate_superstructure_rows_nets_production_and_technosphere():
    df = pd.DataFrame(
        [
            {
                "from key": ("super-db", "act-1"),
                "to key": ("super-db", "act-1"),
                "from activity name": "self supplier",
                "to activity name": "self supplier",
                "flow type": "production",
                "original": 0.0,
                "scenario a": 0.0,
            },
            {
                "from key": ("super-db", "act-1"),
                "to key": ("super-db", "act-1"),
                "from activity name": "self supplier",
                "to activity name": "self supplier",
                "flow type": "technosphere",
                "original": 0.2,
                "scenario a": 0.4,
            },
        ]
    )

    aggregated, exact_duplicates, duplicate_collisions = (
        _aggregate_duplicate_superstructure_rows(
            df=df,
            scenario_columns=["original", "scenario a"],
        )
    )

    assert exact_duplicates == 0
    assert duplicate_collisions == 1
    assert len(aggregated) == 1
    assert aggregated.loc[0, "flow type"] == "production"
    assert aggregated.loc[0, "original"] == pytest.approx(1.2)
    assert aggregated.loc[0, "scenario a"] == pytest.approx(1.4)


def test_generate_superstructure_db_aggregates_duplicate_key_pairs(monkeypatch, tmp_path):
    df = pd.DataFrame(
        [
            {
                "from activity name": "Acetaldehyde",
                "from reference product": None,
                "from location": None,
                "from categories": ("air", "urban air close to ground"),
                "from database": "biosphere3",
                "from key": ("biosphere3", "bio-1"),
                "from unit": "kilogram",
                "to activity name": "consumer 1",
                "to reference product": "product 1",
                "to location": "GLO",
                "to categories": None,
                "to unit": "kilogram",
                "to database": "super-db",
                "to key": ("super-db", "act-1"),
                "flow type": "biosphere",
                "original": 0.0,
                "scenario a": 0.2,
            },
            {
                "from activity name": "Acetaldehyde",
                "from reference product": None,
                "from location": None,
                "from categories": ("air", "urban air close to ground"),
                "from database": "biosphere3",
                "from key": ("biosphere3", "bio-1"),
                "from unit": "kilogram",
                "to activity name": "consumer 1",
                "to reference product": "product 1",
                "to location": "GLO",
                "to categories": None,
                "to unit": "kilogram",
                "to database": "super-db",
                "to key": ("super-db", "act-1"),
                "flow type": "biosphere",
                "original": 0.0,
                "scenario a": 0.3,
            },
            {
                "from activity name": "consumer 2",
                "from reference product": "product 2",
                "from location": "GLO",
                "from categories": None,
                "from database": "super-db",
                "from key": ("super-db", "act-2"),
                "from unit": "kilogram",
                "to activity name": "consumer 2",
                "to reference product": "product 2",
                "to location": "GLO",
                "to categories": None,
                "to unit": "kilogram",
                "to database": "super-db",
                "to key": ("super-db", "act-2"),
                "flow type": "production",
                "original": 1.0,
                "scenario a": 0.0,
            },
            {
                "from activity name": "consumer 2",
                "from reference product": "product 2",
                "from location": "GLO",
                "from categories": None,
                "from database": "super-db",
                "from key": ("super-db", "act-2"),
                "from unit": "kilogram",
                "to activity name": "consumer 2",
                "to reference product": "product 2",
                "to location": "GLO",
                "to categories": None,
                "to unit": "kilogram",
                "to database": "super-db",
                "to key": ("super-db", "act-2"),
                "flow type": "technosphere",
                "original": 0.0,
                "scenario a": 0.4,
            },
        ]
    )

    monkeypatch.setattr(
        "premise.export.generate_scenario_difference_file",
        lambda **kwargs: (df.copy(), [{"name": "dummy"}], []),
    )

    generate_superstructure_db(
        origin_db=[],
        scenarios=[],
        db_name="super-db",
        biosphere_name="biosphere3",
        filepath=tmp_path,
        version="3.12",
        scenario_list=["scenario a"],
        file_format="csv",
    )

    exported = pd.read_csv(tmp_path / "scenario_diff_super-db.csv", sep=";")

    assert len(exported) == 2

    biosphere_row = exported.loc[exported["from activity name"] == "Acetaldehyde"].iloc[0]
    assert biosphere_row["scenario a"] == pytest.approx(0.5)

    self_loop_row = exported.loc[exported["to activity name"] == "consumer 2"].iloc[0]
    assert self_loop_row["flow type"] == "production"
    assert self_loop_row["scenario a"] == pytest.approx(1.4)
