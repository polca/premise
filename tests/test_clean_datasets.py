# content of test_activity_maps.py
from pathlib import Path

import pytest
from bw2data.database import DatabaseChooser

from premise.clean_datasets import DatabaseCleaner


def get_dict():
    dummy_db = {
        ("dummy_db", "6543541"): {
            "name": "fake activity",
            "reference product": "fake product",
            "location": "IAI Area, Africa",
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
        }
    }
    dummy_bio = {
        ("dummy_bio", "123"): {
            "name": "1,4-Butanediol",
            "categories": ("air", "urban air close to ground"),
            "unit": "kilogram",
        }
    }
    return dummy_db, dummy_bio


def test_presence_db():
    with pytest.raises(NameError) as wrapped_error:
        DatabaseCleaner("bla", "brightway", Path("."), version="3.8")
    assert wrapped_error.type == NameError


def test_validity_db():
    dummy_db, dummy_bio = get_dict()
    db_bio = DatabaseChooser("dummy_bio")
    db_bio.write(dummy_bio)

    db_act = DatabaseChooser("dummy_db")
    db_act.write(dummy_db)

    dbc = DatabaseCleaner("dummy_db", "brightway", Path("."), version="3.9")
    assert dbc.database[0]["name"] == "fake activity"


def test_brightway_extraction_omits_redundant_fields():
    dummy_bio = {
        ("dummy_bio_compact", "bio-1"): {
            "name": "Carbon dioxide, fossil",
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
        }
    }
    dummy_db = {
        ("dummy_compact_db", "act-1"): {
            "name": "activity one",
            "reference product": "product one",
            "location": "GLO",
            "unit": "kilogram",
            "parameters": [{"name": "alpha", "amount": 1.0}],
            "exchanges": [
                {
                    "name": "activity one",
                    "product": "product one",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_compact_db", "act-1"),
                },
                {
                    "name": "activity two",
                    "product": "product two",
                    "amount": 0.5,
                    "type": "technosphere",
                    "unit": "kilogram",
                    "input": ("dummy_compact_db", "act-2"),
                },
                {
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air", "non-urban air or from high stacks"),
                    "amount": 0.1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio_compact", "bio-1"),
                },
            ],
        },
        ("dummy_compact_db", "act-2"): {
            "name": "activity two",
            "reference product": "product two",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "activity two",
                    "product": "product two",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_compact_db", "act-2"),
                }
            ],
        },
    }

    DatabaseChooser("dummy_bio_compact").write(dummy_bio)
    DatabaseChooser("dummy_compact_db").write(dummy_db)

    dbc = DatabaseCleaner("dummy_compact_db", "brightway", Path("."), version="3.9")

    activity_one = next(ds for ds in dbc.database if ds["name"] == "activity one")
    tech_exc = next(
        exc for exc in activity_one["exchanges"] if exc["type"] == "technosphere"
    )

    assert "parameters full" not in activity_one
    assert activity_one["parameters"] == {"alpha": 1.0}
    assert tech_exc["name"] == "activity two"
    assert tech_exc["product"] == "product two"
    assert tech_exc["location"] == "RER"
    assert "database" not in tech_exc
