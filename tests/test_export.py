from premise.clean_datasets import remove_uncertainty
from premise.export import *


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
