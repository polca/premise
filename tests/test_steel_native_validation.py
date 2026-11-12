"""Keep physical steel checks while recognizing native fuel spelling."""

import pytest

from premise.validation.validation import SteelValidation


@pytest.mark.parametrize(
    "name,unit,amount",
    [
        ("Hard coal mix, at regional storage", "kilogram", 0.5),
        ("hard coal supply", "kilogram", 0.5),
        ("Natural gas, high pressure, at consumer", "cubic meter", 0.4),
        ("natural gas supply", "cubic meter", 0.4),
        ("Hydrogen supply", "kilogram", 0.1),
        ("hydrogen supply", "kilogram", 0.1),
    ],
)
def test_pig_iron_energy_counts_native_and_legacy_fuel_names(name, unit, amount):
    validator, issues = energy_validator(name, unit, amount)
    validator.check_steel_energy_use()
    assert not issues


def energy_validator(name, unit, amount):
    validator = SteelValidation.__new__(SteelValidation)
    validator.regions = ["EUR"]
    validator.database = [
        {
            "name": "pig iron production, blast furnace",
            "reference product": "pig iron",
            "location": "EUR",
            "exchanges": [
                {"name": name, "unit": unit, "amount": amount, "type": "technosphere"}
            ],
        }
    ]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    return validator, issues


@pytest.mark.parametrize("amount", [0, 0.1, -0.5])
def test_low_or_negative_energy_still_fails(amount):
    validator, issues = energy_validator("Hard coal mix", "kilogram", amount)
    validator.check_steel_energy_use()
    assert len(issues) == 1
    assert issues[0][1]["issue_type"] == "major"


@pytest.mark.parametrize("secondary_amount,expected_issues", [(0.25, 0), (0, 1)])
def test_market_share_check_uses_reviewed_supplier_identity(
    secondary_amount, expected_issues
):
    from types import SimpleNamespace
    import xarray as xr

    name = "Steel, electric, low-alloyed, at plant"
    validator = SteelValidation.__new__(SteelValidation)
    validator.regions = ["EUR"]
    validator.year = 2050
    validator.system_model = "cutoff"
    production = xr.DataArray(
        [[[3], [1]]],
        dims=("region", "variables", "year"),
        coords={
            "region": ["EUR"],
            "variables": ["steel - primary - BF/BOF", "steel - secondary"],
            "year": [2050],
        },
    )
    validator.iam_data = SimpleNamespace(
        steel_technology_mix=production, production_volumes=production
    )
    validator.technology_map = {
        "steel - secondary": [{"name": name, "reference product": name}]
    }
    validator.database = [
        {
            "name": "market for steel, low-alloyed",
            "location": "EUR",
            "exchanges": [
                {
                    "name": name,
                    "product": name,
                    "type": "technosphere",
                    "unit": "kilogram",
                    "amount": secondary_amount,
                },
                {
                    "name": "converter steel",
                    "product": "primary steel",
                    "type": "technosphere",
                    "unit": "kilogram",
                    "amount": 1 - secondary_amount,
                },
            ],
        }
    ]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_steel_markets()
    assert len(issues) == expected_issues
    assert not validator._is_secondary_supplier(
        {"name": name, "product": "chromium steel"}
    )


def test_mapped_native_eaf_still_has_electricity_minimum():
    name = "Steel, electric, low-alloyed, at plant"
    validator, issues = energy_validator(
        "Electricity, medium voltage", "kilowatt hour", 0.2
    )
    validator.database[0].update(name=name, **{"reference product": name})
    validator.technology_map = {
        "steel - secondary": [{"name": name, "reference product": name}]
    }
    validator.check_steel_energy_use()
    assert len(issues) == 1
    assert issues[0][0][1] == "electricity use for EAF steel production too low"


@pytest.mark.parametrize("name", ["Electricity, medium voltage", "electricity supply"])
def test_shared_efficiency_scales_native_and_legacy_fuel_spelling(name):
    from premise.materials.steel import Steel

    steel = Steel.__new__(Steel)
    steel.model, steel.scenario, steel.year = "remind", "SSP2-NPi", 2050
    from types import SimpleNamespace

    steel.iam_data = SimpleNamespace(steel_technology_efficiencies=None)
    steel.find_iam_efficiency_change = lambda **kwargs: 2
    dataset = {
        "name": "Steel, electric, low-alloyed, at plant",
        "location": "EUR",
        "exchanges": [
            {
                "name": name,
                "amount": 1,
                "unit": "kilowatt hour",
                "type": "technosphere",
                "uncertainty type": 3,
                "loc": 1,
                "scale": 0.1,
            },
        ],
    }
    result = steel.adjust_process_efficiency(dataset, "steel - secondary")
    assert result["exchanges"][0]["amount"] == 0.5
    assert result["exchanges"][0]["loc"] == 0.5
    assert result["exchanges"][0]["scale"] == 0.05


def test_reviewed_volume_gas_helper_is_classified_without_renaming_or_rescaling():
    from premise.inventory.activity_maps import InventorySet, FUELS_TECHS, get_mapping
    from test_steel_market_grades import activity

    definition = get_mapping(FUELS_TECHS, "uvek_aliases")["natural gas"]
    name = next(
        n
        for n in definition["fltr"]["name"]
        if n.startswith("global high-pressure gas supply, nominal 36")
    )
    helper = activity(name, "natural gas, high pressure", "GLO")
    helper["unit"] = helper["exchanges"][0]["unit"] = "cubic meter"
    helper["clic provenance"] = {"role": "generated helper"}
    providers = InventorySet([helper], "2026", "remind", family="uvek").generate_map(
        {"natural gas": definition}
    )["natural gas"]
    assert providers == [helper]
    assert "lhv" not in helper  # Classification does not annotate source providers.
    identities = {(a["name"], a["reference product"], a["unit"]) for a in providers}
    validator, issues = energy_validator(name, "cubic meter", 0.3)
    validator.database[0]["exchanges"][0]["product"] = "natural gas, high pressure"
    validator.natural_gas_inputs = identities
    validator.check_steel_energy_use()
    assert not issues  # Existing 36 MJ/m3 equation gives 10.8 MJ, not zero.
    assert validator.database[0]["exchanges"][0]["amount"] == 0.3
    validator.database[0]["exchanges"][0]["product"] = "different material"
    validator.check_steel_energy_use()
    assert len(issues) == 1


@pytest.mark.parametrize(
    "location,amount,expected",
    [("EUR", 1, 0), ("GLO", 1, 1), ("EUR", 0, 1), ("EUR", -1, 1)],
)
def test_mapped_pig_iron_input_keeps_positive_quantity_and_region_checks(
    location, amount, expected
):
    validator = SteelValidation.__new__(SteelValidation)
    validator.regions = ["EUR"]
    name = "Pig iron, at plant"
    validator.pig_iron_inputs = {(name, name, "kilogram")}
    validator.database = [
        {
            "name": "steel production, blast furnace-basic oxygen furnace, low-alloyed",
            "location": "EUR",
            "exchanges": [
                {
                    "name": name,
                    "product": name,
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": amount,
                    "location": location,
                }
            ],
        }
    ]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_pig_iron_input()
    assert len(issues) == expected
    validator.database[0]["exchanges"][0]["product"] = "cast iron"
    issues.clear()
    validator.check_pig_iron_input()
    assert len(issues) == 1


def test_mapped_native_pig_iron_still_has_energy_minimum():
    validator, issues = energy_validator("electricity", "kilowatt hour", 0.1)
    name = "Pig iron, at plant"
    validator.database[0].update(
        name=name, unit="kilogram", **{"reference product": name}
    )
    validator.pig_iron_inputs = {(name, name, "kilogram")}
    validator.check_steel_energy_use()
    assert len(issues) == 1


@pytest.mark.parametrize(
    "unit,product,amount,expected",
    [
        ("kilogram", "Light fuel oil, at regional storage", 0.2, 0),
        ("kilogram", "other oil", 0.2, 1),
        ("megajoule", "Light fuel oil, at regional storage", 0.2, 1),
        ("kilogram", "Light fuel oil, at regional storage", -0.2, 1),
    ],
)
def test_mapped_oil_energy_keeps_reference_basis_and_signed_quantity(
    unit, product, amount, expected
):
    name = "Light fuel oil, at regional storage"
    validator, issues = energy_validator(name, unit, amount)
    validator.database[0]["exchanges"][0]["product"] = product
    validator.additional_fuel_lhvs = {(name, name, "kilogram"): 42.6}
    validator.check_steel_energy_use()
    assert len(issues) == expected
    assert validator.database[0]["exchanges"][0]["amount"] == amount
