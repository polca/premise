"""Validate installed kWh, cell kg and legacy pack/waste mass separately."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

from premise.battery import Battery
from premise.data_collection import IAMDataCollection
from premise.filesystem_constants import INVENTORY_DIR
from premise.validation import BatteryValidation, PremiseValidationError


def exchange(product, amount, unit="kilowatt hour"):
    return {
        "name": "supplier of " + product,
        "product": product,
        "amount": amount,
        "unit": unit,
        "type": "technosphere",
        "location": "GLO",
    }


def market(exchanges, name="market for battery capacity, Li-ion, NMC111"):
    return {
        "name": name,
        "reference product": "electricity storage capacity",
        "location": "GLO",
        "unit": "kilowatt hour",
        "exchanges": exchanges,
    }


def validator(data, year=2050):
    obj = object.__new__(BatteryValidation)
    obj.database = data
    obj.year = year
    obj.model = "remind"
    obj.scenario = "SSP3-rollBack"
    obj.minor_issues_log = []
    obj.major_issues_log = []
    obj.validation_issues = []
    obj.save_log = lambda: None
    return obj


def modular():
    ds = market(
        [
            exchange(
                "battery cell, Li-ion, NMC111, including end-of-life treatment",
                5.0,
                "kilogram",
            ),
            exchange(
                "battery pack component provision, Li-ion, NMC111, without cells and controls",
                1.0,
            ),
            exchange(
                "battery monitoring and central control system, Li-ion, NMC111",
                0.046385,
                "unit",
            ),
        ]
    )
    ds["log parameters"] = {"old battery mass": 1.0 / 0.18}
    return ds


def test_modular_capacity_does_not_sum_incommensurate_units():
    obj = validator([modular()])
    before = deepcopy(obj.database)
    assert obj.run_battery_checks().valid
    assert obj.database == before


@pytest.mark.parametrize("role", [0, 1, 2])
def test_modular_capacity_rejects_missing_components(role):
    ds = modular()
    del ds["exchanges"][role]
    with pytest.raises(PremiseValidationError):
        validator([ds]).run_battery_checks()


@pytest.mark.parametrize("role", [0, 1, 2])
def test_modular_capacity_rejects_wrong_units_and_negative_inputs(role):
    ds = modular()
    ds["exchanges"][role]["unit"] = "megajoule"
    with pytest.raises(PremiseValidationError):
        validator([ds]).run_battery_checks()
    ds = modular()
    ds["exchanges"][role]["amount"] *= -1
    with pytest.raises(PremiseValidationError):
        validator([ds]).run_battery_checks()


@pytest.mark.parametrize("value", [0.01, 10.0])
def test_cell_mass_must_match_documented_density(value):
    ds = modular()
    ds["exchanges"][0]["amount"] = value
    with pytest.raises(PremiseValidationError, match="INCORRECT_BATTERY_CELL_MASS"):
        validator([ds]).run_battery_checks()


def test_hardware_provision_is_per_installed_kwh():
    ds = modular()
    ds["exchanges"][1]["amount"] = 0.5
    with pytest.raises(
        PremiseValidationError, match="INCORRECT_BATTERY_HARDWARE_CAPACITY"
    ):
        validator([ds]).run_battery_checks()


def test_modular_capacity_rejects_duplicate_external_eol():
    ds = modular()
    ds["exchanges"].append(exchange("used Li-ion battery", -5.0, "kilogram"))
    with pytest.raises(
        PremiseValidationError, match="INVALID_MODULAR_BATTERY_CAPACITY"
    ):
        validator([ds]).run_battery_checks()


def test_one_kwh_capacity_alias_is_not_one_kg_of_cells():
    ds = market(
        [exchange("electricity storage capacity", 1.0)],
        "market for battery capacity, Sodium-ion, SiB, stationary",
    )
    assert validator([ds]).run_battery_checks().valid


@pytest.mark.parametrize("shares", [[0.25, 0.75], [0.5, 0.5]])
@pytest.mark.parametrize(
    "name",
    [
        "market for battery capacity (MIX scenario)",
        "market for battery capacity (LFP scenario)",
        "market for battery capacity, stationary (TC scenario)",
    ],
)
def test_capacity_shares_are_checked_regardless_of_name_suffix(shares, name):
    ds = market(
        [exchange("electricity storage capacity", value) for value in shares], name
    )
    assert validator([ds]).run_battery_checks().valid


@pytest.mark.parametrize("shares", [[0.5, 0.25], [1.1, -0.1]])
def test_bad_market_shares_are_not_hidden_by_a_total_of_one(shares):
    ds = market(
        [exchange("electricity storage capacity", value) for value in shares],
        "market for battery capacity (MIX scenario)",
    )
    with pytest.raises(PremiseValidationError, match="INCORRECT_MARKET_SHARES"):
        validator([ds]).run_battery_checks()


def test_grid_electricity_cannot_be_a_capacity_share():
    ds = market(
        [
            exchange("electricity storage capacity", 1.0),
            exchange("electricity, low voltage", 1.0),
        ]
    )
    with pytest.raises(PremiseValidationError, match="INVALID_BATTERY_CAPACITY_MARKET"):
        validator([ds]).run_battery_checks()


@pytest.mark.parametrize(
    "name",
    [
        "market for battery capacity, lead acid, rechargeable, stationary",
        "market for battery capacity, Li-ion, LFP",
    ],
)
def test_legacy_pack_eol_checks_remain_active_including_lfp(name):
    ds = market(
        [
            exchange("battery pack", 5.0, "kilogram"),
            exchange("used battery", -5.0, "kilogram"),
        ],
        name,
    )
    assert validator([ds]).run_battery_checks().valid
    ds["exchanges"][1]["amount"] = -4.0
    with pytest.raises(PremiseValidationError, match="EOL_NOT_BALANCED"):
        validator([ds]).run_battery_checks()


def test_legacy_pack_mass_balance_excludes_operating_electricity():
    ds = market(
        [
            exchange("battery pack", 5.0, "kilogram"),
            exchange("used battery", -5.0, "kilogram"),
            exchange("electricity, low voltage", 42.0),
        ]
    )
    assert validator([ds]).run_battery_checks().valid
    ds["exchanges"][0]["amount"] = 1.5
    ds["exchanges"][1]["amount"] = -1.5
    with pytest.raises(PremiseValidationError, match="ENERGY_DENSITY_TOO_HIGH"):
        validator([ds]).run_battery_checks()


@pytest.mark.parametrize("amount", [float("nan"), float("inf"), None])
def test_nonfinite_or_missing_inputs_fail_with_a_validation_issue(amount):
    ds = modular()
    ds["exchanges"][0]["amount"] = amount
    with pytest.raises(PremiseValidationError, match="INVALID_BATTERY_EXCHANGE_AMOUNT"):
        validator([ds]).run_battery_checks()


@pytest.fixture(scope="module")
def bundled_capacity_inventory():
    return [
        dataset
        for stem in ["lci-battery-capacity", "lci-batteries-ASSB"]
        for dataset in ExcelImporter(INVENTORY_DIR / f"{stem}.xlsx").data
        if dataset["name"].startswith("market for battery capacity")
    ]


@pytest.mark.parametrize("year", [2010, 2020, 2035, 2050, 2070])
def test_actual_bundled_capacities_validate_after_shares_and_mass_updates(
    bundled_capacity_inventory, year
):
    obj = object.__new__(Battery)
    obj.database = deepcopy(bundled_capacity_inventory)
    obj.year = year
    obj.write_log = lambda *a, **kw: None
    obj.iam_data = SimpleNamespace(
        battery_mobile_scenarios=IAMDataCollection.fetch_external_data_battery_mobile_scenarios(
            None
        ),
        battery_stationary_scenarios=IAMDataCollection.fetch_external_data_battery_stationary_scenarios(
            None
        ),
    )
    obj.adjust_battery_market_shares()
    obj.adjust_battery_mass()
    assert len(obj.database) == 32
    before = deepcopy(obj.database)
    assert validator(obj.database, year).run_battery_checks().valid
    assert obj.database == before


def test_relative_density_learning_preserves_source_corrected_baseline():
    ds = modular()
    ds["log parameters"]["old battery mass"] = 10.0
    ds["exchanges"][0]["amount"] = 9.0  # 10 * 0.18 / 0.20
    assert validator([ds]).run_battery_checks().valid


@pytest.mark.parametrize(
    "storage_name",
    [
        "market for electricity storage capacity, battery, lithium-ion, lfp, hydrogen train",
        "market for electricity, low voltage, storage capacity proxy",
    ],
)
def test_storage_capacity_is_preserved_by_electricity_market_update(storage_name):
    from premise.electricity import Electricity
    from premise.validation import ElectricityValidation

    storage = market(
        [exchange("battery pack", 7.5, "kilogram")],
        storage_name,
    )
    storage["reference product"] = (
        "electricity storage capacity, Li-ion, LFP, hydrogen train"
    )
    original = deepcopy(storage)
    grids = [
        market([exchange("electricity, " + v, 1.0)], "market for electricity, " + v)
        for v in ["high voltage", "medium voltage", "low voltage"]
    ]
    for ds in grids:
        ds["reference product"] = ds["exchanges"][0]["product"]
    obj = object.__new__(Electricity)
    obj.database = [storage, *grids]
    obj.ecoinvent_to_iam_loc = {"GLO": "World"}
    removed = []
    obj.write_log = lambda **kw: None
    obj.remove_from_index = lambda ds: removed.append(ds["name"])
    obj.add_new_entry_to_cache = lambda **kw: None
    for voltage in ["high_voltage", "medium_voltage", "low_voltage"]:
        setattr(obj, "create_new_markets_" + voltage, lambda: None)
    obj.update_electricity_markets()
    assert storage == original
    assert removed == [ds["name"] for ds in grids]
    for ds in grids:
        assert len(ds["exchanges"]) == 1
        assert ds["exchanges"][0]["product"] == ds["reference product"]
    # The legacy-electricity check must also distinguish capacity from energy.
    obj = validator([storage, *grids])
    obj.regions = ["World"]
    obj.check_geo_linking = lambda *a: None
    ElectricityValidation.check_old_datasets(obj)
    assert not obj.validation_issues
    grids[0]["exchanges"].append(exchange("unexpected input", 1.0, "kilogram"))
    ElectricityValidation.check_old_datasets(obj)
    assert any(
        "MORE_THAN_ONE_INPUT" in issue.rule_id for issue in obj.validation_issues
    )


def test_legacy_eol_keeps_existing_absolute_balance_tolerance():
    ds = market(
        [
            exchange("battery pack", 5.0, "kilogram"),
            exchange("used battery", -4.999999, "kilogram"),
        ]
    )
    with pytest.raises(PremiseValidationError, match="EOL_NOT_BALANCED"):
        validator([ds]).run_battery_checks()
