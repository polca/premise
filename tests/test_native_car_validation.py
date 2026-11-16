"""Native car labels must enter the same validation checks as legacy labels."""

from premise.validation import CarValidation


def validator(records):
    owner = CarValidation.__new__(CarValidation)
    owner.database = records
    owner.regions = ["EUR"]
    issues = []
    owner.log_issue = lambda ds, reason, message, issue_type="minor": issues.append(
        (reason, issue_type)
    )
    return owner, issues


def car(powertrain="gasoline", amount=0.001):
    name = f"Transport, passenger car, {powertrain}, Medium, 2021, EURO-6d"
    return {
        "name": name,
        "reference product": name,
        "location": "EUR",
        "unit": "kilometer",
        "exchanges": [
            {
                "name": name,
                "product": name,
                "location": "EUR",
                "unit": "kilometer",
                "type": "production",
                "amount": 1,
            },
            {
                "name": "Fuel supply for gasoline vehicles",
                "product": "Fuel supply for gasoline vehicles",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": amount,
            },
        ],
    }


def test_native_combustion_car_is_not_skipped_by_efficiency_checks():
    ds = car()
    owner, issues = validator([ds])
    owner.check_vehicle_efficiency("transport, passenger car")
    assert ("fuel consumption incorrect", "major") in issues


def test_native_grid_label_is_valid_purchased_car_electricity():
    ds = car("battery electric")
    ds["exchanges"][1].update(
        name="Electricity, low voltage, at grid",
        product="Electricity, low voltage, at grid",
        unit="kilowatt hour",
        amount=0.2,
    )
    owner, issues = validator([ds])
    owner.check_car_electric_energy(ds)
    assert issues == []


def test_native_year_label_reports_unavailable_exact_emission_reference():
    owner, issues = validator([car(amount=0.05)])
    owner.exhaust = {"gasoline": {}}
    owner.check_pollutant_emissions("transport, passenger car")
    assert ("transport emission reference unavailable", "minor") in issues


def test_native_car_fuel_carbon_is_checked_against_current_purchases(monkeypatch):
    import premise.fuels.carbon as carbon

    monkeypatch.setattr(
        carbon,
        "vehicle_fuel_carbon",
        lambda dataset, providers: {
            "status": "resolved",
            "fossil CO2 kg": 0.1,
            "non-fossil CO2 kg": 0.1,
        },
    )
    owner, issues = validator([car(amount=0.05)])
    owner.check_vehicle_fuel_carbon("transport, passenger car,")
    assert any(kind == "major" for _, kind in issues)
