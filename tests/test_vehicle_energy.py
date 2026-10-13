import pytest

from premise.vehicle_energy import vehicle_energy_exchange


@pytest.mark.parametrize(
    "product,unit",
    [
        ("diesel, low-sulfur", "kilogram"),
        ("petrol, low-sulfur", "kilogram"),
        ("gasoline", "kilogram"),
        ("natural gas, high pressure", "cubic meter"),
        ("natural gas, high pressure", "kilogram"),
        ("hydrogen, gaseous, low pressure", "kilogram"),
        ("electricity, low voltage", "kilowatt hour"),
        ("liquefied petroleum gas", "kilogram"),
    ],
)
@pytest.mark.parametrize(
    "supplier", ["market for ", "market group for ", "direct supply"]
)
def test_vehicle_energy_identification_uses_products_and_units(product, unit, supplier):
    exchange = dict(
        type="technosphere", name=supplier + product, product=product, unit=unit
    )
    assert vehicle_energy_exchange(exchange)
    if supplier != "direct supply":
        del exchange["product"]
        assert vehicle_energy_exchange(exchange)


@pytest.mark.parametrize(
    "name,product,unit",
    [
        ("lorry production, diesel, 40 metric ton", "truck", "unit"),
        ("market for diesel engine", "diesel engine", "kilogram"),
        ("market for hydrogen fuel cell", "hydrogen fuel cell", "kilogram"),
        ("market for electricity meter", "electricity meter", "unit"),
        ("market for diesel", "diesel", "unit"),
        ("market for electricity", "electricity", "kilogram"),
    ],
)
def test_fuel_labelled_equipment_and_wrong_units_are_not_energy(name, product, unit):
    exchange = dict(type="technosphere", name=name, product=product, unit=unit)
    assert not vehicle_energy_exchange(exchange)
    del exchange["product"]
    assert not vehicle_energy_exchange(exchange)


def test_production_and_biosphere_exchanges_are_not_energy_purchases():
    for kind in ("production", "biosphere"):
        assert not vehicle_energy_exchange(
            dict(type=kind, name="diesel", unit="kilogram")
        )
