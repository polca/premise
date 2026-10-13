"""An energy-intensity projection is not an emission-factor projection."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.car_energy import apply_floor
from premise.inventory_store import compact_exchange_payload
from premise.transport import Transport
from premise.vehicle_emissions import fuel_proportional_emission


def emission(name, categories=("air", "urban air close to ground")):
    return dict(
        name=name,
        categories=categories,
        type="biosphere",
        unit="kilogram",
        amount=0.001,
        loc=0.001,
        scale=0.0001,
        **{"uncertainty type": 3},
    )


def vehicle(kind, compact=False):
    car = kind == "car"
    dataset = {
        "name": (
            "transport, passenger car, gasoline, Small, EURO-6"
            if car
            else "transport, freight, lorry, diesel, 40 metric ton"
        ),
        "location": "EUR",
        "unit": "kilometer" if car else "ton kilometer",
        "exchanges": [
            dict(name="service", type="production", amount=1),
            dict(
                name="market for petrol, low-sulfur" if car else "market for diesel",
                unit="kilogram",
                type="technosphere",
                amount=2 / 42.6 if car else 0.1,
            ),
            # Fuel-based equations in carculator_utils.inventory.
            emission("Carbon dioxide, fossil", ("air",)),
            emission("Carbon dioxide, non-fossil", ("air", "unspecified")),
            emission("Sulfur dioxide", ("air",)),
            emission("Methane, fossil", ("air", "unspecified")),
            # HBEFA distance/start/evaporation factors, including compartmented
            # methane, must not inherit a fuel-efficiency projection.
            *[
                emission(name)
                for name in (
                    "Nitrogen oxides",
                    "Particulates, < 2.5 um",
                    "Carbon monoxide, fossil",
                    "Methane, fossil",
                    "Methane, non-fossil",
                    "Ammonia",
                    "Dinitrogen monoxide",
                    "NMVOC, non-methane volatile organic compounds",
                    "Benzene",
                    "Formaldehyde",
                    "Benzo(a)pyrene",
                )
            ],
            emission("1,1,1,2-Tetrafluoroethane", ("air",)),
            emission("Copper", ("soil", "agricultural")),
            emission("Water", ("natural resource", "in water")),
            dict(
                name="treatment of tyre wear emissions, passenger car",
                type="technosphere",
                unit="kilogram",
                amount=0.002,
            ),
            dict(
                name="lorry production, diesel, 40 metric ton",
                product="truck",
                type="technosphere",
                unit="unit",
                amount=1e-7,
            ),
            dict(
                name="market for diesel engine",
                product="diesel engine",
                type="technosphere",
                unit="kilogram",
                amount=0.01,
            ),
        ],
    }
    if compact:
        dataset["exchanges"] = [
            compact_exchange_payload(e) for e in dataset["exchanges"]
        ]
    return dataset


def transformer(dataset, kind, factor):
    tr = object.__new__(Transport)
    tr.vehicle_type = kind
    tr.year, tr.model, tr.scenario = 2050, "remind", "SSP2-NPi"
    values = xr.DataArray([1], dims="variables", coords={"variables": ["vehicle"]})
    tr.iam_data = SimpleNamespace(
        passenger_car_efficiencies=values, road_freight_efficiencies=values
    )
    tr.rev_map = {dataset["name"]: "vehicle"}
    tr.vehicle_fuel_map = {"vehicle": [{"name": "diesel"}]}
    tr.find_iam_efficiency_change = lambda **kwargs: 1 / factor
    tr.write_log = lambda *args: None
    return tr


@pytest.mark.parametrize("source_kind", ["direct intensity", "energy/service", "unavailable"])
def test_vehicle_records_the_regional_iam_efficiency_source(source_kind):
    dataset = vehicle("car")
    transform = transformer(dataset, "car", 1.0)
    transform.iam_data.passenger_car_efficiencies.attrs[
        "vehicle efficiency sources"
    ] = {
        "vehicle": {
            "intensity variable": "Energy intensity|Car",
            "intensity unit": "MJ/vehkm",
            "reference year": 2020,
            "by region": {"EUR": source_kind, "USA": "direct intensity"},
        }
    }
    transform.adjust_transport_efficiency(dataset)
    record = dataset["log parameters"]["IAM efficiency source"]
    assert record["kind"] == source_kind
    assert record["reference year"] == 2020
    assert record["intensity variable"] == (
        "Energy intensity|Car" if source_kind == "direct intensity" else None
    )


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("factor", [1.0, 0.8, 1.2])
@pytest.mark.parametrize("compact", [False, True])
def test_only_fuel_driven_flows_follow_iam_energy(kind, factor, compact):
    ds = vehicle(kind, compact)
    before = [deepcopy(dict(e)) for e in ds["exchanges"]]
    transformer(ds, kind, factor).adjust_transport_efficiency(ds)
    assert ds["exchanges"][1]["amount"] == pytest.approx(before[1]["amount"] * factor)
    # Deliberately specify the four expected flows independently of the selector.
    for i in range(2, 6):
        for field in ("amount", "loc", "scale"):
            assert ds["exchanges"][i][field] == pytest.approx(before[i][field] * factor)
    for i in [0, *range(6, len(before))]:
        assert dict(ds["exchanges"][i]) == before[i]


@pytest.mark.parametrize("compact", [False, True])
def test_floor_keeps_hbefa_factors_and_uses_final_fuel_demand(compact):
    ds = vehicle("car", compact)
    before = [deepcopy(dict(e)) for e in ds["exchanges"]]
    # 2 -> 0.8 MJ/km under IAM; existing floor then raises it to 0.852.
    transformer(ds, "car", 0.4).adjust_transport_efficiency(ds)
    assert ds["exchanges"][1]["amount"] * 42.6 == pytest.approx(0.852)
    for i in range(2, 6):
        assert ds["exchanges"][i]["amount"] == pytest.approx(0.001 * 0.852 / 2)
    for i in range(6, len(before)):
        assert dict(ds["exchanges"][i]) == before[i]
    assert apply_floor(ds) is None


@pytest.mark.parametrize("powertrain", ["battery electric", "plugin gasoline hybrid"])
def test_electrified_cars_use_the_same_physical_emission_rules(powertrain):
    ds = vehicle("car")
    ds["name"] = f"transport, passenger car, {powertrain}, Small"
    electricity = dict(
        name="market group for electricity, low voltage",
        type="technosphere",
        unit="kilowatt hour",
        amount=0.2,
    )
    ds["exchanges"].append(electricity)
    tr = transformer(ds, "car", 0.8)
    tr.vehicle_fuel_map = {"vehicle": [{"name": "petrol"}, {"name": "electricity"}]}
    before = [deepcopy(dict(e)) for e in ds["exchanges"]]
    tr.adjust_transport_efficiency(ds)
    assert electricity["amount"] == pytest.approx(0.16)
    assert ds["exchanges"][1]["amount"] == pytest.approx(before[1]["amount"] * 0.8)
    for i in range(6, len(before) - 1):
        assert dict(ds["exchanges"][i]) == before[i]


@pytest.mark.parametrize("categories", [("air",), ["air", "unspecified"], "air"])
def test_generic_air_leakage_is_distinct_from_combustion(categories):
    assert fuel_proportional_emission(emission("Methane, fossil", categories))
    assert not fuel_proportional_emission(emission("Methane, fossil"))


@pytest.mark.parametrize(
    "name", ["Carbon monoxide", "Sulfur dioxide", "Methane, fossil"]
)
def test_names_alone_do_not_establish_a_fuel_relationship(name):
    assert not fuel_proportional_emission(emission(name, ("water",)))
    assert not fuel_proportional_emission(emission(name, ()))
    assert not fuel_proportional_emission(
        dict(name=name, categories=("air",), type="technosphere")
    )


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("factor", [-1, float("inf"), float("nan")])
def test_invalid_road_vehicle_scaling_fails_before_exchange_changes(kind, factor):
    ds = vehicle(kind)
    before = deepcopy(ds)
    with pytest.raises(ValueError, match="positive and finite"):
        transformer(ds, kind, factor).adjust_transport_efficiency(ds)
    assert ds == before
