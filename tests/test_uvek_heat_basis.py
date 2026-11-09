"""Physical reference bases, nested boiler services and reviewed energy paths."""

import copy

import pytest

from premise.energy.uvek_heat_basis import resolve_heat_energy_basis


def identity(a):
    return a["name"], a["reference product"], a["location"], a["unit"]


def exchange(a, amount, kind="technosphere"):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": kind,
        "amount": amount,
        "input": (a["database"], a["code"]),
    }


def activity(name, amount=1, unit="megajoule"):
    a = {
        "name": name,
        "reference product": name,
        "location": "CH",
        "unit": unit,
        "database": "synthetic",
        "code": name,
        "exchanges": [],
    }
    a["exchanges"].append(exchange(a, amount, "production"))
    return a


@pytest.fixture
def boiler():
    gas = activity("Reviewed fuel energy")
    auxiliary = activity("Auxiliary electricity", unit="kilowatt hour")
    burner = activity("Fuel combustion service", amount=1.1)
    burner["exchanges"] += [exchange(gas, 1), exchange(auxiliary, 0.04)]
    heat = activity("Delivered heat", amount=2)
    heat["exchanges"].append(exchange(burner, 1.8))
    # Uncertainty and formula belong to the original LCI, not a deterministic
    # reconstructed inventory manufactured from this nominal conversion factor.
    heat["exchanges"][-1].update(
        {"uncertainty type": 3, "loc": 1.8, "scale": 0.1, "formula": "fuel_use"}
    )
    data = [gas, auxiliary, burner, heat]
    routes = {identity(heat): (identity(burner),), identity(burner): (identity(gas),)}
    return data, routes, {identity(gas): 1}


def resolve(case):
    data, routes, carriers = case
    return resolve_heat_energy_basis(
        data, identity(data[-1]), routes=routes, carrier_MJ_per_unit=carriers
    )


def test_nested_burner_uses_both_actual_production_bases_and_preserves_lci(boiler):
    before = copy.deepcopy(boiler)
    result = resolve(boiler)
    assert result.input_MJ_per_delivered_MJ == pytest.approx(1.8 / 2 / 1.1)
    assert result.delivered_heat_factor == pytest.approx(2 * 1.1 / 1.8)
    assert boiler == before
    burner_evidence = result.witnesses[1]
    assert burner_evidence["production_amount"] == 1.1
    assert burner_evidence["other_technosphere_inputs_outside_conversion_boundary"] == [
        identity(boiler[0][1])
    ]
    result.witnesses[-1]["energy_routes"][0]["exchanges"][0]["amount"] = 99
    assert boiler == before


def test_electricity_reference_gives_inventory_derived_heat_pump_factor():
    electricity = activity("Reviewed grid", unit="kilowatt hour")
    pump = activity("Heat pump output")
    pump["exchanges"].append(exchange(electricity, 1 / (3.6 * 4)))
    result = resolve_heat_energy_basis(
        [electricity, pump],
        identity(pump),
        routes={identity(pump): (identity(electricity),)},
        carrier_MJ_per_unit={identity(electricity): 3.6},
    )
    assert result.delivered_heat_factor == pytest.approx(4)


@pytest.mark.parametrize("amount", [-1, 0, float("nan"), float("inf")])
def test_invalid_actual_production_refused(boiler, amount):
    boiler[0][2]["exchanges"][0]["amount"] = amount
    with pytest.raises(ValueError, match="reference production"):
        resolve(boiler)


@pytest.mark.parametrize("amount", [-0.1, float("nan"), float("inf")])
def test_negative_or_nonfinite_energy_routes_are_not_silently_ignored(boiler, amount):
    boiler[0][-1]["exchanges"][-1]["amount"] = amount
    with pytest.raises(ValueError, match="separate review"):
        resolve(boiler)


def test_zero_route_does_not_become_successful_heat_conversion(boiler):
    boiler[0][-1]["exchanges"][-1]["amount"] = 0
    with pytest.raises(ValueError, match="positive energy draw"):
        resolve(boiler)


@pytest.mark.parametrize("value", [-1, 0, float("nan"), float("inf")])
def test_carrier_lhv_is_explicit_and_validated(boiler, value):
    boiler[2][identity(boiler[0][0])] = value
    with pytest.raises(ValueError, match="carrier energy"):
        resolve(boiler)


def test_carrier_identity_is_not_inferred_from_name_substrings(boiler):
    data, routes, carriers = boiler
    carriers[identity(data[0])[:-1] + ("kilogram",)] = carriers.pop(identity(data[0]))
    with pytest.raises(ValueError, match="Missing or duplicate reviewed energy route"):
        resolve(boiler)


def test_missing_actual_route_refused(boiler):
    boiler[0][2]["exchanges"].pop(1)
    with pytest.raises(ValueError, match="absent"):
        resolve(boiler)


def test_disagreeing_linked_supplier_refused(boiler):
    data = boiler[0]
    data[-1]["exchanges"][-1]["input"] = (data[0]["database"], data[0]["code"])
    with pytest.raises(ValueError, match="link disagrees"):
        resolve(boiler)


def test_ambiguous_supplier_refused(boiler):
    boiler[0].insert(0, copy.deepcopy(boiler[0][0]))
    with pytest.raises(ValueError, match="ambiguous"):
        resolve(boiler)


def test_cycle_in_explicit_routes_refused(boiler):
    data, routes, _ = boiler
    routes[identity(data[2])] = (identity(data[-1]),)
    data[2]["exchanges"].append(exchange(data[-1], 1))
    with pytest.raises(ValueError, match="Cycle"):
        resolve(boiler)


def test_parallel_fuel_inputs_are_summed_without_changing_quantities(boiler):
    data, _, _ = boiler
    data[2]["exchanges"].append(exchange(data[0], 0.2))
    result = resolve(boiler)
    assert result.input_MJ_per_delivered_MJ == pytest.approx(1.8 / 2 * 1.2 / 1.1)
