"""Exact native binding, physical versus allocated heat bases and refusal paths."""

import copy
import json

import pytest

import premise.energy.uvek_heat_suppliers as native
from premise.database.background import validate_uvek_scope
from premise.inventory.profiles import identity

ROLE = "heat, buildings, from natural gas boiler"
PUMP = "heat, buildings, from heat pump"
CHP = "heat, secondary, from coal CHP"


def record(i):
    data = {**i, "database": "synthetic", "code": i["name"] + i["location"]}
    data["exchanges"] = [edge(data, 1, "production")]
    return data


def edge(a, amount, kind="technosphere"):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": kind,
        "amount": amount,
        "input": (a["database"], a["code"]),
    }


def source(role=ROLE, energy_amount=0.9):
    row = next(
        r
        for r in json.loads(native.REVIEW_PATH.read_text())["roles"]
        if r["role"] == role
    )
    root = record(row["identity"])
    if row["energy_basis"] == "final_energy":
        terminal = record(row["energy_boundary"]["carriers"][0]["identity"])
    else:
        terminal = record(
            {
                "name": "Allocated fuel",
                "reference product": "fuel",
                "location": "CH",
                "unit": "megajoule",
            }
        )
    root["exchanges"].append(edge(terminal, energy_amount))
    root["exchanges"][-1].update(
        {
            "uncertainty type": 3,
            "scale": 0.1,
            "loc": energy_amount,
            "formula": "original_energy",
        }
    )
    return [terminal, root]


def plan(data, role=ROLE):
    return native.plan_native_heat_suppliers(data, [role], model="remind")[role]


def test_boiler_uses_reviewed_fuel_input_service_and_keeps_original_inventory():
    data = source()
    before = copy.deepcopy(data)
    result = plan(data)
    assert result["delivered_heat_factor"] == pytest.approx(1 / 0.9)
    assert result["supplier"] is data[-1]
    assert result["identity"] == identity(data[-1])
    assert result["limitations"] and result["energy_witnesses"]
    assert data == before


def test_heat_pump_uses_actual_electricity_draw():
    result = plan(source(PUMP, 1 / (3.6 * 4)), PUMP)
    assert result["delivered_heat_factor"] == pytest.approx(4)


def test_exergy_allocated_heat_is_not_treated_as_physical_fuel_efficiency():
    result = plan(source(CHP, 0.1), CHP)
    assert result["energy_basis"] == "heat_output"
    assert result["delivered_heat_factor"] == 1
    assert result["energy_witnesses"] == ()
    assert any("Allocated fuel draw" in message for message in result["limitations"])


@pytest.mark.parametrize("amount", [0.8, 2, -1, True])
def test_unreviewed_production_basis_refused(amount):
    data = source()
    data[-1]["exchanges"][0]["amount"] = amount
    with pytest.raises(ValueError, match="nonempty"):
        plan(data)


def test_geography_is_not_silently_substituted():
    data = source()
    data[-1]["location"] = "DE"
    with pytest.raises(ValueError, match="exact native"):
        plan(data)


def test_same_name_with_different_product_does_not_match():
    data = source()
    data[-1]["reference product"] = "different output"
    with pytest.raises(ValueError, match="exact native"):
        plan(data)


def test_duplicate_identity_refused():
    data = source()
    data.append(copy.deepcopy(data[-1]))
    with pytest.raises(ValueError, match="ambiguous"):
        plan(data)


@pytest.mark.parametrize("field", ["clic provenance", "brightpath_conversion"])
def test_foreground_record_cannot_impersonate_native_provider(field):
    data = source()
    data[-1][field] = {"owner": "foreground"}
    with pytest.raises(ValueError, match="foreground/helper"):
        plan(data)


def test_missing_positive_role_is_actionable_without_mutation():
    data = source()
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="No reviewed native heat supplier"):
        native.plan_native_heat_suppliers(
            data, [ROLE, "heat, industrial, from biofuel boiler"], model="remind"
        )
    assert data == before


def test_unreviewed_model_role_is_refused():
    with pytest.raises(ValueError, match="No reviewed native heat supplier"):
        native.plan_native_heat_suppliers(source(), [ROLE], model="tiam-ucl")


def test_yaml_review_disagreement_is_refused(monkeypatch):
    original = native.get_mapping

    def altered(path, attribute, **kwargs):
        values = copy.deepcopy(original(path, attribute, **kwargs))
        if attribute == "uvek_aliases":
            values[ROLE]["fltr"]["location"] = ["DE"]
        return values

    monkeypatch.setattr(native, "get_mapping", altered)
    with pytest.raises(ValueError, match="disagree with review"):
        plan(source())


def test_an_unphysical_draw_is_not_capped_or_silently_accepted():
    with pytest.raises(ValueError, match="physical heat factor"):
        plan(source(energy_amount=0.5))


def test_source_with_no_inventory_does_not_become_a_successful_supplier():
    data = source()
    data[-1]["exchanges"].pop()
    with pytest.raises(ValueError, match="nonempty"):
        plan(data)


def test_heat_admitted_while_unverified_sectors_stay_gated():
    assert validate_uvek_scope("heat") == ["heat"]
    with pytest.raises(ValueError, match="unsupported sectors"):
        validate_uvek_scope("cement")
