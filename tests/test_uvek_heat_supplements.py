"""Pinned supplemental heat and retained boiler keep exact source energy bases."""

import copy
import json

import pytest

import premise.energy.uvek_heat_supplements as supplements
from premise.inventory.profiles import identity
from test_uvek_heat_suppliers import edge, record

REVIEW = json.loads(supplements.REVIEW_PATH.read_text())
CASES = [(row, role) for row in REVIEW["records"] for role in row["roles"]]


def source(row):
    carrier = record(row["carrier"])
    if row.get("carrier_pin"):
        pin = row["carrier_pin"]
        carrier["clic provenance"] = {
            "pinned": False,
            "slug": pin["slug"],
            "semantic_version": pin["version"],
            "version_id": pin["version_id"],
            "sha256": pin["sha256"],
        }
    carrier["exchanges"].append(
        {
            "name": "synthetic burden",
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.1,
        }
    )
    if row["kind"] == "retained_electric_boiler":
        return [carrier]
    root = record(row["identity"])
    root["clic provenance"] = {
        "pinned": True,
        "slug": row["pin"]["slug"],
        "semantic_version": row["pin"]["version"],
        "version_id": row["pin"]["version_id"],
        "sha256": row["pin"]["sha256"],
        "background": copy.deepcopy(row["source_profile"]),
    }
    fuel = edge(carrier, row["reviewed_fuel_amount_per_MJ"])
    fuel.update(
        {
            "formula": "original_fuel",
            "uncertainty type": 3,
            "loc": fuel["amount"],
            "scale": 0.01,
        }
    )
    root["exchanges"].append(fuel)
    return [carrier, root]


@pytest.mark.parametrize("row,role", CASES, ids=[role for _, role in CASES])
def test_source_energy_basis_is_used_without_changing_the_inventory(row, role):
    data = source(row)
    before = copy.deepcopy(data)
    result = supplements.plan_heat_supplements(data, [role], model=None)
    plan = result["suppliers"][role]
    assert data == before
    assert plan["identity"] == identity(row["identity"])
    expected = (
        1
        if row["role_bases"][role]["energy_basis"] == "heat_output"
        else row["nominal_factor"]
    )
    assert plan["delivered_heat_factor"] == pytest.approx(expected)
    assert plan["limitations"] == row["limitations"]
    assert bool(result["activities"]) is (row["kind"] == "retained_electric_boiler")
    if row["kind"] == "clic":
        assert plan["supplier"] is data[-1]
        assert plan["energy_witnesses"][-1]["supplier"] == identity(data[-1])


def test_electric_boiler_is_added_once_for_final_energy_and_heat_output_roles():
    row = REVIEW["records"][-1]
    data = source(row)
    before = copy.deepcopy(data)
    first = supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before and len(first["activities"]) == 1
    boiler = first["activities"][0]
    electricity = boiler["exchanges"][1]
    assert electricity["amount"] == 0.2805836139169472
    assert electricity["input"] == (data[0]["database"], data[0]["code"])
    assert len(boiler["exchanges"]) == 2
    assert (
        first["suppliers"][row["roles"][0]]["supplier"]
        is first["suppliers"][row["roles"][1]]["supplier"]
    )
    second_data = [*data, boiler]
    second_before = copy.deepcopy(second_data)
    second = supplements.plan_heat_supplements(second_data, row["roles"], model=None)
    assert second_data == second_before and second["activities"] == []
    assert second["suppliers"][row["roles"][0]]["supplier"] is boiler


@pytest.mark.parametrize(
    "field,value",
    [
        ("pinned", None),
        ("slug", "wrong"),
        ("semantic_version", "9.0"),
        ("sha256", "0" * 64),
        ("version_id", -1),
        ("background", {}),
    ],
)
def test_pinned_source_cannot_be_replaced_by_a_similarly_named_record(field, value):
    row = REVIEW["records"][0]
    data = source(row)
    data[-1]["clic provenance"][field] = value
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="pin or ownership"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before


@pytest.mark.parametrize("row", REVIEW["records"][:3], ids=lambda r: r["pin"]["slug"])
def test_automatic_selection_accepts_only_the_exact_reviewed_source_version(row):
    data = source(row)
    data[-1]["clic provenance"]["pinned"] = False
    before = copy.deepcopy(data)
    result = supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before
    assert all(p["supplier"] is data[-1] for p in result["suppliers"].values())
    data[-1]["clic provenance"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="pin or ownership"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


@pytest.mark.parametrize("row", REVIEW["records"][:3], ids=lambda r: r["pin"]["slug"])
@pytest.mark.parametrize("amount", [0, -1, True, 0.1])
def test_changed_fuel_quantity_is_refused_instead_of_replacing_the_source(row, amount):
    data = source(row)
    data[-1]["exchanges"][1]["amount"] = amount
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="quantity changed"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before


@pytest.mark.parametrize("amount", [0, -1, True, 2])
def test_changed_production_reference_is_refused(amount):
    row = REVIEW["records"][0]
    data = source(row)
    data[-1]["exchanges"][0]["amount"] = amount
    with pytest.raises(ValueError, match=r"matching \+1"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


def test_duplicate_native_carrier_or_supplement_is_refused():
    row = REVIEW["records"][0]
    data = source(row)
    with pytest.raises(ValueError, match="ambiguous"):
        supplements.plan_heat_supplements(
            [*data, copy.deepcopy(data[-1])], row["roles"], model=None
        )
    with pytest.raises(ValueError, match="ambiguous"):
        supplements.plan_heat_supplements(
            [*data, copy.deepcopy(data[0])], row["roles"], model=None
        )


def test_invalid_biosphere_or_system_model_is_not_implicitly_converted():
    with pytest.raises(ValueError, match="UVEK2026"):
        supplements.plan_heat_supplements(
            [], [], model="remind", system_model="consequential"
        )
    with pytest.raises(ValueError, match="UVEK2026"):
        supplements.plan_heat_supplements(
            [], [], model="remind", biosphere_version="3.12"
        )


def test_unavailable_model_role_is_refused():
    row = REVIEW["records"][-1]
    with pytest.raises(ValueError, match="No reviewed supplemental"):
        supplements.plan_heat_supplements(
            source(row), ["heat, secondary, from electricity"], model="remind"
        )


def test_changed_workbook_is_refused(monkeypatch, tmp_path):
    row = REVIEW["records"][-1]
    (tmp_path / "additional_inventories").mkdir()
    (tmp_path / row["workbook"]).write_bytes(b"changed")
    monkeypatch.setattr(supplements, "DATA_DIR", tmp_path)
    with pytest.raises(ValueError, match="workbook changed"):
        supplements.plan_heat_supplements(source(row), [row["roles"][0]], model=None)


def test_yaml_and_review_cannot_silently_disagree(monkeypatch):
    real = supplements.get_mapping

    def changed(path, attribute, **kwargs):
        value = copy.deepcopy(real(path, attribute, **kwargs))
        if attribute == "uvek_aliases":
            value[REVIEW["records"][0]["roles"][0]]["fltr"]["location"] = ["GLO"]
        return value

    monkeypatch.setattr(supplements, "get_mapping", changed)
    row = REVIEW["records"][0]
    with pytest.raises(ValueError, match="disagree"):
        supplements.plan_heat_supplements(source(row), row["roles"], model=None)


@pytest.mark.parametrize(
    "row,role", [(r, role) for r, role in CASES if r["kind"] == "clic"]
)
def test_positive_heat_roles_select_their_reviewed_roots_without_legacy_workbooks(
    row, role
):
    from types import SimpleNamespace

    import xarray as xr

    from premise.inventory.clic_requirements import required_roots

    def scenario(value):
        layer = (
            "buildings_heat_end_use"
            if ", buildings," in role
            else "industrial_heat_end_use"
        )
        return {
            "model": "remind",
            "iam data": SimpleNamespace(
                **{
                    layer: xr.DataArray(
                        [value], dims="variables", coords={"variables": [role]}
                    )
                }
            ),
        }

    assert row["pin"]["slug"] in required_roots(
        ["heat"], [scenario(1)], source_files=[], family="uvek"
    )
    assert row["pin"]["slug"] not in required_roots(
        ["heat"], [scenario(0)], source_files=[], family="uvek"
    )


@pytest.mark.parametrize("amount", [0, -1, True, 2])
def test_changed_energy_carrier_reference_is_refused(amount):
    row = REVIEW["records"][0]
    data = source(row)
    data[0]["exchanges"][0]["amount"] = amount
    with pytest.raises(ValueError, match=r"matching \+1"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


def rename(record_, name):
    record_["name"] = name
    record_["exchanges"][0]["name"] = name


@pytest.mark.parametrize("row", REVIEW["records"][:3], ids=lambda r: r["pin"]["slug"])
def test_locked_clic_namespace_wins_over_an_unrelated_plain_name(row):
    data = source(row)
    unrelated = copy.deepcopy(data[-1])
    unrelated.pop("clic provenance")
    unrelated["code"] = "unrelated"
    unrelated["exchanges"][0]["input"] = (unrelated["database"], unrelated["code"])
    rename(data[-1], f"{row['identity']['name']} [CLIC {row['pin']['slug']}]")
    data.append(unrelated)
    before = copy.deepcopy(data)
    result = supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before
    assert all(plan["supplier"] is data[-2] for plan in result["suppliers"].values())


def test_locked_foreground_fuel_namespace_keeps_the_same_nominal_basis():
    row = REVIEW["records"][1]
    data = source(row)
    name = f"{row['carrier']['name']} [CLIC {row['carrier_pin']['slug']}]"
    rename(data[0], name)
    data[-1]["exchanges"][1]["name"] = name
    before = copy.deepcopy(data)
    result = supplements.plan_heat_supplements(data, row["roles"], model=None)
    assert data == before
    assert result["suppliers"][row["roles"][0]][
        "delivered_heat_factor"
    ] == pytest.approx(0.9)


def test_foreground_fuel_version_is_locked_even_when_it_was_an_auto_dependency():
    row = REVIEW["records"][1]
    data = source(row)
    data[0]["clic provenance"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="carrier source pin"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


def test_native_carrier_cannot_be_impersonated_by_a_foreground():
    row = REVIEW["records"][0]
    data = source(row)
    data[0]["clic provenance"] = {"slug": "unrelated"}
    with pytest.raises(ValueError, match="impersonate"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


def test_arbitrary_rename_cannot_inherit_a_pinned_heat_approval():
    row = REVIEW["records"][0]
    data = source(row)
    rename(data[-1], row["identity"]["name"] + " [CLIC unrelated]")
    with pytest.raises(ValueError, match="pin or ownership"):
        supplements.plan_heat_supplements(data, row["roles"], model=None)


def test_retained_boiler_is_reused_after_json_cache_round_trip():
    row = REVIEW["records"][-1]
    data = source(row)
    first = supplements.plan_heat_supplements(data, row["roles"], model=None)
    restored = json.loads(json.dumps([*data, *first["activities"]]))
    before = copy.deepcopy(restored)
    second = supplements.plan_heat_supplements(restored, row["roles"], model=None)
    assert restored == before and second["activities"] == []
    assert all(
        plan["supplier"] is restored[-1] for plan in second["suppliers"].values()
    )
