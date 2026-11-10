"""Physical heat conversion must remain separate from allocated fuel burdens."""

import copy
import json

import pytest

import premise.energy.uvek_heat_suppliers as native
from premise.energy.uvek_heat_basis import resolve_heat_energy_basis
from premise.inventory.profiles import identity
from test_uvek_heat_suppliers import source

ROLES = [
    "heat, buildings, from hydrogen fuel cell",
    "heat, industrial, from hydrogen fuel cell",
]


@pytest.fixture(params=ROLES)
def allocated(request, monkeypatch, tmp_path):
    role = request.param
    review = json.loads(native.REVIEW_PATH.read_text())
    row = next(r for r in review["roles"] if r["role"] == role)
    data = source(role, energy_amount=0.001)
    carrier, supplier = data
    second = copy.deepcopy(supplier["exchanges"][-1])
    second.update(amount=0.002, loc=0.002)
    supplier["exchanges"].append(second)
    supplier["comment"] = " ".join(
        row["documented_physical_conversion"]["documentation_fragments"]
    )
    evidence = resolve_heat_energy_basis(
        data,
        identity(supplier),
        routes={identity(supplier): (identity(carrier),)},
        carrier_MJ_per_unit={identity(carrier): 120},
    )
    row["documented_physical_conversion"]["allocated_energy_routes_sha256"] = (
        native.allocated_energy_fingerprint(evidence.witnesses)
    )
    path = tmp_path / "review.json"
    path.write_text(json.dumps(review))
    monkeypatch.setattr(native, "REVIEW_PATH", path)
    return data, role, row, review, path


def plan(allocated):
    data, role, *_ = allocated
    return native.plan_native_heat_suppliers(data, [role], model="message")[role]


def test_documented_efficiency_does_not_invert_or_rescale_allocated_inventory(
    allocated,
):
    data, *_ = allocated
    before = copy.deepcopy(data)
    result = plan(allocated)
    assert result["delivered_heat_factor"] == 0.33
    assert 1 / (120 * 0.003) > 1
    assert result["supplier"] is data[-1]
    assert data == before
    assert result["energy_witnesses"][-1][
        "allocated_inventory_not_inverted_or_rescaled"
    ]
    assert any("SOFC" in limitation for limitation in result["limitations"])


@pytest.mark.parametrize(
    "field,value",
    [("amount", 0.003), ("scale", 0.2), ("formula", "changed"), ("negative", True)],
)
def test_changed_allocated_energy_evidence_is_refused_without_mutation(
    allocated, field, value
):
    data, *_ = allocated
    data[-1]["exchanges"][-1][field] = value
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="reviewed physical conversion evidence"):
        plan(allocated)
    assert data == before


def test_missing_physical_documentation_is_refused(allocated):
    allocated[0][-1]["comment"] = "No independently documented heat efficiency."
    with pytest.raises(ValueError, match="reviewed physical conversion evidence"):
        plan(allocated)


@pytest.mark.parametrize("factor", [0, -0.1, 1.1, True, float("nan"), float("inf")])
def test_invalid_documented_performance_is_refused(allocated, factor):
    _, _, row, review, path = allocated
    row["documented_physical_conversion"]["factor"] = factor
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError, match="reviewed physical conversion evidence"):
        plan(allocated)


def test_owner_renaming_and_empty_false_flag_cleanup_do_not_change_science(allocated):
    data, *_ = allocated
    for dataset in data:
        dataset["database"] = "export"
        for exchange in dataset["exchanges"]:
            exchange["input"] = ("export", exchange["input"][1])
            exchange["output"] = ("export", dataset["code"])
            exchange["negative"] = False
    assert plan(allocated)["delivered_heat_factor"] == 0.33
