from copy import deepcopy

import numpy as np
import pytest

from premise.stock_lifecycle import (
    scope_capital_chain,
    split_embedded_lifecycle,
    validate_lifecycle_anchor_audits,
)
from premise.stock_vintage import identity


def activity(name, production=1.0, tech=None, bio=None):
    row = {
        "name": name,
        "reference product": name,
        "unit": "unit",
        "location": "GLO",
        "database": "synthetic",
        "code": name,
    }
    row["exchanges"] = [
        {
            "name": name,
            "product": name,
            "unit": "unit",
            "location": "GLO",
            "type": "production",
            "amount": production,
        }
    ]
    for supplier, amount in (tech or {}).items():
        row["exchanges"].append(
            {
                "name": supplier,
                "product": supplier,
                "unit": "unit",
                "location": "GLO",
                "type": "technosphere",
                "amount": amount,
            }
        )
    for flow, amount in (bio or {}).items():
        row["exchanges"].append({"name": flow, "type": "biosphere", "amount": amount})
    return row


def example():
    return [
        activity("service", 2, {"capital": 0.4, "other": 0.5}),
        activity("capital", 2, {"manufacturer-a": 0.6, "manufacturer-b": 1.4}),
        activity("manufacturer-a", 1, {"disposal": -1, "other": 2}, {"manufacture": 2}),
        activity("manufacturer-b", 2, {"disposal": -4, "other": 3}, {"manufacture": 5}),
        activity("disposal", 1, {"other": 0.2}, {"disposal": 7}),
        activity("other", 1, None, {"other": 3}),
        activity("other service", 1, {"capital": 0.7}),
    ]


def solve(database, target):
    # Independent dense static solve, including non-unit reference production.
    indices = {identity(d): i for i, d in enumerate(database)}
    flows = sorted(
        {
            e["name"]
            for d in database
            for e in d["exchanges"]
            if e["type"] == "biosphere"
        }
    )
    a = np.zeros((len(database), len(database)))
    b = np.zeros((len(flows), len(database)))
    for j, d in enumerate(database):
        for e in d["exchanges"]:
            if e["type"] == "biosphere":
                b[flows.index(e["name"]), j] += e["amount"]
            else:
                a[indices[identity(e, exchange=True)], j] += e["amount"] * (
                    1 if e["type"] == "production" else -1
                )
    fu = np.zeros(len(database))
    fu[indices[identity(target)]] = 1
    return b @ np.linalg.solve(a, fu)


def test_capital_chain_copy_preserves_nonunit_signed_inventory_without_lifecycle():
    database = [
        activity("service", 2, {"market": 0.4, "other": -0.5}),
        activity("market", 2, {"manufacturer": 2.0}),
        activity("manufacturer", 3, {"other": 6}, {"manufacture": 1}),
        activity("other", 1, None, {"other": 2}),
        activity("other service", 1, {"market": 0.7}),
    ]
    before = deepcopy(database)
    updated, audit = scope_capital_chain(
        database, caller=database[0], chain=database[1:3], context_id="chain"
    )
    assert database == before
    assert updated[: len(database)] == before
    assert len(updated) == len(database) + 3
    assert audit["preserved_capital_amount"] == 0.4
    assert audit["lifted_exchanges"] == []
    assert len(audit["zero_shift_bindings"]) == 1
    assert np.allclose(
        solve(updated, audit["caller"]), solve(database, database[0]), rtol=1e-13
    )
    assert np.allclose(
        solve(updated, database[4]), solve(database, database[4]), rtol=1e-13
    )
    with pytest.raises(ValueError, match="already exists"):
        scope_capital_chain(
            updated, caller=database[0], chain=database[1:3], context_id="chain"
        )
    database[2]["exchanges"].append(deepcopy(database[0]["exchanges"][1]))
    with pytest.raises(ValueError, match="extra internal edge"):
        scope_capital_chain(
            database, caller=database[0], chain=database[1:3], context_id="other"
        )


def test_scoped_lifecycle_split_preserves_all_static_flows_and_other_users():
    database = example()
    before = deepcopy(database)
    rewritten, audit = split_embedded_lifecycle(
        database,
        caller=database[0],
        paths=[
            [database[1], database[2], database[4]],
            [database[1], database[3], database[4]],
        ],
        context_id="test",
        uncertainty_mode="deterministic",
    )
    assert database == before
    assert rewritten[: len(database)] == before
    assert np.allclose(
        solve(before, before[0]),
        solve(rewritten, audit["caller"]),
        rtol=1e-13,
        atol=1e-13,
    )
    assert np.allclose(
        solve(before, before[-1]), solve(rewritten, before[-1]), rtol=1e-13, atol=1e-13
    )
    assert audit["preserved_capital_amount"] == 0.4
    assert [
        r["amount_per_calling_dataset"] for r in audit["lifted_exchanges"]
    ] == pytest.approx([-0.12, -0.56])
    assert [
        r["amount_per_service_unit"] for r in audit["lifted_exchanges"]
    ] == pytest.approx([-0.06, -0.28])
    assert audit["copied_capital_activities"] == 3
    assert audit["lifecycle_adapters"] == 2
    assert len(audit["zero_shift_bindings"]) == 4
    # Stock service copy has a distinct identity: background loops still call the
    # original activity, rather than reusing this pilot's year/scope bindings.
    assert audit["caller"]["name"] != audit["original_caller"]["name"]


def test_shared_path_prefix_is_not_counted_twice():
    database = example()
    database[2]["exchanges"].append(
        dict(
            database[2]["exchanges"][1],
            name="other disposal",
            product="other disposal",
            amount=0.5,
        )
    )
    database.append(activity("other disposal", bio={"disposal": 3}))
    rewritten, audit = split_embedded_lifecycle(
        database,
        caller=database[0],
        paths=[
            [database[1], database[2], database[4]],
            [database[1], database[2], database[-1]],
        ],
        context_id="two-cuts",
        uncertainty_mode="deterministic",
    )
    assert np.allclose(solve(database, database[0]), solve(rewritten, audit["caller"]))
    assert audit["copied_capital_activities"] == 2
    assert sum(
        r["activity_scale_per_caller"] for r in audit["lifted_exchanges"]
    ) == pytest.approx(0.24)


def test_paths_may_merge_without_duplicating_a_lifecycle_cut():
    database = [
        activity("service", tech={"root": 0.1}),
        activity("root", tech={"a": 0.3, "b": 0.7}),
        activity("a", tech={"maker": 2}),
        activity("b", tech={"maker": 3}),
        activity("maker", tech={"disposal": -1}, bio={"manufacture": 5}),
        activity("disposal", bio={"disposal": 4}),
    ]
    rewritten, audit = split_embedded_lifecycle(
        database,
        caller=database[0],
        paths=[
            [database[1], database[2], database[4], database[5]],
            [database[1], database[3], database[4], database[5]],
        ],
        context_id="diamond",
        uncertainty_mode="deterministic",
    )
    assert audit["lifecycle_adapters"] == 1
    assert audit["lifted_exchanges"][0]["amount_per_calling_dataset"] == pytest.approx(
        -0.27
    )
    assert np.allclose(solve(database, database[0]), solve(rewritten, audit["caller"]))


@pytest.mark.parametrize(
    "case", ["missing", "cycle", "unit", "production", "duplicate", "uncertainty"]
)
def test_invalid_split_never_mutates_the_input(case):
    database = example()
    path = [database[1], database[2], database[4]]
    mode = "deterministic"
    if case == "missing":
        path[-1] = dict(path[-1], name="missing")
    elif case == "cycle":
        path = [database[1], database[2], database[1], database[4]]
    elif case == "unit":
        path[-1] = dict(path[-1], unit="kg")
    elif case == "production":
        database[2]["exchanges"][0]["amount"] = 0
    elif case == "duplicate":
        database[2]["exchanges"].append(deepcopy(database[2]["exchanges"][1]))
    else:
        mode = "monte-carlo"
    before = deepcopy(database)
    with pytest.raises(ValueError):
        split_embedded_lifecycle(
            database,
            caller=database[0],
            paths=[path],
            context_id="invalid",
            uncertainty_mode=mode,
        )
    assert database == before


def test_anchor_check_distinguishes_amortisation_from_cohort_specific_disposal():
    def audit(database):
        return split_embedded_lifecycle(
            database,
            caller=database[0],
            paths=[
                [database[1], database[2], database[4]],
            ],
            context_id="anchors",
            uncertainty_mode="deterministic",
        )[1]

    database = example()
    first = audit(database)
    # Capital per service changes while disposal per capital unit is constant.
    database[0]["exchanges"][1]["amount"] *= 2
    second = audit(database)
    assert validate_lifecycle_anchor_audits([first, second])["anchors_checked"] == 2
    # A manufacture-year-dependent disposal coefficient cannot be lifted with
    # the same simplified service-year marginal.
    database[2]["exchanges"][1]["amount"] *= 2
    with pytest.raises(ValueError, match="vary by manufacture"):
        validate_lifecycle_anchor_audits([first, audit(database)])
