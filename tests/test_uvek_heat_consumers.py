"""Exact consumer routing, protected inventories and literal signed audits."""

import copy
import json

import pytest

import premise.energy.uvek_heat_consumers as consumers
from premise.energy.uvek_heat_markets import LAYERS
from test_uvek_heat_distribution import edge, record

SCENARIO = {"model": "remind", "pathway": "test", "year": 2050}


def nonempty(identity, code):
    result = record(identity, code)
    result["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Retained flow",
            "unit": "kilogram",
            "amount": -0.2,
            "input": ("bio", "flow"),
        }
    )
    return result


@pytest.fixture
def graph(tmp_path, monkeypatch):
    review = json.loads(consumers.REVIEW_PATH.read_text())
    routes = [*consumers.ACTIVE_ROUTES, *consumers.RETAINED_ROUTES]
    rows = [
        next(row for row in review["sources"] if row["route"] == route)
        for route in sorted(routes)
    ]
    review["sources"] = rows
    path = tmp_path / "review.json"
    path.write_text(json.dumps(review))
    monkeypatch.setattr(consumers, "REVIEW_PATH", path)
    sources = {
        row["route"]: nonempty(row["identity"], "source-" + str(i))
        for i, row in enumerate(rows)
    }
    for row in rows:
        if row.get("empty_inventory_expected"):
            sources[row["route"]]["exchanges"] = sources[row["route"]]["exchanges"][:1]
    markets = {layer: [] for layer in LAYERS}
    for layer, (_, name, product) in LAYERS.items():
        for region in ("EUR", "World"):
            a = nonempty(
                {
                    "name": name,
                    "reference product": product,
                    "location": region,
                    "unit": "megajoule",
                },
                layer + region,
            )
            a["premise heat"] = {
                "kind": "uvek_iam_market",
                "layer": layer,
                "scenario": SCENARIO,
            }
            markets[layer].append(a)
    deliveries = []
    for region in ("EUR", "World"):
        a = nonempty(
            {
                "name": "District delivery",
                "reference product": "Delivered heat",
                "location": region,
                "unit": "megajoule",
            },
            "delivery" + region,
        )
        a["premise heat"] = {
            "kind": "uvek_native_district_delivery",
            "scenario": SCENARIO,
        }
        deliveries.append(a)
    caller = record(
        {
            "name": "Material production",
            "reference product": "Material",
            "location": "CH",
            "unit": "kilogram",
        },
        "caller",
    )
    for source in sources.values():
        exchange = edge(source, -2.5)
        exchange.update(
            {
                "formula": "-heat_demand",
                "uncertainty type": 2,
                "loc": 0.7,
                "scale": 0.2,
                "negative": True,
                "minimum": -3,
                "maximum": -1,
            }
        )
        caller["exchanges"].append(exchange)
    data = [
        *sources.values(),
        caller,
        *(a for records in markets.values() for a in records),
        *deliveries,
    ]
    return data, {"markets": markets, "deliveries": deliveries}, caller, sources


def plan(graph, lookup=lambda location: "EUR", **kwargs):
    data, heat, _, _ = graph
    return consumers.plan_native_heat_consumers(
        data, heat, lookup, **SCENARIO, **kwargs
    )


def test_routes_preserve_every_quantity_field_and_leave_inputs_unchanged(graph):
    data, heat, caller, sources = graph
    before, heat_before = copy.deepcopy(data), copy.deepcopy(heat)
    result = plan(graph)
    assert data == before and heat == heat_before
    assert result["diagnostics"]["substituted_exchanges"] == 3
    assert len(result["replacements"]) == 1
    changed = result["replacements"][0]["activity"]
    audit = changed[consumers.AUDIT_FIELD]
    for row in audit["substitutions"]:
        original = caller["exchanges"][row["index"]]
        assert row["source_exchange"] == original
        actual = changed["exchanges"][row["index"]]
        identity_fields = {"name", "product", "reference product", "location", "input"}
        assert {k: v for k, v in actual.items() if k not in identity_fields} == {
            k: v for k, v in original.items() if k not in identity_fields
        }
        if row["source_route"] == "district_delivery":
            assert actual["name"] == "District delivery"
    retained = [
        r
        for r in result["retained"]
        if r["consumer_key"] == (caller["database"], caller["code"])
    ]
    assert {r["reason"] for r in retained} == consumers.RETAINED_ROUTES
    for row in retained:
        assert changed["exchanges"][row["index"]] == caller["exchanges"][row["index"]]


def test_literal_audit_survives_json_and_replanning_is_idempotent(graph):
    data, heat, caller, sources = graph
    result = plan(graph)
    changes = {tuple(r["source_key"]): r["activity"] for r in result["replacements"]}
    augmented = json.loads(
        json.dumps([changes.get((a["database"], a["code"]), a) for a in data])
    )
    before = copy.deepcopy(augmented)
    second = plan((augmented, heat, caller, sources))
    assert not second["replacements"] and second["diagnostics"]["already_applied"] == 3
    assert augmented == before
    changed = next(a for a in augmented if a["code"] == "caller")
    changed["exchanges"][changed[consumers.AUDIT_FIELD]["substitutions"][0]["index"]][
        "amount"
    ] *= 2
    with pytest.raises(ValueError, match="differs from this plan"):
        plan((augmented, heat, caller, sources))


@pytest.mark.parametrize(
    "marker", ["clic provenance", "brightpath_conversion", "premise heat"]
)
def test_owned_foreground_and_helpers_are_not_rewritten(graph, marker):
    _, _, caller, _ = graph
    caller[marker] = {"owned": True}
    result = plan(graph)
    assert result["replacements"] == []
    assert result["diagnostics"]["retained_counts"]


@pytest.mark.parametrize("unit", ["megajoule", "kilowatt hour"])
def test_direct_energy_supply_inventory_is_protected(graph, unit):
    _, _, caller, _ = graph
    caller["unit"] = unit
    caller["exchanges"][0]["unit"] = unit
    assert plan(graph)["replacements"] == []


@pytest.mark.parametrize("lookup", [lambda location: "unserved", lambda location: None])
def test_geographic_fallback_is_audited_and_does_not_change_quantity(graph, lookup):
    result = plan(graph, lookup)
    assert result["diagnostics"]["geographic_proxies"] == 3
    for row in result["replacements"][0]["activity"][consumers.AUDIT_FIELD][
        "substitutions"
    ]:
        assert row["target"][2] == "World" and row["geographic_proxy"]
        assert row["source_exchange"]["amount"] == -2.5


def test_lookup_failures_do_not_hide_programming_errors(graph):
    def missing(location):
        raise KeyError(location)

    assert plan(graph, missing)["diagnostics"]["geographic_proxies"] == 3

    def broken(location):
        raise RuntimeError("lookup bug")

    with pytest.raises(RuntimeError, match="lookup bug"):
        plan(graph, broken)


def test_supply_only_fallback_and_missing_layers_are_explicit(graph):
    _, heat, _, _ = graph
    heat["markets"]["industrial_end_use"] = []
    result = plan(graph)
    assert any(
        row["supply_only_proxy"]
        for row in result["replacements"][0]["activity"][consumers.AUDIT_FIELD][
            "substitutions"
        ]
    )
    heat["markets"]["buildings_end_use"] = []
    assert plan(graph)["diagnostics"]["retained_counts"]["IAM_layer_not_available"] == 1


def test_wrong_source_links_and_owned_market_impersonation_are_refused(graph):
    _, _, caller, sources = graph
    exchange = caller["exchanges"][1]
    original = copy.deepcopy(exchange)
    exchange["input"] = ("native", "wrong")
    with pytest.raises(ValueError, match="exact supplier"):
        plan(graph)
    exchange.update(original)
    exchange["name"] = "Similar but unreviewed heat"
    with pytest.raises(ValueError, match="link and identity disagree"):
        plan(graph)
    exchange.update(original)
    next(iter(sources.values()))["clic provenance"] = {"slug": "impostor"}
    with pytest.raises(ValueError, match="impersonate"):
        plan(graph)


def test_market_without_original_audit_cannot_be_treated_as_already_done(graph):
    _, heat, caller, _ = graph
    caller["exchanges"].append(edge(heat["markets"]["buildings_end_use"][0], 1))
    with pytest.raises(ValueError, match="no reviewed original-exchange audit"):
        plan(graph)


def test_missing_world_supply_invalid_target_and_background_are_refused(graph):
    _, heat, _, _ = graph
    with pytest.raises(ValueError, match="UVEK2026"):
        plan(graph, system_model="consequential")
    heat["deliveries"][:] = [heat["deliveries"][0]]
    with pytest.raises(ValueError, match="both regional and World"):
        plan(graph, lambda location: "missing")
    heat["deliveries"][0]["premise heat"]["scenario"] = {**SCENARIO, "year": 2040}
    with pytest.raises(ValueError, match="this scenario"):
        plan(graph)


def test_zero_allocated_heat_handover_cannot_acquire_new_burdens(graph):
    _, _, caller, sources = graph
    source = sources["retain_zero_allocated_heat"]
    result = plan(graph)
    changed = result["replacements"][0]["activity"]
    index = next(
        i
        for i, e in enumerate(caller["exchanges"])
        if e.get("input") == (source["database"], source["code"])
    )
    assert changed["exchanges"][index] == caller["exchanges"][index]
    source["exchanges"].append(edge(sources["industrial_end_use"], 1))
    with pytest.raises(ValueError, match="Zero-allocated heat handover"):
        plan(graph)


@pytest.mark.parametrize("amount", [float("nan"), float("inf"), True])
def test_invalid_caller_quantities_refused_without_partial_mutation(graph, amount):
    data, _, caller, sources = graph
    source = sources["industrial_end_use"]
    exchange = next(
        e
        for e in caller["exchanges"]
        if e.get("input") == (source["database"], source["code"])
    )
    exchange["amount"] = amount
    before = json.dumps(data, sort_keys=True)
    with pytest.raises(ValueError, match="finite and signed"):
        plan(graph)
    assert json.dumps(data, sort_keys=True) == before
