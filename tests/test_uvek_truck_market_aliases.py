"""Native and reviewed market purchases use the shared regional relinker."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from brightpath.background.uvek_import import _recipe_signature
from brightpath.migrations.resources import load_uvek_technosphere_resource

from premise.transport.transport import Transport, get_vehicles_mapping

REVIEW = json.loads(
    (
        Path(__file__).parents[1]
        / "docs/development/uvek-truck-market-alias-review.json"
    ).read_text()
)


def test_reviewed_aliases_leave_every_older_vehicle_mapping_unchanged():
    mapping = deepcopy(get_vehicles_mapping())
    for row in REVIEW["rows"]:
        assert mapping["truck"]["old"].pop(row["alias"]) == row["targets"]
    assert hashlib.sha256(json.dumps(mapping, sort_keys=True).encode()).hexdigest() == (
        REVIEW["legacy_mapping_sha256"]
    )
    assert (
        "Transport, municipal waste collection, lorry 21t"
        not in mapping["truck"]["old"]
    )
    assert not any(
        name.startswith("Transport, freight, lorry,")
        for name in mapping["truck"]["old"]
    )


def test_converted_market_aliases_bind_the_current_complete_recipe_closure():
    resource = load_uvek_technosphere_resource()
    rows = [
        r for r in REVIEW["rows"] if r["kind"] == "reviewed converted source market"
    ]
    assert len(rows) == REVIEW["converted_market_alias_count"] == 9
    for row in rows:
        signature = _recipe_signature(
            resource, row["recipe_id"], {"family": "ecoinvent", "version": "3.10"}, {}
        )
        expected = (
            f"{resource['recipes'][row['recipe_id']]['name']} "
            f"[BrightPath UVEK conversion {signature[:8]}] "
            f"[CLIC brightpath-uvek-import-{signature}]"
        )
        assert row["alias"] == expected
        assert row["source"]["name"].startswith("market for ")
        assert row["source"]["unit"] == "ton kilometer"
        assert (
            row["targets"]
            == get_vehicles_mapping()["truck"]["old"][row["source"]["name"]]
        )


@pytest.mark.parametrize(
    "kind", ["native complete fleet market", "reviewed converted source market"]
)
@pytest.mark.parametrize("amount", [-0.077, 0.077])
def test_shared_relink_preserves_signed_tkm_and_uncertainty(kind, amount):
    row = next(r for r in REVIEW["rows"] if r["kind"] == kind)
    exchange = {
        "type": "technosphere",
        "name": row["alias"],
        "product": "native freight",
        "unit": "ton kilometer",
        "location": "RER",
        "input": ("native", "supplier"),
        "amount": amount,
        "uncertainty type": 3,
        "loc": amount,
        "scale": 0.2,
    }
    protected = deepcopy(exchange)
    wrong_unit = {**exchange, "unit": "kilogram"}
    transport = Transport.__new__(Transport)
    transport.vehicle_type = "truck"
    transport.model = "remind"
    transport.mapping = get_vehicles_mapping()
    transport.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda _: "EUR")
    transport.is_in_index = lambda candidate, location: (
        candidate["name"] == row["targets"]["remind"] and location == "EUR"
    )
    transport.database = [
        {
            "name": "consumer",
            "location": "CH",
            "unit": "kilogram",
            "exchanges": [exchange, wrong_unit],
        },
        {
            "name": "complete freight service",
            "location": "CH",
            "unit": "ton kilometer",
            "exchanges": [protected],
        },
    ]
    transport.relink_transport_datasets()
    assert exchange["name"] == row["targets"]["remind"]
    assert (
        exchange["location"] == "EUR"
        and exchange["product"] == "transport, freight, lorry"
    )
    assert "input" not in exchange
    assert exchange["amount"] == exchange["loc"] == amount
    assert exchange["uncertainty type"] == 3 and exchange["scale"] == 0.2
    assert row["alias"] in exchange["comment"]
    assert protected["name"] == wrong_unit["name"] == row["alias"]
    assert "input" in protected and "input" in wrong_unit
