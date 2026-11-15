"""Native freight aliases retain complete services, quantities and locations."""

import copy

import pandas as pd
import pytest
from wurst.errors import NoResults

from premise.core.utils import DATA_DIR
from premise.inventory.activity_maps import get_mapping
from premise.materials.metals import (
    Metals,
    MetalsConfigError,
    select_transport_provider,
)


def providers():
    review = get_mapping(DATA_DIR / "metals/freight_providers.yaml", "uvek_aliases")
    return {
        mode: {
            **row["fltr"],
            "exchanges": [
                {"type": "production", "amount": 1},
                {"type": "technosphere", "amount": 0.2, "unit": "kilogram"},
            ],
        }
        for mode, row in review.items()
    }


@pytest.mark.parametrize("mode", ["air", "sea", "railway", "road"])
def test_select_complete_native_service_without_components_or_geography_fallback(mode):
    native = providers()[mode]
    component = copy.deepcopy(native)
    component["name"] += ", Infrastruktur"
    elsewhere = copy.deepcopy(native)
    elsewhere["location"] = "GLO"
    wrong_unit = copy.deepcopy(native)
    wrong_unit["unit"] = "unit"
    graph = [component, elsewhere, wrong_unit, native]
    original = copy.deepcopy(graph)
    assert select_transport_provider(graph, mode, family="uvek") is native
    assert graph == original
    with pytest.raises(NoResults):
        select_transport_provider(graph[:-1], mode, family="uvek")


@pytest.mark.parametrize("change", ["negative output", "helper", "multiple outputs"])
def test_invalid_native_service_reference_is_refused(change):
    native = providers()["air"]
    if change == "negative output":
        native["exchanges"][0]["amount"] = -1
    elif change == "helper":
        native["clic provenance"] = {"role": "generated helper"}
    else:
        native["exchanges"].append({"type": "production", "amount": 1})
    with pytest.raises(MetalsConfigError, match="freight reference"):
        select_transport_provider([native], "air", family="uvek")


def test_existing_market_distance_and_share_arithmetic_keeps_native_provider_locations():
    model = Metals.__new__(Metals)
    model.metals_transport_activities = providers()
    model.alt_names = {"metal": "Metal"}
    model.get_weighted_average_distance = lambda origin, metal: pd.DataFrame(
        [
            {"TransportMode Label": mode, "Weighted Distance (km)": distance * factor}
            for mode, distance in [
                ("air", 1000),
                ("sea", 2000),
                ("railway", 3000),
                ("road", 4000),
            ]
            for factor in [1 if origin == "CH" else 2]
        ]
    )
    market = {
        "exchanges": [
            {
                "type": "technosphere",
                "unit": "kilogram",
                "location": "CH",
                "amount": 0.25,
            },
            {
                "type": "technosphere",
                "unit": "kilogram",
                "location": "ZA",
                "amount": 0.75,
            },
        ]
    }
    original = copy.deepcopy(market)
    edges = model.add_transport_to_market(market, "metal")
    assert len(edges) == 4
    for mode, expected in [("air", 1.75), ("sea", 3.5), ("railway", 5.25), ("road", 7)]:
        native = model.metals_transport_activities[mode]
        (edge,) = [e for e in edges if e["name"] == native["name"]]
        assert edge["location"] == native["location"]
        assert edge["product"] == native["reference product"]
        assert edge["amount"] == expected and edge["unit"] == "ton kilometer"
    assert market == original


def test_resource_lookup_uses_the_selected_biosphere_not_compatibility_version(
    monkeypatch,
):
    model = Metals.__new__(Metals)
    model.version = "3.12"
    model.biosphere_version = "3.10"

    def flows(version):
        assert version == "3.10"
        return [("Iridium", "natural resource", "in ground", "kilogram")]

    monkeypatch.setattr("premise.materials.metals.biosphere_flows_dictionary", flows)
    assert model.get_biosphere_in_ground_resource_flow_names() == {"Iridium"}
