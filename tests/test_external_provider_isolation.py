"""External efficiency targets must not change shared background inventories."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.external import ExternalScenario, adjust_efficiency
from premise.external_data_validation import check_inventories, copy_external_provider


def fixture(pathways=("use_a", "use_b"), location="AA", absolute=False):
    name = "generator_with_underscores"
    source = {
        "name": name,
        "reference product": "electricity",
        "unit": "kilowatt hour",
        "location": location,
        "code": "source",
        "database": "background",
        "current efficiency": 0.4,
        "exchanges": [
            {
                "name": name,
                "product": "electricity",
                "location": location,
                "unit": "kilowatt hour",
                "type": "production",
                "amount": 1,
                "input": ("background", "source"),
                "properties": {"allocation": 0.7},
            },
            {
                "name": "fuel",
                "product": "fuel",
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 2,
                "input": ("background", "fuel"),
            },
            {"name": "emission", "unit": "kilogram", "type": "biosphere", "amount": 3},
        ],
    }
    config = {
        "production pathways": {
            v: {
                "ecoinvent alias": {
                    "name": name,
                    "reference product": "electricity",
                    "exists in original database": True,
                },
                "efficiency": [{"variable": f"eff_{v}", "absolute": absolute}],
            }
            for v in pathways
        }
    }
    production = xr.DataArray(
        [[[1.0, 1.0] for _ in pathways]],
        coords={"region": ["AA"], "variables": list(pathways), "year": [2030, 2040]},
        dims=["region", "variables", "year"],
    )
    efficiency = xr.DataArray(
        [[[2.0, 2.0], [4.0, 4.0]][: len(pathways)]],
        coords={
            "region": ["AA"],
            "variables": [f"eff_{v}" for v in pathways],
            "year": [2030, 2040],
        },
        dims=["region", "variables", "year"],
    )
    if absolute:
        efficiency = efficiency / 10
    return source, config, {"production volume": production, "efficiency": efficiency}


@pytest.fixture(autouse=True)
def geography(monkeypatch):
    monkeypatch.setattr("premise.external_data_validation.Geomap", lambda model: None)


@pytest.mark.parametrize("absolute", [False, True])
@pytest.mark.parametrize("pathways", [("use_a", "use_b"), ("different demand",)])
def test_efficiency_copies_preserve_background_and_independent_targets(
    absolute, pathways
):
    source, config, data = fixture(pathways, absolute=absolute)
    before = deepcopy(source)
    consumer = {
        "name": "background market",
        "reference product": "electricity",
        "unit": "kilowatt hour",
        "location": "AA",
        "exchanges": [
            {
                "name": source["name"],
                "product": "electricity",
                "location": "AA",
                "amount": 0.001,
            }
        ],
    }
    original_consumer = deepcopy(consumer)
    _, database, checked, mapping = check_inventories(
        config, [], data, [source, consumer], 2030, "arbitrary"
    )
    assert source == before and consumer == original_consumer
    copies = [d for d in database if "external scenario source" in d]
    assert len(copies) == len(pathways)
    assert len({d["code"] for d in database if "code" in d}) == len(copies) + 1
    for i, (v, ds) in enumerate(zip(pathways, copies)):
        assert ds["external scenario source"]["name"] == source["name"]
        assert (
            checked["production pathways"][v]["ecoinvent alias"]["name"] == ds["name"]
        )
        assert mapping[v][0]["name"] == ds["name"]
        assert ds["exchanges"][0]["name"] == ds["name"]
        assert ds["exchanges"][0]["properties"] == source["exchanges"][0]["properties"]
        assert "input" not in ds["exchanges"][0]
        assert ds["exchanges"][1]["input"] == ("background", "fuel")
        adjust_efficiency(ds, {}, {})
        factor = (0.4 / (0.2 * (i + 1))) if absolute else 1 / (2 * (i + 1))
        assert ds["exchanges"][1]["amount"] == pytest.approx(2 * factor)
        assert ds["exchanges"][2]["amount"] == pytest.approx(3 * factor)
    assert source == before and consumer == original_consumer
    # Market supplier lookup uses the rewritten alias and selects only its copy.
    transformer = object.__new__(ExternalScenario)
    transformer.database = database
    for v, ds in zip(pathways, copies):
        assert transformer.fetch_potential_suppliers(
            ["AA"], mapping[v][0]["name"], "electricity"
        ) == [ds]


def test_repeated_datapackages_do_not_share_efficiency_copies():
    source, config, data = fixture(("use_a",))
    config2 = deepcopy(config)
    before = deepcopy(source)
    _, database, _, first = check_inventories(
        config, [], data, [source], 2030, "arbitrary"
    )
    _, database, _, second = check_inventories(
        config2, [], data, database, 2030, "arbitrary"
    )
    assert first["use_a"][0]["name"] != second["use_a"][0]["name"]
    assert len(database) == 3
    assert source == before


def test_geographic_proxy_is_cloned_once_for_several_regions(monkeypatch):
    source, config, data = fixture(("use_a",), location="GLO")
    other = deepcopy(source)
    other.update(location="RoW", code="row")
    original = deepcopy([source, other])
    data = {k: v.reindex(region=["AA", "BB"]).fillna(1) for k, v in data.items()}
    monkeypatch.setattr(
        "premise.external_data_validation.Geomap",
        lambda model: SimpleNamespace(
            iam_regions=[],
            geo=SimpleNamespace(contained=lambda loc: [], intersects=lambda loc: []),
            ecoinvent_to_iam_location=lambda loc: "World",
        ),
    )
    _, database, _, mapping = check_inventories(
        config, [], data, [source, other], 2030, "arbitrary"
    )
    assert [source, other] == original
    assert len(database) == 3
    assert database[-1]["region mapping"] == {"AA": "GLO", "BB": "GLO"}
    assert mapping["use_a"][0]["name"] == database[-1]["name"]


def test_explicit_replacement_scope_is_kept_on_copy():
    source, config, data = fixture(("use_a",))
    settings = config["production pathways"]["use_a"]
    settings.update(
        {
            "replaces": [{"name": "old", "product": "electricity"}],
            "replaces in": [{"name": "custom consumer"}],
            "replacement ratio": 0.5,
        }
    )
    _, database, _, _ = check_inventories(config, [], data, [source], 2030, "arbitrary")
    assert "replaces" not in source
    for key in ("replaces", "replaces in", "replacement ratio"):
        assert database[-1][key] == settings[key]


def test_self_input_tracks_copy_and_source_remains_unchanged():
    source, _, _ = fixture(("use_a",))
    source["exchanges"].append(
        {
            "name": source["name"],
            "product": "electricity",
            "location": "AA",
            "unit": "kilowatt hour",
            "type": "technosphere",
            "amount": 0.1,
            "input": ("background", "source"),
        }
    )
    before = deepcopy(source)
    copied = copy_external_provider(source, "another name")
    assert source == before
    assert copied["exchanges"][-1]["name"] == "another name"
    assert "input" not in copied["exchanges"][-1]


def test_efficiency_proxy_requests_regionalization_before_adjustment():
    source, config, data = fixture(("use_a",), location="GLO")
    before = deepcopy(source)
    _, database, checked, mapping = check_inventories(
        config, [], data, [source], 2030, "arbitrary"
    )
    assert source == before
    assert database[-1]["regionalize"] is True
    assert database[-1]["regions"] == ["AA"]
    assert (
        checked["production pathways"]["use_a"]["ecoinvent alias"]["regionalize"]
        is True
    )


def test_shared_non_efficiency_copy_keeps_requested_regionalization():
    source, config, data = fixture(location="GLO")
    for pathway in config["production pathways"].values():
        pathway.pop("efficiency")
    config["production pathways"]["use_a"]["ecoinvent alias"]["regionalize"] = True
    _, database, checked, _ = check_inventories(
        config, [], data, [source], 2030, "arbitrary"
    )
    assert len(database) == 2
    assert database[0]["regionalize"] is True
    assert database[1]["regionalize"] is True
    assert database[1]["production volume variable"] == "use_b"
    assert "adjust efficiency" not in database[1]
