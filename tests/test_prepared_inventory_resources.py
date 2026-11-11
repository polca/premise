"""Screening supplements resolve atomically against the actual destination."""

import copy
import json

import pytest

from premise.inventory.prepared_resources import _identity, prepare_inventory_resource
from premise.core.filesystem_constants import DATA_DIR

GEOTHERMAL_PATH = DATA_DIR / "clic/uvek_2026_geothermal_screening.json"


def background():
    records = json.loads(GEOTHERMAL_PATH.read_text())["activities"]
    codes = {a["code"] for a in records}
    providers = {}
    for a in records:
        for exchange in a["exchanges"]:
            if exchange["type"] == "technosphere" and exchange["input"][1] not in codes:
                providers[_identity(exchange)] = {
                    **{
                        k: exchange[k]
                        for k in ("name", "reference product", "location", "unit")
                    },
                    "database": "destination",
                    "code": str(len(providers)),
                    "exchanges": [],
                }
    return list(providers.values()), records


def test_supplement_relinks_owned_copy_and_keeps_complete_source_quantities():
    database, template = background()
    before = copy.deepcopy(database)
    generated = prepare_inventory_resource(database, GEOTHERMAL_PATH)
    assert database[: len(before)] == before
    assert len(generated) == 3
    assert prepare_inventory_resource(database, GEOTHERMAL_PATH) == []
    for actual, source in zip(generated, template, strict=True):
        assert actual["premise supplementary inventory"]["screening_only"]
        for x, original in zip(actual["exchanges"], source["exchanges"], strict=True):
            for key, value in original.items():
                if key == "input":
                    continue
                if key == "reference product":
                    assert x["product"] == value
                elif key == "categories":
                    assert x[key] == tuple(value)
                else:
                    assert x[key] == value
    generated[0]["exchanges"][1]["amount"] = 999
    assert json.loads(GEOTHERMAL_PATH.read_text())["activities"] == template


@pytest.mark.parametrize("change", ["absent", "ambiguous", "partial"])
def test_missing_ambiguous_or_partial_supplement_does_not_mutate(change):
    database, template = background()
    if change == "absent":
        database.pop()
    elif change == "ambiguous":
        database.append({**database[-1], "code": "duplicate"})
    else:
        database.append(template[0])
    before = copy.deepcopy(database)
    with pytest.raises(ValueError):
        prepare_inventory_resource(database, GEOTHERMAL_PATH)
    assert database == before


def test_supplier_quantity_guard_refuses_changed_basis_without_mutation(tmp_path):
    database, template = background()
    supplier = database[0]
    supplier["exchanges"] = [{"type": "production", "amount": 1}]
    definition = json.loads(GEOTHERMAL_PATH.read_text())
    definition["supplier_checks"] = [
        {
            "identity": {
                key: supplier[key]
                for key in ("name", "reference product", "location", "unit")
            },
            "exchanges": [{"selector": {"type": "production"}, "amount": 1}],
        }
    ]
    path = tmp_path / "guarded.json"
    path.write_text(json.dumps(definition))
    for value in (0.5, True, float("inf")):
        supplier["exchanges"][0]["amount"] = value
        before = copy.deepcopy(database)
        with pytest.raises(ValueError, match="quantity changed"):
            prepare_inventory_resource(database, path)
        assert database == before
    supplier["exchanges"][0]["amount"] = 1
    assert len(prepare_inventory_resource(database, path)) == len(template)


def test_resource_biosphere_uses_selected_database_name():
    database, _ = background()
    generated = prepare_inventory_resource(
        database, GEOTHERMAL_PATH, biosphere_name="custom-biosphere"
    )
    assert all(
        e["input"][0] == "custom-biosphere"
        for a in generated
        for e in a["exchanges"]
        if e["type"] == "biosphere"
    )


def test_active_resource_dependency_is_selected_only_when_its_role_is_used(monkeypatch):
    from types import SimpleNamespace
    import xarray as xr
    from premise.inventory import activity_maps, clic_requirements
    from premise.inventory.prepared_resources import inventory_resource_paths

    definitions = {
        "ordinary": {"inventory_resource": "ordinary.json"},
        "CCS": {
            "inventory_resource": "capture.json",
            "inventory_requires_active_role": True,
        },
    }
    monkeypatch.setattr(
        clic_requirements,
        "registry",
        lambda: {"sector_mappings": {"heat": ["heat.yaml"]}},
    )
    monkeypatch.setattr(
        activity_maps, "get_mapping", lambda *args, **kwargs: definitions
    )

    def scenario(amount):
        array = xr.DataArray(
            [[[amount]]],
            dims=("region", "variables", "year"),
            coords={"region": ["EUR"], "variables": ["CCS"], "year": [2050]},
        )
        return {
            "model": "remind",
            "iam data": SimpleNamespace(secondary_heat_supply=array),
        }

    assert inventory_resource_paths(["heat"], [scenario(0)], "uvek") == {
        "ordinary.json"
    }
    assert inventory_resource_paths(["heat"], [{"model": "remind"}], "uvek") == {
        "ordinary.json"
    }
    assert inventory_resource_paths(["heat"], [scenario(0), scenario(1)], "uvek") == {
        "ordinary.json",
        "capture.json",
    }


def test_prepared_dependencies_validate_and_deduplicate(tmp_path, monkeypatch):
    from premise.core import filesystem_constants
    from premise.inventory.prepared_resources import inventory_resource_roots

    monkeypatch.setattr(filesystem_constants, "DATA_DIR", tmp_path)
    (tmp_path / "one.json").write_text(
        json.dumps({"clic_roots": ["capture", "capture"]})
    )
    assert inventory_resource_roots(["one.json"]) == {"capture"}
    (tmp_path / "bad.json").write_text(json.dumps({"clic_roots": "capture"}))
    with pytest.raises(ValueError, match="nonempty strings"):
        inventory_resource_roots(["one.json", "bad.json"])
