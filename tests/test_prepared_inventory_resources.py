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
