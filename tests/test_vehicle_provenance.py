"""Vehicle provenance must survive import and distinguish baseline from scenario."""

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_store import compact_exchange_payload
from premise.vehicle_provenance import SPEC_KEY, STATE_KEY, record_vehicle_provenance


@pytest.mark.parametrize("family", ["car", "truck"])
@pytest.mark.parametrize("compact", [False, True])
def test_final_context_is_separate_from_baseline_and_repeatable(family, compact):
    baseline = {
        "family": family,
        "role": "transport service",
        "baseline year": 2020,
        "original model": "carculator" if family == "car" else "carculator_truck",
    }
    ds = {
        SPEC_KEY: json.dumps(baseline),
        "name": "vehicle",
        "location": "EUR",
        "unit": "kilometer",
        "reference product": "transport",
        "log parameters": {
            "efficiency change": 0.8,
            "IAM efficiency source": {"kind": "proxy intensity"},
        },
        "premise vehicle battery sizing": {"year": 2050, "capacity kWh": 80},
        "exchanges": [
            dict(type="production", amount=2),
            dict(
                type="technosphere",
                name="market group for electricity, low voltage",
                product="electricity, low voltage",
                location="EUR",
                unit="kilowatt hour",
                amount=0.3,
            ),
        ],
    }
    if compact:
        ds["exchanges"] = [compact_exchange_payload(e) for e in ds["exchanges"]]
    before = deepcopy(ds)
    context = SimpleNamespace(
        database=[ds],
        vehicle_type=family,
        model="remind",
        scenario="SSP2-NPi",
        year=2050,
    )
    record_vehicle_provenance(context)
    state = ds[STATE_KEY]
    assert state["scenario year"] == 2050 and state["baseline model year"] == 2020
    assert state["operating energy inputs"][0]["amount"] == 0.3
    assert state["reference production"] == 2
    assert state["IAM energy multiplier"] == 0.8
    assert ds[SPEC_KEY] == before[SPEC_KEY] and ds["exchanges"] == before["exchanges"]
    once = deepcopy(ds)
    record_vehicle_provenance(context)
    assert ds == once
    ds["premise vehicle battery sizing"]["capacity kWh"] = 70
    assert state["scenario battery specifications"]["capacity kWh"] == 80
    record_vehicle_provenance(context)
    assert ds[STATE_KEY]["scenario battery specifications"]["capacity kWh"] == 70


def test_forwarding_service_does_not_claim_direct_operating_inputs_or_hardware():
    ds = {
        SPEC_KEY: {
            "family": "car",
            "role": "transport service",
            "baseline year": 2020,
            "original model": "carculator",
        },
        "name": "vehicle",
        "reference product": "transport",
        "unit": "kilometer",
        "location": "RER",
        "emptied": True,
        "exchanges": [dict(type="production", amount=1)],
    }
    record_vehicle_provenance(
        SimpleNamespace(
            database=[ds],
            vehicle_type="car",
            model="remind",
            scenario="SSP2-NPi",
            year=2050,
        )
    )
    state = ds[STATE_KEY]
    assert state["role"] == "forwarding service"
    assert state["operating energy inputs"] == []
    assert "hardware basis" not in state and "IAM energy multiplier" not in state


def test_installed_workbooks_have_sources_and_match_their_companions():
    from bw2io import ExcelImporter

    for stem, count in (("lci-pass_cars", 162), ("lci-trucks", 127)):
        path = INVENTORY_DIR / f"{stem}.xlsx"
        importer = ExcelImporter(str(path))
        assert importer.db_name == stem and len(importer.data) == count
        sidecar = json.loads(path.with_suffix(".provenance.json").read_text())
        assert sidecar["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        info = sidecar["generation provenance"]["artifacts"][
            "vehicle_parameters.csv"
        ]
        assert (
            hashlib.sha256(
                (INVENTORY_DIR / info["companion file"]).read_bytes()
            ).hexdigest()
            == info["sha256"]
        )
        for ds in importer.data:
            assert ds["source"] and ds["model"] and ds["model version"]
            spec = json.loads(ds[SPEC_KEY])
            assert spec["baseline year"] == 2020
            assert (
                spec["model generation date"]
                == sidecar["generation provenance"]["model generation date"]
            )
            assert ds["source"] == spec["activity source"]
            assert "https://doi.org/" in ds["model publication"]
    importer = ExcelImporter(str(INVENTORY_DIR / "lci-truck-legacy-components.xlsx"))
    assert importer.db_name == "lci-truck-legacy-components"
    assert len(importer.data) == 26
    sidecar = json.loads((INVENTORY_DIR / "lci-trucks.provenance.json").read_text())
    assert (
        sidecar["legacy_components_sha256"]
        == hashlib.sha256(
            (INVENTORY_DIR / "lci-truck-legacy-components.xlsx").read_bytes()
        ).hexdigest()
    )
    for ds in importer.data:
        assert ds["source"]
        assert SPEC_KEY not in ds and "model publication" not in ds
        assert (
            json.loads(ds["inventory provenance"])["role"]
            == "retained legacy component"
        )
