"""Installed FCEV templates retain net power, physical packs and kWh purchases."""

import csv
import json
from pathlib import Path

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR

PARAMETERS_DIR = Path(__file__).parent / "data" / "vehicle_inventory_parameters"


@pytest.mark.parametrize("family,count", [("pass_cars", 8), ("trucks", 7)])
def test_fcev_power_pack_and_purchased_capacity(family, count):
    datasets = {
        d["name"]: d
        for d in ExcelImporter(str(INVENTORY_DIR / f"lci-{family}.xlsx")).data
    }
    with (PARAMETERS_DIR / f"lci-{family}.parameters.csv").open() as stream:
        rows = [r for r in csv.DictReader(stream) if r["powertrain"] == "FCEV"]
    assert len(rows) == count
    for row in rows:
        number = lambda field: float(row[field])
        assert number("battery power") > 0
        required_dc = (
            number("power") / number("electric motor efficiency")
            + number("auxiliary power demand") / 1000
        )
        net_stack = number("fuel cell power") / number("fuel cell own consumption")
        assert net_stack + number("battery power") == pytest.approx(
            required_dc, rel=2e-6
        )
        assert net_stack / required_dc == pytest.approx(
            number("fuel cell power share"), rel=2e-6
        )
        assert number("battery cell mass") / number(
            "energy battery mass"
        ) == pytest.approx(number("battery cell mass share"), rel=2e-6)
        assert number("battery cell mass") + number(
            "battery BoP mass"
        ) == pytest.approx(number("energy battery mass"), rel=2e-6)
        assert number("battery cell mass") * number(
            "battery cell energy density"
        ) == pytest.approx(number("electric energy stored"), rel=2e-6)
        service = datasets[row["name"]]
        (vehicle,) = [
            e
            for e in service["exchanges"]
            if e["type"] == "technosphere"
            and e["unit"] == "unit"
            and e["name"].lower().startswith(("passenger car,", "lorry production,"))
        ]
        production = datasets[vehicle["name"]]
        (battery,) = [
            e
            for e in production["exchanges"]
            if e["name"] == "market for battery capacity (MIX scenario)"
        ]
        assert battery["amount"] == pytest.approx(
            number("electric energy stored")
            * (1 + number("battery lifetime replacements")),
            rel=2e-6,
        )
        for ds in (service, production):
            spec = json.loads(ds["vehicle inventory provenance"])
            assert spec["baseline vehicle parameters"]["battery power"] == number(
                "battery power"
            )
            assert "complement of net fuel-cell power" in ds["comment"]
            assert (
                "motor shaft power divided by motor/inverter efficiency"
                in ds["comment"]
            )
            if family == "pass_cars":
                assert "30 November 2020" in ds["comment"]
                assert "10 seconds" in ds["comment"]
                assert "charging remains unconstrained" in ds["comment"]
                assert "physical mass mismatch" in ds["comment"]
                assert "FCEV battery profile" in spec
                assert number("electric energy stored") == pytest.approx(
                    number("battery power") / 31.5 * (310.8 * 4 / 1000), rel=3e-6
                )
                assert number("energy battery mass") == pytest.approx(
                    number("battery power") / 31.5 * 44.6, rel=3e-6
                )
            if family == "trucks":
                assert "Documented FCEV traction-pack proxy" in ds["comment"]
                assert "May 2021" in ds["comment"]
                assert "physical mass mismatch" in ds["comment"]
                assert "FCEV battery profile" in spec
                assert number("energy battery mass") == pytest.approx(
                    number("electric energy stored") * 238 / 25, rel=2e-6
                )
                assert number("FCEV continuous charge kW") == pytest.approx(
                    number("electric energy stored") * 36 / 25, rel=2e-6
                )
                assert number("FCEV power minimum capacity kWh") <= number(
                    "electric energy stored"
                ) * (1 + 3e-6)
                assert number("battery power") <= number("FCEV 10s discharge kW")
                if row["size"] in {"3.5t", "7.5t"}:
                    expected = {"3.5t": 57, "7.5t": 129}[row["size"]]
                    assert number("power") == expected
                    assert number("rated power") == expected
                    assert (
                        spec["baseline vehicle parameters"]["rated power"] == expected
                    )
                    assert "Documented motor-only proxy" in ds["comment"]
                    assert "not a complete FCEV calibration" in ds["comment"]
