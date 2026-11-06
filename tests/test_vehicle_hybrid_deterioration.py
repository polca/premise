"""Check exported hybrid exhaust against source factors, separately from the model."""

import csv
from pathlib import Path

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR

PARAMETERS_DIR = Path(__file__).parent / "data" / "vehicle_inventory_parameters"

# Source: carculator_utils/data/emission_factors/{car,truck}/
# EF_HBEFA42_exhaust.csv, car/EF_HBEFA42_non_exhaust.csv and degradation_EF.csv.
# Snapshot of the retained 2020 factors; changing them requires source review.
# Cars: hot g/MJ, cold-start g/event, endpoint at 200,000 vehicle-km.
CAR_FACTORS = {
    ("p", "Carbon monoxide"): (0.166396903, 5.888487816, 1.3),
    ("p", "Nitrogen oxides"): (0.012984357, 0.241189048, 1.3),
    ("d", "Carbon monoxide"): (0.006086595, 0.389243215, 1.4),
    ("d", "Nitrogen oxides"): (0.018483213, 0.306156099, 1.15),
}
# Trucks: hot NOx g/MJ; endpoint 2.6 at 890,000 vehicle-km. No cold-start table.
TRUCK_NOX = {
    "3.5t": 0.158408178,
    "7.5t": 0.15036943,
    "18t": 0.147672791,
    "26t": 0.162031825,
    "32t": 0.15261611,
    "40t": 0.157203227,
    "60t": 0.162514443,
}


@pytest.mark.parametrize("family,count", [("pass_cars", 32), ("trucks", 16)])
def test_installed_hybrid_hot_exhaust_ages_once_with_separate_cold_starts(
    family, count
):
    datasets = {
        d["name"]: d
        for d in ExcelImporter(str(INVENTORY_DIR / f"lci-{family}.xlsx")).data
    }
    with (PARAMETERS_DIR / f"lci-{family}.parameters.csv").open() as stream:
        rows = [
            r
            for r in csv.DictReader(stream)
            if r["powertrain"].startswith(("HEV-", "PHEV-"))
        ]
    assert len(rows) == count
    for row in rows:
        ds = datasets[row["name"]]
        lifetime = float(row["lifetime kilometers"])
        for pollutant in (
            ["Carbon monoxide", "Nitrogen oxides"]
            if family == "pass_cars"
            else ["Nitrogen oxides"]
        ):
            names = {pollutant, pollutant + ", fossil", pollutant + ", non-fossil"}
            actual = sum(
                e["amount"]
                for e in ds["exchanges"]
                if e["type"] == "biosphere"
                and e["name"] in names
                and (
                    e["categories"].split("::")[0]
                    if isinstance(e["categories"], str)
                    else e["categories"][0]
                )
                == "air"
            )
            if family == "pass_cars":
                hot, cold, endpoint = CAR_FACTORS[row["powertrain"][-1], pollutant]
                hot_grams = (
                    float(row["fuel_energy_MJ_per_vkm"])
                    * hot
                    * (1 + (endpoint - 1) * lifetime / 400000)
                )
                mode_share = (
                    1 - float(row["electric utility factor"])
                    if row["powertrain"].startswith("PHEV-")
                    else 1
                )
                cold_grams = (
                    cold
                    * float(row["starts per day"])
                    * 365
                    / float(row["kilometers per year"])
                    * mode_share
                )
                expected = (hot_grams + cold_grams) / 1000
            else:
                expected = (
                    float(row["fuel_energy_MJ_per_tkm"])
                    * TRUCK_NOX[row["size"]]
                    * (1 + 1.6 * lifetime / 1780000)
                    / 1000
                )
            assert actual == pytest.approx(expected, rel=3e-6), (row["name"], pollutant)
            assert "uncalibrated hybrid ageing proxy" in ds["comment"]
