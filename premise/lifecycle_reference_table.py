"""Write declared reference-event assumptions without changing inventory inputs.

Operating-service families with allocated asset disposal use a stationary-stock
proxy: fixed life, constant annual entry and equal contribution of each annual
age. This is an assumption, not reconstructed IAM or empirical cohort data.
"""

import csv
import json
import math
from pathlib import Path


def reference_for(row):
    name = row["name"].lower()
    tag = row["temporal_tag"]
    if name.startswith("market ") or tag in {"maintenance", "end_of_life", "market"}:
        return "lifecycle_event"
    produced_assets = (
        "bicycle, electric",
        "kick scooter, battery electric",
        "motorbike, battery electric",
        "scooter, battery electric",
        "battery auxiliaries supply and end-of-life treatment",
        "battery pack component production and end-of-life treatment",
    )
    if tag == "stock_asset" or name.startswith(produced_assets):
        return "new_asset"
    fleet_services = (
        "carbon dioxide, captured",
        "electricity production,",
        "electricity supply, high voltage, from vanadium-redox",
        "heat, residential, electric storage heater",
        "use phase, for lis battery",
    )
    if name.startswith(fleet_services) or (
        name.startswith("hydrogen production,") and "electrolysis" in name
    ):
        return "existing_fleet"
    return "current_output"


def main():
    path = Path(__file__).resolve().parent / "data/trails/temporal_distributions.csv"
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        rows = list(reader)
    extras = [
        "timing_reference",
        "fleet_lifetime",
        "fleet_age_offsets",
        "fleet_age_weights",
        "timing_reference_notes",
    ]
    fields = [field for field in fields if field not in extras] + extras
    # The existing table can have duplicate identities. Give every copy the
    # reference event and lifetime of its last effective specification.
    effective = {(row["name"], row["reference product"]): row for row in rows}
    records = {}
    notes = {
        "new_asset": "Newly produced asset. Fixed retirement life, without an additional year.",
        "current_output": "Waste treatment allocated to current output. No new full asset lifetime.",
        "lifecycle_event": "Current market handover or lifecycle substep. No new full asset lifetime.",
        "existing_fleet": "Explicit stationary-stock proxy with fixed life and uniform annual ages 1..floor(L). Not empirical or IAM-specific cohort data.",
    }
    for key, row in effective.items():
        reference = reference_for(row)
        record = {
            "timing_reference": reference,
            "fleet_lifetime": "",
            "fleet_age_offsets": "",
            "fleet_age_weights": "",
            "timing_reference_notes": notes[reference],
        }
        if reference == "existing_fleet":
            try:
                lifetime = float(row["lifetime"])
            except (ValueError, TypeError) as error:
                raise ValueError(f"Missing fleet lifetime for {key}") from error
            if not math.isfinite(lifetime) or lifetime < 1:
                raise ValueError(
                    f"Annual fleet proxy requires a finite lifetime >=1 for {key}"
                )
            ages = list(range(1, int(math.floor(lifetime)) + 1))
            record["fleet_lifetime"] = str(lifetime)
            weights = [1 / len(ages)] * len(ages)
            weights[-1] += 1 - sum(weights)
            record["fleet_age_offsets"] = json.dumps([-age for age in ages])
            record["fleet_age_weights"] = json.dumps(weights)
        records[key] = record
    for row in rows:
        row.update(records[row["name"], row["reference product"]])
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    from collections import Counter

    print(
        json.dumps(
            dict(Counter(r["timing_reference"] for r in records.values())), indent=2
        )
    )


if __name__ == "__main__":
    main()
