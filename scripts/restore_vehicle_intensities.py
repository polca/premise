"""Append mapped vehicle intensities from a matching original REMIND scenario.

Example (PREMISE_KEY must be set):
    python scripts/restore_vehicle_intensities.py --model remind \
        --original original.mif --scenario encrypted.csv --output restored.csv

The output must not already exist. Existing plaintext rows are preserved byte
for byte; only missing mapped intensity rows are appended before encryption.
"""

import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from cryptography.fernet import Fernet

IDENTITY = ["Region", "Variable", "Unit"]


def mapped_intensities(model):
    directory = Path(__file__).resolve().parents[1] / "premise/iam_variables_mapping"
    return {
        spec["energy_intensity_aliases"][model]
        for filename in ("transport_passenger_cars.yaml", "transport_road_freight.yaml")
        for spec in yaml.safe_load((directory / filename).read_text()).values()
        if model in spec.get("energy_intensity_aliases", {})
    }


def append_intensity_rows(payload, original, variables):
    """Validate transport provenance and append missing region/variable rows."""
    current = pd.read_csv(io.BytesIO(payload), sep=";")
    for frame in (current, original):
        if frame.duplicated(["Region", "Variable"]).any():
            raise ValueError("Duplicate region/variable identities in scenario")
    for field in ("Model", "Scenario"):
        if set(current[field]) != set(original[field]):
            raise ValueError(f"Original and installed scenario differ in {field}")
    years = [column for column in current if str(column).isdigit()]
    if not years or "2020" not in years or any(y not in original for y in years):
        raise ValueError("Original must contain the installed years, including 2020")
    source = original.set_index(IDENTITY)
    retained = current.set_index(IDENTITY)
    transport = retained.loc[
        retained.index.get_level_values("Variable").str.startswith(
            ("ES|Transport|", "FE|Transport|")
        )
    ]
    if transport.empty or not transport.index.isin(source.index).all():
        raise ValueError("Original does not cover installed transport variables")
    if not np.isclose(
        transport[years].to_numpy(float),
        source.loc[transport.index, years].to_numpy(float),
        rtol=1e-10,
        atol=1e-12,
        equal_nan=True,
    ).all():
        raise ValueError("Original transport values differ: scenario version mismatch")

    additions = original.loc[
        original.Variable.isin(variables)
        & original.Region.isin(current.Region.unique())
    ].copy()
    if additions.empty:
        raise ValueError("No mapped vehicle intensity rows found in original")
    if not additions.Unit.isin(["MJ/vehkm", "MJ/vkm"]).all():
        raise ValueError("Vehicle intensity rows must use MJ/vehicle-km")
    known_units = current.set_index(["Region", "Variable"]).Unit
    incoming_units = additions.set_index(["Region", "Variable"]).Unit
    overlap = known_units.index.intersection(incoming_units.index)
    if not known_units.loc[overlap].equals(incoming_units.loc[overlap]):
        raise ValueError("Existing intensity units conflict with original scenario")
    present = additions.set_index(IDENTITY).index.isin(retained.index)
    existing = additions.loc[present].set_index(IDENTITY)
    if not np.isclose(
        existing[years].to_numpy(float),
        retained.loc[existing.index, years].to_numpy(float),
        rtol=1e-10,
        atol=1e-12,
        equal_nan=True,
    ).all():
        raise ValueError("Existing intensities conflict with original scenario")
    additions = additions.loc[~present]
    appended = io.StringIO()
    writer = csv.writer(appended, delimiter=";", lineterminator="\n")
    # csv rather than a full DataFrame rewrite preserves all existing rows.
    for row in additions.reindex(columns=current.columns).itertuples(
        index=False, name=None
    ):
        writer.writerow(["" if pd.isna(value) else value for value in row])
    separator = b"" if not len(additions) or payload.endswith((b"\n", b"\r")) else b"\n"
    result = payload + separator + appended.getvalue().encode("latin-1")
    return result, {
        "retained_rows": len(current),
        "appended_rows": len(additions),
        "verified_transport_rows": len(transport),
        "retained_payload_sha256": hashlib.sha256(payload).hexdigest(),
        "restored_payload_sha256": hashlib.sha256(result).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=("remind", "remind-eu"), required=True)
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output exists; choose a new file and review before installation")
    key = os.environ.get("PREMISE_KEY")
    if not key:
        parser.error("Set PREMISE_KEY in the environment")
    cipher = Fernet(key.encode())
    original = pd.read_csv(args.original, sep=";")
    encrypted = args.scenario.read_bytes()
    restored, record = append_intensity_rows(
        cipher.decrypt(encrypted), original, mapped_intensities(args.model)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as stream:
        stream.write(cipher.encrypt(restored))
    record.update(
        model=args.model,
        original_sha256=hashlib.sha256(args.original.read_bytes()).hexdigest(),
        previous_encrypted_sha256=hashlib.sha256(encrypted).hexdigest(),
        restored_encrypted_sha256=hashlib.sha256(args.output.read_bytes()).hexdigest(),
    )
    args.output.with_suffix(".manifest.json").write_text(
        json.dumps(record, indent=2) + "\n"
    )
    print(json.dumps(record))


if __name__ == "__main__":
    main()
