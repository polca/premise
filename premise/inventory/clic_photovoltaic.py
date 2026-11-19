"""Derive country PV electricity from reviewed, harmonized CLIC 2026 inputs."""

from __future__ import annotations

import copy
import csv
import hashlib
import json
from dataclasses import replace
from pathlib import Path

from .clic_archive import ClicInventoryError, activity_identity, content_hash

DERIVATION = "pv-country-electricity-2026"
CORE_FILE = "lci-PV-2026.xlsx"
COUNTRY_FILE = "lci-PV-2026-electricity.xlsx"
COMPATIBILITY_NAME = (
    "electricity production, photovoltaic, at 280 kWp flat-roof, single-Si"
)
REFERENCE_NAME = (
    "electricity production, photovoltaic, 250 kwp flat-roof, single-crystalline silicon, "
    "panel, mounted, annual yield 1000 kwh/kwp"
)
COUNTRY_NAMES = {
    f"electricity production, photovoltaic, {segment}"
    for segment in ("residential", "commercial", "production mix")
}


def use_derived_photovoltaics(recipe):
    """Preserve historical complete recipes, including ones using only CLIC PV."""
    return not (
        recipe.resolution_fingerprint is not None
        and recipe.derived_inventories is None
        or recipe.workbooks is not None
        and COUNTRY_FILE in recipe.workbooks
    )


def is_country_node(node):
    """Identify generated outputs in the reviewed workbook identity ledger."""
    return node["file"] == COUNTRY_FILE and node["identity"][0] in COUNTRY_NAMES


def is_derived_node(node):
    """Include the one-kWh compatibility wrapper used by hydrogen templates."""
    return is_country_node(node) or (
        node["file"] == COUNTRY_FILE and node["identity"][0] == COMPATIBILITY_NAME
    )


def source_nodes(graph, *, country=True, compatibility=False):
    """Select explicit reviewed installation and national-mix CLIC bindings."""
    nodes = [
        node
        for node in graph["nodes"]
        if node["file"] == CORE_FILE
        and (
            country
            and (
                node["identity"][0].startswith("photovoltaic installation construction")
                or node["identity"][0] == "electricity production mix, photovoltaic"
            )
            or compatibility
            and node["identity"][0] == REFERENCE_NAME
        )
    ]
    if not nodes or any(not node.get("clic_slug") for node in nodes):
        raise ClicInventoryError(
            "PV 2026 derivation requires reviewed CLIC input bindings."
        )
    return nodes


def derive_country_inventories(prepared, graph, selected_nodes, *, pins=None):
    """Build selected country recipes after migration and before destination linking.

    Source versions retain ordinary CLIC recipe pins and migration audits. A
    separate derivation pin covers the algorithm, parameters, selected outputs,
    source hashes and destination. No workbook is read by this function.

    :param prepared: Harmonized CLIC inventory with validated source cohorts.
    :param graph: Reviewed hybrid identity ledger.
    :param selected_nodes: Ledger indices for required generated outputs.
    :param pins: Optional derivation fingerprints from a locked recipe.
    :return: Owned harmonized inventory including the derived foregrounds.
    """
    from ..electricity import photovoltaic

    indices = sorted(selected_nodes)
    if not indices:
        return prepared
    if any(not is_derived_node(graph["nodes"][i]) for i in indices):
        raise ClicInventoryError(
            "PV derivation received an unsupported output identity."
        )
    country = any(is_country_node(graph["nodes"][i]) for i in indices)
    compatibility = any(not is_country_node(graph["nodes"][i]) for i in indices)
    declarations = source_nodes(graph, country=country, compatibility=compatibility)
    by_slug = {}
    for activity in prepared.activities:
        slug = activity.get("clic provenance", {}).get("slug")
        if slug:
            if slug in by_slug:
                raise ClicInventoryError(f"Duplicate PV input CLIC slug: {slug}.")
            by_slug[slug] = activity
    core, inputs = [], []
    for node in declarations:
        activity = by_slug.get(node["clic_slug"])
        if activity is None or activity_identity(activity) != tuple(node["identity"]):
            raise ClicInventoryError(
                f"PV 2026 input is missing or changed identity: {node['clic_slug']}."
            )
        references = [
            edge for edge in activity["exchanges"] if edge["type"] == "production"
        ]
        if len(references) != 1 or references[0].get("amount") != 1:
            raise ClicInventoryError(
                f"PV 2026 input changed its one-unit production reference: {node['clic_slug']}."
            )
        core.append(copy.deepcopy(activity))
        row = activity["clic provenance"]
        inputs.append({k: row[k] for k in ("slug", "version_id", "sha256")})
    directory = Path(photovoltaic.PV_PARAMETERS_PATH).parent
    parameter_path = Path(photovoltaic.PV_PARAMETERS_PATH)
    generation_path = directory / "pv_generation_2023.csv"
    parameters = json.loads(parameter_path.read_text())
    countries = {
        a["location"]
        for a in core
        if a["name"] == "electricity production mix, photovoltaic"
    }
    if country and countries != set(parameters["report_yields"]):
        raise ClicInventoryError(
            "PV 2026 national-mix inputs do not cover the reviewed yields."
        )
    with generation_path.open(newline="") as stream:
        generation = {
            row["location"]: float(row["generation_gwh"])
            for row in csv.DictReader(stream)
        }
    outputs = [graph["nodes"][i]["identity"] for i in indices]
    fingerprint = content_hash(
        {
            "derivation": DERIVATION,
            "files": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (
                    Path(__file__),
                    Path(photovoltaic.__file__),
                    parameter_path,
                    generation_path,
                )
            },
            "inputs": inputs,
            "prepared_inputs": content_hash(
                [
                    {k: v for k, v in activity.items() if k != "clic provenance"}
                    for activity in core
                ]
            ),
            "outputs": outputs,
            "target": prepared.manifest["target"],
        }
    )
    if pins is not None and dict(pins) != {DERIVATION: fingerprint}:
        raise ClicInventoryError("PV 2026 derivation differs from the locked recipe.")
    generated = {
        activity_identity(a): a
        for a in (
            photovoltaic.country_adapters(core, parameters, generation)
            if country
            else []
        )
    }
    if compatibility:
        references = [a for a in core if a["name"] == REFERENCE_NAME]
        if len(references) != 1 or references[0]["unit"] != "kilowatt hour":
            raise ClicInventoryError(
                "PV compatibility wrapper requires one reviewed 2026 kWh reference."
            )
        reference = references[0]
        for index in indices:
            node = graph["nodes"][index]
            if is_country_node(node):
                continue
            identity = tuple(node["identity"])
            record = dict(
                zip(("name", "reference product", "location", "unit"), identity)
            )
            record.update(
                type="process",
                **{
                    "production amount": 1,
                    "source": parameters["source"],
                    "comment": "Compatibility label only: 1 kWh from the 2026 250 kWp flat-roof single-Si reference at 1000 kWh/kWp/year; no capacity scaling.",
                    "exchanges": [
                        dict(record, type="production", amount=1),
                        {
                            **dict(
                                zip(
                                    ("name", "reference product", "location", "unit"),
                                    activity_identity(reference),
                                )
                            ),
                            "type": "technosphere",
                            "amount": 1,
                        },
                    ],
                },
            )
            generated[identity] = record
    core_by_identity = {activity_identity(a): a for a in core}
    roles = {}
    for filename, definitions in graph["roles"].items():
        for role, members in definitions.items():
            for index in set(indices).intersection(members):
                roles.setdefault(index, []).append(f"{filename}:{role}")
    result = []
    for index in indices:
        identity = tuple(graph["nodes"][index]["identity"])
        if identity not in generated:
            raise ClicInventoryError(f"PV generator did not produce {identity!r}.")
        activity = generated[identity]
        code = content_hash([DERIVATION, identity])
        activity.update(database="__clic__", code=code)
        activity["clic provenance"] = {
            "role": "derived inventory",
            "activity_code": code,
            "derivation": DERIVATION,
            "derivation_fingerprint": fingerprint,
            "legacy_identities": [list(identity)],
            "derived_role_bindings": sorted(roles.get(index, ())),
        }
        for edge in activity["exchanges"]:
            if edge["type"] == "production":
                edge["input"] = ("__clic__", code)
            elif edge["type"] == "technosphere":
                supplier = core_by_identity.get(activity_identity(edge))
                if supplier is not None:
                    edge["input"] = ("__clic__", supplier["code"])
        result.append(activity)
    manifest, lockfile = copy.deepcopy(prepared.manifest), copy.deepcopy(
        prepared.lockfile
    )
    manifest["derived_inventories"] = [
        {
            "id": DERIVATION,
            "fingerprint": fingerprint,
            "activity_count": len(result),
            "inputs": inputs,
            "source": parameters["source"],
        }
    ]
    lockfile["derived_inventories"] = {DERIVATION: fingerprint}
    return replace(
        prepared,
        activities=[*prepared.activities, *result],
        manifest=manifest,
        lockfile=lockfile,
    )
