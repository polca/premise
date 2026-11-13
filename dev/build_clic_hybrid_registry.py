"""Index retained workbooks and their public CLIC replacements for maintainer review.

The generated registry contains identities, dependency references and hashes;
it does not copy inventory coefficients out of licensed workbooks.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml
from bw2io import ExcelImporter

from premise.database.new_database import NewDatabase, config
from premise.inventory.clic_archive import ClicArchive, activity_identity
from premise.inventory.profiles import scientific_record
from build_clic_role_registry import SECTOR_FILES, matches, format_registry


def key(record):
    result = list(activity_identity(record))
    for index in (0, 1):
        if isinstance(result[index], str):
            result[index] = result[index].strip().casefold()
    return tuple(result)


def cdr_support_roles(nodes, definitions):
    """Bind the existing support filters to retained, unit-bearing identities."""
    fields = ("name", "reference product", "location", "unit")
    return {
        role: [
            node["id"]
            for node in nodes
            if matches(dict(zip(fields, node["identity"])), definition)
        ]
        for role, definition in definitions.items()
    }


def build(archive, registry):
    sources = defaultdict(set)
    paths = {}
    for version in config["SUPPORTED_EI_VERSIONS"]:
        for model in ("cutoff", "consequential", "EN15804"):
            owner = NewDatabase.__new__(NewDatabase)
            owner.version, owner.system_model = version, model
            for path, native_version in owner._default_inventory_sources():
                sources[path.name].add(native_version)
                paths[path.name] = path
    current, history, original = (defaultdict(set) for _ in range(3))
    for did, version in archive.latest.items():
        for record in (version, version["inventory_snapshot"]):
            current[key(record)].add(did)
    for version in archive.versions.values():
        did, snapshot = version["dataset_id"], version["inventory_snapshot"]
        for record in (version, snapshot):
            history[key(record)].add(did)
        if snapshot.get("database") and snapshot.get("code"):
            original[(snapshot["database"], snapshot["code"])].add(did)

    def public_match(record, source_versions):
        found = current.get(key(record)) or history.get(key(record), set())
        if len(found) > 1:
            same_profile = {
                did
                for did in found
                if archive.latest[did]["background_version"] in source_versions
            }
            if same_profile:
                found = same_profile
        if len(found) > 1 and record.get("database") and record.get("code"):
            by_code = original.get((record["database"], record["code"]), set())
            found = (found & by_code) or found
        return archive.datasets[next(iter(found))]["slug"] if len(found) == 1 else None

    filenames = sorted(paths)
    with contextlib.redirect_stdout(io.StringIO()):
        with ThreadPoolExecutor(max_workers=4) as executor:
            importers = list(
                executor.map(ExcelImporter, (paths[name] for name in filenames))
            )
    nodes, by_identity, by_code = [], defaultdict(list), defaultdict(list)
    file_rows = {}
    raw_nodes = []
    for filename, importer in zip(filenames, importers):
        node_ids = []
        for record in importer.data:
            index = len(nodes)
            node = {
                "id": index,
                "file": filename,
                "identity": list(activity_identity(record)),
                "clic_slug": public_match(record, sources[filename]),
                "inputs": [],
                "science_hash": hashlib.sha256(
                    json.dumps(scientific_record(record), sort_keys=True).encode()
                ).hexdigest(),
            }
            nodes.append(node)
            raw_nodes.append(record)
            node_ids.append(index)
            by_identity[key(record)].append(index)
            if record.get("database") and record.get("code"):
                by_code[(record["database"], record["code"])].append(index)
        file_rows[filename] = {
            "sha256": hashlib.sha256(paths[filename].read_bytes()).hexdigest(),
            "source_versions": sorted(sources[filename]),
            "nodes": node_ids,
        }
    for node, record in zip(nodes, raw_nodes):
        for exchange in record["exchanges"]:
            if exchange.get("type") != "technosphere":
                continue
            candidates = by_identity.get(key(exchange), [])
            reference = exchange.get("input")
            if (
                not candidates
                and isinstance(reference, (list, tuple))
                and len(reference) == 2
            ):
                candidates = [
                    index
                    for index in by_code.get(tuple(reference), ())
                    if nodes[index]["identity"][3] == exchange["unit"]
                ]
            if candidates:
                node["inputs"].append({"nodes": candidates})
            else:
                match = public_match(exchange, sources[node["file"]])
                if match:
                    node["inputs"].append({"clic_slug": match})
    role_nodes = {}
    mapping_dir = Path(__file__).resolve().parents[1] / "premise/iam_variables_mapping"
    mapping_files = [
        mapping_dir / name for name in dict.fromkeys(SECTOR_FILES.values())
    ]
    mapping_files.append(mapping_dir.parent / "data/mining/tailings_activities.yaml")
    for mapping_path in mapping_files:
        filename = mapping_path.name
        definitions = yaml.safe_load(mapping_path.read_text())
        role_nodes[filename] = {
            role: [
                node["id"]
                for node, record in zip(nodes, raw_nodes)
                if matches(record, definition["ecoinvent_aliases"])
            ]
            for role, definition in definitions.items()
            if "ecoinvent_aliases" in definition
        }
    registry["hybrid_sources"] = {
        "files": file_rows,
        "nodes": nodes,
        "roles": role_nodes,
    }
    support_path = mapping_dir.parent / "data/cdr/cdr_activities.yaml"
    role_nodes[support_path.name] = cdr_support_roles(
        nodes, yaml.safe_load(support_path.read_text())
    )
    return registry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    args = parser.parse_args()
    registry = build(
        ClicArchive.read(args.archive), json.loads(args.registry.read_text())
    )
    args.registry.write_text(format_registry(registry) + "\n")
    data = registry["hybrid_sources"]
    print(
        json.dumps(
            {
                "files": len(data["files"]),
                "activities": len(data["nodes"]),
                "clic_replacements": sum(
                    bool(node["clic_slug"]) for node in data["nodes"]
                ),
                "retained_nodes": sum(not node["clic_slug"] for node in data["nodes"]),
            }
        )
    )


if __name__ == "__main__":
    main()
