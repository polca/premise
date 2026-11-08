"""Retain licensed workbook records alongside public CLIC inventories.

The registry stores only identities, hashes and dependency references. Actual
licensed coefficients continue to come from the retained workbooks and the
user's background database, under the existing premise preparation rules.
"""

from __future__ import annotations

import hashlib
import gzip
import json
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import portalocker

from .clic_archive import ClicInventoryError, activity_identity, content_hash
from .clic_requirements import registry, required_roots, applicable_roles


def _key(identity):
    return tuple(
        (
            value.casefold().strip()
            if index in (0, 1) and isinstance(value, str)
            else value
        )
        for index, value in enumerate(identity)
    )


def managed_inventory_key(activity):
    """Stable provider identity retained in source and scenario caches."""
    clic = activity.get("clic provenance", {})
    if clic.get("activity_code"):
        return "clic", clic["activity_code"]
    workbook = activity.get("inventory provenance", {})
    if workbook.get("record_id"):
        return "workbook", workbook["record_id"]
    return None


def source_store_signature(store):
    """Reuse mutation-aware ingestion fingerprints instead of rescanning exchanges."""
    underlying = getattr(store, "_store", store)
    getter = getattr(underlying, "_activity_fingerprint", None)
    if getter is None:
        from ..validation.validation_framework import inventory_store_fingerprint

        return inventory_store_fingerprint(store)
    digest = hashlib.sha256(b"premise-clic-source-structural-v1")
    for activity_id in store.iter_activity_ids():
        digest.update(str(activity_id).encode("ascii"))
        digest.update(getter(activity_id).encode("ascii"))
    return digest.hexdigest()


@dataclass
class HybridInventoryPlan:
    clic_roots: tuple[str, ...]
    selected_nodes: frozenset[int]
    sources: tuple
    guard_files: frozenset[str]
    fingerprint: str


def plan_hybrid_inventories(
    sectors,
    scenarios,
    sources,
    *,
    version=None,
    system_model=None,
    explicit_roots=(),
    extra_nodes=(),
):
    """Walk the legacy dependency graph, stopping at published CLIC replacements."""
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping
    from .profiles import companion_projection_required, profiles

    specification = registry()
    graph = specification["hybrid_sources"]
    nodes = graph["nodes"]
    active = {Path(path).name: (Path(path), version) for path, version in sources}
    requested = set(
        explicit_roots or required_roots(sectors, scenarios, source_files=sources)
    )
    pending = list(extra_nodes)
    scenarios = list(scenarios) or [{}]
    for sector in sectors:
        for filename in specification["sector_mappings"].get(sector, ()):
            for scenario in scenarios:
                definitions = get_mapping(
                    VARIABLES_DIR / filename,
                    "ecoinvent_aliases",
                    model=scenario.get("model"),
                )
                for role in applicable_roles(sector, definitions, scenario):
                    pending.extend(graph["roles"].get(filename, {}).get(role, ()))
        if sector == "battery":
            pending.extend(
                node["id"]
                for node in nodes
                if node["identity"][0].startswith("market for battery capacity")
            )
        if sector == "mining":
            pending.extend(
                graph["files"].get("lci-sulfidic-tailings.xlsx", {}).get("nodes", ())
            )
        if sector == "steel":
            # Pig iron, DRI and CCS templates are selected procedurally by the
            # transformation as well as through the IAM steel roles.
            pending.extend(graph["files"].get("lci-steel.xlsx", {}).get("nodes", ()))
        if sector == "metals":
            for filename in (
                "lci-lithium.xlsx",
                "lci-cobalt.xlsx",
                "lci-graphite.xlsx",
                "lci-germanium.xlsx",
                "lci-rhenium.xlsx",
                "lci-PGM.xlsx",
                "lci-iridium-economic-proxy.xlsx",
            ):
                pending.extend(graph["files"].get(filename, {}).get("nodes", ()))
    selected, guards = set(), set()
    vehicle_sources = defaultdict(list)
    for node in nodes:
        if node["file"] in active:
            vehicle_sources[node["identity"][0].casefold()].append(node["id"])

    def choose(candidates, consumer):
        candidates = [index for index in candidates if nodes[index]["file"] in active]
        if not candidates:
            return None
        if consumer is not None:
            same_file = [
                index
                for index in candidates
                if nodes[index]["file"] == consumer["file"]
            ]
            if same_file:
                candidates = same_file
            same_profile = [
                index
                for index in candidates
                if active[nodes[index]["file"]][1] == active[consumer["file"]][1]
            ]
            if same_profile:
                candidates = same_profile
        if len(candidates) > 1:
            hashes = {nodes[index]["science_hash"] for index in candidates}
            slugs = {nodes[index]["clic_slug"] for index in candidates}
            if len(hashes) > 1 and (len(slugs) > 1 or None in slugs):
                raise ClicInventoryError(
                    f"Ambiguous retained workbook supplier: {[nodes[index]['identity'] for index in candidates]!r}."
                )
        return min(candidates)

    def drain():
        while pending:
            index = pending.pop()
            node = nodes[index]
            if node["file"] not in active:
                continue
            if node["clic_slug"]:
                requested.add(node["clic_slug"])
                continue
            if index in selected:
                continue
            selected.add(index)
            if node["file"] == "lci-final-energy.xlsx":
                # Energy-equipment wrappers declare a vehicle service through
                # metadata, rather than purchasing that whole service. It is
                # nevertheless required to reconstruct their physical inputs.
                from ..transport.vehicle_energy_use import source_name

                name = source_name(node["identity"][0])
                if name is not None:
                    candidates = vehicle_sources.get(name.casefold(), ())
                    for location in dict.fromkeys(
                        (
                            node["identity"][2],
                            "RER",
                            "Europe without Switzerland",
                            "GLO",
                            "CH",
                        )
                    ):
                        local = [
                            child
                            for child in candidates
                            if nodes[child]["identity"][2] == location
                        ]
                        if local:
                            child = choose(local, node)
                            if child is not None:
                                pending.append(child)
                            break
            for edge in node["inputs"]:
                if "clic_slug" in edge:
                    requested.add(edge["clic_slug"])
                else:
                    child = choose(edge["nodes"], node)
                    if child is not None:
                        pending.append(child)

    drain()
    # Existing scientific source profiles may validate companion workbooks.
    # Only companions of retained sources are read; their unused records are
    # never imported merely because a guard needs to inspect them.
    identities = defaultdict(list)
    for node in nodes:
        if node["file"] in active:
            identities[_key(node["identity"])].append(node["id"])
    touched = set()
    while True:
        before = (len(selected), len(guards), len(requested))
        files = {nodes[index]["file"] for index in selected} | guards
        retained_identities = defaultdict(set)
        for index in selected:
            retained_identities[nodes[index]["file"]].add(
                tuple(nodes[index]["identity"])
            )
        for scenario in scenarios:
            target_version = version or scenario.get("version")
            target_model = system_model or scenario.get("system_model")
            for profile in profiles(target_version, target_model):
                if profile[
                    "phase"
                ] == "projection" and not companion_projection_required(
                    profile, retained_identities
                ):
                    continue
                affected = profile.get("source") in files or any(
                    requirement.get("source") in files
                    for requirement in profile.get("requires", ())
                )
                marker = (profile["id"], target_version, target_model)
                if not affected or marker in touched:
                    continue
                touched.add(marker)
                for requirement in profile.get("requires", ()):
                    name = requirement["source"]
                    if name in active:
                        guards.add(name)
                targets = [rule["replacement"] for rule in profile.get("relink", ())]
                targets.extend(
                    rule["identity"] for rule in profile.get("require_suppliers", ())
                )
                if profile.get("replace_inputs"):
                    targets.append(
                        activity_identity(profile["replace_inputs"]["exchange"])
                    )
                for target in targets:
                    candidate = choose(identities.get(_key(target), ()), None)
                    if candidate is not None:
                        pending.append(candidate)
        drain()
        after = (len(selected), len(guards), len(requested))
        if before == after:
            break
    names = {nodes[index]["file"] for index in selected} | guards
    selected_sources = tuple(
        (path, version) for path, version in sources if Path(path).name in names
    )
    fingerprint = content_hash(
        {
            "selected_nodes": sorted(selected),
            "clic_roots": sorted(requested),
            "files": {name: graph["files"][name]["sha256"] for name in sorted(names)},
        }
    )
    return HybridInventoryPlan(
        tuple(sorted(requested)),
        frozenset(selected),
        selected_sources,
        frozenset(guards),
        fingerprint,
    )


def import_retained_inventories(
    plan,
    database,
    clic_activities,
    *,
    version,
    system_model,
    keep_uncertainty=True,
    workbook_pins=None,
    cache_dir=None,
    cache_signature=None,
    use_cache=True,
):
    """Reuse only preparation certified for the same source graph and CLIC inputs."""
    from .clic_download import _write_json
    from .clic_harmonize import _gzip_write

    graph = registry()["hybrid_sources"]
    hashes = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path, _ in plan.sources
    }
    for name, digest in hashes.items():
        expected = (
            graph["files"][name]["sha256"]
            if workbook_pins is None
            else workbook_pins.get(name)
        )
        if digest != expected or digest != graph["files"][name]["sha256"]:
            raise ClicInventoryError(
                f"Retained workbook checksum differs from registry or recipe: {name}."
            )
    if cache_dir is None or cache_signature is None or not use_cache:
        return _import_retained_inventories(
            plan,
            database,
            clic_activities,
            version=version,
            system_model=system_model,
            keep_uncertainty=keep_uncertainty,
            workbook_pins=workbook_pins,
        )
    directory = Path(cache_dir)
    directory.mkdir(parents=True, exist_ok=True)
    fingerprint = content_hash(
        {
            "schema": 1,
            "plan": plan.fingerprint,
            "workbooks": hashes,
            "version": version,
            "system_model": system_model,
            "uncertainty": keep_uncertainty,
            "signature": cache_signature,
        }
    )
    metadata_path = directory / f"{fingerprint}.json"
    payload_path = directory / f"{fingerprint}.json.gz"
    with portalocker.Lock(str(directory / f"{fingerprint}.lock"), timeout=120):
        if metadata_path.exists() and payload_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text())
                payload = payload_path.read_bytes()
                if metadata["sha256"] == hashlib.sha256(payload).hexdigest():
                    data = json.loads(gzip.decompress(payload))
                    # JSON represents Brightway's qualified reference tuples
                    # as lists. Restore the native contract before the records
                    # reach linking, checkpointing or either Brightway writer.
                    for activity in data["records"]:
                        for row in (activity, *activity.get("exchanges", ())):
                            for field in ("input", "output"):
                                reference = row.get(field)
                                if isinstance(reference, list) and len(reference) == 2:
                                    row[field] = tuple(reference)
                    return data["records"], data["workbooks"], data["diagnostics"]
            except (OSError, ValueError, EOFError, KeyError):
                pass
        records, workbooks, diagnostics = _import_retained_inventories(
            plan,
            database,
            clic_activities,
            version=version,
            system_model=system_model,
            keep_uncertainty=keep_uncertainty,
            workbook_pins=workbook_pins,
        )
        digest = _gzip_write(
            payload_path,
            {"records": records, "workbooks": workbooks, "diagnostics": diagnostics},
        )
        _write_json(metadata_path, {"fingerprint": fingerprint, "sha256": digest})
        return records, workbooks, diagnostics


def _import_retained_inventories(
    plan,
    database,
    clic_activities,
    *,
    version,
    system_model,
    keep_uncertainty=True,
    workbook_pins=None,
):
    """Run retained records through existing profiles, migration and cleaning."""
    from bw2io import ExcelImporter
    from .inventory_imports import DefaultInventory
    from .profiles import select_companion_profiles

    graph = registry()["hybrid_sources"]
    nodes = graph["nodes"]
    locked_workbooks = workbook_pins is not None
    workbook_pins = workbook_pins or {}
    expected_nodes = defaultdict(dict)
    for index in plan.selected_nodes:
        node = nodes[index]
        expected_nodes[node["file"]][_key(node["identity"])] = index
    # Attach public replacements under both published and legacy qualified names.
    by_slug = {
        a["clic provenance"]["slug"]: a
        for a in clic_activities
        if a.get("clic provenance", {}).get("slug")
    }
    aliases = defaultdict(list)
    for node in nodes:
        if node["clic_slug"] in by_slug:
            aliases[_key(node["identity"])].append(
                (node["file"], by_slug[node["clic_slug"]])
            )
    for activity in clic_activities:
        row = activity.get("clic provenance", {})
        if row.get("slug"):
            aliases[_key(activity_identity(row))].append((None, activity))
    metadata = []
    for path, source_version in plan.sources:
        actual = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        if locked_workbooks and Path(path).name not in workbook_pins:
            raise ClicInventoryError(
                f"Retained workbook is absent from the locked recipe: {path.name}."
            )
        expected = workbook_pins.get(
            Path(path).name, graph["files"][Path(path).name]["sha256"]
        )
        if actual != expected or actual != graph["files"][Path(path).name]["sha256"]:
            raise ClicInventoryError(
                f"Retained workbook changed and requires a reviewed registry or recipe: {path.name}."
            )
        metadata.append(
            {
                "file": path.name,
                "sha256": actual,
                "source_version": source_version,
                "role": "licensed inventory and/or source-profile guard",
                "target_version": str(version),
            }
        )
    with ThreadPoolExecutor(max_workers=min(4, len(plan.sources) or 1)) as executor:
        importers = list(
            executor.map(ExcelImporter, (path for path, _ in plan.sources))
        )
    for (path, _), importer in zip(plan.sources, importers):
        for activity in importer.data:
            index = expected_nodes[path.name].get(_key(activity_identity(activity)))
            if index is not None:
                activity["_premise_hybrid_id"] = index
    select_companion_profiles(
        plan.sources,
        importers,
        version,
        system_model,
        active_sources={path.name for path, _ in plan.sources},
        retained_identities={
            filename: {tuple(nodes[index]["identity"]) for index in indices.values()}
            for filename, indices in expected_nodes.items()
        },
    )
    result, diagnostics = [], []
    for (path, source_version), importer, provenance in zip(
        plan.sources, importers, metadata
    ):
        if path.name not in expected_nodes:
            continue

        def rebind(records):
            for activity in records:
                activity["inventory provenance"] = {
                    **provenance,
                    "provider": "retained workbook",
                    "record_id": content_hash(
                        [
                            path.name,
                            activity.get("_premise_hybrid_id"),
                            activity_identity(activity),
                        ]
                    ),
                }
                for edge in activity["exchanges"]:
                    if edge["type"] != "technosphere":
                        continue
                    candidates = aliases.get(_key(activity_identity(edge)), ())
                    same_file = [
                        a for filename, a in candidates if filename == path.name
                    ]
                    choices = same_file or [a for _, a in candidates]
                    unique = {a["code"]: a for a in choices}
                    if len(unique) > 1:
                        matching = {
                            code: a
                            for code, a in unique.items()
                            if a["clic provenance"]["background"]["technosphere"][
                                "version"
                            ]
                            == source_version
                        }
                        unique = matching or unique
                    if len(unique) > 1:
                        raise ClicInventoryError(
                            f"Ambiguous CLIC replacement in retained {path.name}: {activity_identity(edge)!r}."
                        )
                    if unique:
                        target = next(iter(unique.values()))
                        if edge["unit"] != target["unit"]:
                            raise ClicInventoryError(
                                f"CLIC replacement changes a retained purchase unit in {path.name}: "
                                f"{activity_identity(edge)!r} -> {activity_identity(target)!r}."
                            )
                        previous = activity_identity(edge)
                        edge.update(
                            name=target["name"],
                            product=target["reference product"],
                            location=target["location"],
                            input=(target["database"], target["code"]),
                        )
                        edge["reference product"] = target["reference product"]
                        if previous != activity_identity(edge):
                            diagnostics.append(
                                {
                                    "code": "clic.retained_supplier_rebound",
                                    "file": path.name,
                                    "from": list(previous),
                                    "to": list(activity_identity(edge)),
                                }
                            )

        inventory = DefaultInventory(
            database=database,
            version_in=source_version,
            version_out=version,
            path=path,
            system_model=system_model,
            keep_uncertainty_data=keep_uncertainty,
            preloaded_importer=importer,
            source_inventories=importers,
            record_filter=lambda record: record.get("_premise_hybrid_id")
            in plan.selected_nodes,
            before_linking=rebind,
        )
        records = inventory.merge_inventory()
        # Legacy cleanup normalizes spelling. Restore the exact canonical CLIC
        # names after those rules, so name-based exporters see the same links.
        rebind(records)
        if inventory.list_unlinked:
            raise ClicInventoryError(
                f"Retained workbook {path.name} contains unlinked exchanges."
            )
        for activity in records:
            activity.pop("_premise_hybrid_id", None)
            if not keep_uncertainty:
                # The legacy cleaner represents unused distribution parameters
                # as NaN. Omit those optional fields in the portable cache.
                for edge in activity["exchanges"]:
                    for field in ("scale", "shape", "minimum", "maximum"):
                        value = edge.get(field)
                        if isinstance(value, float) and not math.isfinite(value):
                            edge.pop(field)
        result.extend(records)
        database.extend(records)
    return result, metadata, diagnostics
