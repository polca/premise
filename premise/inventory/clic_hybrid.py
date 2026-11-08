"""Retain licensed workbook records alongside public CLIC inventories.

The registry stores only identities, hashes and dependency references. Actual
licensed coefficients continue to come from the retained workbooks and the
user's background database, under the existing premise preparation rules.
"""

from __future__ import annotations

import hashlib
import copy
import gzip
import json
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import portalocker

from .clic_archive import ClicInventoryError, activity_identity, content_hash
from .clic_requirements import (
    registry,
    required_roots,
    applicable_roles,
    legacy_templates_required,
)
from .clic_target import resolve_inventory_target


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
    source_context: dict | None = None


def plan_hybrid_inventories(
    sectors,
    scenarios,
    sources,
    *,
    version=None,
    system_model=None,
    family="ecoinvent",
    source_version=None,
    explicit_roots=(),
    extra_nodes=(),
):
    """Walk the legacy dependency graph, stopping at published CLIC replacements."""
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping
    from .profiles import companion_projection_required, profiles

    source_context = None
    if str(family).strip().casefold() != "ecoinvent":
        resolve_inventory_target(version, family=family, system_model=system_model)
        # The reviewed role ledger describes ecoinvent templates. Its selection
        # baseline is independent of the destination's two background axes.
        version = source_version or "3.12"
        if version != "3.12":
            raise ClicInventoryError(
                "UVEK workbook selection requires the reviewed ecoinvent 3.12 source-role baseline."
            )
        source_context = {
            "family": "ecoinvent",
            "version": version,
            "system_model": "cutoff",
        }
    elif source_version is not None:
        version = source_version

    specification = registry()
    graph = specification["hybrid_sources"]
    nodes = graph["nodes"]
    active = {Path(path).name: (Path(path), version) for path, version in sources}
    requested = set(
        explicit_roots
        or required_roots(
            sectors,
            scenarios,
            source_files=sources,
            **({"family": family} if family != "ecoinvent" else {}),
        )
    )
    pending = list(extra_nodes)
    scenarios = list(scenarios) or [{}]
    for sector in sectors:
        for filename in specification["sector_mappings"].get(sector, ()):
            for scenario in scenarios:
                definitions = get_mapping(
                    VARIABLES_DIR / filename,
                    f"{family}_aliases",
                    model=scenario.get("model"),
                )
                for role in applicable_roles(sector, definitions, scenario):
                    if family != "ecoinvent" and (
                        definitions[role].get("clic_family_override")
                        or not legacy_templates_required(definitions[role])
                    ):
                        continue
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
            "source_context": source_context,
        }
    )
    return HybridInventoryPlan(
        tuple(sorted(requested)),
        frozenset(selected),
        selected_sources,
        frozenset(guards),
        fingerprint,
        source_context,
    )


def import_retained_inventories(
    plan,
    database,
    clic_activities,
    *,
    version,
    system_model,
    family="ecoinvent",
    biosphere_name="biosphere3",
    catalog_provider=None,
    migration_policy="pragmatic",
    allow_documented_proxies=None,
    keep_uncertainty=True,
    workbook_pins=None,
    cache_dir=None,
    cache_signature=None,
    use_cache=True,
):
    """Reuse only preparation certified for the same source graph and CLIC inputs."""
    from .clic_download import _write_json
    from .clic_harmonize import _gzip_write

    target = resolve_inventory_target(version, family=family, system_model=system_model)
    family = target.technosphere.family
    family_options = dict(
        family=family,
        biosphere_name=biosphere_name,
        catalog_provider=catalog_provider,
        migration_policy=migration_policy,
        allow_documented_proxies=allow_documented_proxies,
        cache_dir=cache_dir,
        use_cache=use_cache,
    )

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
    # Brightpath's cohort cache includes catalog, resource and policy digests.
    # Do not bypass those checks through the legacy prepared-record cache.
    if (
        family != "ecoinvent"
        or cache_dir is None
        or cache_signature is None
        or not use_cache
    ):
        return _import_retained_inventories(
            plan,
            database,
            clic_activities,
            version=version,
            system_model=system_model,
            keep_uncertainty=keep_uncertainty,
            workbook_pins=workbook_pins,
            **family_options,
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
            "family": family,
            "biosphere_name": biosphere_name,
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
            **family_options,
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
    family="ecoinvent",
    biosphere_name="biosphere3",
    catalog_provider=None,
    migration_policy="pragmatic",
    allow_documented_proxies=None,
    cache_dir=None,
    use_cache=True,
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
                "target_family": family,
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
    profile_version = (
        (plan.source_context or {}).get("version", "3.12")
        if family != "ecoinvent"
        else version
    )
    select_companion_profiles(
        plan.sources,
        importers,
        profile_version,
        system_model,
        active_sources={path.name for path, _ in plan.sources},
        retained_identities={
            filename: {tuple(nodes[index]["identity"]) for index in indices.values()}
            for filename, indices in expected_nodes.items()
        },
    )
    result, diagnostics, batches = [], [], []
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

        if family != "ecoinvent":
            from .profiles import apply_inventory_profiles, profiles

            if source_version != profile_version and profiles(
                profile_version, "cutoff", "migrated", path
            ):
                raise ClicInventoryError(
                    f"Retained workbook {path.name} requires migrated ecoinvent {profile_version} source-profile preparation before UVEK conversion."
                )

            for phase in ("raw", "migrated"):
                apply_inventory_profiles(
                    importer,
                    path,
                    profile_version,
                    "cutoff",
                    database,
                    phase=phase,
                    other_importers=importers,
                )
            records = [
                copy.deepcopy(record)
                for record in importer.data
                if record.get("_premise_hybrid_id") in plan.selected_nodes
            ]
            found = {record["_premise_hybrid_id"] for record in records}
            if found != set(expected_nodes[path.name].values()):
                raise ClicInventoryError(
                    f"Retained workbook {path.name} lost selected records during source preparation."
                )
            rebind(records)
            batches.append((records, _workbook_background(source_version)))
            continue

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
            if biosphere_name != "biosphere3":
                for edge in activity["exchanges"]:
                    if edge["type"] == "biosphere" and edge.get("input"):
                        edge["input"] = (biosphere_name, edge["input"][1])
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
    if batches:
        result, migration_diagnostics = _prepare_family_records(
            batches,
            database,
            version=version,
            family=family,
            system_model=system_model,
            biosphere_name=biosphere_name,
            keep_uncertainty=keep_uncertainty,
            catalog_provider=catalog_provider,
            migration_policy=migration_policy,
            allow_documented_proxies=allow_documented_proxies,
            cache_dir=cache_dir,
            use_cache=use_cache,
        )
        diagnostics.extend(migration_diagnostics)
        for row in metadata:
            row.update(_workbook_migration_metadata(migration_diagnostics))
        database.extend(result)
    return result, metadata, diagnostics


def _workbook_background(version):
    """Bundled and additional workbooks declare ecoinvent cut-off source data."""
    from .inventory_imports import normalize_version

    version = normalize_version(str(version))
    return {
        "technosphere": {
            "family": "ecoinvent",
            "version": version,
            "system_model": "cutoff",
        },
        "biosphere": {"family": "ecoinvent", "version": version},
    }


def _workbook_migration_metadata(diagnostics):
    """Expose portable audit references without embedding local cache paths."""
    for item in diagnostics:
        if item.get("code") == "clic.workbook_harmonized":
            manifest = copy.deepcopy(item["manifest"])
            manifest["cohorts"] = [
                {key: cohort[key] for key in ("fingerprint", "audit_sha256")}
                for cohort in manifest["cohorts"]
            ]
            return {
                "migration_manifest": manifest,
                "migration_cohorts": copy.deepcopy(manifest["cohorts"]),
            }
    return {}


def _prepare_family_records(
    batches,
    database,
    *,
    version,
    family,
    system_model,
    biosphere_name,
    keep_uncertainty=True,
    catalog_provider=None,
    migration_policy="pragmatic",
    allow_documented_proxies=None,
    cache_dir=None,
    use_cache=True,
):
    """Migrate owned workbook graphs with the CLIC policy and destination gates.

    Already prepared foreground purchases are explicit boundary anchors, not
    background suppliers to be migrated again. Production-only anchors let the
    harmonizer validate cross-cohort foreground references without copying or
    converting the destination's inventories.
    """
    from dataclasses import asdict
    from .clic_adapter import prepare_clic_for_premise
    from .clic_harmonize import ClicHarmonizer
    from ..core.filesystem_constants import DIR_CACHED_FILES

    target = resolve_inventory_target(version, family=family, system_model=system_model)
    records, rows, originals = [], [], {}
    by_identity, qualified = defaultdict(list), {}
    for batch, background in batches:
        for raw in batch:
            activity = copy.deepcopy(raw)
            provenance = activity["inventory provenance"]
            code = content_hash([provenance["provider"], provenance["record_id"]])
            if code in originals:
                raise ClicInventoryError("Duplicate owned workbook record identity.")
            originals[code] = copy.deepcopy(provenance)
            if activity.get("database") and activity.get("code"):
                qualified[(activity["database"], activity["code"])] = code
            activity.update(database="__clic__", code=code)
            by_identity[_key(activity_identity(activity))].append(code)
            records.append(activity)
            rows.append({"activity_code": code, "background": background})
    destination = {}
    for activity in database:
        if not activity.get("database") or not activity.get("code"):
            continue
        key = (activity["database"], activity["code"])
        if key in destination:
            raise ClicInventoryError(f"Duplicate destination activity code: {key!r}.")
        destination[key] = activity
    foreground_destinations = defaultdict(list)
    for supplier in destination.values():
        if managed_inventory_key(supplier):
            foreground_destinations[_key(activity_identity(supplier))].append(supplier)
    anchors = {}
    for activity in records:
        for edge in activity["exchanges"]:
            if edge["type"] == "production":
                edge["input"] = ("__clic__", activity["code"])
            elif edge["type"] == "technosphere":
                reference = edge.get("input")
                reference = (
                    tuple(reference) if isinstance(reference, (tuple, list)) else None
                )
                choices = by_identity.get(_key(activity_identity(edge)), ())
                external = foreground_destinations.get(
                    _key(activity_identity(edge)), ()
                )
                code = qualified.get(reference)
                if code is None and len(choices) == 1:
                    code = choices[0]
                if code is not None:
                    edge["input"] = ("__clic__", code)
                elif len(choices) > 1:
                    raise ClicInventoryError(
                        f"Ambiguous owned workbook purchase: {activity_identity(edge)!r}."
                    )
                elif (
                    reference in destination
                    and managed_inventory_key(destination[reference])
                ) or external:
                    if reference in destination and managed_inventory_key(
                        destination[reference]
                    ):
                        supplier = destination[reference]
                    elif len(external) == 1:
                        supplier = external[0]
                    else:
                        raise ClicInventoryError(
                            f"Ambiguous destination foreground purchase: {activity_identity(edge)!r}."
                        )
                    reference = (supplier["database"], supplier["code"])
                    code = content_hash(["workbook boundary", reference])
                    anchors[code] = supplier
                    edge.update(
                        name=supplier["name"],
                        product=supplier["reference product"],
                        location=supplier["location"],
                    )
                    edge["reference product"] = supplier["reference product"]
                    edge["input"] = ("__clic__", code)
                else:
                    # Source-project UUIDs must never masquerade as target links.
                    edge.pop("input", None)
    for code, supplier in anchors.items():
        anchor = {
            key: copy.deepcopy(supplier[key])
            for key in ("name", "reference product", "location", "unit")
        }
        anchor.update(
            database="__clic__",
            code=code,
            exchanges=[
                {
                    "type": "production",
                    "amount": 1,
                    "input": ("__clic__", code),
                    **{
                        key: anchor[key]
                        for key in ("name", "reference product", "location", "unit")
                    },
                }
            ],
        )
        records.append(anchor)
        rows.append({"activity_code": code, "background": asdict(target)})
    resolved = SimpleNamespace(
        activities=records,
        versions=rows,
        archive={"source": "owned workbooks"},
        fingerprint=content_hash(records),
        diagnostics=[],
        lockfile=lambda: {},
    )
    harmonized = ClicHarmonizer(
        Path(cache_dir or DIR_CACHED_FILES / "clic") / "workbook-harmonized",
        catalog_provider=catalog_provider,
        migration_policy=migration_policy,
        allow_documented_proxies=allow_documented_proxies,
    ).harmonize(
        resolved,
        version=version,
        family=family,
        system_model=system_model,
        use_cache=use_cache,
    )
    harmonized.activities = [
        a for a in harmonized.activities if a["code"] not in anchors
    ]
    if not set(originals).issubset({a["code"] for a in harmonized.activities}):
        raise ClicInventoryError(
            "Background migration dropped selected workbook records."
        )
    for activity in harmonized.activities:
        for edge in activity["exchanges"]:
            reference = edge.get("input")
            if (
                isinstance(reference, (tuple, list))
                and reference[0] == "__clic__"
                and reference[1] in anchors
            ):
                supplier = anchors[reference[1]]
                edge["input"] = (supplier["database"], supplier["code"])
    prepared, diagnostics = prepare_clic_for_premise(
        harmonized,
        database,
        version=version,
        family=family,
        system_model=system_model,
        biosphere_name=biosphere_name,
        keep_uncertainty=keep_uncertainty,
        catalog_provider=catalog_provider,
    )
    unique = []
    for activity in prepared:
        key = (activity["database"], activity["code"])
        if key in destination:
            if activity.get("clic provenance", {}).get("role") != "generated helper":
                raise ClicInventoryError(
                    f"Workbook activity code already exists in destination: {key!r}."
                )

            def scientific_hash(record):
                return content_hash(
                    {
                        field: value
                        for field, value in record.items()
                        if field not in {"clic provenance", "inventory provenance"}
                    }
                )

            digest = scientific_hash(activity)
            if digest != scientific_hash(destination[key]):
                raise ClicInventoryError(
                    f"Conflicting generated workbook helper at destination code {key!r}."
                )
            diagnostics.append(
                {
                    "code": "clic.workbook_helper_reused",
                    "database": key[0],
                    "activity_code": key[1],
                    "scientific_sha256": digest,
                }
            )
            continue
        if activity["code"] in originals:
            activity.pop("clic provenance", None)
            activity.pop("_premise_hybrid_id", None)
            activity["inventory provenance"] = {
                **originals[activity["code"]],
                "target": harmonized.manifest["target"],
                "migration_policy": harmonized.manifest["policy"],
                "migration_cohorts": harmonized.manifest["cohorts"],
            }
        unique.append(activity)
    diagnostics.append(
        {
            "code": "clic.workbook_harmonized",
            "manifest": copy.deepcopy(harmonized.manifest),
        }
    )
    return unique, harmonized.diagnostics + diagnostics
