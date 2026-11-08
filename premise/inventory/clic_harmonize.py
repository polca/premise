"""Harmonize selected CLIC source cohorts with auditable Brightpath caching."""

from __future__ import annotations

import copy
import gzip
import hashlib
import importlib.metadata
import json
import os
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import portalocker

from .clic_archive import ClicInventoryError, activity_identity, content_hash
from .clic_download import _write_json
from .clic_target import resolve_inventory_target

CLIC_HARMONIZATION_SCHEMA = 3


@dataclass
class HarmonizedClicInventory:
    activities: list[dict]
    manifest: dict
    lockfile: dict
    diagnostics: list[dict] = field(default_factory=list)


def _gzip_write(path, value):
    def scalar(item):
        # Legacy migration tables can return NumPy scalar quantities.
        import numpy as np

        if isinstance(item, np.generic):
            return item.item()
        raise TypeError(f"Unsupported portable inventory value: {type(item).__name__}")

    raw = json.dumps(
        value, sort_keys=True, allow_nan=False, separators=(",", ":"), default=scalar
    ).encode()
    packed = gzip.compress(raw, mtime=0)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(packed)
        Path(name).replace(path)
    finally:
        Path(name).unlink(missing_ok=True)
    return hashlib.sha256(packed).hexdigest()


def _context(background):
    from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile

    return BackgroundContext(
        technosphere=TechnosphereProfile(**background["technosphere"]),
        biosphere=BiosphereProfile(**background["biosphere"]),
    )


def _catalog_fingerprint(provider, context):
    try:
        technosphere = provider.load_technosphere(context.technosphere)
        biosphere = provider.load_biosphere(context.biosphere)
    except FileNotFoundError as error:
        raise ClicInventoryError(
            f"CLIC requires reviewed Brightpath reference catalogs for {context!r}: "
            f"{error}"
        ) from error
    return {
        "technosphere": technosphere.digest
        or content_hash(sorted(technosphere.identities)),
        "biosphere": biosphere.digest or content_hash(sorted(biosphere.identities)),
    }


class ClicHarmonizer:
    """Cache independent source cohorts, leaving unrelated cohorts reusable.

    Source and target background validation remain mandatory. Pragmatic mode
    accepts inferred reverse routes, their aggregation/proxy losses and packaged
    rule precedence, recording them in audits. Missing links, ambiguous suppliers
    and unsupported unit changes still fail.
    """

    def __init__(
        self,
        cache_dir,
        *,
        catalog_provider=None,
        allow_documented_proxies=None,
        migration_policy="pragmatic",
    ):
        self.cache_dir = Path(cache_dir)
        self.catalog_provider = catalog_provider
        if migration_policy not in {"pragmatic", "strict"}:
            raise ValueError("CLIC migration_policy must be 'pragmatic' or 'strict'.")
        self.migration_policy = migration_policy
        self.allow_documented_proxies = (
            migration_policy == "pragmatic"
            if allow_documented_proxies is None
            else allow_documented_proxies
        )

    def harmonize(
        self,
        resolved,
        *,
        version,
        family="ecoinvent",
        system_model="cutoff",
        use_cache=True,
    ):
        target_context = resolve_inventory_target(
            version, family=family, system_model=system_model
        )
        from brightpath import BrightwayInventory, FormatProfile, InventoryContext
        from brightpath.background import (
            catalog_provider_from_environment,
            migration_resource_fingerprint,
        )
        from brightpath.core.policies import MigrationPolicy, PolicyAction

        target = asdict(target_context)
        provider = self.catalog_provider or catalog_provider_from_environment()
        engine_version = importlib.metadata.version("brightpath")
        resources = migration_resource_fingerprint()
        policy = MigrationPolicy.strict()
        allowed_losses = set()
        if self.migration_policy == "pragmatic":
            policy = replace(
                policy,
                on_inferred_reverse=PolicyAction.WARN,
                on_ambiguous_rule=PolicyAction.WARN,
            )
            allowed_losses.update(
                {
                    "migration.reverse_disaggregation",
                    "migration.reverse_aggregation",
                    "migration.reverse_proxy",
                }
            )
        if self.allow_documented_proxies:
            allowed_losses.add("migration.documented_proxy")
            if target_context.technosphere.family == "uvek":
                # Forward reviewed UVEK helpers report their boundary differences
                # under this code. Omissions and biosphere losses remain separate
                # gates, even in pragmatic mode.
                allowed_losses.add("migration.technosphere_information_loss")
        if allowed_losses:
            policy = replace(policy, on_information_loss=PolicyAction.WARN)
        policy_metadata = {
            "mode": self.migration_policy,
            "allow_inferred_reverse": self.migration_policy == "pragmatic",
            "overlapping_rule_policy": (
                "last packaged rule"
                if self.migration_policy == "pragmatic"
                else "error"
            ),
            "permitted_information_losses": sorted(allowed_losses),
            "allow_documented_proxies": self.allow_documented_proxies,
            "validate_source": True,
            "validate_target": True,
            "minimum_coverage": 1.0,
        }
        groups, contexts = defaultdict(list), {}
        rows = {row["activity_code"]: row for row in resolved.versions}
        native_by_code = {a["code"]: a for a in resolved.activities}
        for activity in resolved.activities:
            background = rows[activity["code"]]["background"]
            key = content_hash(background)
            groups[key].append(activity)
            contexts[key] = background
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        activities, summaries, diagnostics = {}, [], list(resolved.diagnostics)
        target_catalogs = _catalog_fingerprint(provider, target_context)
        for key, records in groups.items():
            source = contexts[key]
            source_context = _context(source)
            own_codes = {a["code"] for a in records}
            foreign = set()
            for activity in records:
                for edge in activity["exchanges"]:
                    reference = edge.get("input")
                    if (
                        edge["type"] == "technosphere"
                        and isinstance(reference, (tuple, list))
                        and reference[0] == "__clic__"
                        and reference[1] not in own_codes
                    ):
                        foreign.add(activity_identity(native_by_code[reference[1]]))
            # Per-build roles and pinning affect reporting, not migration math.
            scientific_records = [
                {k: v for k, v in a.items() if k != "clic provenance"} for a in records
            ]
            signature = {
                "schema": CLIC_HARMONIZATION_SCHEMA,
                "source": source,
                "target": target,
                "records": scientific_records,
                "foreign_foreground": sorted(foreign),
                "brightpath": engine_version,
                "resources": resources,
                "policy": policy_metadata,
                "source_catalogs": _catalog_fingerprint(provider, source_context),
                "target_catalogs": target_catalogs,
            }
            fingerprint = content_hash(signature)
            metadata_path = self.cache_dir / f"{fingerprint}.json"
            payload_path = self.cache_dir / f"{fingerprint}.inventory.json.gz"
            audit_path = self.cache_dir / f"{fingerprint}.audit.json.gz"
            started = time.perf_counter()
            with portalocker.Lock(
                str(self.cache_dir / f"{fingerprint}.lock"), timeout=120
            ):
                cached = None
                if use_cache and metadata_path.exists() and payload_path.exists():
                    try:
                        metadata = json.loads(metadata_path.read_text())
                        packed = payload_path.read_bytes()
                        if (
                            metadata["fingerprint"] == fingerprint
                            and hashlib.sha256(packed).hexdigest()
                            == metadata["payload_sha256"]
                            and audit_path.exists()
                            and hashlib.sha256(audit_path.read_bytes()).hexdigest()
                            == metadata["audit_sha256"]
                        ):
                            cached = json.loads(gzip.decompress(packed))
                    except (OSError, ValueError, KeyError, EOFError):
                        cached = None
                cache_hit = cached is not None
                if not cache_hit:
                    inventory = BrightwayInventory.from_data(
                        scientific_records,
                        context=InventoryContext(
                            format=FormatProfile("brightway_excel", dialect="bw2io"),
                            background=source_context,
                        ),
                        database_name="__clic__",
                    ).normalize()
                    try:
                        migrated = inventory.migrate_background(
                            target_context,
                            policy=policy,
                            catalog_provider=provider,
                            additional_foreground_targets=tuple(sorted(foreign)),
                        )
                    except Exception as error:
                        report = getattr(error, "report", None)
                        if report is not None:
                            _gzip_write(audit_path, json.loads(report.to_json()))
                        raise ClicInventoryError(
                            f"CLIC background migration from {source!r} to {target!r} failed: {error}. "
                            f"Diagnostic: {audit_path}"
                        ) from error
                    report = migrated.last_migration_report
                    report_data = (
                        json.loads(report.to_json()) if report is not None else {}
                    )
                    _gzip_write(audit_path, report_data)
                    unexpected_losses = (
                        [
                            loss.code
                            for loss in report.losses
                            if loss.code not in allowed_losses
                        ]
                        if report
                        else []
                    )
                    if unexpected_losses:
                        raise ClicInventoryError(
                            f"CLIC migration has unapproved losses: {unexpected_losses!r}. Diagnostic: {audit_path}"
                        )
                    cached = migrated.data
                    # Brightpath's immutable details contain nested mappings;
                    # its public report serializer supplies portable values.
                    issues = [
                        issue
                        for stage in report_data.get("stages", ())
                        for issue in stage.get("issues", ())
                    ]
                    metadata = {
                        "fingerprint": fingerprint,
                        "source": source,
                        "target": target,
                        "activity_count": len(cached),
                        "issues": issues,
                        "issue_counts": dict(Counter(item["code"] for item in issues)),
                        "audit_sha256": hashlib.sha256(
                            audit_path.read_bytes()
                        ).hexdigest(),
                        "payload_sha256": _gzip_write(payload_path, cached),
                    }
                    _write_json(metadata_path, metadata)
                summary = {
                    k: metadata[k]
                    for k in (
                        "fingerprint",
                        "source",
                        "target",
                        "activity_count",
                        "issue_counts",
                    )
                }
                summary.update(
                    cache_hit=cache_hit,
                    audit_file=str(audit_path),
                    audit_sha256=metadata["audit_sha256"],
                    elapsed_seconds=time.perf_counter() - started,
                )
            summaries.append(summary)
            diagnostics.extend(metadata["issues"])
            for activity in cached:
                activity = copy.deepcopy(activity)
                activity["database"] = "__clic__"
                if activity["code"] in rows:
                    activity["clic provenance"] = copy.deepcopy(rows[activity["code"]])
                else:
                    activity["clic provenance"] = {
                        "role": "generated helper",
                        "activity_code": activity["code"],
                        "brightpath_version": engine_version,
                        "conversion_fingerprint": fingerprint,
                    }
                if activity["code"] in activities:
                    previous = {
                        k: v
                        for k, v in activities[activity["code"]].items()
                        if k != "clic provenance"
                    }
                    current = {
                        k: v for k, v in activity.items() if k != "clic provenance"
                    }
                    if content_hash(previous) != content_hash(current):
                        raise ClicInventoryError(
                            f"Conflicting generated CLIC activity code {activity['code']!r}."
                        )
                activities[activity["code"]] = activity
        for activity in activities.values():
            for edge in activity["exchanges"]:
                reference = edge.get("input")
                if (
                    isinstance(reference, (list, tuple))
                    and len(reference) == 2
                    and reference[1] in activities
                ):
                    edge["input"] = ("__clic__", reference[1])
        manifest = {
            "schema_version": CLIC_HARMONIZATION_SCHEMA,
            "source": "CLIC",
            "archive": resolved.archive,
            "resolution_fingerprint": resolved.fingerprint,
            "resolution_diagnostics": copy.deepcopy(resolved.diagnostics),
            "datasets": copy.deepcopy(resolved.versions),
            "target": {**copy.deepcopy(target), "premise_system_model": system_model},
            "brightpath_version": engine_version,
            "migration_resource_fingerprint": resources,
            "policy": policy_metadata,
            "cohorts": summaries,
            "generated_helpers": sorted(
                code for code in activities if code not in rows
            ),
        }
        return HarmonizedClicInventory(
            list(activities.values()), manifest, resolved.lockfile(), diagnostics
        )
