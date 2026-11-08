"""CLIC archive provider for versioned, cached premise foreground inventories."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from collections import defaultdict

import portalocker

from .clic_adapter import prepare_clic_for_premise
from .clic_archive import (
    ClicArchive,
    ClicInventoryError,
    ClicRecipe,
    CLIC_GRAPH_SCHEMA,
    ResolvedClicInventory,
    activity_identity,
    content_hash,
)
from .clic_download import (
    DEFAULT_CLIC_RECORD,
    ClicArchiveStore,
    verify_latest_versions,
    _write_json,
)
from .clic_harmonize import ClicHarmonizer, _gzip_write
from .clic_target import resolve_inventory_target


@dataclass(frozen=True)
class ClicInventoryOptions:
    """Distribution and migration policy for a CLIC-backed build.

    ``freshness='live'`` verifies every unpinned selected version with CLIC.
    ``freshness='snapshot'`` explicitly accepts latest as of the archive date.
    Offline runs always report snapshot freshness. No credentials are needed
    to use public Zenodo archives or public CLIC version metadata.
    """

    archive_path: str | Path | None = None
    cache_dir: str | Path | None = None
    record_id: str = DEFAULT_CLIC_RECORD
    offline: bool = False
    freshness: str = "live"
    allow_documented_proxies: bool | None = None
    migration_policy: str = "pragmatic"

    def __post_init__(self):
        if self.freshness not in {"live", "snapshot"}:
            raise ValueError("CLIC freshness must be 'live' or 'snapshot'.")
        if self.migration_policy not in {"pragmatic", "strict"}:
            raise ValueError("CLIC migration_policy must be 'pragmatic' or 'strict'.")
        if self.allow_documented_proxies is None:
            object.__setattr__(
                self, "allow_documented_proxies", self.migration_policy == "pragmatic"
            )


class ClicInventoryProvider:
    """Resolve once per build, migrate by source context, and link locally."""

    def __init__(
        self,
        *,
        recipe=None,
        options=None,
        archive=None,
        session=None,
        catalog_provider=None,
    ):
        if options is None:
            options = ClicInventoryOptions()
        elif isinstance(options, Mapping):
            options = ClicInventoryOptions(**options)
        if not isinstance(options, ClicInventoryOptions):
            raise TypeError("clic_options must be ClicInventoryOptions or a mapping.")
        self.options = options
        self.recipe = ClicRecipe.read(recipe)
        self.archive = archive
        self.session = session
        self.catalog_provider = catalog_provider
        self.manifest = None
        self.lockfile = None
        self.diagnostics = []

    @property
    def cache_dir(self):
        if self.options.cache_dir is not None:
            return Path(self.options.cache_dir)
        from ..core.filesystem_constants import DIR_CACHED_FILES

        return DIR_CACHED_FILES / "clic"

    def get_archive(self):
        if self.archive is None:
            if self.options.archive_path is not None:
                self.archive = ClicArchive.read(self.options.archive_path)
            else:
                locked_record = (
                    (self.recipe.archive or {}).get("record_id")
                    if self.recipe.resolution_fingerprint
                    else None
                )
                self.archive = ClicArchiveStore(
                    self.cache_dir / "archives", session=self.session
                ).get(
                    locked_record or self.options.record_id,
                    latest=locked_record is None,
                    offline=self.options.offline,
                )
        return self.archive

    def resolve(self, roots=None, *, use_cache=True):
        archive = self.get_archive()
        roots = tuple(
            sorted(
                set(
                    self.recipe.roots
                    if roots is None
                    else ([roots] if isinstance(roots, str) else roots)
                )
            )
        )
        if use_cache and archive.manifest.get("payloads"):
            signature = content_hash(
                {
                    "schema": CLIC_GRAPH_SCHEMA,
                    "archive": archive.manifest,
                    "roots": roots,
                    "pins": {
                        slug: asdict(pin) for slug, pin in self.recipe.versions.items()
                    },
                    "locked_resolution": self.recipe.resolution_fingerprint,
                    "locked_archive": self.recipe.archive,
                }
            )
            directory = self.cache_dir / "resolved"
            directory.mkdir(parents=True, exist_ok=True)
            metadata_path, payload_path = (
                directory / f"{signature}.json",
                directory / f"{signature}.json.gz",
            )
            with portalocker.Lock(str(directory / f"{signature}.lock"), timeout=120):
                resolved = None
                if metadata_path.exists() and payload_path.exists():
                    try:
                        metadata = json.loads(metadata_path.read_text())
                        packed = payload_path.read_bytes()
                        if (
                            metadata["signature"] == signature
                            and hashlib.sha256(packed).hexdigest() == metadata["sha256"]
                        ):
                            data = json.loads(gzip.decompress(packed))
                            data["roots"] = tuple(data["roots"])
                            resolved = ResolvedClicInventory(**data)
                    except (OSError, ValueError, KeyError, TypeError, EOFError):
                        pass
                if resolved is None:
                    resolved = archive.resolve(roots, recipe=self.recipe)
                    digest = _gzip_write(payload_path, asdict(resolved))
                    _write_json(
                        metadata_path, {"signature": signature, "sha256": digest}
                    )
        else:
            resolved = archive.resolve(roots, recipe=self.recipe)
        if not self.options.offline and self.options.freshness == "live":
            verify_latest_versions(resolved, session=self.session)
        return resolved

    def prepare(
        self,
        roots,
        database,
        *,
        version,
        family="ecoinvent",
        system_model="cutoff",
        biosphere_name="biosphere3",
        keep_uncertainty=True,
        use_cache=True,
        biosphere_codes=None,
        classifications=None,
    ):
        target = resolve_inventory_target(
            version, family=family, system_model=system_model
        )
        version = target.technosphere.version
        family = target.technosphere.family
        resolved = self.resolve(roots, use_cache=use_cache)
        harmonized = ClicHarmonizer(
            self.cache_dir / "harmonized",
            catalog_provider=self.catalog_provider,
            allow_documented_proxies=self.options.allow_documented_proxies,
            migration_policy=self.options.migration_policy,
        ).harmonize(
            resolved,
            version=version,
            family=family,
            system_model=system_model,
            use_cache=use_cache,
        )
        # Names remain stable when more sectors are requested later in a build.
        identities = defaultdict(set)
        for did, native in self.get_archive()._selection(self.recipe).items():
            identities[activity_identity(native)].add(did)
        collisions = {key for key, datasets in identities.items() if len(datasets) > 1}
        if self.recipe.namespace_slugs is not None:
            collisions = {
                activity_identity(activity)
                for activity in harmonized.activities
                if activity.get("clic provenance", {}).get("slug")
                in self.recipe.namespace_slugs
            }
        namespace_slugs = sorted(
            activity["clic provenance"]["slug"]
            for activity in harmonized.activities
            if activity.get("clic provenance", {}).get("slug")
            and activity_identity(activity) in collisions
        )
        records, diagnostics = prepare_clic_for_premise(
            harmonized,
            database,
            version=version,
            family=family,
            system_model=system_model,
            biosphere_name=biosphere_name,
            keep_uncertainty=keep_uncertainty,
            biosphere_codes=biosphere_codes,
            classifications=classifications,
            namespace_identities=collisions,
            catalog_provider=self.catalog_provider,
        )
        from .clic_requirements import registry

        aliases = defaultdict(set)
        for node in registry().get("hybrid_sources", {}).get("nodes", ()):
            if node.get("clic_slug"):
                aliases[node["clic_slug"]].add(tuple(node["identity"]))
        for activity in records:
            row = activity["clic provenance"]
            row["legacy_identities"] = [
                list(identity)
                for identity in sorted(aliases.get(row.get("slug"), ()))
                if identity[3] == activity["unit"]
            ]
        self.manifest = harmonized.manifest
        self.manifest["adapter_diagnostics"] = diagnostics
        self.manifest["freshness"] = (
            "offline snapshot" if self.options.offline else self.options.freshness
        )
        self.manifest["resolved_at"] = datetime.now(timezone.utc).isoformat()
        self.manifest["destination_biosphere"] = biosphere_name
        self.manifest["keep_uncertainty"] = keep_uncertainty
        self.manifest["adapter_fingerprint"] = content_hash(
            [
                (
                    a["code"],
                    activity_identity(a),
                    [(e.get("input"), activity_identity(e)) for e in a["exchanges"]],
                )
                for a in records
            ]
        )
        self.lockfile = harmonized.lockfile
        self.lockfile["namespace_slugs"] = namespace_slugs
        self.diagnostics = harmonized.diagnostics + diagnostics
        return records

    def write_recipe(self, path):
        """Write a complete native-version lockfile after successful preparation."""
        if self.lockfile is None:
            raise ClicInventoryError(
                "Prepare CLIC inventories before exporting their recipe."
            )
        from .clic_download import _write_json

        path = Path(path)
        _write_json(path, self.lockfile)
        return path
