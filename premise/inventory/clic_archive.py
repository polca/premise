"""Read and resolve native CLIC inventories without loading an LCA application.

Archive versions are immutable inputs to a build. A recipe chooses one version
per dataset; a graph walk then selects roots and their foreground suppliers.
Background migration and matching to a user's database happen separately.
"""

from __future__ import annotations

import copy
import hashlib
import json
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml
from packaging.version import InvalidVersion, Version

CLIC_ARCHIVE_FORMATS = {"clic-inventory-archive-v3", "clic-inventory-archive-v4"}
CLIC_RECIPE_SCHEMA = 1
CLIC_GRAPH_SCHEMA = 2
PUBLIC_VERSION_STATUSES = {"published", "superseded"}


class ClicInventoryError(ValueError):
    """A CLIC inventory cannot be used without changing the requested contract."""


class ClicArchiveError(ClicInventoryError):
    """The archive is corrupt, unsupported, or internally inconsistent."""


class ClicResolutionError(ClicInventoryError):
    """A requested version or foreground supplier cannot be resolved uniquely."""


def content_hash(value: Any) -> str:
    """Hash JSON content, rejecting quantities that JSON cannot represent."""
    try:
        data = json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ClicInventoryError(f"Nonportable CLIC content: {error}") from error
    return hashlib.sha256(data).hexdigest()


def activity_identity(record: Mapping) -> tuple:
    """Return the qualified identity used by native and Brightway inventories."""
    product = (
        record.get("reference product")
        or record.get("reference_product")
        or record.get("product")
    )
    return record.get("name"), product, record.get("location"), record.get("unit")


def version_hash(version: Mapping) -> str:
    """Preserve pins when publication of a successor supersedes a version."""
    return content_hash(
        {
            key: value
            for key, value in version.items()
            if key not in {"status", "updated_at", "metadata_corrected_at"}
        }
    )


@dataclass(frozen=True)
class VersionPin:
    version: str
    version_id: int | None = None
    sha256: str | None = None


@dataclass(frozen=True)
class ClicRecipe:
    """Requested versions; unlisted datasets use their latest published version.

    Pins never add inventories to a build. They affect a dataset only if it is
    required as a root or encountered in that build's foreground graph.
    """

    versions: Mapping[str, VersionPin]
    roots: tuple[str, ...] = ()
    archive: Mapping | None = None
    resolution_fingerprint: str | None = None
    workbooks: Mapping[str, str] | None = None
    user_inventories: Mapping[str, str] | None = None
    namespace_slugs: tuple[str, ...] | None = None

    @classmethod
    def read(cls, value: ClicRecipe | Mapping | str | Path | None) -> ClicRecipe:
        if isinstance(value, cls):
            return value
        if value is None:
            return cls({})
        if isinstance(value, (str, Path)):
            with Path(value).open(encoding="utf-8") as stream:
                value = yaml.safe_load(stream)
        if not isinstance(value, Mapping):
            raise ClicResolutionError("A CLIC recipe must be a JSON/YAML mapping.")
        if value.get("schema_version", CLIC_RECIPE_SCHEMA) != CLIC_RECIPE_SCHEMA:
            raise ClicResolutionError("Unsupported CLIC recipe schema.")
        pins = value.get("versions", {})
        if not isinstance(pins, Mapping):
            raise ClicResolutionError("Recipe 'versions' must map slugs to versions.")
        result = {}
        for slug, pin in pins.items():
            if not isinstance(slug, str) or not slug:
                raise ClicResolutionError("Recipe dataset slugs must be strings.")
            if isinstance(pin, str):
                pin = {"version": pin}
            if not isinstance(pin, Mapping) or not isinstance(pin.get("version"), str):
                raise ClicResolutionError(f"Invalid version pin for {slug!r}.")
            version = pin["version"]
            if version != "latest":
                try:
                    Version(version)
                except InvalidVersion as error:
                    raise ClicResolutionError(
                        f"Invalid version {version!r} for {slug!r}."
                    ) from error
            version_id = pin.get("version_id")
            if version_id is not None and (
                isinstance(version_id, bool) or not isinstance(version_id, int)
            ):
                raise ClicResolutionError(f"Invalid numeric version ID for {slug!r}.")
            sha256 = pin.get("sha256")
            if sha256 is not None and (
                not isinstance(sha256, str)
                or len(sha256) != 64
                or any(c not in "0123456789abcdef" for c in sha256)
            ):
                raise ClicResolutionError(f"Invalid content checksum for {slug!r}.")
            result[slug] = VersionPin(version, version_id, sha256)
        roots = value.get("roots", ())
        if not isinstance(roots, (list, tuple)) or any(
            not isinstance(slug, str) for slug in roots
        ):
            raise ClicResolutionError("Recipe roots must be a list of dataset slugs.")
        for field in ("workbooks", "user_inventories"):
            pins = value.get(field)
            if pins is not None and (
                not isinstance(pins, Mapping)
                or any(
                    not isinstance(name, str)
                    or Path(name).name != name
                    or not isinstance(digest, str)
                    or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)
                    for name, digest in pins.items()
                )
            ):
                raise ClicResolutionError(
                    f"Recipe {field} must map filenames to SHA256 checksums."
                )
        namespaces = value.get("namespace_slugs")
        if namespaces is not None and (
            not isinstance(namespaces, (list, tuple))
            or any(not isinstance(slug, str) for slug in namespaces)
        ):
            raise ClicResolutionError(
                "Recipe namespace_slugs must be a list of dataset slugs."
            )
        return cls(
            result,
            tuple(roots),
            copy.deepcopy(value.get("archive")),
            value.get("resolution_fingerprint"),
            copy.deepcopy(value.get("workbooks")),
            copy.deepcopy(value.get("user_inventories")),
            tuple(namespaces) if namespaces is not None else None,
        )


@dataclass
class ResolvedClicInventory:
    """An owned native foreground graph and its complete version manifest."""

    activities: list[dict]
    versions: list[dict]
    roots: tuple[str, ...]
    archive: dict
    fingerprint: str
    diagnostics: list[dict]

    def lockfile(self) -> dict:
        """Return a recipe pinning every selected root and upstream version."""
        return {
            "schema_version": CLIC_RECIPE_SCHEMA,
            "roots": list(self.roots),
            "archive": copy.deepcopy(self.archive),
            "resolution_fingerprint": self.fingerprint,
            "versions": {
                row["slug"]: {
                    "version": row["semantic_version"],
                    "version_id": row["version_id"],
                    "sha256": row["sha256"],
                }
                for row in self.versions
            },
        }


class ClicArchive:
    """An integrity-checked public catalog with native inventory history."""

    def __init__(self, manifest, datasets, versions, dependencies=(), *, path=None):
        self.manifest = copy.deepcopy(manifest)
        self.path = Path(path) if path is not None else None
        self.datasets = {}
        self.slugs = {}
        self.versions = {}
        self.dataset_versions = defaultdict(list)
        self.dependencies = defaultdict(set)
        for dataset in datasets:
            did, slug = dataset["id"], dataset["slug"]
            if did in self.datasets or slug in self.slugs:
                raise ClicArchiveError(f"Duplicate CLIC dataset identity: {slug!r}.")
            self.datasets[did] = dataset
            self.slugs[slug] = did
        for version in versions:
            vid, did = version["id"], version["dataset_id"]
            if vid in self.versions or did not in self.datasets:
                raise ClicArchiveError(f"Invalid CLIC version identity: {vid!r}.")
            if not isinstance(version.get("inventory_snapshot"), Mapping):
                raise ClicArchiveError(f"Missing native inventory for version {vid}.")
            self.versions[vid] = version
            self.dataset_versions[did].append(version)
        for dependency in dependencies:
            if "version_id" in dependency:
                self.dependencies[dependency["version_id"]].add(
                    dependency["exchange_index"]
                )
        self.latest = {}
        for did, candidates in self.dataset_versions.items():
            published = [v for v in candidates if v["status"] == "published"]
            if published:
                try:
                    self.latest[did] = max(
                        published, key=lambda v: Version(v["semantic_version"])
                    )
                except InvalidVersion as error:
                    raise ClicArchiveError(
                        "Invalid archived semantic version."
                    ) from error

    @classmethod
    def read(cls, path: str | Path) -> ClicArchive:
        """Verify payload hashes before accepting an archive's inventory data."""
        try:
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                if len(names) != len(set(names)):
                    raise ClicArchiveError("Duplicate ZIP entries in CLIC archive.")
                manifest = json.loads(archive.read("manifest.json"))
                if manifest.get("format") not in CLIC_ARCHIVE_FORMATS:
                    raise ClicArchiveError("Unsupported CLIC archive format.")
                payloads = manifest.get("payloads", {})
                for name in ("datasets.jsonl", "versions.jsonl"):
                    if name not in payloads:
                        raise ClicArchiveError(f"Missing checksum for {name}.")
                decoded = {}
                for name, expected in payloads.items():
                    payload = archive.read(name)
                    if hashlib.sha256(payload).hexdigest() != expected:
                        raise ClicArchiveError(f"CLIC checksum mismatch: {name}.")
                    if name.endswith(".jsonl"):
                        decoded[name] = [
                            json.loads(line) for line in payload.splitlines() if line
                        ]
                datasets = decoded["datasets.jsonl"]
                versions = decoded["versions.jsonl"]
                if len(datasets) != manifest.get("dataset_count") or len(
                    versions
                ) != manifest.get("version_count"):
                    raise ClicArchiveError(
                        "CLIC archive counts do not match its manifest."
                    )
                return cls(
                    manifest,
                    datasets,
                    versions,
                    decoded.get("dependencies.jsonl", ()),
                    path=path,
                )
        except (OSError, zipfile.BadZipFile, KeyError, json.JSONDecodeError) as error:
            raise ClicArchiveError(f"Cannot read CLIC archive: {error}") from error

    def _selection(self, recipe: ClicRecipe) -> dict[int, dict]:
        selected = dict(self.latest)
        for slug, pin in recipe.versions.items():
            if slug not in self.slugs:
                raise ClicResolutionError(
                    f"Recipe refers to unknown CLIC slug {slug!r}."
                )
            did = self.slugs[slug]
            if pin.version == "latest":
                if did not in selected:
                    raise ClicResolutionError(f"No published version for {slug!r}.")
                candidate = selected[did]
            else:
                matches = [
                    v
                    for v in self.dataset_versions[did]
                    if v["semantic_version"] == pin.version
                    and v["status"] in PUBLIC_VERSION_STATUSES
                ]
                if len(matches) != 1:
                    raise ClicResolutionError(
                        f"Expected one public version {pin.version!r} for {slug!r}."
                    )
                candidate = matches[0]
            if pin.version_id is not None and pin.version_id != candidate["id"]:
                raise ClicResolutionError(
                    f"Version ID does not match recipe for {slug!r}."
                )
            if pin.sha256 is not None and pin.sha256 != version_hash(candidate):
                raise ClicResolutionError(
                    f"Archived content changed for pinned {slug!r}."
                )
            selected[did] = candidate
        return selected

    def resolve(
        self,
        roots: Iterable[str] | None = None,
        *,
        recipe: ClicRecipe | Mapping | str | Path | None = None,
    ) -> ResolvedClicInventory:
        """Resolve only roots and their upstream foreground, allowing graph cycles.

        Qualified current identities take priority over stale source-project
        input codes. CLIC revisions can add explicit unit-output helpers while
        preserving an old input code in a consuming source snapshot.
        """
        recipe = ClicRecipe.read(recipe)
        if isinstance(roots, str):
            roots = (roots,)
        roots = tuple(sorted(set(recipe.roots if roots is None else roots)))
        unknown = [slug for slug in roots if slug not in self.slugs]
        if unknown:
            raise ClicResolutionError(
                f"Unknown CLIC root slug(s): {', '.join(unknown)}."
            )
        selected = self._selection(recipe)
        unavailable = [slug for slug in roots if self.slugs[slug] not in selected]
        if unavailable:
            raise ClicResolutionError(f"No public version for roots: {unavailable!r}.")
        if recipe.resolution_fingerprint is not None:
            # A lockfile is a complete graph, not an invitation to discover newly
            # published foreground replacements for its background exchanges.
            selected = {
                did: version
                for did, version in selected.items()
                if self.datasets[did]["slug"] in recipe.versions
            }
            if any(self.slugs[slug] not in selected for slug in roots):
                raise ClicResolutionError("A lockfile must pin every requested root.")
        current, historical, codes = (defaultdict(set) for _ in range(3))
        for did, version in selected.items():
            current[activity_identity(version)].add(did)
            current[activity_identity(version["inventory_snapshot"])].add(did)
            codes[("__clic__", version["activity_code"])].add(did)
        for version in self.versions.values():
            did, snapshot = version["dataset_id"], version["inventory_snapshot"]
            if did not in selected:
                continue
            historical[activity_identity(version)].add(did)
            historical[activity_identity(snapshot)].add(did)
            if snapshot.get("database") and snapshot.get("code"):
                codes[(snapshot["database"], snapshot["code"])].add(did)

        profile_fields = (
            "background_family",
            "background_version",
            "background_system_model",
            "biosphere_family",
            "biosphere_version",
        )

        def supplier(edge, consumer):
            key = activity_identity(edge)
            candidates = current.get(key, set())
            reference = edge.get("input")
            if isinstance(reference, Mapping):
                reference = (reference.get("database"), reference.get("code"))
            by_code = (
                codes.get(tuple(reference), set())
                if isinstance(reference, (list, tuple)) and len(reference) == 2
                else set()
            )
            if len(candidates) != 1:
                if candidates and by_code:
                    candidates = (candidates & by_code) or candidates
                elif not candidates:
                    candidates = historical.get(key, set()) or by_code
            if len(candidates) > 1:
                same_profile = {
                    did
                    for did in candidates
                    if all(
                        selected[did].get(field) == consumer.get(field)
                        for field in profile_fields
                    )
                }
                if same_profile:
                    candidates = same_profile
            if len(candidates) > 1:
                original_database = consumer["inventory_snapshot"].get("database")
                same_source = {
                    did
                    for did in candidates
                    if original_database
                    and selected[did]["inventory_snapshot"].get("database")
                    == original_database
                }
                if same_source:
                    candidates = same_source
            if (
                candidates == {consumer["dataset_id"]}
                and not current.get(key)
                and not (
                    isinstance(reference, (list, tuple)) and reference[0] == "__clic__"
                )
            ):
                # A renamed reference can intentionally purchase the native
                # market that formerly shared its label (e.g. NMC622 v1.0.2).
                # Historical aliases alone must not turn that purchase into a
                # unit self-loop. Source-catalog validation and destination
                # linking still have to prove the external supplier exists.
                return None
            if len(candidates) > 1:
                raise ClicResolutionError(
                    f"Ambiguous CLIC supplier in {self.datasets[consumer['dataset_id']]['slug']!r}: "
                    f"{key!r}; candidates={sorted(self.datasets[did]['slug'] for did in candidates)!r}."
                )
            if not candidates:
                return None
            version = selected[next(iter(candidates))]
            if edge.get("unit") != version["unit"]:
                raise ClicResolutionError(
                    f"Supplier unit changed for {key!r}: {version['unit']!r}. "
                    "An explicit quantity conversion or a reviewed supplier is required."
                )
            return version

        chosen, links = {}, {}
        pending = [selected[self.slugs[slug]] for slug in roots]
        while pending:
            version = pending.pop()
            did = version["dataset_id"]
            if did in chosen:
                continue
            chosen[did] = version
            for index, edge in enumerate(version["inventory_snapshot"]["exchanges"]):
                if edge["type"] != "technosphere":
                    continue
                child = supplier(edge, version)
                if child is not None:
                    links[(did, index)] = child["dataset_id"]
                    pending.append(child)
                elif index in self.dependencies.get(version["id"], ()):
                    raise ClicResolutionError(
                        f"Known missing foreground supplier in {self.datasets[did]['slug']!r}, "
                        f"version {version['semantic_version']}, exchange {index}."
                    )

        activities, rows, diagnostics = [], [], []
        for did in sorted(chosen, key=lambda key: self.datasets[key]["slug"]):
            version = chosen[did]
            slug = self.datasets[did]["slug"]
            activity = copy.deepcopy(version["inventory_snapshot"])
            activity.update(
                code=version["activity_code"],
                database="__clic__",
                type="process",
                name=version["name"],
                location=version["location"],
                unit=version["unit"],
            )
            activity["reference product"] = version["reference_product"]
            for index, edge in enumerate(activity["exchanges"]):
                child_id = links.get((did, index))
                if child_id is not None:
                    child = chosen[child_id]
                    old_identity = activity_identity(edge)
                    edge.update(
                        name=child["name"],
                        location=child["location"],
                        unit=child["unit"],
                        input=("__clic__", child["activity_code"]),
                    )
                    edge["reference product"] = child["reference_product"]
                    if "product" in edge:
                        edge["product"] = child["reference_product"]
                    if old_identity != activity_identity(edge):
                        diagnostics.append(
                            {
                                "code": "clic.supplier_identity_updated",
                                "slug": slug,
                                "exchange_index": index,
                                "old": list(old_identity),
                                "new": list(activity_identity(edge)),
                            }
                        )
                elif edge["type"] == "production":
                    edge.update(
                        name=activity["name"],
                        location=activity["location"],
                        unit=activity["unit"],
                        input=("__clic__", activity["code"]),
                    )
                    edge["reference product"] = activity["reference product"]
                    if "product" in edge:
                        edge["product"] = activity["reference product"]
                else:
                    # Source-project keys are not links in a user's destination database.
                    edge.pop("input", None)
            row = {
                "slug": slug,
                "version_id": version["id"],
                "name": version["name"],
                "reference_product": version["reference_product"],
                "location": version["location"],
                "unit": version["unit"],
                "semantic_version": version["semantic_version"],
                "activity_code": version["activity_code"],
                "role": "root" if slug in roots else "upstream",
                "sha256": version_hash(version),
                "license": version.get("selected_license", ""),
                "citation": version.get("original_citation")
                or version.get("source", ""),
                "doi": version.get("original_doi", ""),
                "background": {
                    "technosphere": {
                        "family": version["background_family"],
                        "version": version["background_version"],
                        "system_model": version["background_system_model"],
                    },
                    "biosphere": {
                        "family": version["biosphere_family"],
                        "version": version["biosphere_version"],
                    },
                },
                "pinned": slug in recipe.versions
                and recipe.versions[slug].version != "latest",
                "public_url": f"https://clic.psi.ch/datasets/{slug}/versions/{version['semantic_version']}/",
            }
            activity["clic provenance"] = copy.deepcopy(row)
            activities.append(activity)
            rows.append(row)
        fingerprint = content_hash(
            {
                "schema": CLIC_GRAPH_SCHEMA,
                "versions": [(row["slug"], row["sha256"]) for row in rows],
                "roots": roots,
                "links": sorted(
                    (did, index, child) for (did, index), child in links.items()
                ),
            }
        )
        if (
            recipe.resolution_fingerprint is not None
            and fingerprint != recipe.resolution_fingerprint
        ):
            raise ClicResolutionError(
                "Resolved inventory graph does not match the CLIC lockfile."
            )
        archive_info = {
            "format": self.manifest.get("format"),
            "fingerprint": self.manifest.get("fingerprint"),
            "created_at": self.manifest.get("created_at"),
            "source": self.manifest.get("source"),
        }
        for field in ("record_id", "doi", "record_version"):
            if field in self.manifest:
                archive_info[field] = self.manifest[field]
        return ResolvedClicInventory(
            activities, rows, roots, archive_info, fingerprint, diagnostics
        )
