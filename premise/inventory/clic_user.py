"""Discover CLIC purchases in user inventories without replacing licensed backgrounds."""

from __future__ import annotations

import copy
import hashlib
from collections import defaultdict
from pathlib import Path

from .clic_archive import ClicInventoryError, activity_identity, content_hash
from .clic_hybrid import _key
from .clic_requirements import registry


def _references(edge):
    yield edge, False
    if edge.get("replacement name"):
        product = edge.get("replacement product", activity_identity(edge)[1])
        yield {
            "name": edge["replacement name"],
            "reference product": product,
            "location": edge.get("replacement location", edge.get("location")),
            "unit": edge.get("unit"),
        }, True


class ClicUserInventories:
    """Freeze supplied files once, then prepare owned copies after CLIC selection."""

    def __init__(self, specifications, database, *, version, system_model, pins=None):
        from .inventory_imports import AdditionalInventory

        self.sources = []
        self.required_nodes = set()
        self.version, self.system_model = version, system_model
        for position, specification in enumerate(specifications or (), 1):
            path = Path(specification["filepath"])
            label = f"{position}-{path.name}"
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if pins is not None and pins.get(label) != digest:
                raise ClicInventoryError(
                    f"User inventory differs from the locked recipe: {label}."
                )
            importer = AdditionalInventory(
                database=database,
                version_in=specification["ecoinvent version"],
                version_out=version,
                path=path,
                system_model=system_model,
            )
            self.sources.append(
                (
                    specification,
                    importer.import_db,
                    {
                        "file": path.name,
                        "recipe_key": label,
                        "sha256": digest,
                        "source_version": specification["ecoinvent version"],
                    },
                )
            )
        if pins is not None and set(pins) != {
            row[2]["recipe_key"] for row in self.sources
        }:
            raise ClicInventoryError(
                "Supply all user inventories required by the locked recipe."
            )

    def required_roots(self, archive, recipe, database, *, source_files=()):
        from .inventory_imports import migrate_import_db

        native = {_key(activity_identity(activity)) for activity in database}
        public = defaultdict(set)
        retained = defaultdict(list)
        active = {Path(path).name for path, _ in source_files}
        for node in registry()["hybrid_sources"]["nodes"]:
            if node["clic_slug"]:
                public[_key(node["identity"])].add(node["clic_slug"])
            elif node["file"] in active:
                retained[_key(node["identity"])].append(node)
        for did, version in archive._selection(recipe).items():
            public[_key(activity_identity(version))].add(archive.datasets[did]["slug"])
        inventories = []
        for specification, importer, _ in self.sources:
            owned = copy.deepcopy(importer)
            migrate_import_db(owned, specification["ecoinvent version"], self.version)
            inventories.extend(owned.data)
        local = {_key(activity_identity(activity)) for activity in inventories}
        roots = set()
        self.required_nodes = set()
        for activity in inventories:
            for edge in activity["exchanges"]:
                if edge.get("type") != "technosphere":
                    continue
                for reference, _ in _references(edge):
                    explicit = reference.get("clic slug")
                    if explicit:
                        if explicit not in archive.slugs:
                            raise ClicInventoryError(
                                f"Unknown user inventory CLIC slug: {explicit!r}."
                            )
                        roots.add(explicit)
                        continue
                    key = _key(activity_identity(reference))
                    if key in native or key in local:
                        continue
                    choices = public.get(key, ())
                    if len(choices) > 1:
                        raise ClicInventoryError(
                            f"Ambiguous CLIC purchase in user inventory: {activity_identity(reference)!r}; "
                            "set 'clic slug' on that exchange."
                        )
                    roots.update(choices)
                    if not choices:
                        private = retained.get(key, ())
                        if len({node["science_hash"] for node in private}) > 1:
                            raise ClicInventoryError(
                                f"Ambiguous retained purchase in user inventory: {activity_identity(reference)!r}."
                            )
                        if private:
                            self.required_nodes.add(min(node["id"] for node in private))
        return roots

    def prepare(self, database, clic_activities):
        from .inventory_imports import AdditionalInventory

        if not self.sources:
            return [], []
        native = {
            _key(activity_identity(activity))
            for activity in database
            if not activity.get("clic provenance")
        }
        aliases, by_slug = defaultdict(dict), {}
        for activity in clic_activities:
            row = activity.get("clic provenance", {})
            if not row.get("slug"):
                continue
            by_slug[row["slug"]] = activity
            for identity in [
                activity_identity(activity),
                activity_identity(row),
                *row.get("legacy_identities", ()),
            ]:
                aliases[_key(identity)][activity["code"]] = activity
        records, metadata = [], []
        for specification, importer, source in self.sources:
            local = {_key(activity_identity(activity)) for activity in importer.data}

            def rebind(activities):
                for activity in activities:
                    activity["inventory provenance"] = {
                        **source,
                        "provider": "user inventory",
                        "record_id": content_hash(
                            [source["recipe_key"], activity_identity(activity)]
                        ),
                    }
                    for edge in activity["exchanges"]:
                        if edge.get("type") != "technosphere":
                            continue
                        for reference, replacement in _references(edge):
                            key = _key(activity_identity(reference))
                            slug = reference.get("clic slug")
                            if slug:
                                if slug not in by_slug:
                                    raise ClicInventoryError(
                                        f"Unselected user inventory CLIC slug: {slug!r}."
                                    )
                                choices = [by_slug[slug]]
                            elif key in native or key in local:
                                continue
                            else:
                                choices = list(aliases.get(key, {}).values())
                            if len(choices) > 1:
                                raise ClicInventoryError(
                                    f"Ambiguous CLIC link in user inventory: {activity_identity(reference)!r}."
                                )
                            if choices:
                                target = choices[0]
                                if reference["unit"] != target["unit"]:
                                    raise ClicInventoryError(
                                        "A user inventory CLIC purchase changes unit; review its quantity."
                                    )
                                if replacement:
                                    edge.update(
                                        {
                                            "replacement name": target["name"],
                                            "replacement product": target[
                                                "reference product"
                                            ],
                                            "replacement location": target["location"],
                                        }
                                    )
                                else:
                                    edge.update(
                                        name=target["name"],
                                        product=target["reference product"],
                                        location=target["location"],
                                        input=(target["database"], target["code"]),
                                    )
                                    edge["reference product"] = target[
                                        "reference product"
                                    ]

            prepared = AdditionalInventory(
                database=database,
                version_in=specification["ecoinvent version"],
                version_out=self.version,
                path=specification["filepath"],
                system_model=self.system_model,
                preloaded_importer=copy.deepcopy(importer),
                before_linking=rebind,
            )
            result = prepared.merge_inventory()
            if prepared.list_unlinked:
                raise ClicInventoryError(
                    f"User inventory {source['file']} has unlinked exchanges."
                )
            records.extend(result)
            database.extend(result)
            metadata.append({**source, "activity_count": len(result)})
        return records, metadata
