"""CLIC role bindings must survive published inventory identity revisions."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from premise.core.filesystem_constants import DATA_DIR, VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping
from premise.inventory.clic_archive import ClicArchive
from premise.inventory.clic_requirements import (
    bind_activity_roles,
    registry,
    role_slugs,
)


def _reviewed_roles():
    requirements = registry()
    for filename, roles in requirements["mappings"].items():
        for role in roles:
            slugs = role_slugs(filename, role)
            if slugs:
                yield filename, role, slugs
    filename = "activities_mapping.yml"
    definitions = get_mapping(_mapping_path(filename), "ecoinvent_aliases")
    for role, definition in definitions.items():
        slugs = tuple(definition.get("clic_slugs", ()))
        if slugs:
            yield filename, role, slugs


def _mapping_path(filename):
    if filename == "activities_mapping.yml":
        return DATA_DIR / "metals" / filename
    return VARIABLES_DIR / filename


def _reviewed_slugs():
    return {slug for _, _, slugs in _reviewed_roles() for slug in slugs}


def test_every_reviewed_role_exists_and_exposes_its_clic_slugs():
    """The generated registry and the runtime YAML filters cannot drift apart."""
    by_file = {}
    for filename, role, slugs in _reviewed_roles():
        definitions = by_file.setdefault(
            filename,
            get_mapping(_mapping_path(filename), "ecoinvent_aliases"),
        )
        assert role in definitions, f"Missing ecoinvent_aliases role {filename}:{role}"
        assert set(definitions[role]["clic_slugs"]) == set(slugs)
        if filename != "activities_mapping.yml":
            assert definitions[role]["clic_role_key"] == f"{filename}:{role}"


def test_all_role_filters_accept_reviewed_clic_datasets_after_identity_changes():
    """Names and products may change; explicit reviewed bindings must remain stable."""
    roles_by_file = {}
    for filename, role, slugs in _reviewed_roles():
        roles_by_file.setdefault(filename, {})[role] = slugs

    for filename, roles in roles_by_file.items():
        records = []
        expected_codes = {}
        for role, slugs in roles.items():
            role_key = f"{filename}:{role}"
            expected_codes[role] = set()
            for index, slug in enumerate(slugs):
                code = f"{role_key}:{index}"
                expected_codes[role].add(code)
                records.append(
                    {
                        "name": f"renamed CLIC dataset {code}",
                        "reference product": f"renamed CLIC product {code}",
                        "location": "CLIC-filter-audit",
                        "unit": "unit",
                        "code": code,
                        "clic provenance": {
                            "slug": slug,
                            "role_bindings": [role_key],
                        },
                    }
                )

        definitions = get_mapping(_mapping_path(filename), "ecoinvent_aliases")
        selected = InventorySet(records).generate_sets_from_filters(definitions)
        for role, codes in expected_codes.items():
            actual = {record["code"] for record in selected[role]}
            assert actual == codes, (
                f"CLIC identity changes broke {filename}:{role}; "
                f"missing={sorted(codes - actual)!r}, "
                f"unexpected={sorted(actual - codes)!r}"
            )


def test_unreviewed_procedural_role_rejects_renamed_clic_false_positive():
    definitions = get_mapping(
        _mapping_path("activities_mapping.yml"), "ecoinvent_aliases"
    )
    assert definitions["CdTe"]["clic_slugs"] == ()
    record = {
        "name": "photovoltaic laminate, CdTe, first solar series 4, at plant",
        "reference product": "photovoltaic laminate, CdTe",
        "location": "MY",
        "unit": "square meter",
        "clic provenance": {"slug": "renamed-unreviewed-cdte"},
    }
    selected = InventorySet([record]).generate_sets_from_filters(
        {"CdTe": definitions["CdTe"]}
    )
    assert selected["CdTe"] == []


@pytest.fixture(scope="module")
def live_clic_archive():
    path = os.environ.get("PREMISE_CLIC_ARCHIVE")
    if not path:
        pytest.skip("Set PREMISE_CLIC_ARCHIVE to audit a downloaded CLIC snapshot")
    return ClicArchive.read(Path(path))


def test_live_archive_contains_every_reviewed_slug(live_clic_archive):
    missing = _reviewed_slugs() - set(live_clic_archive.slugs)
    assert (
        not missing
    ), f"Current CLIC archive is missing reviewed slugs: {sorted(missing)!r}"


def test_live_archive_records_pass_all_reviewed_role_filters(live_clic_archive):
    """Resolve current names and exercise binding and filtering as production does."""
    resolved = live_clic_archive.resolve(sorted(_reviewed_slugs()))
    requirements = registry()
    source_files = [
        (Path(filename), "3.12") for filename in requirements["hybrid_sources"]["files"]
    ]
    bind_activity_roles(resolved.activities, source_files)
    bindings_by_slug = {
        record["clic provenance"]["slug"]: set(
            record["clic provenance"]["role_bindings"]
        )
        for record in resolved.activities
    }

    roles_by_file = {}
    for filename, role, slugs in _reviewed_roles():
        roles_by_file.setdefault(filename, {})[role] = set(slugs)

    for filename, roles in roles_by_file.items():
        definitions = get_mapping(_mapping_path(filename), "ecoinvent_aliases")
        selected = InventorySet(resolved.activities).generate_sets_from_filters(
            definitions
        )
        for role, expected in roles.items():
            role_key = definitions[role].get("clic_role_key")
            if role_key is not None:
                expected = {
                    slug for slug in expected if role_key in bindings_by_slug[slug]
                }
            actual = {
                record["clic provenance"]["slug"]
                for record in selected[role]
                if record.get("clic provenance")
            }
            assert actual == expected, (
                f"Current CLIC identities broke {filename}:{role}; "
                f"missing={sorted(expected - actual)!r}, "
                f"unexpected={sorted(actual - expected)!r}"
            )
