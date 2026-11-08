"""Inventory destination families and biospheres cannot be inferred from a label."""

import pytest

from premise.inventory.clic_target import resolve_inventory_target


def test_uvek_2026_uses_the_explicit_canonical_biosphere():
    target = resolve_inventory_target("2026", family="uvek")
    assert target.technosphere.family == "uvek"
    assert target.technosphere.version == "2026"
    assert target.technosphere.system_model == "cutoff"
    assert target.biosphere.family == "ecoinvent"
    assert target.biosphere.version == "3.10"


@pytest.mark.parametrize("version", ["2025", "3.12", "2027"])
def test_other_uvek_releases_are_not_implicitly_supported(version):
    with pytest.raises(ValueError, match="release 2026"):
        resolve_inventory_target(version, family="uvek")


@pytest.mark.parametrize("system_model", ["consequential", "EN15804", "unknown"])
def test_uvek_never_acquires_a_cross_model_route(system_model):
    with pytest.raises(ValueError, match="requires system_model='cutoff'"):
        resolve_inventory_target("2026", family="uvek", system_model=system_model)


@pytest.mark.parametrize(
    "version,expected", [("3.7.1", "3.7"), ("3.9.1", "3.9"), ("3.12", "3.12")]
)
def test_existing_ecoinvent_target_aliases_remain_unchanged(version, expected):
    target = resolve_inventory_target(version)
    assert target.technosphere.family == target.biosphere.family == "ecoinvent"
    assert target.technosphere.version == target.biosphere.version == expected


@pytest.mark.parametrize("version", ["3.5", "3.6", "2026"])
def test_ecoinvent_targets_still_use_the_existing_version_guard(version):
    with pytest.raises(ValueError, match="Unsupported ecoinvent background version"):
        resolve_inventory_target(version)


def test_en15804_bridge_remains_explicit():
    target = resolve_inventory_target("3.12", system_model="EN15804")
    assert target.technosphere.system_model == "cutoff"
    assert target.technosphere.family == "ecoinvent"


def test_unknown_target_family_is_refused():
    with pytest.raises(ValueError, match="Unsupported inventory target family"):
        resolve_inventory_target("2026", family="other")
