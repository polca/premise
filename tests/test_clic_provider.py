"""Resolved graph caches are owned, integrity-checked, and still checked for freshness."""

from copy import deepcopy
from unittest.mock import Mock
import pytest

from premise.inventory import clic as module
from premise.inventory.clic_archive import ClicArchive
from test_clic_archive import write_archive
from test_clic_archive import catalog, native
from test_clic_harmonize import provider


@pytest.mark.parametrize("version", ["3.5", "3.6", 3.6])
def test_old_target_fails_before_archive_download(tmp_path, version):
    client = module.ClicInventoryProvider(options={"cache_dir": tmp_path})
    client.get_archive = Mock(
        side_effect=AssertionError("Must reject before downloads")
    )
    with pytest.raises(ValueError, match="minimum supported version is 3.7"):
        client.prepare([], [], version=version)
    client.get_archive.assert_not_called()


def test_pragmatic_defaults_and_strict_opt_in():
    assert module.ClicInventoryOptions().allow_documented_proxies is True
    assert (
        module.ClicInventoryOptions(migration_policy="strict").allow_documented_proxies
        is False
    )
    assert (
        module.ClicInventoryOptions(
            allow_documented_proxies=False
        ).allow_documented_proxies
        is False
    )
    with pytest.raises(ValueError, match="migration_policy"):
        module.ClicInventoryOptions(migration_policy="unknown")


def test_lockfile_freezes_namespace_choices_when_unselected_catalog_changes(
    tmp_path, provider
):
    original = native(1, 1, "root")

    def client(archive, recipe=None):
        return module.ClicInventoryProvider(
            archive=archive,
            recipe=recipe,
            catalog_provider=provider,
            options={"cache_dir": tmp_path, "offline": True},
        )

    def comparable(records):
        records = deepcopy(records)
        for activity in records:
            activity["clic provenance"].pop("pinned")
        return records

    first = client(catalog(original))
    before = first.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    later = client(catalog(original, native(2, 2, "root")), first.lockfile)
    after = later.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert first.lockfile["namespace_slugs"] == []
    assert before[0]["clic provenance"]["pinned"] is False
    assert after[0]["clic provenance"]["pinned"] is True
    assert comparable(before) == comparable(after)
    forced = client(catalog(original, native(2, 2, "root")))
    namespaced = forced.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert forced.lockfile["namespace_slugs"] == ["dataset-1"]
    replay = client(catalog(original), forced.lockfile)
    replayed = replay.prepare(
        ["dataset-1"], [], version="3.12", biosphere_codes={}, classifications={}
    )
    assert comparable(namespaced) == comparable(replayed)


def test_resolved_cache_is_owned_and_rebuilt_when_corrupt(tmp_path):
    archive = ClicArchive.read(write_archive(tmp_path / "archive.zip"))
    archive.resolve = Mock(wraps=archive.resolve)
    provider = module.ClicInventoryProvider(
        archive=archive, options={"cache_dir": tmp_path / "cache", "offline": True}
    )
    first = provider.resolve(["dataset-1"])
    first.activities[0]["name"] = "caller mutation"
    again = provider.resolve(["dataset-1"])
    assert archive.resolve.call_count == 1
    assert again.activities[0]["name"] != "caller mutation"
    next((tmp_path / "cache/resolved").glob("*.json.gz")).write_bytes(b"truncated")
    repaired = provider.resolve(["dataset-1"])
    assert archive.resolve.call_count == 2
    assert repaired.fingerprint == again.fingerprint
    provider.resolve(["dataset-1"], use_cache=False)
    assert archive.resolve.call_count == 3


def test_cached_resolution_still_verifies_live_versions_each_build(
    monkeypatch, tmp_path
):
    archive = ClicArchive.read(write_archive(tmp_path / "archive.zip"))
    verify = Mock()
    monkeypatch.setattr(module, "verify_latest_versions", verify)
    for _ in range(2):
        provider = module.ClicInventoryProvider(
            archive=archive, options={"cache_dir": tmp_path / "cache"}
        )
        provider.resolve(["dataset-1"])
    assert verify.call_count == 2
