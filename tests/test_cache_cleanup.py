import gc
import json
import logging
import multiprocessing
from pathlib import Path
import time
import uuid

import pytest

from premise import cache_cleanup as cleanup
from premise.inventory_store import CompactInventoryStore, InventoryStore
from premise.new_database import NewDatabase


@pytest.fixture
def cache_root(tmp_path, monkeypatch):
    root = tmp_path / "cached_files"
    root.mkdir()
    monkeypatch.setattr(cleanup.fs, "DIR_CACHED_FILES", root)
    monkeypatch.setattr(cleanup.time, "time", lambda: 200_000)
    return root


def checkpoint(root, *, age=86_401, base=None, marked=True, temporary=False):
    name = f"{uuid.uuid4().hex}.inventory-store"
    if temporary:
        name = f".{name}.tmp-test"
    path = root / name
    path.mkdir()
    if not temporary:
        (path / "manifest.json").write_text(
            json.dumps({"base_checkpoint": base.name} if base else {})
        )
    if marked:
        (path / cleanup.EXPIRY_FILE).write_text(
            json.dumps(
                {
                    "version": 1,
                    "created": 200_000 - age,
                    "last_used": 200_000 - age,
                }
            )
        )
    (path / "payload").write_bytes(b"inventory")
    return path


def sweep():
    session = cleanup.CacheSession()
    session.close()


def test_expiry_boundary_and_exclusions(cache_root, tmp_path):
    expired = checkpoint(cache_root)
    temporary = checkpoint(cache_root, temporary=True)
    young = checkpoint(cache_root, age=86_399)
    boundary = checkpoint(cache_root, age=86_400)
    legacy = checkpoint(cache_root, marked=False)
    external = checkpoint(tmp_path)
    unrelated = cache_root / "unrelated"
    unrelated.mkdir()
    sweep()
    assert not expired.exists()
    assert not temporary.exists()
    assert all(p.exists() for p in (young, boundary, legacy, external, unrelated))


def test_explicit_reuse_refreshes_expiry_without_adopting_legacy(cache_root):
    path = checkpoint(cache_root)
    legacy = checkpoint(cache_root, marked=False)
    cleanup.mark_checkpoint(path)
    cleanup.mark_checkpoint(legacy)
    sweep()
    assert path.exists()
    assert cleanup._metadata(path)["last_used"] == 200_000
    assert not (legacy / cleanup.EXPIRY_FILE).exists()


def test_keeps_transitive_bases_and_legacy_dependencies(cache_root):
    base = checkpoint(cache_root)
    middle = checkpoint(cache_root, base=base)
    retained = checkpoint(cache_root, base=middle, marked=False)
    unreferenced = checkpoint(cache_root)
    sweep()
    assert all(p.exists() for p in (base, middle, retained))
    assert not unreferenced.exists()


def test_active_instance_and_opt_out_suppress_cleanup(cache_root):
    path = checkpoint(cache_root)
    active = cleanup.CacheSession(cleanup=False)
    try:
        sweep()
        assert path.exists()
    finally:
        active.close()
        active.close()
    sweep()
    assert not path.exists()


@pytest.mark.parametrize(
    "marker", ["not-json", "{}", '{"version": 1, "created": 0, "last_used": NaN}']
)
def test_malformed_metadata_is_retained(cache_root, caplog, marker):
    path = checkpoint(cache_root)
    (path / cleanup.EXPIRY_FILE).write_text(marker)
    sweep()
    assert path.exists()
    assert "unreadable expiry metadata" in caplog.text


def test_unknown_dependencies_stop_sweep(cache_root, caplog):
    expired = checkpoint(cache_root)
    unknown = checkpoint(cache_root, marked=False)
    (unknown / "manifest.json").write_text("broken")
    sweep()
    assert expired.exists()
    assert "unreadable manifest" in caplog.text


def test_symlinks_are_not_followed(cache_root, tmp_path):
    outside = checkpoint(tmp_path)
    link = cache_root / f"{uuid.uuid4().hex}.inventory-store"
    link.symlink_to(outside, target_is_directory=True)
    expired = checkpoint(cache_root)
    (expired / "linked-payload").symlink_to(outside, target_is_directory=True)
    marker_link = checkpoint(cache_root)
    (marker_link / cleanup.EXPIRY_FILE).unlink()
    (marker_link / cleanup.EXPIRY_FILE).symlink_to(outside / cleanup.EXPIRY_FILE)
    sweep()
    assert link.is_symlink()
    assert outside.exists()
    assert marker_link.exists()
    assert not expired.exists()


def test_failed_dependent_deletion_retains_base(cache_root, monkeypatch, caplog):
    base = checkpoint(cache_root)
    dependent = checkpoint(cache_root, base=base)
    original = cleanup.shutil.rmtree

    def fail(path):
        if path == dependent:
            raise PermissionError("busy")
        original(path)

    monkeypatch.setattr(cleanup.shutil, "rmtree", fail)
    sweep()
    assert base.exists() and dependent.exists()
    assert "Cannot remove expired checkpoint" in caplog.text


def test_cleanup_reports_space_and_respects_quiet(cache_root, caplog):
    checkpoint(cache_root)
    with caplog.at_level(logging.INFO, logger=cleanup.__name__):
        sweep()
    assert "Removed 1 expired inventory cache entries" in caplog.text
    caplog.clear()
    checkpoint(cache_root)
    with caplog.at_level(logging.INFO, logger=cleanup.__name__):
        session = cleanup.CacheSession(quiet=True)
        session.close()
    assert "Removed" not in caplog.text


def test_instance_gc_and_failed_constructor_release_lock(cache_root):
    class Builder:
        @cleanup.with_cache_session
        def __init__(self, cleanup_expired_caches=True, quiet=False, fail=False):
            if fail:
                raise ValueError("failed constructor")

    expired = checkpoint(cache_root)
    builder = Builder(cleanup_expired_caches=False)
    sweep()
    assert expired.exists()
    del builder
    gc.collect()
    sweep()
    assert not expired.exists()
    with pytest.raises(ValueError, match="failed constructor"):
        Builder(fail=True)
    expired = checkpoint(cache_root)
    sweep()
    assert not expired.exists()


def test_new_database_constructor_failure_releases_lock(cache_root):
    with pytest.raises(ValueError):
        NewDatabase(scenarios=[], source_version="invalid", quiet=True)
    expired = checkpoint(cache_root)
    sweep()
    assert not expired.exists()


def _hold_session(root, ready):
    cleanup.fs.DIR_CACHED_FILES = Path(root)
    session = cleanup.CacheSession(cleanup=False)
    ready.set()
    time.sleep(30)
    session.close()


def test_process_lock_and_abrupt_exit(cache_root):
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_session, args=(str(cache_root), ready))
    process.start()
    try:
        assert ready.wait(30)
        expired = checkpoint(cache_root)
        sweep()
        assert expired.exists()
        process.terminate()
        process.join(30)
        assert not process.is_alive()
        sweep()
        assert not expired.exists()
    finally:
        if process.is_alive():
            process.terminate()
        process.join(30)


def _start_session(root, start, connection):
    cleanup.fs.DIR_CACHED_FILES = Path(root)
    start.wait(30)
    session = cleanup.CacheSession()
    connection.send("ready")
    connection.recv()
    session.close()
    connection.close()


def test_simultaneous_startups_protect_subsequent_work(cache_root):
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    processes, connections = [], []
    expired = checkpoint(cache_root)
    try:
        for _ in range(2):
            parent, child = context.Pipe()
            process = context.Process(
                target=_start_session, args=(str(cache_root), start, child)
            )
            process.start()
            child.close()
            processes.append(process)
            connections.append(parent)
        start.set()
        for connection in connections:
            assert connection.poll(30)
            assert connection.recv() == "ready"
        assert not expired.exists()
        protected = checkpoint(cache_root)
        sweep()
        assert protected.exists()
        for connection in connections:
            connection.send("stop")
        for process in processes:
            process.join(30)
            assert process.exitcode == 0
        sweep()
        assert not protected.exists()
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(30)
        for connection in connections:
            connection.close()


def test_checkpoint_write_and_reopen_metadata_do_not_change_checksums(
    cache_root, monkeypatch
):
    path = cache_root / f"{uuid.uuid4().hex}.inventory-store"
    store = CompactInventoryStore(
        [
            {
                "name": "test",
                "reference product": "test",
                "unit": "kilogram",
                "location": "GLO",
                "code": "test",
                "exchanges": [],
            }
        ]
    )
    active = cleanup.CacheSession(cleanup=False)
    try:
        store.checkpoint(path)
        checksums = (path / "checksums.json").read_bytes()
        assert cleanup.EXPIRY_FILE not in json.loads(checksums)
        assert cleanup._metadata(path)["created"] == 200_000
        monkeypatch.setattr(cleanup.time, "time", lambda: 300_000)
        sweep()
        assert path.exists()
        for _ in range(2):
            reopened = InventoryStore.open(path)
            assert len(reopened) == 1
        assert cleanup._metadata(path)["last_used"] == 300_000
        assert (path / "checksums.json").read_bytes() == checksums
    finally:
        active.close()


def test_interrupted_write_gets_expiry_marker(cache_root, monkeypatch):
    from premise import inventory_store as stores

    def interrupt(store, temporary):
        assert (temporary / cleanup.EXPIRY_FILE).is_file()
        raise KeyboardInterrupt()

    monkeypatch.setattr(stores, "_write_checkpoint_payloads", interrupt)
    path = cache_root / f"{uuid.uuid4().hex}.inventory-store"
    with pytest.raises(KeyboardInterrupt):
        CompactInventoryStore([]).checkpoint(path)
    leftovers = list(cache_root.iterdir())
    assert len(leftovers) == 1
    monkeypatch.setattr(cleanup.time, "time", lambda: 300_000)
    sweep()
    assert not leftovers[0].exists()


@pytest.fixture
def manual_cache_roots(cache_root, tmp_path, monkeypatch):
    from premise import utils

    source = tmp_path / "cache"
    source.mkdir()
    (source / "source.pickle").write_bytes(b"source")
    monkeypatch.setattr(utils, "DIR_CACHED_DB", source)
    monkeypatch.setattr(utils, "DIR_CACHED_FILES", cache_root)
    return source, cache_root


def test_clear_cache_removes_all_defaults_without_following_symlinks(
    manual_cache_roots, tmp_path, capsys
):
    from premise import clear_cache

    source, root = manual_cache_roots
    checkpoint(root, age=0)
    checkpoint(root, marked=False)
    checkpoint(root, temporary=True)
    clic = root / "clic" / "archives"
    clic.mkdir(parents=True)
    (clic / "archive.zip").write_bytes(b"archive")
    (root / "scenario.pickle").write_bytes(b"scenario")
    external = tmp_path / "custom-clic-cache"
    external.mkdir()
    (external / "keep").write_bytes(b"external")
    (root / "external-link").symlink_to(external, target_is_directory=True)

    clear_cache()

    assert not list(source.iterdir())
    assert not list(root.iterdir())
    assert (external / "keep").read_bytes() == b"external"
    assert (
        "Database, scenario and default CLIC caches cleared!" in capsys.readouterr().out
    )


def test_clear_cache_refuses_active_build_before_deleting_anything(manual_cache_roots):
    from premise import clear_cache

    source, root = manual_cache_roots
    path = checkpoint(root)
    active = cleanup.CacheSession(cleanup=False)
    try:
        with pytest.raises(RuntimeError, match="NewDatabase instance is active"):
            clear_cache()
        assert (source / "source.pickle").exists()
        assert path.exists()
    finally:
        active.close()
    clear_cache()
    assert not list(source.iterdir())
    assert not list(root.iterdir())


def test_clear_cache_refuses_build_in_another_process(manual_cache_roots):
    from premise import clear_cache

    source, root = manual_cache_roots
    path = checkpoint(root)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_session, args=(str(root), ready))
    process.start()
    try:
        assert ready.wait(30)
        with pytest.raises(RuntimeError, match="NewDatabase instance is active"):
            clear_cache()
        assert (source / "source.pickle").exists()
        assert path.exists()
    finally:
        process.terminate()
        process.join(30)


def test_manual_cleanup_holds_lock_until_deletion_finishes(manual_cache_roots):
    with cleanup.exclusive_cache_access():
        with pytest.raises(RuntimeError, match="NewDatabase instance is active"):
            with cleanup.exclusive_cache_access():
                pytest.fail("Concurrent cleanup acquired the exclusive lock")
    with cleanup.exclusive_cache_access():
        pass
