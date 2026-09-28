"""Bounded expiry of managed scenario checkpoints, guarded by build lifetimes."""

from __future__ import annotations

import functools
import inspect
import json
import logging
import math
import os
from pathlib import Path
import re
import shutil
import time
import weakref

import portalocker

from . import filesystem_constants as fs

logger = logging.getLogger(__name__)
MAX_AGE_SECONDS = 24 * 60 * 60
EXPIRY_FILE = ".premise-expiry.json"
_CHECKPOINT = re.compile(r"[0-9a-f]{32}\.inventory-store\Z")
_TEMPORARY = re.compile(r"\.[0-9a-f]{32}\.inventory-store\.tmp-[\w-]+\Z")


def _recognized(path: Path) -> bool:
    return bool(_CHECKPOINT.fullmatch(path.name) or _TEMPORARY.fullmatch(path.name))


def _metadata(path: Path) -> dict:
    marker = path / EXPIRY_FILE
    if marker.is_symlink():
        raise ValueError("expiry metadata is a symlink")
    data = json.loads(marker.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("unknown expiry metadata")
    for key in ("created", "last_used"):
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("invalid expiry timestamp")
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid expiry timestamp")
    if data["last_used"] < data["created"]:
        raise ValueError("expiry timestamp predates creation")
    return data


def mark_checkpoint(path: Path, *, create: bool = False) -> None:
    """Mark a new managed write, or refresh explicit reuse; never adopt legacy data."""
    path = Path(path)
    if (
        not _recognized(path)
        or path.is_symlink()
        or path.parent.resolve() != fs.DIR_CACHED_FILES.resolve()
    ):
        return
    try:
        now = time.time()
        if create:
            if (path / EXPIRY_FILE).is_symlink():
                raise ValueError("expiry metadata is a symlink")
            data = {"version": 1, "created": now, "last_used": now}
        else:
            if not (path / EXPIRY_FILE).exists():
                return
            data = _metadata(path)
            data["last_used"] = max(now, data["last_used"])
        # A partial marker after interruption is conservatively retained.
        (path / EXPIRY_FILE).write_text(json.dumps(data), encoding="utf-8")
    except (OSError, ValueError) as error:
        logger.warning(
            "Cannot update checkpoint expiry metadata for %s: %s", path, error
        )


def _allocated_bytes(path: Path) -> int:
    total = 0
    for root, directories, files in os.walk(path, followlinks=False):
        directories[:] = [d for d in directories if not (Path(root) / d).is_symlink()]
        for name in files:
            stat = (Path(root) / name).lstat()
            total += getattr(stat, "st_blocks", stat.st_size / 512) * 512
    return int(total)


def _sweep(root: Path, *, quiet: bool) -> tuple[int, int]:
    """Delete expired managed entries. Caller must hold the exclusive build lock."""
    now = time.time()
    entries = {
        p.name: p
        for p in root.iterdir()
        if _recognized(p) and not p.is_symlink() and p.is_dir()
    }
    candidates = set()
    for name, path in entries.items():
        if not (path / EXPIRY_FILE).exists():
            continue
        try:
            if now - _metadata(path)["last_used"] > MAX_AGE_SECONDS:
                candidates.add(name)
        except (OSError, ValueError) as error:
            logger.warning(
                "Skipping checkpoint with unreadable expiry metadata %s: %s",
                path,
                error,
            )
    if not candidates:
        return 0, 0

    # Legacy or young checkpoints can reference an expired managed base. Inspect
    # all direct checkpoint manifests, but only when deletion is actually needed.
    dependencies = {name: set() for name in entries}
    for path in root.iterdir():
        if (
            path.is_symlink()
            or not path.is_dir()
            or not path.name.endswith(".inventory-store")
        ):
            continue
        try:
            manifest_path = path / "manifest.json"
            if manifest_path.is_symlink():
                raise ValueError("manifest is a symlink")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise ValueError("manifest must be an object")
            reference = manifest.get("base_checkpoint")
            dependencies.setdefault(path.name, set())
            if reference is not None:
                if not isinstance(reference, str) or not reference:
                    raise ValueError("invalid base checkpoint reference")
                base = (path.parent / reference).resolve()
                if base.parent == root.resolve() and base.name in entries:
                    dependencies[path.name].add(base.name)
        except (OSError, ValueError) as error:
            # Unknown dependency information must not permit deleting its base.
            logger.warning(
                "Skipping inventory cache cleanup: unreadable manifest %s: %s",
                path,
                error,
            )
            return 0, 0

    def retain_bases(names):
        pending = list(names)
        seen = set()
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            candidates.discard(name)
            pending.extend(dependencies.get(name, ()))

    retain_bases(set(dependencies) - candidates)
    count = size = 0
    while candidates:
        # Dependents go first, so a failed deletion can still protect its base.
        referenced = set().union(*(dependencies[n] for n in candidates))
        ready = candidates - referenced
        if not ready:
            logger.warning(
                "Skipping cyclic checkpoint dependencies during cache cleanup"
            )
            break
        for name in sorted(ready):
            if name not in candidates:
                continue
            path = entries[name]
            candidates.remove(name)
            try:
                if path.is_symlink():
                    raise ValueError("checkpoint became a symlink")
                allocated = _allocated_bytes(path)
                shutil.rmtree(path)
                count += 1
                size += allocated
            except (OSError, ValueError) as error:
                retain_bases(dependencies[name])
                logger.warning("Cannot remove expired checkpoint %s: %s", path, error)
    if count and not quiet:
        logger.info(
            "Removed %d expired inventory cache entries (approximately %.2f GiB)",
            count,
            size / 2**30,
        )
    return count, size


class CacheSession:
    """Hold a shared process lock until the owning NewDatabase is released."""

    def __init__(self, *, cleanup: bool = True, quiet: bool = False):
        root = fs.DIR_CACHED_FILES
        root.mkdir(parents=True, exist_ok=True)
        self._handle = (root.parent / ".premise-inventory-cache.lock").open("a+b")
        try:
            if cleanup:
                try:
                    portalocker.lock(
                        self._handle, portalocker.LOCK_EX | portalocker.LOCK_NB
                    )
                except portalocker.exceptions.LockException:
                    pass
                else:
                    try:
                        _sweep(root, quiet=quiet)
                    except OSError as error:
                        logger.warning("Cannot scan inventory cache: %s", error)
                    finally:
                        # Explicit unlock works on Windows too. No inventory has
                        # been accessed yet, so another sweep in this gap is safe.
                        portalocker.unlock(self._handle)
            portalocker.lock(self._handle, portalocker.LOCK_SH)
        except BaseException:
            self.close()
            raise

    def close(self):
        handle = self._handle
        if handle is not None:
            self._handle = None
            # Closing releases the OS lock, including after process termination.
            handle.close()


def with_cache_session(initializer):
    """Acquire before construction and release promptly even if construction fails."""
    signature = inspect.signature(initializer)

    @functools.wraps(initializer)
    def wrapped(self, *args, **kwargs):
        arguments = signature.bind(self, *args, **kwargs).arguments
        session = CacheSession(
            cleanup=arguments.get("cleanup_expired_caches", True),
            quiet=arguments.get("quiet", False),
        )
        finalizer = weakref.finalize(self, session.close)
        try:
            return initializer(self, *args, **kwargs)
        except BaseException:
            finalizer()
            raise

    return wrapped
