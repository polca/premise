"""Bounded, process-local caches for immutable constructor resources."""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

_YAML_CACHE_MAXSIZE = 128
_IAM_CACHE_MAXSIZE = 8
_YAML_CACHE: OrderedDict[tuple[str, int, int], Any] = OrderedDict()
_IAM_CACHE: OrderedDict[tuple[Any, ...], Any] = OrderedDict()
_CACHE_LOCK = threading.RLock()


def file_signature(path: str | Path) -> tuple[str, int, int]:
    """Return the resolved path, size, and nanosecond modification time."""

    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    return str(resolved), int(stat.st_size), int(stat.st_mtime_ns)


def _bounded_put(cache: OrderedDict, key: Any, value: Any, maxsize: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > maxsize:
        cache.popitem(last=False)


def load_yaml_cached(path: str | Path) -> Any:
    """Parse a local YAML file once per stable file signature."""

    signature = file_signature(path)
    with _CACHE_LOCK:
        cached = _YAML_CACHE.get(signature)
        if cached is not None:
            _YAML_CACHE.move_to_end(signature)
            return copy.deepcopy(cached)
    with Path(signature[0]).open("r", encoding="utf-8") as stream:
        parsed = yaml.safe_load(stream)
    with _CACHE_LOCK:
        existing = _YAML_CACHE.get(signature)
        if existing is not None:
            _YAML_CACHE.move_to_end(signature)
            return copy.deepcopy(existing)
        _bounded_put(_YAML_CACHE, signature, parsed, _YAML_CACHE_MAXSIZE)
    return copy.deepcopy(parsed)


def _stable_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return {"file": file_signature(value)}
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_stable_value(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))

    descriptor = getattr(value, "descriptor", None)
    if isinstance(descriptor, Mapping):
        payload = {"descriptor": _stable_value(descriptor)}
        base_path = getattr(value, "base_path", None)
        if base_path is not None:
            payload["base_path"] = str(Path(base_path).expanduser().resolve())
        return payload
    raise TypeError(f"No stable resource fingerprint for {type(value).__name__}.")


def stable_fingerprint(value: Any) -> str | None:
    """Return a deterministic fingerprint, or ``None`` for unstable resources."""

    try:
        normalized = _stable_value(value)
        encoded = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OSError):
        return None
    return hashlib.sha256(encoded).hexdigest()


def secret_fingerprint(secret: bytes | str | None) -> str | None:
    """Hash secret cache identity without retaining or exposing the secret."""

    if secret is None:
        return None
    encoded = secret.encode("utf-8") if isinstance(secret, str) else secret
    return hashlib.sha256(encoded).hexdigest()


def get_cached_iam_resource(key: tuple[Any, ...]) -> Any | None:
    """Return an isolated deep xarray copy of one cached raw IAM resource."""

    with _CACHE_LOCK:
        cached = _IAM_CACHE.get(key)
        if cached is None:
            return None
        _IAM_CACHE.move_to_end(key)
        return cached.copy(deep=True)


def cache_iam_resource(key: tuple[Any, ...], resource: Any) -> None:
    """Retain an immutable deep copy without aliasing scenario-year state."""

    with _CACHE_LOCK:
        _bounded_put(_IAM_CACHE, key, resource.copy(deep=True), _IAM_CACHE_MAXSIZE)


def clear_constructor_caches() -> None:
    """Clear all constructor resource caches."""

    with _CACHE_LOCK:
        _YAML_CACHE.clear()
        _IAM_CACHE.clear()


def runtime_cache_sizes() -> tuple[int, int]:
    """Return YAML and IAM entry counts for diagnostics and tests."""

    with _CACHE_LOCK:
        return len(_YAML_CACHE), len(_IAM_CACHE)


__all__ = [
    "cache_iam_resource",
    "clear_constructor_caches",
    "file_signature",
    "get_cached_iam_resource",
    "load_yaml_cached",
    "runtime_cache_sizes",
    "secret_fingerprint",
    "stable_fingerprint",
]
