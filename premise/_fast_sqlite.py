"""Scoped SQLite tuning for premise's replaceable fast export databases."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)

FAST_SQLITE_CACHE_MIB = 64
FAST_SQLITE_EXCHANGE_BATCH_SIZE = 20_000
_PRAGMAS = (
    "journal_mode",
    "synchronous",
    "temp_store",
    "cache_size",
    "foreign_keys",
)


def _sql_database(database: Any) -> Any:
    return getattr(database, "db", database)


def _read_pragma(database: Any, name: str) -> Any:
    return _sql_database(database).execute_sql(f"PRAGMA {name};").fetchone()[0]


def _write_pragma(database: Any, name: str, value: Any) -> None:
    if name == "journal_mode":
        value = str(value).upper()
        if value not in {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}:
            raise ValueError(f"Unsupported SQLite journal mode: {value!r}.")
    elif not isinstance(value, int):
        value = int(value)
    _sql_database(database).execute_sql(f"PRAGMA {name} = {value};")


@contextmanager
def fast_sqlite_settings(
    database: Any,
    *,
    cache_mib: int = FAST_SQLITE_CACHE_MIB,
) -> Iterator[None]:
    """Temporarily reduce durability for one premise fast-write transaction.

    Restoration failures are reported but never replace an exception raised by
    the write itself. With no primary failure, a restoration failure remains a
    real error so callers do not unknowingly continue with altered settings.
    """

    original: dict[str, Any] = {}
    primary_error: BaseException | None = None
    try:
        for name in _PRAGMAS:
            original[name] = _read_pragma(database, name)
        _write_pragma(database, "journal_mode", "MEMORY")
        _write_pragma(database, "synchronous", 0)
        _write_pragma(database, "temp_store", 2)
        _write_pragma(database, "foreign_keys", 0)
        # Negative cache_size values are kibibytes, independent of page size.
        _write_pragma(database, "cache_size", -int(cache_mib * 1024))
        yield
    except BaseException as error:
        primary_error = error
        raise
    finally:
        restoration_error = None
        # Restore journal mode last; SQLite can reject changing it while a
        # transaction is active, whereas the remaining connection settings are
        # safe immediately after commit or rollback.
        restore_order = (
            "foreign_keys",
            "cache_size",
            "temp_store",
            "synchronous",
            "journal_mode",
        )
        for name in restore_order:
            if name not in original:
                continue
            try:
                _write_pragma(database, name, original[name])
            except Exception as error:  # pragma: no cover - backend-specific
                restoration_error = restoration_error or error
                logger.exception("Could not restore SQLite PRAGMA %s", name)
        if restoration_error is not None and primary_error is None:
            raise restoration_error


__all__ = [
    "FAST_SQLITE_CACHE_MIB",
    "FAST_SQLITE_EXCHANGE_BATCH_SIZE",
    "fast_sqlite_settings",
]
