"""Compatibility imports for Brightway backend internals."""

from __future__ import annotations

try:
    from bw2data.backends import SQLiteBackend
except ImportError:
    from bw2data.backends.peewee import SQLiteBackend

try:
    from bw2data.backends import ActivityDataset, ExchangeDataset
except ImportError:
    try:
        from bw2data.backends.schema import ActivityDataset, ExchangeDataset
    except ImportError:
        from bw2data.backends.peewee.schema import ActivityDataset, ExchangeDataset

try:
    from bw2data.backends import sqlite3_lci_db
except ImportError:
    from bw2data.backends.peewee import sqlite3_lci_db
