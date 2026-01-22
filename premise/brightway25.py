"""
This module contains functions to write a Brightway 2.5 database.
"""

from contextlib import contextmanager

from bw2data import Database, databases
from bw2io.importers.base_lci import LCIImporter
from wurst.linking import change_db_name, check_internal_linking, link_internal


class BW25Importer(LCIImporter):
    """
    Class to write a Brightway 2.5 database from a Wurst database.
    """

    def __init__(self, db_name: str, data: list) -> None:
        """
        :param db_name: Name of the database to write
        :type db_name: str
        :param data: Wurst database
        :type data: list
        """
        super().__init__(db_name)
        self.db_name = db_name
        self.data = data
        for act in self.data:
            act["database"] = self.db_name


@contextmanager
def _fast_sqlite_writes(enabled: bool):
    if not enabled:
        yield
        return

    original_settings = {}
    original_vacuum = {}
    original_base_checks = None
    original_substitutable_vacuum = None
    db_settings = {}

    try:
        from bw2data.backends import base as bw_base
        from bw2data.backends import sqlite3_lci_db as bw_sqlite3_lci_db
        from bw2data import sqlite as bw_sqlite
        from bw2data.configuration import config
    except Exception:
        yield
        return

    sqlite3_lci_db = bw_sqlite3_lci_db
    candidates = [bw_sqlite3_lci_db, getattr(bw_base, "sqlite3_lci_db", None)]
    try:
        for relative_path, substitutable_db in config.sqlite3_databases:
            if relative_path.replace("\\", "/").endswith("lci/databases.db"):
                candidates.append(substitutable_db)
    except Exception:
        pass
    unique_dbs = []
    seen_ids = set()
    for db in candidates:
        if db is None:
            continue
        db_id = id(db)
        if db_id not in seen_ids:
            unique_dbs.append(db)
            seen_ids.add(db_id)

    if not unique_dbs:
        yield
        return

    try:
        primary_db = unique_dbs[0].db
        original_settings["synchronous"] = (
            primary_db.execute_sql("PRAGMA synchronous;").fetchone()[0]
        )
        original_settings["journal_mode"] = (
            primary_db.execute_sql("PRAGMA journal_mode;").fetchone()[0]
        )
        original_settings["temp_store"] = (
            primary_db.execute_sql("PRAGMA temp_store;").fetchone()[0]
        )
    except Exception:
        original_settings = {}

    try:
        for db in unique_dbs:
            db_settings[db] = {
                "synchronous": db.db.execute_sql("PRAGMA synchronous;").fetchone()[0],
                "journal_mode": db.db.execute_sql("PRAGMA journal_mode;").fetchone()[0],
                "temp_store": db.db.execute_sql("PRAGMA temp_store;").fetchone()[0],
            }
            db.db.execute_sql("PRAGMA synchronous = OFF;")
            db.db.execute_sql("PRAGMA journal_mode = MEMORY;")
            db.db.execute_sql("PRAGMA temp_store = MEMORY;")
    except Exception:
        pass

    def _noop_vacuum(*_args, **_kwargs):
        return None

    for db in unique_dbs:
        original_vacuum[db] = db.vacuum
        db.vacuum = _noop_vacuum

    if bw_sqlite is not None and hasattr(bw_sqlite, "SubstitutableDatabase"):
        original_substitutable_vacuum = bw_sqlite.SubstitutableDatabase.vacuum
        bw_sqlite.SubstitutableDatabase.vacuum = _noop_vacuum

    original_base_checks = {
        "check_exchange_type": bw_base.check_exchange_type,
        "check_exchange_keys": bw_base.check_exchange_keys,
        "check_activity_type": bw_base.check_activity_type,
        "check_activity_keys": bw_base.check_activity_keys,
    }

    def _noop_check(*_args, **_kwargs):
        return None

    bw_base.check_exchange_type = _noop_check
    bw_base.check_exchange_keys = _noop_check
    bw_base.check_activity_type = _noop_check
    bw_base.check_activity_keys = _noop_check

    try:
        yield
    finally:
        try:
            for db, vacuum_func in original_vacuum.items():
                db.vacuum = vacuum_func
            if original_substitutable_vacuum is not None:
                bw_sqlite.SubstitutableDatabase.vacuum = original_substitutable_vacuum

            for db, settings in db_settings.items():
                db.db.execute_sql(
                    f"PRAGMA synchronous = {settings['synchronous']};"
                )
                db.db.execute_sql(
                    f"PRAGMA journal_mode = {settings['journal_mode']};"
                )
                db.db.execute_sql(
                    f"PRAGMA temp_store = {settings['temp_store']};"
                )
            if original_base_checks is not None:
                bw_base.check_exchange_type = original_base_checks[
                    "check_exchange_type"
                ]
                bw_base.check_exchange_keys = original_base_checks[
                    "check_exchange_keys"
                ]
                bw_base.check_activity_type = original_base_checks[
                    "check_activity_type"
                ]
                bw_base.check_activity_keys = original_base_checks[
                    "check_activity_keys"
                ]
        except Exception:
            pass


def write_brightway_database(data: list, name: str, fast: bool = False) -> None:
    # Restore parameters to Brightway2 format
    # which allows for uncertainty and comments
    change_db_name(data, name)
    link_internal(data)
    check_internal_linking(data)

    if name in databases:
        print(f"Database {name} already exists: it will be overwritten.")
        del databases[name]

    with _fast_sqlite_writes(fast):
        BW25Importer(name, data).write_database()
