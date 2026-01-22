"""
Class to write a Brightway2 database from a Wurst database.
"""

from contextlib import contextmanager

from bw2data import databases
from bw2io.importers.base_lci import LCIImporter
from wurst.linking import change_db_name, check_internal_linking, link_internal


class BW2Importer(LCIImporter):
    """
    Class to write a Brightway2 database from a Wurst database.
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

    # we override `write_database`
    # to allow existing databases
    # to be overwritten
    def write_database(self):
        if self.db_name in databases:
            print(f"Database {self.db_name} already exists: it will be overwritten.")
            del databases[self.db_name]
        super().write_database()


@contextmanager
def _fast_sqlite_writes(enabled: bool):
    if not enabled:
        yield
        return

    original_settings = {}
    original_vacuum = None

    try:
        from bw2data.configuration import config
    except Exception:
        yield
        return

    sqlite3_lci_db = None
    for relative_path, substitutable_db in config.sqlite3_databases:
        if relative_path.replace("\\", "/").endswith("lci/databases.db"):
            sqlite3_lci_db = substitutable_db
            break

    if sqlite3_lci_db is None:
        yield
        return

    db = sqlite3_lci_db.db
    original_vacuum = sqlite3_lci_db.vacuum

    try:
        original_settings["synchronous"] = (
            db.execute_sql("PRAGMA synchronous;").fetchone()[0]
        )
        original_settings["journal_mode"] = (
            db.execute_sql("PRAGMA journal_mode;").fetchone()[0]
        )
        original_settings["temp_store"] = (
            db.execute_sql("PRAGMA temp_store;").fetchone()[0]
        )
    except Exception:
        original_settings = {}

    try:
        db.execute_sql("PRAGMA synchronous = OFF;")
        db.execute_sql("PRAGMA journal_mode = MEMORY;")
        db.execute_sql("PRAGMA temp_store = MEMORY;")
    except Exception:
        pass

    def _noop_vacuum():
        return None

    sqlite3_lci_db.vacuum = _noop_vacuum

    try:
        yield
    finally:
        try:
            if original_vacuum is not None:
                sqlite3_lci_db.vacuum = original_vacuum

            if original_settings:
                db = sqlite3_lci_db.db
                db.execute_sql(
                    f"PRAGMA synchronous = {original_settings['synchronous']};"
                )
                db.execute_sql(
                    f"PRAGMA journal_mode = {original_settings['journal_mode']};"
                )
                db.execute_sql(
                    f"PRAGMA temp_store = {original_settings['temp_store']};"
                )
        except Exception:
            pass


def write_brightway_database(data: list, name: str, fast: bool = False) -> None:
    """
    Write a Brightway2 database from a Wurst database.
    """
    # Restore parameters to Brightway2 format
    # which allows for uncertainty and comments
    change_db_name(data, name)
    link_internal(data)
    check_internal_linking(data)
    with _fast_sqlite_writes(fast):
        BW2Importer(name, data).write_database()
