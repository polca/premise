"""
Class to write a Brightway2 database from a Wurst database.
"""

from contextlib import contextmanager
import math
import pickle

from bw2data import databases
from bw2io.importers.base_lci import LCIImporter
from wurst.linking import change_db_name, check_internal_linking, link_internal

FAST_EXCHANGE_REQUIRED_FIELDS = {
    "input",
    "amount",
    "type",
    "name",
    "product",
    "unit",
    "location",
    "output",
}

FAST_DATASET_REQUIRED_FIELDS = {
    "database",
    "code",
    "name",
    "reference product",
    "unit",
    "location",
    "type",
}

FAST_STRING_FIELDS = {
    "name",
    "reference product",
    "product",
    "unit",
    "location",
}

PROCESS_NODE_DEFAULT = "process"
CHIMAERA_NODE_DEFAULT = "processwithreferenceproduct"
PROCESS_LIKE_NODE_TYPES = {
    PROCESS_NODE_DEFAULT,
    CHIMAERA_NODE_DEFAULT,
    None,
}


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


def _print_database_written(name: str) -> None:
    print(f"Brightway database written: {name}")


def _get_geocollection_compat(location):
    try:
        from bw2data.utils import get_geocollection
    except ImportError:
        if not location:
            return None
        if isinstance(location, tuple):
            return location[0]
        if isinstance(location, str) and (
            len(location) == 2 or location.lower() == "glo"
        ):
            return "world"
        return None

    return get_geocollection(location)


def _collect_fast_export_geography(data: list) -> tuple[list, set]:
    geocollections = sorted(
        {
            geocollection
            for geocollection in (
                _get_geocollection_compat(dataset.get("location")) for dataset in data
            )
            if geocollection is not None
        }
    )
    locations = {dataset["location"] for dataset in data if dataset.get("location")}
    return geocollections, locations


def _set_correct_process_type_compat(dataset: dict) -> dict:
    try:
        from bw2data.utils import set_correct_process_type
    except ImportError:
        # bw2data<4 only supports plain ``process`` datasets. Collapse newer
        # process-like node types to the legacy Brightway2 representation.
        if dataset.get("type") not in PROCESS_LIKE_NODE_TYPES:
            return dataset
        dataset["type"] = PROCESS_NODE_DEFAULT
        return dataset

    return set_correct_process_type(dataset)


@contextmanager
def _fast_sqlite_writes(enabled: bool):
    if not enabled:
        yield
        return

    original_settings = {}
    original_vacuum = {}
    original_make_searchable = {}
    original_base_checks = None
    original_substitutable_vacuum = None
    original_efficient_write_many_data = None
    db_settings = {}

    try:
        from bw2data.backends import base as bw_base
        from bw2data import sqlite as bw_sqlite
        from bw2data.configuration import config
        from bw2data.snowflake_ids import snowflake_id_generator
        from ._bw2_backend_compat import (
            ActivityDataset,
            ExchangeDataset,
            sqlite3_lci_db as bw_sqlite3_lci_db,
        )
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
        original_settings["synchronous"] = primary_db.execute_sql(
            "PRAGMA synchronous;"
        ).fetchone()[0]
        original_settings["journal_mode"] = primary_db.execute_sql(
            "PRAGMA journal_mode;"
        ).fetchone()[0]
        original_settings["temp_store"] = primary_db.execute_sql(
            "PRAGMA temp_store;"
        ).fetchone()[0]
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

    def _noop_make_searchable(*_args, **_kwargs):
        return None

    for db in unique_dbs:
        original_vacuum[db] = db.vacuum
        db.vacuum = _noop_vacuum
        original_make_searchable[db] = getattr(db, "make_searchable", None)
        if hasattr(db, "make_searchable"):
            db.make_searchable = _noop_make_searchable

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
    original_efficient_write_many_data = (
        bw_base.SQLiteBackend._efficient_write_many_data
    )

    def _raw_fast_write_many_data(
        self, data, indices: bool = True, check_typos: bool = True
    ):
        be_complicated = len(data) >= 100 and indices
        if be_complicated:
            self._drop_indices()

        activity_sql = (
            f'INSERT INTO "{ActivityDataset._meta.table_name}" '
            "(id, data, code, database, location, name, product, type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        exchange_sql = (
            f'INSERT INTO "{ExchangeDataset._meta.table_name}" '
            "(data, input_code, input_database, output_code, output_database, type) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        activity_batch = []
        exchange_batch = []
        activity_batch_size = 250
        exchange_batch_size = 2_000
        connection = sqlite3_lci_db.db.connection()

        sqlite3_lci_db.db.autocommit = False
        try:
            sqlite3_lci_db.db.begin()
            self.delete(keep_params=True, warn=False, vacuum=False)

            for ds in bw_base.tqdm_wrapper(data, getattr(config, "is_test", False)):
                database = ds["database"]
                code = ds["code"]

                for exchange in ds.get("exchanges", []):
                    exchange_payload = exchange
                    if "output" not in exchange_payload:
                        exchange_payload = {
                            **exchange,
                            "output": (database, code),
                        }

                    exchange_batch.append(
                        (
                            pickle.dumps(exchange_payload, protocol=4),
                            exchange_payload["input"][1],
                            exchange_payload["input"][0],
                            exchange_payload["output"][1],
                            exchange_payload["output"][0],
                            exchange_payload["type"],
                        )
                    )

                    if len(exchange_batch) >= exchange_batch_size:
                        connection.executemany(exchange_sql, exchange_batch)
                        exchange_batch = []

                activity_data = {k: v for k, v in ds.items() if k != "exchanges"}
                activity_batch.append(
                    (
                        next(snowflake_id_generator),
                        pickle.dumps(activity_data, protocol=4),
                        code,
                        database,
                        activity_data.get("location"),
                        activity_data.get("name"),
                        activity_data.get("reference product"),
                        activity_data.get("type"),
                    )
                )

                if len(activity_batch) >= activity_batch_size:
                    connection.executemany(activity_sql, activity_batch)
                    activity_batch = []

            if activity_batch:
                connection.executemany(activity_sql, activity_batch)
            if exchange_batch:
                connection.executemany(exchange_sql, exchange_batch)

            sqlite3_lci_db.db.commit()
            sqlite3_lci_db.vacuum()
        except Exception:
            sqlite3_lci_db.db.rollback()
            raise
        finally:
            sqlite3_lci_db.db.autocommit = True
            if be_complicated:
                self._add_indices()

    bw_base.SQLiteBackend._efficient_write_many_data = _raw_fast_write_many_data

    try:
        yield
    finally:
        try:
            for db, vacuum_func in original_vacuum.items():
                db.vacuum = vacuum_func
            for db, make_searchable_func in original_make_searchable.items():
                if make_searchable_func is not None:
                    db.make_searchable = make_searchable_func
            if original_substitutable_vacuum is not None:
                bw_sqlite.SubstitutableDatabase.vacuum = original_substitutable_vacuum

            for db, settings in db_settings.items():
                db.db.execute_sql(f"PRAGMA synchronous = {settings['synchronous']};")
                db.db.execute_sql(f"PRAGMA journal_mode = {settings['journal_mode']};")
                db.db.execute_sql(f"PRAGMA temp_store = {settings['temp_store']};")
            if original_efficient_write_many_data is not None:
                bw_base.SQLiteBackend._efficient_write_many_data = (
                    original_efficient_write_many_data
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


def _keep_fast_export_value(value) -> bool:
    if value is None:
        return False

    if isinstance(value, str) and value in {"", "None", "nan"}:
        return False

    if isinstance(value, (list, tuple, dict, set)):
        return True

    try:
        return not math.isnan(value)
    except (TypeError, ValueError):
        return True


def _prepare_fast_exchange_payload(exchange: dict) -> dict:
    compact_exchange = {
        field: value
        for field, value in exchange.items()
        if _keep_fast_export_value(value)
    }

    for field in FAST_EXCHANGE_REQUIRED_FIELDS:
        if field not in compact_exchange and field in exchange:
            if field in FAST_STRING_FIELDS and exchange[field] is None:
                compact_exchange[field] = ""
            else:
                compact_exchange[field] = exchange[field]

    return compact_exchange


def _compact_payload_for_fast_write(data: list) -> list:
    for dataset in data:
        _set_correct_process_type_compat(dataset)
        exchanges = dataset.get("exchanges", [])
        compact_dataset = {
            field: value
            for field, value in dataset.items()
            if field != "exchanges" and _keep_fast_export_value(value)
        }

        for field in FAST_DATASET_REQUIRED_FIELDS:
            if field not in compact_dataset and field in dataset:
                if field in FAST_STRING_FIELDS and dataset[field] is None:
                    compact_dataset[field] = ""
                else:
                    compact_dataset[field] = dataset[field]

        compact_dataset["exchanges"] = [
            _prepare_fast_exchange_payload(exchange) for exchange in exchanges
        ]
        dataset.clear()
        dataset.update(compact_dataset)

    return data


def write_brightway_database(
    data: list,
    name: str,
    fast: bool = False,
    check_internal: bool = True,
) -> None:
    """
    Write a Brightway2 database from a Wurst database.
    """
    for act in data:
        act.setdefault("database", name)

    needs_relink = any(
        "input" not in exchange
        for dataset in data
        for exchange in dataset.get("exchanges", [])
    )

    # Restore parameters to Brightway2 format
    # which allows for uncertainty and comments
    change_db_name(data, name)
    if needs_relink:
        link_internal(data)
    if check_internal:
        check_internal_linking(data)
    if fast:
        _compact_payload_for_fast_write(data)
    else:
        for dataset in data:
            _set_correct_process_type_compat(dataset)
    with _fast_sqlite_writes(fast):
        BW2Importer(name, data).write_database()
    if name in databases:
        geocollections, _ = _collect_fast_export_geography(data)
        if geocollections:
            databases[name]["geocollections"] = geocollections
        else:
            databases[name].pop("geocollections", None)
        databases.flush()
    _print_database_written(name)
