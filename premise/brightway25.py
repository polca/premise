"""
This module contains functions to write a Brightway 2.5 database.
"""

from contextlib import contextmanager
import datetime
import pickle
import shutil
import warnings

from bw2data import Database, databases
from bw2io.importers.base_lci import LCIImporter
from tqdm import tqdm
from wurst.linking import change_db_name, check_internal_linking, link_internal

FAST_EXCHANGE_REQUIRED_FIELDS = {
    "input",
    "amount",
    "type",
}

FAST_EXCHANGE_OPTIONAL_FIELDS = {
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "production volume",
}

FAST_EXCHANGE_STORED_FIELDS = (
    FAST_EXCHANGE_REQUIRED_FIELDS | FAST_EXCHANGE_OPTIONAL_FIELDS
)


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


def _progress(iterable=None, *, total=None, desc=None, unit="dataset", leave=False):
    try:
        from bw2data.configuration import config

        disable = getattr(config, "is_test", False)
    except Exception:
        disable = False

    return tqdm(
        iterable,
        total=total,
        desc=desc,
        unit=unit,
        leave=leave,
        dynamic_ncols=True,
        disable=disable,
    )


def _cleanup_legacy_fast_export_sidecars(name: str) -> None:
    """Remove metadata and files left by older experimental sidecar exports."""

    sidecar_dir = databases[name].get("premise_fast_exchange_sidecar")
    reverse_sidecar_dir = databases[name].get("premise_fast_reverse_exchange_sidecar")
    for filepath in (sidecar_dir, reverse_sidecar_dir):
        if filepath:
            shutil.rmtree(filepath, ignore_errors=True)

    databases[name].pop("premise_fast_exchange_sidecar", None)
    databases[name].pop("premise_fast_reverse_exchange_sidecar", None)


@contextmanager
def _fast_sqlite_writes(enabled: bool):
    if not enabled:
        yield
        return

    original_vacuum = {}
    original_make_searchable = {}
    original_base_checks = None
    original_substitutable_vacuum = None
    original_efficient_write_many_data = None
    db_settings = {}

    try:
        from bw2data.backends import base as bw_base
        from bw2data.backends.schema import ActivityDataset, ExchangeDataset
        from bw2data.backends import sqlite3_lci_db as bw_sqlite3_lci_db
        from bw2data import sqlite as bw_sqlite
        from bw2data.configuration import config
        from bw2data.snowflake_ids import snowflake_id_generator
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


def _compact_payload_for_fast_write(data: list, name: str) -> list:
    from bw2data.utils import set_correct_process_type

    def keep_value(value):
        if value is None:
            return False
        if isinstance(value, str) and value in {"None", "nan"}:
            return False
        return True

    progress = _progress(
        total=len(data),
        desc=f"Compacting export payload [{name}]",
        unit="dataset",
        leave=False,
    )
    try:
        for dataset in data:
            set_correct_process_type(dataset)
            exchanges = dataset.get("exchanges", [])
            compact_dataset = {
                field: value
                for field, value in dataset.items()
                if field != "exchanges" and keep_value(value)
            }

            compact_exchanges = []
            for exchange in exchanges:
                compact_exchange = {
                    field: value
                    for field, value in exchange.items()
                    if field in FAST_EXCHANGE_STORED_FIELDS and keep_value(value)
                }
                for field in FAST_EXCHANGE_REQUIRED_FIELDS:
                    if field not in compact_exchange and field in exchange:
                        compact_exchange[field] = exchange[field]

                compact_exchanges.append(compact_exchange)

            compact_dataset["exchanges"] = compact_exchanges
            dataset.clear()
            dataset.update(compact_dataset)
            progress.update(1)
    finally:
        progress.close()

    return data


def _write_search_index_fast(database_filename: str, data: list, name: str) -> None:
    from bw2data.search.indices import IndexManager
    from bw2data.search.schema import BW2Schema

    def format_dataset(ds):
        location = ds.get("location") or ""
        if isinstance(location, tuple):
            location = location[1]
        if isinstance(location, str):
            if location.lower() == "none":
                location = ""
            else:
                location = location.lower().strip()
        else:
            location = ""

        return {
            "name": (ds.get("name") or "").lower(),
            "comment": (ds.get("comment") or "").lower(),
            "product": (ds.get("reference product") or "").lower(),
            "categories": ", ".join(ds.get("categories") or []).lower(),
            "synonyms": ", ".join(ds.get("synonyms") or []).lower(),
            "location": location,
            "database": ds["database"],
            "code": ds["code"],
        }

    index = IndexManager(database_filename)
    index.create()
    batch = []
    batch_size = 2_000
    with index.db.bind_ctx((BW2Schema,)):
        progress = _progress(
            total=len(data),
            desc=f"Building search index [{name}]",
            unit="dataset",
            leave=False,
        )
        try:
            for dataset in data:
                batch.append(format_dataset(dataset))
                if len(batch) >= batch_size:
                    BW2Schema.insert_many(batch).execute()
                    batch = []
                progress.update(1)
            if batch:
                BW2Schema.insert_many(batch).execute()
        finally:
            progress.close()
    index.close()


def _write_processed_database_fast(data: list, name: str) -> None:
    from bw_processing import clean_datapackage_name, create_datapackage
    from fsspec.implementations.zip import ZipFileSystem

    from bw2data import geomapping
    from bw2data.backends import sqlite3_lci_db
    from bw2data.backends.schema import ActivityDataset, ExchangeDataset, get_id
    from bw2data.configuration import config, labels
    from bw2data.utils import as_uncertainty_dict, get_geocollection

    db = Database(name)
    if name in databases:
        _cleanup_legacy_fast_export_sidecars(name)
        db.delete(warn=False, vacuum=False)
        del databases[name]
        db = Database(name)

    if name not in databases:
        db.register(write_empty=False)

    databases[name]["number"] = len(data)
    databases.set_modified(name)

    geocollections = {
        get_geocollection(dataset.get("location"))
        for dataset in data
        if dataset.get("type") in labels.process_node_types
    }
    if None in geocollections:
        warnings.warn(
            "Not able to determine geocollections for all datasets. "
            "This database is not ready for regionalization."
        )
        geocollections.discard(None)
    databases[name]["geocollections"] = sorted(geocollections)
    geomapping.add({dataset["location"] for dataset in data if dataset.get("location")})
    _cleanup_legacy_fast_export_sidecars(name)

    activity_sql = (
        f'INSERT INTO "{ActivityDataset._meta.table_name}" '
        "(data, code, database, location, name, product, type) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    exchange_sql = (
        f'INSERT INTO "{ExchangeDataset._meta.table_name}" '
        "(data, input_code, input_database, output_code, output_database, type) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    activity_rows = []
    exchange_rows = []
    activity_row_batch_size = 1_000
    exchange_row_batch_size = 5_000
    activity_ids = {}
    connection = sqlite3_lci_db.db.connection()
    total_datasets = len(data)

    sqlite3_lci_db.db.autocommit = False
    row_progress = _progress(
        total=total_datasets,
        desc=f"Writing Brightway rows [{name}]",
        unit="dataset",
        leave=False,
    )
    try:
        sqlite3_lci_db.db.begin()
        for dataset in data:
            activity_rows.append(
                (
                    pickle.dumps(
                        {
                            key: value
                            for key, value in dataset.items()
                            if key != "exchanges"
                        },
                        protocol=4,
                    ),
                    dataset["code"],
                    name,
                    dataset.get("location"),
                    dataset.get("name"),
                    dataset.get("reference product"),
                    dataset.get("type"),
                )
            )

            for exchange in dataset.get("exchanges", []):
                input_key = exchange.get("input")
                if input_key is None:
                    raise KeyError(
                        f"Missing input for exchange in dataset {dataset['name']!r}."
                    )

                exchange_rows.append(
                    (
                        pickle.dumps(
                            {
                                key: value
                                for key, value in exchange.items()
                                if key in FAST_EXCHANGE_STORED_FIELDS
                            },
                            protocol=4,
                        ),
                        input_key[1],
                        input_key[0],
                        dataset["code"],
                        name,
                        exchange["type"],
                    )
                )
                if len(exchange_rows) >= exchange_row_batch_size:
                    connection.executemany(exchange_sql, exchange_rows)
                    exchange_rows = []

            if len(activity_rows) >= activity_row_batch_size:
                connection.executemany(activity_sql, activity_rows)
                activity_rows = []
            row_progress.update(1)

        if activity_rows:
            connection.executemany(activity_sql, activity_rows)
        if exchange_rows:
            connection.executemany(exchange_sql, exchange_rows)

        sqlite3_lci_db.db.commit()
    except Exception:
        sqlite3_lci_db.db.rollback()
        raise
    finally:
        sqlite3_lci_db.db.autocommit = True
        row_progress.close()

    activity_ids = {
        (name, code): activity_id
        for activity_id, code in ActivityDataset.select(
            ActivityDataset.id, ActivityDataset.code
        )
        .where(ActivityDataset.database == name)
        .tuples()
    }

    db.metadata["processed"] = datetime.datetime.now().isoformat()
    datapackage_path = str(db.dirpath_processed() / db.filename_processed())
    datapackage = create_datapackage(
        fs=ZipFileSystem(datapackage_path, mode="w"),
        name=clean_datapackage_name(name),
        sum_intra_duplicates=True,
        sum_inter_duplicates=False,
    )

    dependents = set()
    input_id_cache = {}

    def resolve_input_id(input_key):
        if input_key[0] == name:
            return activity_ids[input_key]
        if input_key not in input_id_cache:
            input_id_cache[input_key] = get_id(input_key)
        return input_id_cache[input_key]

    process_dataset_total = sum(
        1 for dataset in data if dataset.get("type") in labels.process_node_types
    )

    def iter_geomapping():
        progress = _progress(
            total=process_dataset_total,
            desc=f"Serializing geomapping [{name}]",
            unit="dataset",
            leave=False,
        )
        try:
            for dataset in data:
                if dataset.get("type") not in labels.process_node_types:
                    continue
                progress.update(1)
                yield {
                    "row": activity_ids[(name, dataset["code"])],
                    "col": geomapping[
                        dataset.get("location") or config.global_location
                    ],
                    "amount": 1,
                }
        finally:
            progress.close()

    datapackage.add_persistent_vector_from_iterator(
        matrix="inv_geomapping_matrix",
        name=clean_datapackage_name(name + " inventory geomapping matrix"),
        dict_iterator=iter_geomapping(),
    )

    def iter_biosphere():
        progress = _progress(
            total=total_datasets,
            desc=f"Serializing biosphere matrix [{name}]",
            unit="dataset",
            leave=False,
        )
        try:
            for dataset in data:
                col = activity_ids[(name, dataset["code"])]
                for exchange in dataset.get("exchanges", []):
                    if exchange["type"] not in labels.biosphere_edge_types:
                        continue
                    input_key = exchange.get("input")
                    if input_key is None:
                        raise KeyError(
                            f"Missing biosphere input for exchange in dataset {dataset['name']!r}."
                        )
                    if input_key[0] != name:
                        dependents.add(input_key[0])
                    yield {
                        **as_uncertainty_dict(exchange),
                        "row": resolve_input_id(input_key),
                        "col": col,
                    }
                progress.update(1)
        finally:
            progress.close()

    negative_edge_types = set(labels.technosphere_negative_edge_types)
    positive_edge_types = set(labels.technosphere_positive_edge_types)

    def iter_technosphere():
        progress = _progress(
            total=total_datasets,
            desc=f"Serializing technosphere matrix [{name}]",
            unit="dataset",
            leave=False,
        )
        try:
            for dataset in data:
                col = activity_ids[(name, dataset["code"])]
                has_positive_production = False
                for exchange in dataset.get("exchanges", []):
                    edge_type = exchange["type"]
                    if (
                        edge_type not in negative_edge_types
                        and edge_type not in positive_edge_types
                    ):
                        continue
                    input_key = exchange.get("input")
                    if input_key is None:
                        raise KeyError(
                            f"Missing technosphere input for exchange in dataset {dataset['name']!r}."
                        )
                    if input_key[0] != name:
                        dependents.add(input_key[0])
                    payload = {
                        **as_uncertainty_dict(exchange),
                        "row": resolve_input_id(input_key),
                        "col": col,
                    }
                    if edge_type in negative_edge_types:
                        payload["flip"] = True
                    else:
                        has_positive_production = True
                    yield payload

                if (
                    dataset.get("type") in labels.implicit_production_allowed_node_types
                    and not has_positive_production
                ):
                    yield {
                        "row": activity_ids[(name, dataset["code"])],
                        "col": activity_ids[(name, dataset["code"])],
                        "amount": 1,
                    }

                progress.update(1)
        finally:
            progress.close()

    datapackage.add_persistent_vector_from_iterator(
        matrix="biosphere_matrix",
        name=clean_datapackage_name(name + " biosphere matrix"),
        dict_iterator=iter_biosphere(),
    )

    datapackage.add_persistent_vector_from_iterator(
        matrix="technosphere_matrix",
        name=clean_datapackage_name(name + " technosphere matrix"),
        dict_iterator=iter_technosphere(),
    )

    datapackage.finalize_serialization()
    db.metadata["depends"] = sorted(dependents)
    db.metadata["dirty"] = False
    db._metadata.flush()
    databases[name]["searchable"] = True
    databases.flush(signal=False)
    _write_search_index_fast(db.filename, data, name)


def write_brightway_database(
    data: list,
    name: str,
    fast: bool = False,
    check_internal: bool = True,
) -> None:
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
        _compact_payload_for_fast_write(data, name)
        _write_processed_database_fast(data, name)
        return

    if name in databases:
        print(f"Database {name} already exists: it will be overwritten.")
        del databases[name]

    with _fast_sqlite_writes(fast):
        BW25Importer(name, data).write_database()
