"""Build sequential Brightway scenario-array datapackages."""

import os
import tempfile
import zipfile
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd


def _load_scenario_array_dependencies():
    """Import modern-Brightway-only dependencies lazily."""

    try:
        import bw_processing
        from bw2data import get_id
        from bw2data.configuration import labels
        from fsspec.implementations.zip import ZipFileSystem
    except ImportError as error:
        raise ImportError(
            "Scenario-array export requires the modern Brightway dependencies. "
            "Install premise with the 'bw25' optional dependencies."
        ) from error

    return bw_processing, get_id, labels, ZipFileSystem


def _missing_key(key) -> bool:
    return key is None or (isinstance(key, float) and np.isnan(key))


def _exchange_identity(row: pd.Series) -> str:
    return (
        f"{row.get('from activity name')!r} ({row.get('from key')!r}) -> "
        f"{row.get('to activity name')!r} ({row.get('to key')!r}), "
        f"flow type {row.get('flow type')!r}"
    )


def _resolve_key(
    *,
    key,
    role: str,
    row: pd.Series,
    get_id: Callable,
    database_name: str,
    project_name: str,
) -> int:
    """Resolve one Brightway key and add export context to lookup failures."""

    try:
        if _missing_key(key):
            raise KeyError(f"Missing {role} key")
        return get_id(key)
    except Exception as error:
        raise KeyError(
            f"Could not resolve the {role} key {key!r} for exchange "
            f"{_exchange_identity(row)} after writing database {database_name!r} "
            f"in Brightway project {project_name!r}."
        ) from error


def _scenario_dataframe_to_arrays(
    *,
    dataframe: pd.DataFrame,
    scenario_labels: list[str],
    get_id: Callable,
    indices_dtype: np.dtype,
    biosphere_edge_types,
    technosphere_negative_edge_types,
    technosphere_positive_edge_types,
    database_name: str,
    project_name: str,
) -> dict[str, dict[str, np.ndarray]]:
    """Convert finalized scenario rows to Brightway matrix array resources."""

    missing_columns = [
        column for column in scenario_labels if column not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Scenario dataframe is missing value columns: {missing_columns}."
        )

    values = dataframe.loc[:, scenario_labels].to_numpy(dtype=np.float64)
    if not np.isfinite(values).all():
        raise ValueError("Scenario arrays cannot contain non-finite values.")

    varying = ~np.all(values == values[:, :1], axis=1)
    dataframe = dataframe.loc[varying].reset_index(drop=True)
    values = values[varying]

    if dataframe.empty:
        raise ValueError(
            "Cannot export scenario arrays because no exchanges change across scenarios."
        )

    biosphere_edge_types = set(biosphere_edge_types)
    negative_edge_types = set(technosphere_negative_edge_types)
    positive_edge_types = set(technosphere_positive_edge_types)
    supported_edge_types = (
        biosphere_edge_types | negative_edge_types | positive_edge_types
    )

    unsupported = sorted(
        {
            flow_type
            for flow_type in dataframe["flow type"]
            if flow_type not in supported_edge_types
        },
        key=lambda value: str(value),
    )
    if unsupported:
        raise ValueError(
            f"Unsupported Brightway flow type(s) in scenario arrays: {unsupported}."
        )

    rows = np.empty(len(dataframe), dtype=np.int64)
    cols = np.empty(len(dataframe), dtype=np.int64)
    for position, (_, row) in enumerate(dataframe.iterrows()):
        rows[position] = _resolve_key(
            key=row["from key"],
            role="from",
            row=row,
            get_id=get_id,
            database_name=database_name,
            project_name=project_name,
        )
        cols[position] = _resolve_key(
            key=row["to key"],
            role="to",
            row=row,
            get_id=get_id,
            database_name=database_name,
            project_name=project_name,
        )

    resources = {}
    flow_types = dataframe["flow type"].to_numpy()
    matrix_masks = {
        "biosphere_matrix": np.fromiter(
            (flow_type in biosphere_edge_types for flow_type in flow_types),
            dtype=bool,
            count=len(flow_types),
        ),
        "technosphere_matrix": np.fromiter(
            (
                flow_type in negative_edge_types or flow_type in positive_edge_types
                for flow_type in flow_types
            ),
            dtype=bool,
            count=len(flow_types),
        ),
    }

    for matrix, mask in matrix_masks.items():
        if not mask.any():
            continue

        indices = np.empty(int(mask.sum()), dtype=indices_dtype)
        indices["row"] = rows[mask]
        indices["col"] = cols[mask]
        resource = {
            "data_array": np.ascontiguousarray(values[mask], dtype=np.float64),
            "indices_array": indices,
        }
        if matrix == "technosphere_matrix":
            resource["flip_array"] = np.fromiter(
                (flow_type in negative_edge_types for flow_type in flow_types[mask]),
                dtype=bool,
                count=int(mask.sum()),
            )
        resources[matrix] = resource

    if not resources:
        raise ValueError(
            "Cannot export scenario arrays because no supported matrix coordinates change."
        )

    return resources


def _write_scenario_array_datapackage(
    *,
    dataframe: pd.DataFrame,
    scenario_labels: list[str],
    filepath: Path,
    name: str,
    metadata: dict,
    dependencies=None,
) -> Path:
    """Write a compressed datapackage to a temporary file and atomically replace it."""

    bw_processing, get_id, labels, ZipFileSystem = (
        dependencies or _load_scenario_array_dependencies()
    )
    resources = _scenario_dataframe_to_arrays(
        dataframe=dataframe,
        scenario_labels=scenario_labels,
        get_id=get_id,
        indices_dtype=bw_processing.INDICES_DTYPE,
        biosphere_edge_types=labels.biosphere_edge_types,
        technosphere_negative_edge_types=labels.technosphere_negative_edge_types,
        technosphere_positive_edge_types=labels.technosphere_positive_edge_types,
        database_name=name,
        project_name=metadata["brightway_project"],
    )

    filepath = filepath.expanduser().resolve()
    filepath.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{filepath.stem}-",
        suffix=".zip",
        dir=filepath.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    filesystem = None

    try:
        filesystem = ZipFileSystem(
            str(temporary_path), mode="w", compression=zipfile.ZIP_DEFLATED
        )
        datapackage_name = (
            bw_processing.clean_datapackage_name(name) or "scenario_array"
        )
        datapackage = bw_processing.create_datapackage(
            fs=filesystem,
            name=datapackage_name,
            metadata=metadata,
            sequential=True,
            sum_intra_duplicates=False,
            sum_inter_duplicates=False,
        )

        for matrix, resource in resources.items():
            datapackage.add_persistent_array(
                matrix=matrix,
                name=bw_processing.clean_datapackage_name(
                    f"{name} {matrix.replace('_', ' ')}"
                ),
                **resource,
            )

        datapackage.finalize_serialization()
        filesystem = None
        os.replace(temporary_path, filepath)
    except Exception:
        if filesystem is not None:
            filesystem.close()
        raise
    finally:
        temporary_path.unlink(missing_ok=True)

    return filepath
