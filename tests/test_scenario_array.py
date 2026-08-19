from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pandas as pd
import pytest

from premise.scenario_array import (
    _load_scenario_array_dependencies,
    _scenario_dataframe_to_arrays,
    _write_scenario_array_datapackage,
)

INDICES_DTYPE = np.dtype([("row", np.int64), ("col", np.int64)])
SCENARIO_LABELS = ["original", "scenario one", "scenario two"]


def scenario_dataframe():
    return pd.DataFrame(
        [
            {
                "from activity name": "CO2",
                "from key": ("biosphere", "co2"),
                "to activity name": "supplier",
                "to key": ("scenario-db", "supplier"),
                "flow type": "biosphere",
                "original": 1,
                "scenario one": 10,
                "scenario two": 100,
            },
            {
                "from activity name": "supplier",
                "from key": ("scenario-db", "supplier"),
                "to activity name": "consumer",
                "to key": ("scenario-db", "consumer"),
                "flow type": "technosphere",
                "original": 1,
                "scenario one": 2,
                "scenario two": 0,
            },
            {
                "from activity name": "consumer",
                "from key": ("scenario-db", "consumer"),
                "to activity name": "consumer",
                "to key": ("scenario-db", "consumer"),
                "flow type": "production",
                "original": 1,
                "scenario one": 0.6,
                "scenario two": 1,
            },
            {
                "from activity name": "substitute",
                "from key": ("scenario-db", "substitute"),
                "to activity name": "consumer",
                "to key": ("scenario-db", "consumer"),
                "flow type": "substitution",
                "original": 0,
                "scenario one": 0.2,
                "scenario two": 0.3,
            },
            {
                "from activity name": "constant",
                "from key": ("scenario-db", "constant"),
                "to activity name": "consumer",
                "to key": ("scenario-db", "consumer"),
                "flow type": "technosphere",
                "original": 5,
                "scenario one": 5,
                "scenario two": 5,
            },
        ]
    )


def convert(dataframe):
    ids = {
        ("biosphere", "co2"): 1,
        ("scenario-db", "supplier"): 2,
        ("scenario-db", "consumer"): 3,
        ("scenario-db", "substitute"): 4,
        ("scenario-db", "constant"): 5,
    }
    return _scenario_dataframe_to_arrays(
        dataframe=dataframe,
        scenario_labels=SCENARIO_LABELS,
        get_id=ids.__getitem__,
        indices_dtype=INDICES_DTYPE,
        biosphere_edge_types={"biosphere"},
        technosphere_negative_edge_types={"technosphere", "generic consumption"},
        technosphere_positive_edge_types={
            "production",
            "generic production",
            "substitution",
        },
        database_name="scenario-db",
        project_name="scenario-project",
    )


def test_scenario_dataframe_to_arrays_preserves_columns_dtypes_flips_and_zeros():
    resources = convert(scenario_dataframe())

    biosphere = resources["biosphere_matrix"]
    assert biosphere["data_array"].dtype == np.float64
    assert biosphere["data_array"].tolist() == [[1.0, 10.0, 100.0]]
    assert biosphere["indices_array"].dtype == INDICES_DTYPE
    assert biosphere["indices_array"].tolist() == [(1, 2)]
    assert "flip_array" not in biosphere

    technosphere = resources["technosphere_matrix"]
    assert technosphere["data_array"].dtype == np.float64
    assert technosphere["data_array"].tolist() == [
        [1.0, 2.0, 0.0],
        [1.0, 0.6, 1.0],
        [0.0, 0.2, 0.3],
    ]
    assert technosphere["indices_array"].tolist() == [(2, 3), (3, 3), (4, 3)]
    assert technosphere["flip_array"].tolist() == [True, False, False]


def test_scenario_dataframe_to_arrays_omits_empty_matrix_resource():
    dataframe = scenario_dataframe()
    dataframe = dataframe[dataframe["flow type"] != "biosphere"]

    resources = convert(dataframe)

    assert set(resources) == {"technosphere_matrix"}


def test_scenario_dataframe_to_arrays_rejects_no_changes():
    dataframe = scenario_dataframe().iloc[[-1]]

    with pytest.raises(ValueError, match="no exchanges change"):
        convert(dataframe)


def test_scenario_dataframe_to_arrays_rejects_unsupported_flow_type():
    dataframe = scenario_dataframe().iloc[[0]].copy()
    dataframe["flow type"] = "unsupported"

    with pytest.raises(ValueError, match="Unsupported Brightway flow type"):
        convert(dataframe)


def test_scenario_dataframe_to_arrays_wraps_unresolved_keys_with_context():
    dataframe = scenario_dataframe().iloc[[0]].copy()
    dataframe.at[dataframe.index[0], "from key"] = ("biosphere", "missing")

    with pytest.raises(KeyError) as error:
        convert(dataframe)

    message = str(error.value)
    assert "('biosphere', 'missing')" in message
    assert "'CO2'" in message
    assert "scenario-db" in message
    assert "scenario-project" in message


def test_write_scenario_array_datapackage_is_compressed_and_replaces_atomically(
    tmp_path,
):
    bw_processing = pytest.importorskip("bw_processing")
    from fsspec.implementations.zip import ZipFileSystem

    ids = {
        ("biosphere", "co2"): 1,
        ("scenario-db", "supplier"): 2,
        ("scenario-db", "consumer"): 3,
        ("scenario-db", "substitute"): 4,
        ("scenario-db", "constant"): 5,
    }
    labels = SimpleNamespace(
        biosphere_edge_types={"biosphere"},
        technosphere_negative_edge_types={"technosphere", "generic consumption"},
        technosphere_positive_edge_types={
            "production",
            "generic production",
            "substitution",
        },
    )
    destination = tmp_path / "nested" / "arrays.zip"
    destination.parent.mkdir()
    destination.write_bytes(b"old artifact")

    result = _write_scenario_array_datapackage(
        dataframe=scenario_dataframe(),
        scenario_labels=SCENARIO_LABELS,
        filepath=destination,
        name="scenario-db",
        metadata={"brightway_project": "scenario-project", "scenario_count": 3},
        dependencies=(bw_processing, ids.__getitem__, labels, ZipFileSystem),
    )

    assert result == destination.resolve()
    with ZipFile(result) as archive:
        assert archive.testzip() is None
        assert all(item.compress_type == ZIP_DEFLATED for item in archive.infolist())
        assert not any(
            path.name.startswith(".arrays-") for path in result.parent.iterdir()
        )


def test_scenario_array_datapackage_advances_matrices_together_and_wraps(tmp_path):
    bd = pytest.importorskip("bw2data")
    bc = pytest.importorskip("bw2calc")
    pytest.importorskip("bw_processing")
    if int(bd.__version__[0]) < 4:
        pytest.skip("Scenario-array integration requires modern Brightway")

    from bw2data.tests import bw2test

    @bw2test
    def run_in_temporary_brightway_project():
        biosphere = bd.Database("biosphere")
        biosphere.write(
            {
                ("biosphere", "co2"): {
                    "name": "CO2",
                    "unit": "kilogram",
                    "categories": ("air",),
                    "type": "emission",
                }
            }
        )
        database = bd.Database("scenario-db")
        database.write(
            {
                ("scenario-db", "supplier"): {
                    "name": "supplier",
                    "reference product": "product",
                    "unit": "kilogram",
                    "location": "GLO",
                    "exchanges": [
                        {
                            "input": ("scenario-db", "supplier"),
                            "amount": 1,
                            "type": "production",
                        },
                        {
                            "input": ("biosphere", "co2"),
                            "amount": 1,
                            "type": "biosphere",
                        },
                    ],
                },
                ("scenario-db", "consumer"): {
                    "name": "consumer",
                    "reference product": "service",
                    "unit": "unit",
                    "location": "GLO",
                    "exchanges": [
                        {
                            "input": ("scenario-db", "consumer"),
                            "amount": 1,
                            "type": "production",
                        },
                        {
                            "input": ("scenario-db", "supplier"),
                            "amount": 1,
                            "type": "technosphere",
                        },
                    ],
                },
            }
        )
        method = ("scenario-array-test", "GWP")
        bd.Method(method).write([(("biosphere", "co2"), 1)])
        dataframe = pd.DataFrame(
            [
                {
                    "from activity name": "supplier",
                    "from key": ("scenario-db", "supplier"),
                    "to activity name": "consumer",
                    "to key": ("scenario-db", "consumer"),
                    "flow type": "technosphere",
                    "original": 1,
                    "scenario one": 2,
                    "scenario two": 3,
                },
                {
                    "from activity name": "CO2",
                    "from key": ("biosphere", "co2"),
                    "to activity name": "supplier",
                    "to key": ("scenario-db", "supplier"),
                    "flow type": "biosphere",
                    "original": 1,
                    "scenario one": 10,
                    "scenario two": 100,
                },
            ]
        )
        array_path = _write_scenario_array_datapackage(
            dataframe=dataframe,
            scenario_labels=SCENARIO_LABELS,
            filepath=tmp_path / "integration-arrays.zip",
            name="scenario-db",
            metadata={"brightway_project": bd.projects.current},
            dependencies=_load_scenario_array_dependencies(),
        )

        demand, data_objs, remapping = bd.prepare_lca_inputs(
            {database.get("consumer"): 1}, method=method
        )
        lca = bc.LCA(
            demand,
            data_objs=[*data_objs, array_path],
            remapping_dicts=remapping,
            use_arrays=True,
            use_distributions=False,
        )
        lca.lci()
        lca.lcia()
        scores = [lca.score]
        for _ in range(3):
            next(lca)
            scores.append(lca.score)

        base_lca = bc.LCA(
            demand,
            data_objs=data_objs,
            remapping_dicts=remapping,
        )
        base_lca.lci()
        base_lca.lcia()

        assert scores == pytest.approx([1, 20, 300, 1])
        assert base_lca.score == pytest.approx(1)

    run_in_temporary_brightway_project()
