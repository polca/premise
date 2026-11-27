"""Keep matrix CSV numbering and uncertainty fields stable across releases."""

import csv
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

import premise.export.export as module

VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")
MATRIX_FILES = (
    "A_matrix.csv",
    "A_matrix_index.csv",
    "B_matrix.csv",
    "B_matrix_index.csv",
)


UNCERTAINTY_ROWS = (
    {"amount": 0.0},
    {"amount": -2.0, "uncertainty type": 1, "loc": 99.0},
    {"amount": 1.25, "uncertainty type": 2, "loc": 0.1, "scale": 0.2},
    {"amount": -1.25, "uncertainty type": 3, "loc": -1.25, "scale": 0.3},
    {"amount": 1.0, "uncertainty type": 4, "loc": 1.0, "minimum": 0.5, "maximum": 2.0},
    {
        "amount": 1.0,
        "uncertainty type": 8,
        "loc": 1.0,
        "scale": 2.0,
        "shape": 1.5,
        "minimum": 0.0,
        "maximum": 4.0,
    },
)


@pytest.mark.parametrize("version", ["3.7", "3.7.1", "3.13"])
def test_matrix_biosphere_catalog_rejects_unsupported_backgrounds(version):
    with pytest.raises(ValueError, match=r"Premise supports ecoinvent 3\.8 through 3\.12"):
        module.biosphere_flows_dictionary(version)


def matrix_scenario(version):
    """Exercise every packaged biosphere UUID and each uncertainty column."""
    supplier = {
        "name": 'supplier; "quoted"',
        "reference product": "material",
        "unit": "kilogram",
        "location": "CH",
    }
    consumer = {
        "name": "consumer, café",
        "reference product": "product",
        "unit": "kilogram",
        "location": "GLO",
    }
    for activity in (supplier, consumer):
        activity["exchanges"] = [
            {
                "name": activity["name"],
                "product": activity["reference product"],
                "unit": activity["unit"],
                "location": activity["location"],
                "type": "production",
                "amount": 1.0,
            }
        ]
    consumer["exchanges"].extend(
        {
            "name": supplier["name"],
            "product": supplier["reference product"],
            "unit": supplier["unit"],
            "location": supplier["location"],
            "type": "technosphere",
            **parameters,
        }
        for parameters in UNCERTAINTY_ROWS
    )
    for position, (flow, code) in enumerate(
        module.biosphere_flows_dictionary(version).items()
    ):
        consumer["exchanges"].append(
            {
                "name": flow[0],
                "categories": flow[1:3],
                "unit": flow[3],
                "input": ("biosphere", code),
                "type": "biosphere",
                **UNCERTAINTY_ROWS[position % len(UNCERTAINTY_ROWS)],
            }
        )
    return {
        "database": [supplier, consumer],
        "model": "image",
        "pathway": "test",
        "year": 2050,
    }


@pytest.mark.parametrize("version", VERSIONS)
def test_matrix_files_match_frozen_export_for_every_biosphere_uuid(version, tmp_path):
    expected = json.loads(
        (Path(__file__).parent / "data/matrix_export/expected.json").read_text()
    )["versions"][version]
    scenario = matrix_scenario(version)
    original = deepcopy(scenario)

    module.Export(scenario, tmp_path, version, "cutoff").export_db_to_matrices()

    assert {path.name for path in tmp_path.glob("*.csv")} == set(MATRIX_FILES)
    assert {
        name: hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
        for name in MATRIX_FILES
    } == expected
    assert scenario == original


def test_public_matrix_helpers_preserve_last_duplicate_code_and_row(
    tmp_path, monkeypatch
):
    first = ("first", "air", "unspecified", "kilogram")
    middle = ("middle", "water", "unspecified", "kilogram")
    last = ("last", "soil", "unspecified", "kilogram")
    flows = {first: "shared", middle: "unique", last: "shared"}
    monkeypatch.setattr(module, "biosphere_flows_dictionary", lambda version: flows)
    scenario = matrix_scenario("3.12")
    exporter = module.Export(scenario, tmp_path, "3.12", "cutoff")

    assert module.create_codes_index_of_biosphere_flows_matrix("3.12") == {
        "shared": 2,
        "unique": 1,
    }
    assert module.create_index_of_biosphere_flows_matrix("3.12") == {
        first: 0,
        middle: 1,
        last: 2,
    }
    assert exporter.create_rev_index_of_B_matrix("3.12") == {
        "shared": last,
        "unique": middle,
    }
    assert (
        exporter.rev_index({"first": 7, "middle": 8, "last": 7})
        == module.rev_index({"first": 7, "middle": 8, "last": 7})
        == {7: "last", 8: "middle"}
    )
    assert [row[1] for row in exporter.create_B_matrix_coordinates()] == [2, 1, 2]
    assert exporter.create_A_matrix_coordinates()[0][:3] == [0, 0, 1.0]


def test_missing_biosphere_uuid_keeps_warning_and_blank_csv_row(
    tmp_path, monkeypatch, capsys
):
    flow = ("known", "air", "unspecified", "kilogram")
    monkeypatch.setattr(
        module, "biosphere_flows_dictionary", lambda version: {flow: "known"}
    )
    scenario = matrix_scenario("3.12")
    scenario["database"][1]["exchanges"][-1]["input"] = ("biosphere", "missing")
    exporter = module.Export(scenario, tmp_path, "3.12", "cutoff")

    assert exporter.create_B_matrix_coordinates() == [()]
    exporter.export_db_to_matrices()

    assert "Cannot find the biosphere flow known" in capsys.readouterr().out
    assert (tmp_path / "B_matrix.csv").read_bytes().endswith(b"\n\n")


def test_reused_exporter_refreshes_database_and_version_indexes(tmp_path, monkeypatch):
    first = ("first", "air", "unspecified", "kilogram")
    last = ("last", "water", "unspecified", "kilogram")
    catalogs = {
        "first": {first: "first", last: "last"},
        "second": {last: "last", first: "first"},
    }
    monkeypatch.setattr(
        module, "biosphere_flows_dictionary", lambda version: catalogs[version]
    )
    scenario = matrix_scenario("first")
    exporter = module.Export(scenario, tmp_path / "before", "first", "cutoff")
    exporter.export_db_to_matrices()
    exporter.db.reverse()
    exporter.version = "second"
    exporter.filepath = tmp_path / "after"

    exporter.export_db_to_matrices()

    with (exporter.filepath / "B_matrix.csv").open(newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    assert [row[:2] for row in rows[1:]] == [["0", "1"], ["0", "0"]]
    assert [row[:2] for row in exporter.create_B_matrix_coordinates()] == [
        [0, 1],
        [0, 0],
    ]


def test_matrix_indexes_are_built_once_per_file_export(tmp_path, monkeypatch):
    flow = ("known", "air", "unspecified", "kilogram")
    monkeypatch.setattr(
        module, "biosphere_flows_dictionary", lambda version: {flow: "known"}
    )
    calls = {}
    for name in (
        "create_index_of_A_matrix",
        "create_index_of_biosphere_flows_matrix",
        "create_codes_index_of_biosphere_flows_matrix",
    ):
        build = getattr(module, name)
        calls[name] = 0

        def counted(*args, _name=name, _build=build):
            calls[_name] += 1
            return _build(*args)

        monkeypatch.setattr(module, name, counted)
    exporter = module.Export(matrix_scenario("3.12"), tmp_path, "3.12", "cutoff")
    assert set(calls.values()) == {0}

    exporter.export_db_to_matrices()
    assert set(calls.values()) == {1}
    exporter.export_db_to_matrices()
    assert set(calls.values()) == {2}


@pytest.mark.parametrize(
    "matrix,exchanges",
    [
        ("A", []),
        ("A", [{"type": "biosphere"}]),
        ("B", []),
        ("B", [{"type": "production"}]),
    ],
)
def test_coordinate_helpers_ignore_activities_without_relevant_exchanges(
    matrix, exchanges
):
    exporter = object.__new__(module.Export)
    exporter.db = [{"exchanges": exchanges}]

    if matrix == "A":
        assert exporter._create_A_matrix_coordinates({}) == []
    else:
        assert exporter._create_B_matrix_coordinates({}, {}) == []


def test_missing_technosphere_provider_keeps_partial_rows_and_stops(tmp_path, capsys):
    scenario = matrix_scenario("3.12")
    consumer = scenario["database"][1]
    consumer["exchanges"] = consumer["exchanges"][:3]
    consumer["exchanges"][1]["location"] = "missing"
    exporter = module.Export(scenario, tmp_path, "3.12", "cutoff")
    before = deepcopy(scenario)

    rows = exporter.create_A_matrix_coordinates()

    assert [row[:3] for row in rows] == [[0, 0, 1.0], [1, 1, 1.0]]
    assert capsys.readouterr().out == (
        f"KeyError for {consumer['exchanges'][1]} in {consumer['name']}\n"
    )
    assert scenario == before


def test_missing_biosphere_uuid_before_valid_flow_does_not_stop_export(
    tmp_path, monkeypatch, capsys
):
    flow = ("known", "air", "unspecified", "kilogram")
    monkeypatch.setattr(
        module, "biosphere_flows_dictionary", lambda version: {flow: "known"}
    )
    scenario = matrix_scenario("3.12")
    consumer = scenario["database"][1]
    known = consumer["exchanges"][-1]
    missing = {**known, "input": ("biosphere", "missing")}
    consumer["exchanges"] = [missing, known]
    exporter = module.Export(scenario, tmp_path, "3.12", "cutoff")

    assert exporter.create_B_matrix_coordinates() == [
        (),
        [1, 0, 0.0, 0, 0.0, None, None, None, None, 0, 1],
    ]
    assert capsys.readouterr().out.count("Cannot find the biosphere flow known") == 1


def test_activity_index_uses_positions_and_last_stripped_duplicate():
    class ReversedIterationList(list):
        def __iter__(self):
            return reversed(self)

    first = {
        "name": " a ",
        "reference product": " p ",
        "unit": " kg ",
        "location": " GLO ",
    }
    second = {**first, "name": "b"}
    duplicate = {key: value.strip() for key, value in first.items()}
    database = ReversedIterationList([first, second, duplicate])
    before = deepcopy([database[i] for i in range(len(database))])

    index = module.create_index_of_A_matrix(database)

    assert list(index.items()) == [
        (("a", "p", "kg", "GLO"), 2),
        (("b", "p", "kg", "GLO"), 1),
    ]
    assert database == before
