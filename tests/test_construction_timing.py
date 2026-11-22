"""Check the route markers in the exported matrix without changing timing."""

import csv
from shutil import copyfile

from premise.construction_timing import mark_construction_routes


def csv_file(path, fields, rows, delimiter=","):
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream, delimiter=delimiter)
        writer.writerow(fields)
        writer.writerows(rows)


def test_construction_markers_keep_fallback_and_retirement(tmp_path):
    root = tmp_path / "inventories/remind/test/2050"
    root.mkdir(parents=True)
    csv_file(
        root / "A_matrix_index.csv",
        ("name", "reference product", "unit", "location", "index"),
        [
            ("electricity production", "electricity", "kilowatt hour", "FRA", 0),
            ("market for turbine", "turbine", "unit", "GLO", 1),
            ("turbine construction", "turbine", "unit", "GLO", 2),
            ("turbine installation", "installation", "unit", "GLO", 3),
            ("treatment of used turbine", "turbine", "unit", "GLO", 4),
        ],
        delimiter=";",
    )
    csv_file(
        tmp_path / "temporal_selector_matches.csv",
        (
            "scenario",
            "calling_dataset_name",
            "calling_dataset_reference_product",
            "calling_dataset_location",
            "name",
            "reference_product",
            "unit",
            "location",
            "iam_vintage_asset",
            "iam_vintage_region",
        ),
        [
            (
                "remind | test | 2050",
                "electricity production",
                "electricity",
                "FRA",
                "market for turbine",
                "turbine",
                "unit",
                "GLO",
                "wind",
                "FRA",
            )
        ],
    )
    matrix = root / "A_matrix.csv"
    csv_file(
        matrix,
        (
            "index of activity",
            "index of product",
            "value",
            "flip",
            "temporal_distribution",
            "uncertainty type",
            "negative",
        ),
        [
            (0, 1, 3, 1, 6, 0, 0),
            (1, 2, 1, 1, 5, 0, 0),
            (2, 3, 2, 1, 5, 0, 0),
            (1, 4, 1, 1, 6, 0, 0),
        ],
        delimiter=";",
    )
    mapping = tmp_path / "mapping.yaml"
    mapping.write_text(
        "construction_pass_through:\n"
        '  - callers: ["turbine construction"]\n'
        '    suppliers: ["turbine installation"]\n'
    )

    for year in (2020, 2030, 2040):
        earlier = root.with_name(str(year))
        earlier.mkdir()
        copyfile(root / "A_matrix_index.csv", earlier / "A_matrix_index.csv")
        header, construction, *remaining_rows = matrix.read_text().splitlines()
        if year == 2020:
            # A fallback distribution on the existing row stays in the CSV.
            remaining_rows.insert(0, construction.replace(";6;", ";5;"))
        (earlier / matrix.name).write_text("\n".join([header, *remaining_rows]) + "\n")

    mark_construction_routes(
        tmp_path, mapping, {("wind", "FRA", 2020), ("wind", "FRA", 2030)}
    )

    with matrix.open(newline="") as stream:
        actual = list(csv.DictReader(stream, delimiter=";"))
    assert [row["construction_timing"] for row in actual] == [
        "dated",
        "pass_through",
        "pass_through",
        "",
    ]
    assert [row["value"] for row in actual] == ["3", "1", "2", "1"]
    assert [row["temporal_distribution"] for row in actual] == ["6", "5", "5", "6"]
    for year, expected_count in ((2020, 1), (2030, 1), (2040, 0)):
        with (root.with_name(str(year)) / matrix.name).open(newline="") as stream:
            zero_rows = [
                row
                for row in csv.DictReader(stream, delimiter=";")
                if row["construction_timing"] == "zero_fleet"
            ]
        assert len(zero_rows) == expected_count
        if year == 2020:
            assert zero_rows[0]["temporal_distribution"] == "5"
            assert zero_rows[0]["value"] == "3"
        if year == 2030:
            assert zero_rows[0]["value"] == "0"
            assert zero_rows[0]["uncertainty type"] == zero_rows[0]["negative"] == "0"
