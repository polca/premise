"""Key linking must follow rows, without relying on DataFrame index labels."""

import uuid

import pandas as pd
import pytest

from premise.export import find_technosphere_keys


def inventory():
    return [
        {
            "name": "supplier",
            "reference product": "material",
            "location": "CH",
            "unit": "kilogram",
            "database": "test",
            "code": "old-supplier",
            "exchanges": [
                {"type": "production", "input": ("old", "supplier")},
                {"type": "technosphere", "input": ("old", "consumer")},
                {"type": "biosphere", "input": ("biosphere", "flow")},
            ],
        },
        {
            "name": "consumer",
            "reference product": "product",
            "location": "US",
            "unit": "kilogram",
            "database": "test",
            "code": "old-consumer",
            "exchanges": [{"type": "production", "input": ("old", "consumer")}],
        },
    ]


def key_frame(index):
    stale = ("old", "stale")
    return pd.DataFrame(
        [
            (
                "production",
                "supplier",
                "material",
                "CH",
                "supplier",
                "material",
                "CH",
                stale,
                stale,
            ),
            (
                "technosphere",
                "supplier",
                "material",
                "CH",
                "consumer",
                "product",
                "US",
                stale,
                stale,
            ),
            (
                "biosphere",
                "emission",
                None,
                None,
                "consumer",
                "product",
                "US",
                ("biosphere", "flow"),
                stale,
            ),
            (
                "custom",
                "supplier",
                "material",
                "CH",
                "consumer",
                "product",
                "US",
                ("keep", "source"),
                ("keep", "target"),
            ),
            (
                "custom",
                "supplier",
                "material",
                "CH",
                "consumer",
                "product",
                "US",
                None,
                ("keep", "target"),
            ),
            (
                "technosphere",
                "missing",
                "material",
                "CH",
                "supplier",
                "material",
                "CH",
                None,
                stale,
            ),
        ],
        columns=[
            "flow type",
            "from activity name",
            "from reference product",
            "from location",
            "to activity name",
            "to reference product",
            "to location",
            "from key",
            "to key",
        ],
        index=index,
    )


@pytest.mark.parametrize(
    "index",
    [
        pd.RangeIndex(6, name="flow"),
        pd.Index([4, 9, 12, 20, 21, 27], name="flow"),
        pd.Index(["a", "b", "c", "d", "e", "f"], name="flow"),
        pd.Index([4, 4, 9, 12, 9, 12], name="flow"),
        pd.MultiIndex.from_tuples(
            [(1, "a"), (1, "b"), (2, "a"), (2, "a"), (3, "a"), (3, "b")],
            names=["group", "flow"],
        ),
    ],
)
def test_key_linking_refreshes_codes_and_preserves_row_alignment(index, monkeypatch):
    database = inventory()
    frame = key_frame(index)
    metadata = frame.drop(columns=["from key", "to key"]).copy(deep=True)
    codes = iter([uuid.UUID(int=1), uuid.UUID(int=2)])
    monkeypatch.setattr(uuid, "uuid4", lambda: next(codes))

    returned_database, returned_frame = find_technosphere_keys(database, frame)

    supplier, consumer = ("test", str(uuid.UUID(int=1))), (
        "test",
        str(uuid.UUID(int=2)),
    )
    assert returned_database is database
    assert returned_frame is frame
    assert list(frame["from key"].iloc[:5]) == [
        supplier,
        supplier,
        ("biosphere", "flow"),
        ("keep", "source"),
        supplier,
    ]
    assert pd.isna(frame["from key"].iloc[5])
    assert list(frame["to key"]) == [
        supplier,
        consumer,
        consumer,
        ("keep", "target"),
        ("keep", "target"),
        supplier,
    ]
    pd.testing.assert_frame_equal(frame.drop(columns=["from key", "to key"]), metadata)
    assert [activity["code"] for activity in database] == [supplier[1], consumer[1]]
    for activity in database:
        for exchange in activity["exchanges"]:
            if exchange["type"] in {"production", "technosphere"}:
                assert "input" not in exchange
            else:
                assert exchange["input"] == ("biosphere", "flow")


def test_empty_key_table_still_refreshes_inventory_codes(monkeypatch):
    database = inventory()
    frame = key_frame(pd.RangeIndex(6)).iloc[:0]
    expected = frame.copy(deep=True)
    codes = iter([uuid.UUID(int=1), uuid.UUID(int=2)])
    monkeypatch.setattr(uuid, "uuid4", lambda: next(codes))

    find_technosphere_keys(database, frame)

    pd.testing.assert_frame_equal(frame, expected)
    assert database[0]["code"] == str(uuid.UUID(int=1))
    assert database[1]["code"] == str(uuid.UUID(int=2))
