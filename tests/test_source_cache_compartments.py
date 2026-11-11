"""Source-cache trimming preserves biosphere compartments and scalar null rules."""

import pytest

from premise.core.utils import create_cache, load_cached_database


@pytest.mark.parametrize(
    "categories",
    [["air", "urban air close to ground"], ("air", "urban air close to ground"), []],
)
def test_source_cache_preserves_compartments_and_drops_scalar_nulls(
    tmp_path, categories
):
    database = [
        {
            "name": "synthetic emission source",
            "reference product": "heat",
            "location": "GLO",
            "unit": "megajoule",
            "exchanges": [
                {
                    "type": "biosphere",
                    "name": "synthetic emission",
                    "categories": categories,
                    "amount": -2.0,
                    "scale": None,
                    "minimum": float("nan"),
                    "uncertainty type": 0,
                }
            ],
        }
    ]
    runtime, _ = create_cache(database, tmp_path / "compartments.pickle")
    restored = load_cached_database(tmp_path / "compartments.pickle")
    assert restored == runtime
    exchange = restored[0]["exchanges"][0]
    assert exchange["categories"] == categories
    assert exchange["amount"] == -2.0
    assert exchange["uncertainty type"] == 0
    assert "scale" not in exchange and "minimum" not in exchange
