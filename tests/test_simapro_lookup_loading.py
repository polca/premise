"""Lookup loading preserves duplicate precedence and original row values."""

import json

import pytest

import premise.export.export as module


@pytest.mark.parametrize("kind", ["category", "biosphere"])
def test_later_duplicate_replaces_value_without_moving_key(tmp_path, monkeypatch, kind):
    directory = tmp_path / "utils" / "export"
    directory.mkdir(parents=True)
    monkeypatch.setattr(module, "DATA_DIR", tmp_path)
    if kind == "category":
        (directory / "simapro_categories.csv").write_text(
            "name,product,category\nFlow A,Product,first\n"
            "Flow B,Other,middle\nFLOW A,PRODUCT,last\n",
            encoding="latin-1",
        )
        result = module.get_simapro_category_of_exchange()
        expected = {
            ("flow a", "product"): {
                "name": "FLOW A",
                "product": "PRODUCT",
                "category": "last",
            },
            ("flow b", "other"): {
                "name": "Flow B",
                "product": "Other",
                "category": "middle",
            },
        }
    else:
        (directory / "simapro-biosphere.json").write_text(
            json.dumps([[0, "first", "a"], [0, "middle", "b"], [0, "last", "a"]]),
            encoding="utf-8",
        )
        result = module.get_simapro_biosphere_dictionnary()
        expected = {"a": "last", "b": "middle"}

    assert list(result.items()) == list(expected.items())
