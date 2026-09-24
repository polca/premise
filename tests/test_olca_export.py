"""Premise-to-Brightpath integration, using a synthetic local method package."""

import csv
import json
import sys
import uuid
import zipfile
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from premise import olca_export


def uid(value):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value))


@pytest.fixture
def mapping(tmp_path):
    if sys.version_info < (3, 12):
        pytest.skip("Brightpath requires Python 3.12")
    methods = pytest.importorskip("brightpath.formats.openlca_methods")
    root = tmp_path / "methods"
    entities = {
        "flows": {
            "@id": uid("emission"),
            "name": "Emission",
            "flowType": "ELEMENTARY_FLOW",
            "category": "Elementary flows/Emission to air/unspecified",
            "flowProperties": [
                {
                    "flowProperty": {"@id": uid("mass")},
                    "conversionFactor": 1,
                    "isRefFlowProperty": True,
                }
            ],
        },
        "flow_properties": {
            "@id": uid("mass"),
            "name": "Mass",
            "unitGroup": {"@id": uid("units")},
        },
        "unit_groups": {
            "@id": uid("units"),
            "name": "Mass units",
            "units": [
                {
                    "@id": uid("kg"),
                    "name": "kg",
                    "isRefUnit": True,
                    "conversionFactor": 1,
                }
            ],
        },
    }
    for folder, entity in entities.items():
        (root / folder).mkdir(parents=True)
        (root / folder / "entity.json").write_text(json.dumps(entity))
    source = tmp_path / "source.csv"
    with source.open("w", newline="") as stream:
        csv.writer(stream).writerow(
            ["Emission", "air", "unspecified", "kilogram", uid("emission")]
        )
    return methods.OpenLCAMethodMapping(root, source)


@pytest.fixture
def scenario():
    def activity(name, amount=1):
        return {
            "name": name,
            "reference product": name,
            "unit": "kilogram",
            "location": "GLO",
            "code": "original_" + name,
            "parameters": {"efficiency": np.float64(0.8)},
            "exchanges": [
                {
                    "type": "production",
                    "name": name,
                    "product": name,
                    "unit": "kilogram",
                    "amount": amount,
                }
            ],
        }

    a, b = activity("supplier", -1), activity("consumer")
    a["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Emission",
            "categories": ("air", "unspecified"),
            "unit": "kilogram",
            "amount": np.float64(2),
        }
    )
    b["exchanges"].append(
        {
            "type": "technosphere",
            "name": "supplier",
            "product": "supplier",
            "unit": "kilogram",
            "location": "GLO",
            "amount": -3,
        }
    )
    return {"model": "remind", "pathway": "SSP2-Test", "year": 2050, "database": [a, b]}


def entities(path, folder):
    with zipfile.ZipFile(path) as archive:
        return [
            json.loads(archive.read(n))
            for n in archive.namelist()
            if n.startswith(folder + "/")
        ]


def test_closed_scenario_signs_mapping_and_nonmutation(scenario, mapping, tmp_path):
    original = deepcopy(scenario)
    path = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    processes = {p["name"]: p for p in entities(path, "processes")}
    supplier = processes["supplier"]
    exchange = processes["consumer"]["exchanges"][1]
    assert exchange["defaultProvider"]["@id"] == supplier["@id"]
    assert exchange["flow"]["@id"] == supplier["exchanges"][0]["flow"]["@id"]
    assert exchange["amount"] == -3
    assert supplier["exchanges"][0]["amount"] == -1
    assert supplier["exchanges"][1]["flow"]["@id"] == uid("emission")
    assert supplier["exchanges"][1]["unit"]["@id"] == uid("kg")
    assert all(str(uuid.UUID(p["@id"])) == p["@id"] for p in processes.values())
    assert path.with_suffix(".biosphere-coverage.json").exists()
    assert scenario == original
    again = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    assert {p["name"]: p for p in entities(again, "processes")} == processes


def test_scenario_ids_do_not_collide(scenario, mapping, tmp_path):
    first = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    scenario["year"] = 2060
    second = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    for folder in ("processes", "flows"):
        assert not {p["@id"] for p in entities(first, folder)} & {
            p["@id"] for p in entities(second, folder)
        }


@pytest.mark.parametrize("problem", ["missing", "ambiguous", "production", "amount"])
def test_invalid_scenario_does_not_publish(scenario, mapping, tmp_path, problem):
    if problem == "missing":
        scenario["database"].pop(0)
    elif problem == "ambiguous":
        scenario["database"].append(deepcopy(scenario["database"][0]))
    elif problem == "production":
        scenario["database"][0]["exchanges"].pop(0)
    else:
        scenario["database"][0]["exchanges"][0]["amount"] = np.nan
    with pytest.raises(ValueError):
        olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    assert not list(tmp_path.glob("*.zip"))


def test_writer_failure_preserves_previous_output(
    scenario, mapping, tmp_path, monkeypatch
):
    from brightpath.formats import openlca_jsonld

    path = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    before = path.read_bytes()

    def fail(document, destination, **kwargs):
        Path(destination).write_text("incomplete")
        raise RuntimeError("render failed")

    monkeypatch.setattr(openlca_jsonld, "write_openlca_jsonld", fail)
    with pytest.raises(RuntimeError, match="render failed"):
        olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".openlca-*"))


def test_optional_runtime_errors(monkeypatch):
    monkeypatch.setattr(olca_export.sys, "version_info", (3, 11))
    with pytest.raises(RuntimeError, match="Python 3.12"):
        olca_export.load_method_mapping("missing", "3.12")
    monkeypatch.setattr(olca_export.sys, "version_info", (3, 12))
    with pytest.raises(ValueError, match="3.8 and 3.12"):
        olca_export.load_method_mapping("missing", "3.11")
    with pytest.raises(ValueError, match="method_package"):
        olca_export.load_method_mapping(None, "3.12")


def test_newdatabase_keeps_preparation_and_reports(
    scenario, mapping, tmp_path, monkeypatch
):
    import premise.new_database as module

    ndb = module.NewDatabase.__new__(module.NewDatabase)
    definition = {k: v for k, v in scenario.items() if k != "database"}
    ndb.scenarios = [definition]
    ndb.version, ndb.system_model, ndb.biosphere_name = "3.12", "cutoff", "biosphere3"
    calls = []
    monkeypatch.setattr(olca_export, "load_method_mapping", lambda *a: mapping)
    monkeypatch.setattr(ndb, "_load_original_database", lambda: [])
    monkeypatch.setattr(
        ndb, "_ensure_semantic_certification", lambda s: calls.append("certify")
    )
    monkeypatch.setattr(module, "load_database", lambda **kw: deepcopy(scenario))
    monkeypatch.setattr(
        module, "_prepare_database", lambda **kw: calls.append("prepare")
    )
    monkeypatch.setattr(
        ndb, "_record_export_validation_phase", lambda *a: calls.append("record")
    )
    monkeypatch.setattr(ndb, "_run_automatic_reports", lambda: calls.append("reports"))
    monkeypatch.setattr(module, "delete_all_pickles", lambda: calls.append("cleanup"))
    result = ndb.write_db_to_olca(tmp_path, method_package="methods")
    assert len(result) == 1 and result[0].is_file()
    assert calls == ["certify", "prepare", "record", "reports", "cleanup"]
    assert "database" not in definition


def test_unspecified_subcompartment_is_normalized(scenario, mapping, tmp_path):
    scenario["database"][0]["exchanges"][1]["categories"] = ("air",)
    path = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    supplier = next(p for p in entities(path, "processes") if p["name"] == "supplier")
    assert supplier["exchanges"][1]["flow"]["@id"] == uid("emission")
    assert scenario["database"][0]["exchanges"][1]["categories"] == ("air",)


def test_process_categories_use_source_classifications_and_safe_fallbacks():
    def dataset(name, product, classification=()):
        return {
            "name": name,
            "reference product": product,
            "unit": "kilogram",
            "location": "GLO",
            "classifications": classification,
        }

    a = dataset(
        "producer", "chemical", [("ISIC rev.4 ecoinvent", "2011:Basic chemicals")]
    )
    b = dataset("market", "chemical")
    c = dataset(
        "other", "other", [("CPC", "123:Goods with slashes / and backslashes\\ here")]
    )
    d = dataset("unknown", "unknown")
    rows = [a, b, c, d]
    before = deepcopy(rows)
    result = olca_export.build_process_categories(rows)
    assert (
        result[olca_export._identity(a)]
        == "20 - Manufacture of chemicals and chemical products/201 - Manufacture of basic chemicals, fertilizers and nitrogen compounds, plastics and synthetic rubber in primary forms/2011 - Manufacture of basic chemicals"
    )
    assert (
        result[olca_export._identity(b)]
        == "20 - Manufacture of chemicals and chemical products/201 - Manufacture of basic chemicals, fertilizers and nitrogen compounds, plastics and synthetic rubber in primary forms/2011 - Manufacture of basic chemicals"
    )
    assert (
        result[olca_export._identity(c)]
        == "CPC 123 - Goods with slashes - and backslashes - here"
    )
    assert result[olca_export._identity(d)] == "Unclassified"
    assert result == olca_export.build_process_categories(list(reversed(rows)))
    assert rows == before
    rows.append(
        dataset(
            "second producer",
            "chemical",
            [("ISIC rev.4 ecoinvent", "2020:Other chemicals")],
        )
    )
    assert (
        olca_export.build_process_categories(rows)[olca_export._identity(b)]
        == "Unclassified"
    )


def test_export_categories_have_no_scenario_wrapper(scenario, mapping, tmp_path):
    scenario["database"][0]["classifications"] = [
        ("ISIC rev.4 ecoinvent", "2011:Basic chemicals")
    ]
    path = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    categories = {p["name"]: p["category"] for p in entities(path, "processes")}
    assert categories == {
        "supplier": "20 - Manufacture of chemicals and chemical products/201 - Manufacture of basic chemicals, fertilizers and nitrogen compounds, plastics and synthetic rubber in primary forms/2011 - Manufacture of basic chemicals",
        "consumer": "Unclassified",
    }


def test_official_isic_hierarchy_and_extensions():
    titles = olca_export._isic_titles()
    path = olca_export._isic_category_path("0111", "source wording", titles)
    assert path.split("/") == [
        "01 - Crop and animal production, hunting and related service activities",
        "011 - Growing of non-perennial crops",
        "0111 - Growing of cereals (except rice), leguminous crops and oil seeds",
    ]
    assert (
        olca_export._isic_category_path("01", "ignored", titles) == path.split("/")[0]
    )
    assert olca_export._isic_category_path("011", "ignored", titles) == "/".join(
        path.split("/")[:2]
    )
    assert (
        olca_export._isic_category_path("0111a", "Local extension", titles)
        == path + "/0111a - Local extension"
    )
    assert (
        olca_export._isic_category_path("unknown", "Unknown", titles)
        == "Unclassified ISIC/unknown - Unknown"
    )


def test_ei38_biosphere_has_unique_uuids_and_preserves_compartments():
    path = olca_export.DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"
    with path.open() as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    by_code = {row[-1]: row[:-1] for row in rows}
    assert len(by_code) == len(rows)
    assert by_code["dd786e61-d387-4dc4-8314-2a5f958e7168"] == [
        "Beta-cyfluthrin",
        "soil",
        "agricultural",
        "kilogram",
    ]
    assert by_code["ffaaffd3-5deb-4508-9e5f-e47f551ac2b8"] == [
        "Beta-cyfluthrin",
        "air",
        "non-urban air or from high stacks",
        "kilogram",
    ]


@pytest.mark.parametrize(
    "version,filename",
    [("3.8", "flows_biosphere_38.csv"), ("3.12", "flows_biosphere_312.csv")],
)
def test_method_mapping_selects_exact_source_version(monkeypatch, version, filename):
    module = pytest.importorskip("brightpath.formats.openlca_methods")
    calls = []
    monkeypatch.setattr(
        module,
        "OpenLCAMethodMapping",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    olca_export.load_method_mapping("local-methods.zip", version)
    assert calls == [
        (
            ("local-methods.zip", olca_export.DATA_DIR / "utils" / "export" / filename),
            {
                "biosphere_version": version,
                "conflict_policy": "preserve" if version == "3.8" else "error",
            },
        )
    ]


def test_export_orders_categories_numerically(scenario, mapping, tmp_path):
    # The original source order deliberately puts division 20 before 01.
    scenario["database"][0]["classifications"] = [
        ("ISIC rev.4 ecoinvent", "2011:Chemicals")
    ]
    scenario["database"][1]["classifications"] = [
        ("ISIC rev.4 ecoinvent", "0111:Crops")
    ]
    path = olca_export.export_scenario(scenario, tmp_path, "3.12", "cutoff", mapping)
    processes = entities(path, "processes")
    assert [p["name"] for p in processes] == ["consumer", "supplier"]
    assert processes[0]["category"].startswith("01 - ")
    assert processes[1]["category"].startswith("20 - ")
    assert processes[0]["exchanges"][1]["defaultProvider"]["@id"] == processes[1]["@id"]
