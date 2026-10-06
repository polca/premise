"""Exercise the real Brightpath writer and Premise's export lifecycle."""

from copy import deepcopy
import csv
import json

import pytest

from premise import simapro_export


@pytest.fixture
def brightpath():
    simapro_export.check_brightpath()


def activity(name="material production", product="material", amount=2, unit="kilogram"):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": unit,
        "code": name,
        "comment": "Source paragraph.\nSecond paragraph.",
        "classifications": [
            ("ISIC rev.4 ecoinvent", "2410:Manufacture of basic iron and steel")
        ],
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "product": product,
                "location": "GLO",
                "unit": unit,
                "amount": amount,
            }
        ],
    }


@pytest.fixture
def scenario():
    waste = activity("treatment of waste", "waste", -1)
    waste["classifications"] = [
        ("ISIC rev.4 ecoinvent", "3821:Waste treatment"),
        ("CPC", "39990:Other wastes"),
    ]
    consumer = activity()
    consumer["parameters"] = {"efficiency": 0.8}
    consumer["exchanges"] += [
        {
            "type": "technosphere",
            "name": waste["name"],
            "product": "waste",
            "location": "GLO",
            "unit": "kilogram",
            "amount": -0.123456789,
            "uncertainty type": 3,
            "loc": -0.123456789,
            "scale": 0.01,
        },
        {
            "type": "biosphere",
            "name": "Water",
            "categories": ("air", "unspecified"),
            "unit": "cubic meter",
            "amount": 0.00123456789,
            "uncertainty type": 3,
            "loc": 0.00123456789,
            "scale": 0.0001,
        },
    ]
    return {
        "model": "remind",
        "pathway": "SSP2-Test",
        "year": 2050,
        "database": [waste, consumer],
    }


def rows(path):
    with path.open(encoding="latin-1", newline="") as stream:
        return list(csv.reader(stream, delimiter=";"))


@pytest.mark.parametrize(
    "compartment,label", [("unspecified", ""), ("surface water", "river")]
)
def test_real_writer_preserves_quantities_uncertainty_metadata_and_source(
    brightpath, scenario, tmp_path, compartment, label
):
    scenario["database"][1]["exchanges"][-1]["categories"] = ("water", compartment)
    source = deepcopy(scenario)
    path = simapro_export.export_scenario(scenario, tmp_path, "3.12", "cutoff")
    written = rows(path)
    assert scenario == source
    products = [written[i + 1] for i, row in enumerate(written) if row == ["Products"]]
    assert any(row[2] == "2" for row in products)
    waste_rows = [
        written[i + 1]
        for i, row in enumerate(written)
        if row == ["Waste to treatment"] and written[i + 1]
    ]
    assert waste_rows[0][2:6] == ["0.123456789", "Normal", "0.0001", "0"]
    water = next(row for row in written if row and row[0] == "Water" and len(row) == 9)
    assert water[1] == label
    assert water[2] == "kg"
    assert float(water[3]) == pytest.approx(1.23456789)
    assert float(water[5]) == pytest.approx(0.01)
    text = path.read_text(encoding="latin-1")
    assert "Brightpath" in text and "SSP2-Test" in text
    assert "Second paragraph." in text and "efficiency" in text
    assert "241 - " in text
    report = json.loads(path.with_suffix(".export-report.json").read_text())
    assert report["excluded_exchanges"] == []
    assert report["classification_fallbacks"] == []


def test_exclusions_and_reviewed_classification_fallback_are_reported(
    brightpath, scenario, tmp_path, monkeypatch
):
    import premise.export

    dataset = scenario["database"][1]
    dataset["classifications"] = []
    monkeypatch.setattr(
        premise.export,
        "get_simapro_category_of_exchange",
        lambda: {
            (dataset["name"], dataset["reference product"]): {
                "category": "material",
                "sub_category": "Reviewed",
            }
        },
    )
    for name, compartment in (("Oxygen", "air"), ("indicator", "inventory indicator")):
        dataset["exchanges"].append(
            {
                "type": "biosphere",
                "name": name,
                "categories": (compartment,),
                "unit": "kilogram",
                "amount": 0.5,
            }
        )
    with pytest.warns(
        UserWarning,
        match="2 excluded biosphere exchanges and 1 classification fallbacks",
    ):
        path = simapro_export.export_scenario(scenario, tmp_path, "3.12", "cutoff")
    report = json.loads(path.with_suffix(".export-report.json").read_text())
    assert {entry["reason"] for entry in report["excluded_exchanges"]} == {
        "inventory_indicator",
        "brightpath_blacklist",
    }
    assert report["classification_fallbacks"][0]["category"] == "material/Reviewed"
    assert report["issue_counts"]["simapro_exchange_unused"] == 1


@pytest.mark.parametrize(
    "failure",
    [
        "missing_supplier",
        "duplicate_supplier",
        "bad_production",
        "invalid_amount",
    ],
)
def test_invalid_inventory_does_not_replace_existing_export(
    brightpath, scenario, tmp_path, failure
):
    destination = tmp_path / "simapro_export_remind_SSP2-Test_2050.csv"
    destination.write_text("existing export")
    if failure == "missing_supplier":
        scenario["database"].pop(0)
    elif failure == "duplicate_supplier":
        scenario["database"].append(deepcopy(scenario["database"][0]))
    elif failure == "bad_production":
        scenario["database"][1]["exchanges"][0]["unit"] = "unit"
    else:
        scenario["database"][1]["exchanges"][1]["amount"] = float("nan")
    with pytest.raises(ValueError):
        simapro_export.export_scenario(scenario, tmp_path, "3.12", "cutoff")
    assert destination.read_text() == "existing export"
    assert not destination.with_suffix(".export-report.json").exists()


@pytest.mark.parametrize(
    "unit,label",
    [
        ("cubic meter-year", "m3y"),
        ("kilogram day", "kg*day"),
        ("guest night", "guestnight"),
        ("ton-kilometer", "tkm"),
    ],
)
def test_full_inventory_units_and_fossil_well_render(brightpath, tmp_path, unit, label):
    dataset = activity(unit=unit)
    dataset["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Gas, natural, in ground",
            "categories": ("natural resource", "fossil well"),
            "unit": "cubic meter",
            "amount": 0.01,
        }
    )
    scenario = {
        "model": "image",
        "pathway": "SSP2-Test",
        "year": 2050,
        "database": [dataset],
    }
    path = simapro_export.export_scenario(scenario, tmp_path, "3.12", "consequential")
    written = rows(path)
    output = written[written.index(["Products"]) + 1]
    assert output[1:3] == [label, "2"]
    assert "Consequential, U" in output[0]


def test_runtime_guard_runs_before_loading_scenarios(monkeypatch, tmp_path):
    from premise import NewDatabase

    ndb = NewDatabase.__new__(NewDatabase)
    monkeypatch.setattr(simapro_export.sys, "version_info", (3, 11))
    with pytest.raises(RuntimeError, match="Python 3.12"):
        ndb.write_db_to_simapro(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_missing_classification_records_default_category(
    brightpath, scenario, tmp_path
):
    scenario["database"][1]["classifications"] = []
    with pytest.warns(UserWarning, match="1 classification fallbacks"):
        path = simapro_export.export_scenario(scenario, tmp_path, "3.12", "cutoff")
    report = json.loads(path.with_suffix(".export-report.json").read_text())
    assert report["classification_fallbacks"][0]["source"] == "default_category"
    assert (
        report["classification_fallbacks"][0]["category"]
        == "material/Others/Transformation"
    )


def test_public_export_keeps_certification_and_reporting(
    brightpath, scenario, tmp_path, monkeypatch
):
    import premise.new_database as module

    ndb = module.NewDatabase.__new__(module.NewDatabase)
    ndb.scenarios = [{k: v for k, v in scenario.items() if k != "database"}]
    ndb.version, ndb.system_model, ndb.biosphere_name = "3.12", "cutoff", "biosphere3"
    calls = []
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
    result = ndb.write_db_to_simapro(tmp_path)
    assert calls == [
        "certify",
        "prepare",
        "record",
        "reports",
        "cleanup",
    ]
    assert len(result) == 1 and result[0].is_file()
