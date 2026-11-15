"""Reviewed output boundaries and real workbook metadata-only classification."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
from types import SimpleNamespace

from bw2io import ExcelImporter
import pytest
import yaml

from premise.core.filesystem_constants import DATA_DIR, INVENTORY_DIR
from premise.inventory.inventory_imports import (
    DefaultInventory,
    get_classification_entry,
    get_classifications,
)

REVIEW = DATA_DIR / "utils" / "import" / "classification_review.yaml"


def test_reviewed_workbook_outputs_receive_only_classification_changes(capsys):
    review = yaml.safe_load(REVIEW.read_text())
    expected = {}
    required = {}
    runtime_pairs = set()
    for group in review["groups"].values():
        classification = group["classification"]
        assert group["rationale"]
        for entry in group["activities"]:
            runtime_pairs.add((entry["name"], entry["product"]))
            for identity in [entry, *entry.get("raw_aliases", [])]:
                key = identity["name"], identity["product"]
                assert key not in expected or expected[key] == classification
                expected[key] = classification
            for source in entry["workbooks"]:
                key = source["name"], source["product"]
                required.setdefault(source["file"], {})[key] = source["unit"]
        for entry in group.get("related_legacy_entries", []):
            expected[entry["name"], entry["product"]] = classification

    assert len(runtime_pairs) == review["scope"]["distinct_missing_pairs"] == 221
    classifications = get_classifications()
    for key, value in expected.items():
        assert get_classification_entry(classifications, *key) == value

    for filename, identities in required.items():
        with redirect_stdout(io.StringIO()):
            data = ExcelImporter(INVENTORY_DIR / filename).data
        selected = [
            ds for ds in data if (ds["name"], ds["reference product"]) in identities
        ]
        assert {(ds["name"], ds["reference product"]) for ds in selected} == set(
            identities
        )
        for ds in selected:
            assert ds["unit"] == identities[ds["name"], ds["reference product"]]
        original = deepcopy(selected)
        importer = object.__new__(DefaultInventory)
        importer.import_db = SimpleNamespace(data=selected)
        importer.classifications = classifications
        importer.add_classifications()
        assert "missing classification" not in capsys.readouterr().out
        for before, after in zip(original, selected, strict=True):
            assert (
                dict(after["classifications"])
                == expected[after["name"], after["reference product"]]
            )
            assert {k: v for k, v in after.items() if k != "classifications"} == {
                k: v for k, v in before.items() if k != "classifications"
            }


@pytest.mark.parametrize(
    "name,product,cpc",
    [
        (
            "battery monitoring integrated circuit production, copper wire bonding",
            "battery monitoring integrated circuit, copper wire bonding",
            "47160",
        ),
        (
            "silicon carbide switch production, photovoltaic inverter, 1200 v, to-247",
            "silicon carbide switch, photovoltaic inverter, 1200 V, TO-247",
            "47150",
        ),
        (
            "printed circuit board production, battery central control, mounted",
            "printed circuit board, battery central control, mounted",
            "46220",
        ),
        (
            "printed circuit board production, power electronics, bare",
            "printed circuit board, power electronics, bare",
            "47130",
        ),
        (
            "market for non-cell battery assembly, for Li-ion battery, LTO proxy",
            "non-cell battery assembly, for Li-ion battery, LTO proxy",
            "46430",
        ),
        (
            "market for battery capacity, Li-ion, LFP",
            "electricity storage capacity",
            "46420",
        ),
        (
            "inventory service package, inherited EPR enrichment reference",
            "inventory service, inherited EPR enrichment reference",
            "88421",
        ),
        (
            "qualified reject and retirement handling, 3760, per finished reference",
            "qualified life-cycle waste handling, component 3760",
            "943",
        ),
        (
            "photovoltaic module production, gallium arsenide-silicon tandem, without cells",
            "photovoltaic module, gallium arsenide-silicon tandem, without cells",
            "47173",
        ),
        (
            "hydrogen fuel tank production, bus, type 4 cylinder, 350 bar",
            "hydrogen fuel tank, bus, type IV cylinder, 350 bar",
            "36990",
        ),
        (
            "cryogenic hydrogen storage tank production, with retirement",
            "cryogenic hydrogen storage tank",
            "42220",
        ),
        (
            "ferroboron production, carbothermic reduction, 19.6% boron",
            "ferroboron, 19.6% boron",
            "411",
        ),
        (
            "zirconium cladding alloy production, epr, m5 type",
            "zirconium cladding alloy, EPR, M5 type",
            "4160",
        ),
    ],
)
def test_distinct_output_boundaries_have_distinct_product_codes(name, product, cpc):
    entry = get_classification_entry(get_classifications(), name, product)
    assert entry["CPC"].split(":", 1)[0] == cpc


def test_unreviewed_names_still_report_missing_classification(capsys):
    ds = {
        "name": "battery custom unreviewed assembly production",
        "reference product": "custom unreviewed battery assembly",
        "unit": "unit",
        "exchanges": [],
    }
    importer = object.__new__(DefaultInventory)
    importer.import_db = SimpleNamespace(data=[ds])
    importer.classifications = get_classifications()
    importer.add_classifications()
    assert "classifications" not in ds
    assert "WARNING: missing classification" in capsys.readouterr().out
