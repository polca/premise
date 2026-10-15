"""Vehicle source provenance must survive workbook import."""

import hashlib
import json
from pathlib import Path

from premise.filesystem_constants import INVENTORY_DIR

SPEC_KEY = "vehicle inventory provenance"
AUDIT_DIR = Path(__file__).parents[1] / "dev" / "data" / "vehicle_inventory_audit"


def test_installed_workbooks_have_sources_and_provenance():
    from bw2io import ExcelImporter

    for stem, count in (("lci-pass_cars", 162), ("lci-trucks", 127)):
        path = INVENTORY_DIR / f"{stem}.xlsx"
        importer = ExcelImporter(str(path))
        assert importer.db_name == stem and len(importer.data) == count
        sidecar = json.loads((AUDIT_DIR / f"{stem}.provenance.json").read_text())
        assert sidecar["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        for ds in importer.data:
            assert ds["source"] and ds["model"] and ds["model version"]
            spec = json.loads(ds[SPEC_KEY])
            assert spec["baseline year"] == 2020
            assert (
                spec["model generation date"]
                == sidecar["generation provenance"]["model generation date"]
            )
            assert ds["source"] == spec["activity source"]
            assert "https://doi.org/" in ds["model publication"]
    importer = ExcelImporter(str(INVENTORY_DIR / "lci-truck-legacy-components.xlsx"))
    assert importer.db_name == "lci-truck-legacy-components"
    assert len(importer.data) == 26
    sidecar = json.loads((AUDIT_DIR / "lci-trucks.provenance.json").read_text())
    assert (
        sidecar["legacy_components_sha256"]
        == hashlib.sha256(
            (INVENTORY_DIR / "lci-truck-legacy-components.xlsx").read_bytes()
        ).hexdigest()
    )
    for ds in importer.data:
        assert ds["source"]
        assert SPEC_KEY not in ds and "model publication" not in ds
        assert (
            json.loads(ds["inventory provenance"])["role"]
            == "retained legacy component"
        )
    assert not list(INVENTORY_DIR.glob("*.provenance.json"))
