"""Vehicle source provenance must survive workbook import."""

import json
from pathlib import Path

from premise.filesystem_constants import INVENTORY_DIR

SPEC_KEY = "vehicle inventory provenance"
AUDIT_DIR = Path(__file__).parents[1] / "dev" / "data" / "vehicle_inventory_audit"


def test_installed_workbooks_have_sources_and_provenance():
    from bw2io import ExcelImporter

    for stem, count in (("lci-pass_cars", 147), ("lci-trucks", 114)):
        path = INVENTORY_DIR / f"{stem}.xlsx"
        importer = ExcelImporter(str(path))
        assert importer.db_name == stem and len(importer.data) == count
        sidecar = json.loads((AUDIT_DIR / f"{stem}.provenance.json").read_text())
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
    assert not (INVENTORY_DIR / "lci-truck-legacy-components.xlsx").exists()
    assert not list(INVENTORY_DIR.glob("*.provenance.json"))
