"""Vehicle source provenance must survive workbook import."""

import json

from premise.filesystem_constants import INVENTORY_DIR

SPEC_KEY = "vehicle inventory provenance"


def test_installed_workbooks_have_sources_and_provenance():
    from bw2io import ExcelImporter

    for stem, count in (("lci-pass_cars", 147), ("lci-trucks", 114)):
        path = INVENTORY_DIR / f"{stem}.xlsx"
        importer = ExcelImporter(str(path))
        assert importer.db_name == stem
        generated = [ds for ds in importer.data if SPEC_KEY in ds]
        assert len(generated) == count
        assert all(ds.get("source") for ds in importer.data)
        for ds in generated:
            assert ds["source"] and ds["model"] and ds["model version"]
            spec = json.loads(ds[SPEC_KEY])
            assert spec["baseline year"] == 2020
            from datetime import date

            date.fromisoformat(spec["model generation date"])
            assert spec["model generation date"]

            assert ds["source"] == spec["activity source"]
            assert "https://doi.org/" in ds["model publication"]
    assert not (INVENTORY_DIR / "lci-truck-legacy-components.xlsx").exists()
    assert not list(INVENTORY_DIR.glob("*.provenance.json"))
