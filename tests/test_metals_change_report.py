import openpyxl

from premise.change_report import ReportScenario, generate_structured_change_report
from premise.inventory_store import CompactInventoryStore
from premise.provenance import ProvenanceCollector, record_change_event


def test_preserved_material_rule_appears_in_methodology_without_inventory_diff(
    tmp_path,
):
    activity = {
        "database": "source-db",
        "code": "epr",
        "name": "EPR construction",
        "reference product": "EPR construction",
        "location": "CH",
        "unit": "unit",
        "exchanges": [
            {
                "name": "EPR construction",
                "product": "EPR construction",
                "location": "CH",
                "unit": "unit",
                "amount": 1,
                "type": "production",
            }
        ],
    }
    source = CompactInventoryStore([activity])
    final = CompactInventoryStore([activity])
    identity = ("image", "SSP2-L", 2020, ())
    collector = ProvenanceCollector("build-id")
    with collector.session(identity, "metals"):
        record_change_event(
            object(),
            activity,
            "skipped",
            sector="metals",
            reason_code="metals.material_rule.preserved_source",
            explanation="Preserved EPR material structure.",
            configuration_reference=(
                "premise/data/metals/metal_products.yaml#nuclear-lead-lead"
            ),
            computed_target_values={"target direct amount": 60_000},
        )

    generated = generate_structured_change_report(
        source_store=source,
        scenarios=(
            ReportScenario(
                identity=identity,
                store=final,
                provenance_payload=collector.payload_for(identity),
            ),
        ),
        build_id="1234567890abcdef",
        source_fingerprint="source-fingerprint",
        filepath=tmp_path,
        source_database="source-db",
        source_type="brightway",
        version="3.11",
        system_model="cutoff",
        premise_version="2.5.1",
    )

    workbook = openpyxl.load_workbook(generated.artifacts.workbook_path, read_only=True)
    rows = list(workbook["Methodology"].iter_rows(values_only=True))
    headers = rows[0]
    reason_index = headers.index("reason code")
    values_index = headers.index("computed target values")
    assert any(
        row[reason_index] == "metals.material_rule.preserved_source" for row in rows[1:]
    )
    material_row = next(
        row
        for row in rows[1:]
        if row[reason_index] == "metals.material_rule.preserved_source"
    )
    assert '"target direct amount":60000' in material_row[values_index]
