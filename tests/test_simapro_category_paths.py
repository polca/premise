from copy import deepcopy

from premise.olca_export import _identity, build_process_categories
from premise.simapro_export import (
    assign_simapro_category_paths,
    assign_simapro_provenance,
)


def test_provenance_fills_blanks_preserves_existing_metadata_and_is_idempotent():
    datasets = [
        {
            "comment": "Source comment",
            "generator": "",
            "simapro metadata": {"External documents": ""},
        },
        {
            "generator": "Original generator",
            "simapro metadata": {
                "System description": "Original system",
                "External documents": "https://example.org/source",
            },
        },
    ]
    scenario = {"model": "remind", "pathway": "SSP1-PkBudg1000", "year": 2050}
    metadata = assign_simapro_provenance(
        datasets, scenario, "3.12", "cutoff", brightpath_version="1.0.0a1"
    )
    once = deepcopy(datasets)
    assert (
        assign_simapro_provenance(
            datasets, scenario, "3.12", "cutoff", brightpath_version="1.0.0a1"
        )
        == metadata
    )
    assert datasets == once
    assert datasets[0]["comment"] == "Source comment"
    assert "Brightpath 1.0.0a1" in datasets[0]["generator"]
    assert (
        datasets[0]["simapro metadata"]["System description"]
        == metadata["system description"]["name"]
    )
    assert "ecoinvent 3.12 (cutoff)" in metadata["system description"]["description"]
    assert (
        "remind / SSP1-PkBudg1000 / 2050"
        in metadata["system description"]["description"]
    )
    assert datasets[1]["generator"] == "Original generator"
    assert datasets[1]["simapro metadata"]["System description"] == "Original system"
    assert (
        datasets[1]["simapro metadata"]["External documents"]
        == "https://example.org/source"
    )


def test_simapro_paths_reuse_openlca_hierarchy_without_changing_waste_status():
    datasets = [
        {
            "name": "crop",
            "reference product": "crop",
            "unit": "kilogram",
            "location": "GLO",
            "classifications": [("ISIC rev.4 ecoinvent", "0111:Crops")],
            "exchanges": [
                {
                    "type": "production",
                    "amount": -1,
                    "simapro category": "waste treatment/Old",
                }
            ],
        },
        {
            "name": "market",
            "reference product": "crop",
            "unit": "kilogram",
            "location": "GLO",
        },
        {
            "name": "energy",
            "reference product": "electricity",
            "unit": "kilowatt hour",
            "location": "GLO",
            "classifications": [("CPC", "17100:Electrical energy")],
        },
        {
            "name": "unknown",
            "reference product": "unknown",
            "unit": "unit",
            "location": "GLO",
        },
    ]
    before = deepcopy(datasets)
    expected = build_process_categories(datasets)
    assign_simapro_category_paths(datasets)
    assign_simapro_category_paths(datasets)
    for dataset, original in zip(datasets, before):
        assert dataset == {
            **original,
            "simapro category path": expected[_identity(original)],
        }
    assert datasets[0]["simapro category path"].startswith("01 - ")
    assert "/011 - " in datasets[0]["simapro category path"]
    assert "/0111 - " in datasets[0]["simapro category path"]
    assert datasets[2]["simapro category path"] == "CPC 17100 - Electrical energy"
    assert datasets[3]["simapro category path"] == "Unclassified"
