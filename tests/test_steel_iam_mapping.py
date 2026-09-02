from pathlib import Path

import yaml


MAPPING = (
    Path(__file__).parents[1]
    / "premise"
    / "iam_variables_mapping"
    / "steel.yaml"
)


def test_message_dri_routes_use_mutually_exclusive_processed_variables():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))

    assert mapping["steel - primary - DRI"]["iam_aliases"]["message"] == (
        "Production|Industry|Iron and Steel|NG-DRI/EAF"
    )
    assert mapping["steel - primary - DRI CCS"]["iam_aliases"]["message"] == (
        "Production|Industry|Iron and Steel|NG-DRI/EAF + CCS"
    )
    assert mapping["steel - primary - H-DRI"]["iam_aliases"]["message"] == (
        "Production|Industry|Iron and Steel|H-DRI/EAF"
    )


def test_message_dri_routes_do_not_reuse_aggregate_energy_inputs():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))

    for route in (
        "steel - primary - DRI",
        "steel - primary - DRI CCS",
        "steel - primary - H-DRI",
    ):
        assert "message" not in mapping[route].get("energy_use_aliases", {})
