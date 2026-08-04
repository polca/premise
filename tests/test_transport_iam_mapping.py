from pathlib import Path

import yaml

MAPPING = (
    Path(__file__).parents[1]
    / "premise"
    / "iam_variables_mapping"
    / "transport_road_freight.yaml"
)


def test_image_bev_truck_aliases_use_preprocessed_residual_variables():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))

    expected = {
        "truck, battery electric, 18 metric ton": (
            "Energy Service|Transportation|Freight|Medium Truck|Battery Electric",
            "Final Energy|Transportation|Freight|Medium Truck|Battery Electric|Electricity",
        ),
        "truck, battery electric, 40 metric ton": (
            "Energy Service|Transportation|Freight|Heavy Truck|Battery Electric",
            "Final Energy|Transportation|Freight|Heavy Truck|Battery Electric|Electricity",
        ),
    }

    for technology, (service, energy) in expected.items():
        assert mapping[technology]["iam_aliases"]["image"] == service
        assert mapping[technology]["energy_use_aliases"]["image"] == energy


def test_image_aggregate_truck_electricity_is_not_mapped_as_bev_service():
    mapping = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))
    bev_aliases = {
        mapping[technology]["energy_use_aliases"]["image"]
        for technology in (
            "truck, battery electric, 18 metric ton",
            "truck, battery electric, 40 metric ton",
        )
    }

    assert (
        "Final Energy|Transportation|Freight|Medium Truck|Electricity"
        not in bev_aliases
    )
    assert (
        "Final Energy|Transportation|Freight|Heavy Truck|Electricity" not in bev_aliases
    )
