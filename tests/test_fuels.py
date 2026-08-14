from types import SimpleNamespace

from premise.fuels.base import Fuels


def test_gcam_coal_methane_inventory_is_regionalized():
    coal_methane = {
        "name": (
            "methane, synthetic, gaseous, 5 bar, from coal-based hydrogen, "
            "at fuelling station"
        ),
        "reference product": "methane, high pressure",
        "location": "RER",
    }
    fuels = object.__new__(Fuels)
    fuels.database = [coal_methane]
    fuels.fuel_map = {"methane, from coal": [coal_methane]}
    fuels.iam_data = SimpleNamespace(
        production_volumes=None,
        natural_gas_blend=None,
    )
    fuels.mapping = SimpleNamespace(generate_fuel_map=lambda: {})

    captured = {}

    def capture_regionalization(mapping, production_volumes):
        captured.update(mapping)

    fuels.process_and_add_activities = capture_regionalization

    fuels.generate_biogas_activities()

    assert captured == {"methane, from coal": [coal_methane]}
