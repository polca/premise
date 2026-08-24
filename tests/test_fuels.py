from types import SimpleNamespace

import pytest

import premise.fuels.base as fuels_base
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


@pytest.mark.parametrize(
    ("failure_stage", "expected_calls"),
    [
        ("logistics", ["logistics"]),
        ("logging", ["logistics", "logging"]),
        ("market creation", ["logistics", "logging", "market creation"]),
    ],
)
def test_fuel_update_fails_before_partial_hydrogen_processing(
    monkeypatch, failure_stage, expected_calls
):
    """Mandatory hydrogen steps must not produce a partial fuel update."""
    calls = []
    original_database = [{"name": "original dataset"}]

    class FailingFuels:
        def __init__(self, database, **_kwargs):
            self.database = database
            self.hydrogen_demand_nodes = "calculated demand nodes"

        @staticmethod
        def _fail_if_selected(stage):
            calls.append(stage)
            if failure_stage == stage:
                raise RuntimeError(f"failed during {stage}")

        def set_hydrogen_logistics(self):
            self._fail_if_selected("logistics")

        def write_hydrogen_demand_node_logs(self):
            self._fail_if_selected("logging")

        def generate_hydrogen_activities(self):
            self._fail_if_selected("market creation")

        def relink_hydrogen_consumers_to_sector_markets(self):
            calls.append("consumer relinking")

    iam_data = SimpleNamespace(
        petrol_blend=None,
        diesel_blend=None,
        natural_gas_blend=None,
        hydrogen_blend=object(),
    )
    scenario = {
        "database": original_database,
        "iam data": iam_data,
        "model": "test-model",
        "pathway": "test-pathway",
        "year": 2030,
    }

    monkeypatch.setattr(fuels_base, "Fuels", FailingFuels)

    with pytest.raises(RuntimeError, match=f"failed during {failure_stage}"):
        fuels_base._update_fuels(
            scenario=scenario,
            version="3.10",
            system_model="cutoff",
        )

    assert calls == expected_calls
    assert "consumer relinking" not in calls
    assert scenario["database"] is original_database
