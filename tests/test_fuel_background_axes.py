"""Shared fuel transformations keep technosphere and biosphere axes separate."""

from types import SimpleNamespace

import pytest
import xarray as xr

from premise.fuels.base import Fuels
from premise.inventory.inventory_imports import get_biosphere_code


@pytest.mark.parametrize(
    "family,version,biosphere",
    [
        ("ecoinvent", "3.10", None),
        ("ecoinvent", "3.12", None),
        ("uvek", "2026", "3.10"),
    ],
)
def test_shared_fuels_uses_selected_family_and_biosphere(family, version, biosphere):
    iam = SimpleNamespace(
        regions=["EUR"],
        production_volumes=xr.DataArray([], dims="variables", coords={"variables": []}),
        petrol_technology_efficiencies=None,
        diesel_technology_efficiencies=None,
        gas_technology_efficiencies=None,
        hydrogen_technology_efficiencies=None,
    )
    fuels = Fuels(
        database=[],
        iam_data=iam,
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        version=version,
        system_model="cutoff",
        family=family,
        biosphere_version=biosphere,
    )
    assert fuels.mapping.family == family
    assert fuels.biosphere_flows == get_biosphere_code(biosphere or version)
    assert fuels.version == version


@pytest.mark.parametrize(
    "family,version,biosphere",
    [("ecoinvent", "3.12", None), ("uvek", "2026", "3.10")],
)
@pytest.mark.parametrize("gas", [None, 0, 1])
def test_fuel_update_uses_explicit_biosphere_for_vehicle_carbon(
    monkeypatch, family, version, biosphere, gas
):
    import premise.fuels.base as module

    received = {}
    database = []
    fake = SimpleNamespace(database=database, fuel_map={}, cache={}, index={})
    handover_calls = []
    fake._handover_fuel_market_aliases = lambda target: handover_calls.append(target)
    for name in (
        "generate_hydrogen_activities",
        "generate_synthetic_fuel_activities",
        "generate_biogas_activities",
        "clear_validation_provenance",
        "relink_datasets",
    ):
        setattr(fake, name, lambda: None)

    def construct(**kwargs):
        received.update(kwargs)
        return fake

    carbon_versions = []
    carrier_calls = []
    monkeypatch.setattr(module, "Fuels", construct)
    monkeypatch.setattr(module, "get_scenario_inventory", lambda scenario: database)
    monkeypatch.setattr(module, "replace_scenario_inventory", lambda *args: None)
    monkeypatch.setattr(
        module,
        "annotate_cng_properties",
        lambda data, **kwargs: carrier_calls.append(kwargs.get("carrier_specs", [])),
    )
    monkeypatch.setattr(
        module,
        "update_vehicle_fuel_co2",
        lambda data, bio_version, **kwargs: carbon_versions.append(
            (bio_version, kwargs["recorded_exhaust_filters"])
        ),
    )
    monkeypatch.setattr(module, "record_validation_phase", lambda *args: None)
    monkeypatch.setattr(
        module,
        "FuelsValidation",
        lambda **kwargs: SimpleNamespace(run_fuel_checks=lambda **kwargs: None),
    )
    scenario = {
        "iam data": SimpleNamespace(
            petrol_blend=1,
            diesel_blend=None,
            natural_gas_blend=xr.DataArray(gas) if gas is not None else None,
            hydrogen_blend=None,
            regions=["EUR"],
        ),
        "model": "remind",
        "pathway": "SSP2-NPi",
        "year": 2050,
    }
    module._update_fuels(
        scenario, version, "cutoff", family=family, biosphere_version=biosphere
    )
    assert received["family"] == family
    assert received["biosphere_version"] == biosphere
    assert received["version"] == version
    assert carbon_versions[0][0] == (biosphere or version)
    assert bool(carbon_versions[0][1]) == (family == "uvek")

    assert bool(carrier_calls[0]) == (family == "uvek" and gas is not None and gas > 0)
    assert bool(handover_calls) == (family == "uvek" and gas is not None and gas > 0)


@pytest.mark.parametrize("family", ["ecoinvent", "uvek"])
def test_fuel_dispatch_forwards_axes_before_preparation(monkeypatch, family):
    from unittest.mock import Mock

    from premise import NewDatabase
    from premise.database import background

    owner = object.__new__(NewDatabase)
    owner.source_family = family
    owner.version = "2026" if family == "uvek" else "3.12"
    owner.biosphere_version = "3.10" if family == "uvek" else "3.12"
    owner.system_model = "cutoff"
    owner.use_absolute_efficiency = False
    owner.gains_scenario = "CLE"
    owner.prepare_inventories = Mock(
        side_effect=RuntimeError("stop before preparation")
    )
    # Inspect registration only; this does not qualify or open the public fuel gate.
    monkeypatch.setattr(background, "validate_uvek_scope", lambda sectors: [sectors])
    with pytest.raises(RuntimeError, match="stop before preparation"):
        owner.update("fuels")
    expected = (owner.version, "cutoff")
    if family == "uvek":
        expected += (family, "3.10")
    assert owner.sector_update_methods["fuels"]["args"] == expected
    owner.prepare_inventories.assert_called_once_with(["fuels"])


def test_public_fuels_gate_remains_closed_until_qualified():
    from unittest.mock import Mock

    from premise import NewDatabase

    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    owner.prepare_inventories = Mock()
    with pytest.raises(ValueError, match="unsupported sectors.*fuels"):
        owner.update("fuels")
    owner.prepare_inventories.assert_not_called()
