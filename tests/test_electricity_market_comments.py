"""Check electricity market metadata and supplier coverage for each period."""

from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from premise.electricity import Electricity
from premise.validation import ElectricityValidation, PremiseValidationError


@pytest.fixture(
    params=[("remind", "EUR", ["DE", "FR"]), ("image", "WEU", ["CH", "FR"])]
)
def electricity(request):
    model, region, locations = request.param
    obj = Electricity.__new__(Electricity)
    obj.model = model
    obj.scenario = "test-pathway"
    obj.year = 2050
    obj.regions = [region, "USA", "World"]
    obj.database = []
    obj.iam_to_ecoinvent_loc = {region: locations, "USA": ["US"], "World": ["GLO"]}
    obj.ecoinvent_to_iam_loc = {"RoW": "World"}
    obj.geo = SimpleNamespace(
        iam_to_ecoinvent_location=lambda location: obj.iam_to_ecoinvent_loc[location]
    )
    mix = xr.DataArray(
        np.ones((3, 1, 2)),
        dims=("region", "variables", "year"),
        coords={"region": obj.regions, "variables": ["Coal PC"], "year": [2050, 2110]},
    )
    obj.iam_data = SimpleNamespace(electricity_mix=mix, production_volumes=mix * 100)
    obj.network_loss = {
        location: {
            voltage: {"transf_loss": 0.01, "distr_loss": 0.02}
            for voltage in ("high", "medium", "low")
        }
        for location in obj.regions
    }
    obj.powerplant_map = {
        "Coal PC": [
            {
                "name": "electricity production, coal",
                "reference product": "electricity, high voltage",
                "location": "RoW",
                "unit": "kilowatt hour",
            }
        ]
    }
    obj.production_per_tech = {}
    obj.biosphere_dict = {
        (
            "Sulfur hexafluoride",
            "air",
            "non-urban air or from high stacks",
            "kilogram",
        ): "sf6"
    }
    obj.write_log = lambda dataset: None
    obj.add_to_index = lambda dataset: None
    obj.track_validation_provenance = lambda dataset, field: None
    return obj


@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
def test_market_comments_include_coverage_and_preserve_provenance(
    electricity, system_model
):
    electricity.system_model = system_model
    for voltage in ("high", "medium", "low"):
        getattr(electricity, f"create_new_markets_{voltage}_voltage")()

    periods = [0, 20, 40, 60] if system_model == "cutoff" else [0]
    assert len(electricity.database) == 3 * (2 * len(periods) + 1)
    coverage_prefix = "This IAM region covers the following ecoinvent location:"
    for dataset in electricity.database:
        comment = dataset["comment"]
        assert f"IAM model {electricity.model}" in comment
        assert "test-pathway for the year 2050." in comment
        if dataset["location"] == "World":
            # A global market must not inherit one of its suppliers' coverage.
            assert coverage_prefix not in comment
            continue
        expected = electricity.iam_to_ecoinvent_loc[dataset["location"]]
        assert f"{coverage_prefix} {expected}" in comment
        assert comment.count(coverage_prefix) == 1
        for period in periods[1:]:
            if f", {period}-year period" in dataset["name"]:
                assert (
                    f"Average electricity mix over a {period}-year period 2050-{2050 + period}."
                    in comment
                )


@pytest.mark.parametrize(
    "period,fault",
    [(0, None), (20, "high voltage"), (40, "low voltage"), (60, "missing market")],
)
def test_supplier_vectors_certify_each_period_and_voltage(electricity, period, fault):
    electricity.system_model = "cutoff"
    mix = electricity.iam_data.electricity_mix
    regions = {"region": electricity.regions, "year": mix.year}
    for technology, endpoint, voltage in (
        ("Storage, Hydrogen", 0.016, "high voltage"),
        ("Solar PV Residential", 0.012, "low voltage"),
    ):
        added_mix = xr.DataArray(
            [[0.0, endpoint]] * len(electricity.regions),
            dims=("region", "year"),
            coords=regions,
        ).expand_dims(variables=[technology])
        mix = xr.concat([mix, added_mix], dim="variables")
        electricity.powerplant_map[technology] = [
            {
                "name": technology,
                "reference product": f"electricity, {voltage}",
                "location": "RoW",
                "unit": "kilowatt hour",
            }
        ]
    electricity.iam_data.electricity_mix = mix / mix.sum(dim="variables")
    electricity.iam_data.production_volumes = mix * 100
    electricity.create_new_markets_high_voltage()
    electricity.create_new_markets_low_voltage()

    if fault is not None:
        voltage = "high voltage" if fault == "missing market" else fault
        name = f"market group for electricity, {voltage}, {period}-year period"
        market = next(
            ds
            for ds in electricity.database
            if (ds["name"], ds["location"]) == (name, electricity.regions[0])
        )
        if fault == "missing market":
            electricity.database.remove(market)
        else:
            technology = (
                "Storage, Hydrogen"
                if fault == "high voltage"
                else "Solar PV Residential"
            )
            market["exchanges"] = [
                exchange
                for exchange in market["exchanges"]
                if exchange.get("premise electricity technology") != technology
            ]

    validator = ElectricityValidation(
        model=electricity.model,
        scenario=electricity.scenario,
        year=electricity.year,
        regions=electricity.regions,
        database=electricity.database,
        iam_data=electricity.iam_data,
        technology_map=electricity.powerplant_map,
    )
    if fault is None:
        assert not validator.run_supplier_vector_checks().errors
    else:
        with pytest.raises(PremiseValidationError) as error:
            validator.run_supplier_vector_checks()
        (issue,) = error.value.report.errors
        assert issue.activity_key[0] == name


def test_inactive_residential_solar_does_not_require_a_supplier(electricity):
    electricity.system_model = "cutoff"
    mix = electricity.iam_data.electricity_mix
    residential_solar = xr.zeros_like(mix).assign_coords(
        variables=["Solar PV Residential"]
    )
    electricity.iam_data.electricity_mix = xr.concat(
        [mix, residential_solar], dim="variables"
    )
    electricity.iam_data.production_volumes = electricity.iam_data.electricity_mix * 100
    electricity.powerplant_map["Solar PV Residential"] = []

    electricity.create_new_markets_low_voltage()

    assert not any(
        exchange.get("premise electricity technology") == "Solar PV Residential"
        for dataset in electricity.database
        for exchange in dataset["exchanges"]
    )
