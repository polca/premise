from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import yaml
from cryptography.fernet import Fernet

from premise.data_collection import IAMDataCollection


def collection(year=2050):
    iam = object.__new__(IAMDataCollection)
    iam.year = year
    iam.min_year = 2020
    iam.filepath_iam_files = "synthetic fixture"
    return iam


def scenario(values, regions=("EUR",), years=(2020, 2030, 2050), unit="MJ/vehkm"):
    return xr.DataArray(
        np.array(list(values.values()), dtype=float)
        .reshape(len(values), len(regions), len(years))
        .transpose(1, 0, 2),
        dims=("region", "variables", "year"),
        coords={
            "region": list(regions),
            "variables": list(values),
            "year": list(years),
        },
        attrs={"unit": {name: unit for name in values}},
    )


def calculate(data, **kwargs):
    return collection().get_vehicle_efficiencies(
        data,
        production_labels=kwargs.pop("production_labels", {"car": "ES"}),
        energy_labels=kwargs.pop("energy_labels", {"car": "FE"}),
        intensity_labels=kwargs.pop("intensity_labels", {"car": "I"}),
        **kwargs,
    )


def test_direct_intensity_overrides_pooled_carrier_energy_and_is_inverted():
    data = scenario({"ES": [10, 20, 30], "FE": [1, 1, 1], "I": [2, 1.8, 1.6]})
    original = data.copy(deep=True)
    result = calculate(data)
    # 20% lower energy use is 25% higher efficiency, irrespective of fleet size.
    np.testing.assert_allclose(result.sel(variables="car"), [[1, 2 / 1.8, 1.25]])
    xr.testing.assert_identical(data, original)
    assert result.attrs["vehicle efficiency sources"]["car"]["by region"] == {
        "EUR": "direct intensity"
    }


def test_zero_fleet_baseline_does_not_remove_available_modelled_intensity():
    data = scenario({"ES": [0, 5, 10], "FE": [0, 1, 1], "I": [10, 9, 8]})
    result = calculate(data)
    assert result.sel(year=2020).item() == 1
    assert result.sel(year=2050).item() == pytest.approx(1.25)


def test_missing_region_never_borrows_another_region_or_technology():
    data = scenario(
        {"Large": [[np.nan] * 3, [2, 1.8, 1.6]], "Small": [[1, 0.9, 0.8]] * 2},
        regions=("EUR", "USA"),
    )
    result = calculate(
        data,
        production_labels={},
        energy_labels={},
        intensity_labels={"large car": "Large", "small car": "Small"},
    )
    np.testing.assert_array_equal(result.sel(region="EUR", variables="large car"), 1)
    assert result.sel(region="USA", variables="large car", year=2050).item() == 1.25
    sources = result.attrs["vehicle efficiency sources"]
    assert sources["large car"]["by region"]["EUR"] == "unavailable"
    assert sources["small car"]["by region"]["EUR"] == "direct intensity"


def test_explicit_proxy_supplies_only_missing_regions_and_preserves_input():
    data = scenario(
        {"Large": [[np.nan] * 3, [2, 1.9, 1.8]], "SUV": [[4, 3.6, 3.2], [4, 3, 2]]},
        regions=("EUR", "USA"),
    )
    original = data.copy(deep=True)
    result = calculate(
        data,
        production_labels={},
        energy_labels={},
        intensity_labels={"large": "Large"},
        intensity_fallback_labels={"large": "SUV"},
    )
    np.testing.assert_allclose(result.sel(region="EUR"), [[1, 4 / 3.6, 1.25]])
    np.testing.assert_allclose(result.sel(region="USA"), [[1, 2 / 1.9, 2 / 1.8]])
    assert result.attrs["vehicle efficiency sources"]["large"]["by region"] == {
        "EUR": "proxy intensity",
        "USA": "direct intensity",
    }
    assert (
        result.attrs["vehicle efficiency sources"]["large"][
            "fallback intensity variable"
        ]
        == "SUV"
    )
    xr.testing.assert_identical(data, original)


def test_native_energy_service_takes_priority_over_proxy():
    result = calculate(
        scenario({"ES": [10, 20, 30], "FE": [1, 1.9, 2.7], "SUV": [4, 3, 2]}),
        intensity_fallback_labels={"car": "SUV"},
    )
    np.testing.assert_allclose(result, [[[1, 1 / 0.95, 1 / 0.9]]])
    assert (
        result.attrs["vehicle efficiency sources"]["car"]["by region"]["EUR"]
        == "energy/service"
    )


@pytest.mark.parametrize("baseline", [0, -1, np.nan, np.inf])
def test_proxy_requires_its_own_positive_2020_reference(baseline):
    result = calculate(
        scenario({"SUV": [baseline, 3, 2]}), intensity_fallback_labels={"car": "SUV"}
    )
    np.testing.assert_array_equal(result, 1)
    assert (
        result.attrs["vehicle efficiency sources"]["car"]["by region"]["EUR"]
        == "unavailable"
    )


@pytest.mark.parametrize(
    "values,expected", [([4, np.nan, 3.2], [1, 1.25, 1.25]), ([4, 5, 6], [1, 1, 1])]
)
def test_proxy_keeps_existing_gap_and_monotonicity_policy(values, expected):
    result = calculate(
        scenario({"SUV": values}), intensity_fallback_labels={"car": "SUV"}
    )
    np.testing.assert_allclose(result, [[expected]])


def test_proxy_never_borrows_a_different_regions_series():
    data = scenario({"SUV": [[np.nan] * 3, [4, 3.6, 3.2]]}, regions=("EUR", "USA"))
    result = calculate(data, intensity_fallback_labels={"car": "SUV"})
    np.testing.assert_array_equal(result.sel(region="EUR"), 1)
    assert result.sel(region="USA", year=2050).item() == 1.25


def test_absent_proxy_is_explicitly_unavailable():
    result = calculate(
        scenario({"other": [1, 2, 3]}), intensity_fallback_labels={"car": "SUV"}
    )
    np.testing.assert_array_equal(result, 1)
    assert (
        result.attrs["vehicle efficiency sources"]["car"]["by region"]["EUR"]
        == "unavailable"
    )


def test_proxy_requires_vehicle_kilometre_units():
    with pytest.raises(ValueError, match="unsupported unit"):
        calculate(
            scenario({"SUV": [4, 3, 2]}, unit="MJ/pkm"),
            intensity_fallback_labels={"car": "SUV"},
        )


def test_multiple_proxy_intensities_cannot_be_summed():
    with pytest.raises(ValueError, match="cannot be summed"):
        calculate(
            scenario({"SUV": [4, 3, 2]}), intensity_fallback_labels={"car": ["SUV"]}
        )


def test_proxy_without_2020_does_not_infer_baseline():
    result = calculate(
        scenario({"SUV": [4, 3]}, years=(2025, 2050)),
        intensity_fallback_labels={"car": "SUV"},
    )
    np.testing.assert_array_equal(result, 1)


def test_large_car_proxy_mapping_is_powertrain_specific_and_efficiency_only():
    path = (
        Path(__file__).parents[1]
        / "premise/iam_variables_mapping/transport_passenger_cars.yaml"
    )
    mapping = yaml.safe_load(path.read_text())
    proxies = {
        k: v for k, v in mapping.items() if "energy_intensity_fallback_aliases" in v
    }
    assert len(proxies) == 5
    for technology, spec in proxies.items():
        assert technology.endswith(", large")
        assert set(spec["energy_intensity_fallback_aliases"]) == {"remind", "remind-eu"}
        for model, proxy in spec["energy_intensity_fallback_aliases"].items():
            native = spec["energy_intensity_aliases"][model]
            assert proxy == native.replace("|Large Car|", "|Large Car and SUV|")
            for field in ("iam_aliases", "energy_use_aliases"):
                assert "|Large Car|" in spec[field][model]
                assert "|Large Car and SUV|" not in spec[field][model]


@pytest.mark.parametrize("baseline", [0, -1, np.nan, np.inf])
def test_invalid_direct_baseline_is_not_backfilled_from_future(baseline):
    result = calculate(scenario({"I": [baseline, 2, 1]}))
    np.testing.assert_array_equal(result, 1)
    assert (
        result.attrs["vehicle efficiency sources"]["car"]["by region"]["EUR"]
        == "unavailable"
    )


def test_missing_reference_year_does_not_use_nearest_direct_intensity():
    result = calculate(scenario({"I": [2, 1.5]}, years=(2025, 2050)))
    np.testing.assert_array_equal(result, 1)


def test_missing_direct_variable_uses_available_energy_service_pair():
    result = calculate(scenario({"ES": [10, 20, 30], "FE": [1, 1.8, 2.4]}))
    np.testing.assert_allclose(result, [[[1, 1 / 0.9, 1.25]]])
    assert (
        result.attrs["vehicle efficiency sources"]["car"]["by region"]["EUR"]
        == "energy/service"
    )


def test_missing_regional_fallback_is_neutral_despite_neighbouring_technology():
    data = scenario({"ES": [np.nan] * 3, "FE": [np.nan] * 3, "I2": [2, 1.8, 1.6]})
    result = calculate(data, intensity_labels={"car": "I", "other": "I2"})
    np.testing.assert_array_equal(result.sel(variables="car"), 1)
    assert result.sel(variables="other", year=2050).item() == 1.25


def test_direct_intensity_keeps_existing_no_deterioration_policy():
    result = calculate(scenario({"I": [2, 2.5, 3]}))
    np.testing.assert_array_equal(result, 1)


def test_intensity_gaps_are_filled_within_the_same_series_only():
    result = calculate(scenario({"I": [2, np.nan, 1.6]}))
    np.testing.assert_allclose(result, [[[1, 1.25, 1.25]]])


@pytest.mark.parametrize("unit", ["MJ/pkm", "EJ/yr", None])
def test_direct_intensities_require_vehicle_distance_units(unit):
    with pytest.raises(ValueError, match="unsupported unit"):
        calculate(scenario({"I": [2, 1.8, 1.6]}, unit=unit))


def test_intensive_series_cannot_be_summed():
    with pytest.raises(ValueError, match="cannot be summed"):
        calculate(scenario({"I": [2, 1.8, 1.6]}), intensity_labels={"car": ["I"]})


@pytest.mark.parametrize("model", ["image", "tiam-ucl", "gcam", "witch"])
def test_models_without_direct_aliases_retain_existing_multifuel_calculation(model):
    mapping = yaml.safe_load(
        (
            Path(__file__).parents[1]
            / "premise/iam_variables_mapping/transport_passenger_cars.yaml"
        ).read_text()
    )
    assert all(
        model not in spec.get("energy_intensity_aliases", {})
        for spec in mapping.values()
    )
    assert all(
        model not in spec.get("energy_intensity_fallback_aliases", {})
        for spec in mapping.values()
    )
    data = scenario(
        {"ES": [10, 20, 30], "liquid": [0.8, 1.2, 1.5], "electric": [0.2, 0.6, 0.9]}
    )
    labels = {
        "production_labels": {"car": "ES"},
        "energy_labels": {"car": ["liquid", "electric"]},
    }
    iam = collection()
    original = iam.get_iam_efficiencies(data, **labels)
    result = iam.get_vehicle_efficiencies(data, intensity_labels={}, **labels)
    xr.testing.assert_identical(result, original)
    assert result.sel(year=2050).item() == pytest.approx(1.25)


def test_message_with_no_vehicle_mappings_has_no_inferred_vehicle_efficiency():
    assert (
        calculate(
            scenario({"transport energy": [1, 2, 3]}),
            production_labels={},
            energy_labels={},
            intensity_labels={},
        )
        is None
    )


def test_encrypted_reader_preserves_vehicle_intensity_values_and_units(tmp_path):
    key = Fernet.generate_key()
    content = (
        "Region;Variable;Unit;2020;2050\nEUR;Energy intensity|Car|BEV;MJ/vehkm;2;1.6\n"
    )
    (tmp_path / "remind_SSP2-NPi.csv").write_bytes(
        Fernet(key).encrypt(content.encode())
    )
    iam = collection()
    iam.model = "remind"
    iam.pathway = "SSP2-NPi"
    data = iam._IAMDataCollection__get_iam_data(
        key=key, filedir=tmp_path, variables=["Energy intensity|Car|BEV"]
    )
    assert data.attrs["unit"]["Energy intensity|Car|BEV"] == "MJ/vehkm"
    assert data.sel(region="EUR", year=2050).item() == pytest.approx(1.6)
    result = iam.get_vehicle_efficiencies(
        data, {}, {}, {"car": "Energy intensity|Car|BEV"}
    )
    assert result.sel(year=2050).item() == pytest.approx(1.25)
