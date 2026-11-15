"""CDR coverage diagnostics explain gaps without changing the loaded quantities."""

from copy import deepcopy

import numpy as np
import pytest
import xarray as xr

from premise.iam.data_collection import (
    IAM_CDR_VARS,
    IAMDataCollection,
    cdr_alias_candidates,
)

MISSING_IMAGE_LABELS = {
    "Carbon Removal|Geological Storage|Direct Air Capture|Solid Sorbent",
    "Final Energy|Carbon Management|Direct Air Capture|Solid Sorbent|Electricity",
    "Carbon Removal|Enhanced Weathering",
    "Final Energy|Carbon Management|Enhanced Weathering|Electricity",
    "Carbon Removal|Geological Storage|Biomass|Heat",
    "Carbon Removal|Geological Storage|Biomass|Industry",
    "Carbon Removal|Land Use|Re/Afforestation|Plantation",
}


def iam():
    obj = object.__new__(IAMDataCollection)
    obj.model, obj.pathway = "image", "fixture"
    obj.year, obj.min_year = 2050, 2020
    obj.filepath_iam_files = "fixture-image.csv"
    return obj


def data_for(names):
    return xr.DataArray(
        np.arange(1, 2 * len(names) + 1, dtype=float).reshape(1, len(names), 2),
        dims=("region", "variables", "year"),
        coords={"region": ["EUR"], "variables": names, "year": [2020, 2050]},
        attrs={"unit": {n: "EJ/yr" if "Energy" in n else "Mt CO2/yr" for n in names}},
    )


def consume(obj, data, raw, reported=()):
    production, energy, indices = obj._IAMDataCollection__resolve_cdr_alias_fallbacks(
        data, *raw, reported_missing_variables=reported
    )
    return (
        obj._IAMDataCollection__fetch_market_data(
            data, production, reported_missing_variables=reported
        ),
        obj._IAMDataCollection__fetch_market_data(
            data,
            obj._IAMDataCollection__flatten_cdr_energy_variable_labels(energy),
            normalize=False,
            reported_missing_variables=reported,
        ),
        obj._IAMDataCollection__get_iam_production_volumes(
            production, data, reported_missing_variables=reported
        ),
        obj.get_iam_efficiencies_by_carrier(
            data,
            production,
            obj._IAMDataCollection__group_cdr_energy_variable_labels(energy),
            obj._IAMDataCollection__group_cdr_energy_variable_labels(indices),
            reported_missing_variables=reported,
        ),
    )


def assert_same_outputs(before, after):
    for left, right in zip(before, after):
        if left is None:
            assert right is None
        else:
            xr.testing.assert_identical(left, right)


def test_reviewed_image_gaps_report_once_and_preserve_all_cdr_arrays(capsys):
    obj = iam()
    production = obj._IAMDataCollection__get_iam_variable_labels(
        IAM_CDR_VARS, "iam_aliases"
    )
    energy = obj._IAMDataCollection__get_cdr_energy_variable_labels(IAM_CDR_VARS)
    indices = obj._IAMDataCollection__get_cdr_energy_variable_labels(
        IAM_CDR_VARS, "efficiency_use_aliases"
    )
    raw = production, energy, indices
    specs = list(production.values()) + [
        spec
        for group in (energy, indices)
        for carriers in group.values()
        for spec in carriers.values()
    ]
    names = {
        name
        for spec in specs
        for name in (
            cdr_alias_candidates(spec)[-1:]
            if isinstance(spec, dict)
            else cdr_alias_candidates(spec)
        )
    } - MISSING_IMAGE_LABELS
    names.update(
        {
            "Final Energy|Carbon Management|Direct Air Capture",
            "Secondary Energy|Heat|Biomass",
            "Carbon Capture|Energy|Demand|Industry|Biomass",
        }
    )
    data = data_for(sorted(names))
    original, original_raw = data.copy(deep=True), deepcopy(raw)
    baseline = consume(obj, data, raw)
    assert capsys.readouterr().out.count("The following variables are missing") == 3

    reported = obj._IAMDataCollection__report_cdr_data_coverage(data, *raw)
    assert reported == MISSING_IMAGE_LABELS
    output = capsys.readouterr().out
    assert output.count("CDR data coverage") == 1
    assert "does not establish that the model lacks the technology" in output
    for name in MISSING_IMAGE_LABELS:
        assert output.count(f"Missing: {name}\n") == 1
    gaps = obj.cdr_data_coverage
    assert len(gaps) == 7
    assert sum(len(g["requirements"]) for g in gaps) == 8
    assert sum(g["status"] == "insufficient technology detail" for g in gaps) == 4
    assert sum(g["status"] == "missing export" for g in gaps) == 3
    plantation = next(g for g in gaps if "Plantation" in g["missing_variables"][0])
    assert {r["technology"] for r in plantation["requirements"]} == {
        "afforestation, eucalyptus plantation",
        "afforestation, poplar plantation",
    }
    assert_same_outputs(baseline, consume(obj, data, raw, reported))
    assert capsys.readouterr().out == ""
    assert raw == original_raw
    xr.testing.assert_identical(data, original)


@pytest.mark.parametrize("value", [0.0, np.nan])
def test_named_zero_or_nan_series_is_covered_and_does_not_warn(value, capsys):
    obj = iam()
    data = data_for(["present"])
    data[:] = value
    raw = {"technology": "present"}, {}, {}
    assert obj._IAMDataCollection__report_cdr_data_coverage(data, *raw) == set()
    assert obj.cdr_data_coverage == []
    assert capsys.readouterr().out == ""


def test_partial_additive_gaps_preserve_sums_and_efficiency_defaults(capsys):
    obj = iam()
    data = data_for(["present", "energy"])
    raw = (
        {"technology": ["present", "missing component"]},
        {"technology": {"electricity": "energy"}},
        {},
    )
    baseline = consume(obj, data, raw)
    capsys.readouterr()
    reported = obj._IAMDataCollection__report_cdr_data_coverage(data, *raw)
    (gap,) = obj.cdr_data_coverage
    assert gap["status"] == "missing additive components"
    assert gap["available_components"] == ["present"]
    assert reported == {"missing component"}
    assert "Available additive component: present" in capsys.readouterr().out
    output = consume(obj, data, raw, reported)
    assert_same_outputs(baseline, output)
    np.testing.assert_array_equal(output[2].values, [[[1.0, 2.0]]])
    np.testing.assert_array_equal(output[3].values, np.ones_like(output[3].values))
    assert capsys.readouterr().out == ""

    # A combined volume read must still report missing non-CDR quantities.
    obj._IAMDataCollection__get_iam_production_volumes(
        {**raw[0], "other sector": "unrelated missing quantity"},
        data,
        reported_missing_variables=reported,
    )
    diagnostic = capsys.readouterr().out
    assert "unrelated missing quantity" in diagnostic
    assert "missing component" not in diagnostic


def test_missing_fallback_group_and_index_warn_once_and_stay_unavailable(capsys):
    obj = iam()
    data = data_for(["other"])
    raw = (
        {"technology": {"fallback": ["preferred", "legacy"]}},
        {},
        {"technology": {"heat": "missing index"}},
    )
    reported = obj._IAMDataCollection__report_cdr_data_coverage(data, *raw)
    assert reported == {"preferred", "legacy", "missing index"}
    output = capsys.readouterr().out
    assert output.count("Missing: ") == 3
    assert output.count("None of the alternative names is present.") == 1
    assert consume(obj, data, raw, reported) == (None, None, None, None)
    assert capsys.readouterr().out == ""


def test_related_context_only_appears_when_available_in_supplied_file(capsys):
    obj = iam()
    obj.iam_source_file = "/actual/scenario.csv"
    technology = "cement production, non-fossil CO2, with CCS"
    raw = {technology: "missing cement quantity"}, {}, {}
    data = data_for(["other"])
    obj._IAMDataCollection__report_cdr_data_coverage(data, *raw)
    (gap,) = obj.cdr_data_coverage
    assert gap["status"] == "missing export"
    assert gap["related_variables"] == []
    output = capsys.readouterr().out
    assert "/actual/scenario.csv" in output
    assert "Related series (not substituted):" not in output
