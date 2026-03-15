"""Reusable IAM scenario summary helpers shared by reporting and the UI."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .filesystem_constants import DATA_DIR, VARIABLES_DIR

IAM_ELEC_VARS = VARIABLES_DIR / "electricity.yaml"
IAM_FUELS_VARS = VARIABLES_DIR / "fuels.yaml"
IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass.yaml"
IAM_CEMENT_VARS = VARIABLES_DIR / "cement.yaml"
IAM_STEEL_VARS = VARIABLES_DIR / "steel.yaml"
IAM_CDR_VARS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"
IAM_HEATING_VARS = VARIABLES_DIR / "heat.yaml"
IAM_TRSPT_TWO_WHEELERS_VARS = VARIABLES_DIR / "transport_two_wheelers.yaml"
IAM_TRSPT_CARS_VARS = VARIABLES_DIR / "transport_passenger_cars.yaml"
IAM_TRSPT_BUSES_VARS = VARIABLES_DIR / "transport_bus.yaml"
IAM_TRSPT_TRUCKS_VARS = VARIABLES_DIR / "transport_road_freight.yaml"
IAM_TRSPT_TRAINS_VARS = VARIABLES_DIR / "transport_rail_freight.yaml"
IAM_TRSPT_SHIPS_VARS = VARIABLES_DIR / "transport_sea_freight.yaml"
IAM_OTHER_VARS = VARIABLES_DIR / "other.yaml"
REPORT_METADATA_FILEPATH = DATA_DIR / "utils" / "report" / "report.yaml"

BATTERY_SECTORS = {"Battery (mobile)", "Battery (stationary)"}
WORLD_ONLY_SECTORS = {"GMST", "CO2"}

SECTOR_SPECS = {
    "Population": {
        "filepath": IAM_OTHER_VARS,
        "variables": ["population"],
        "source_attr": "other_vars",
    },
    "GDP": {
        "filepath": IAM_OTHER_VARS,
        "variables": ["gdp"],
        "source_attr": "other_vars",
    },
    "CO2": {
        "filepath": IAM_OTHER_VARS,
        "variables": ["CO2"],
        "source_attr": "other_vars",
    },
    "GMST": {
        "filepath": IAM_OTHER_VARS,
        "variables": ["GMST"],
        "source_attr": "other_vars",
    },
    "Electricity - generation": {
        "filepath": IAM_ELEC_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "electricity_mix",
    },
    "Electricity (biom) - generation": {
        "filepath": IAM_BIOMASS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "biomass_mix",
    },
    "Electricity - efficiency": {
        "filepath": IAM_ELEC_VARS,
        "source_attr": "electricity_technology_efficiencies",
    },
    "Heat (buildings) - generation": {
        "filepath": IAM_HEATING_VARS,
        "filter": ["heat, buildings"],
        "source_attr": "production_volumes",
    },
    "Heat (industrial) - generation": {
        "filepath": IAM_HEATING_VARS,
        "filter": ["heat, industrial"],
        "source_attr": "production_volumes",
    },
    "Fuel (gasoline) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["gasoline", "ethanol", "bioethanol", "methanol"],
        "source_attr": "production_volumes",
    },
    "Fuel (gasoline) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["gasoline", "ethanol", "bioethanol", "methanol"],
        "source_attr": "petrol_technology_efficiencies",
    },
    "Fuel (diesel) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["diesel", "biodiesel"],
        "source_attr": "production_volumes",
    },
    "Fuel (diesel) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["diesel", "biodiesel"],
        "source_attr": "diesel_technology_efficiencies",
    },
    "Fuel (gas) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["natural gas", "biogas", "methane", "biomethane"],
        "source_attr": "production_volumes",
    },
    "Fuel (gas) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["natural gas", "biogas", "methane", "biomethane"],
        "source_attr": "gas_technology_efficiencies",
    },
    "Fuel (hydrogen) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["hydrogen"],
        "source_attr": "production_volumes",
    },
    "Fuel (hydrogen) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["hydrogen"],
        "source_attr": "hydrogen_technology_efficiencies",
    },
    "Fuel (kerosene) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["kerosene"],
        "source_attr": "production_volumes",
    },
    "Fuel (kerosene) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["kerosene"],
        "source_attr": "kerosene_technology_efficiencies",
    },
    "Fuel (LPG) - generation": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["liquefied petroleum gas"],
        "source_attr": "production_volumes",
    },
    "Fuel (LPG) - efficiency": {
        "filepath": IAM_FUELS_VARS,
        "filter": ["liquefied petroleum gas"],
        "source_attr": "lpg_technology_efficiencies",
    },
    "Cement - generation": {
        "filepath": IAM_CEMENT_VARS,
        "source_attr": "production_volumes",
    },
    "Cement - efficiency": {
        "filepath": IAM_CEMENT_VARS,
        "source_attr": "cement_technology_efficiencies",
    },
    "Steel - generation": {
        "filepath": IAM_STEEL_VARS,
        "source_attr": "production_volumes",
    },
    "Steel - efficiency": {
        "filepath": IAM_STEEL_VARS,
        "source_attr": "steel_technology_efficiencies",
    },
    "CDR - generation": {
        "filepath": IAM_CDR_VARS,
        "source_attr": "production_volumes",
    },
    "Direct Air Capture - energy mix": {
        "filepath": IAM_HEATING_VARS,
        "variables": [
            "energy, for DACCS, from hydrogen turbine",
            "energy, for DACCS, from gas boiler",
            "energy, for DACCS, from other",
            "energy, for DACCS, from electricity",
        ],
        "source_attr": "daccs_energy_use",
    },
    "Direct Air Capture - heat eff.": {
        "filepath": IAM_CDR_VARS,
        "variables": ["dac_solvent"],
        "source_attr": "dac_heat_efficiencies",
    },
    "Direct Air Capture - elec eff.": {
        "filepath": IAM_CDR_VARS,
        "variables": ["dac_solvent"],
        "source_attr": "dac_electricity_efficiencies",
    },
    "Transport (two-wheelers)": {
        "filepath": IAM_TRSPT_TWO_WHEELERS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "two_wheelers_fleet",
    },
    "Transport (two-wheelers) - eff": {
        "filepath": IAM_TRSPT_TWO_WHEELERS_VARS,
        "source_attr": "two_wheelers_efficiencies",
    },
    "Transport (cars)": {
        "filepath": IAM_TRSPT_CARS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "passenger_car_fleet",
    },
    "Transport (cars) - eff": {
        "filepath": IAM_TRSPT_CARS_VARS,
        "source_attr": "passenger_car_efficiencies",
    },
    "Transport (buses)": {
        "filepath": IAM_TRSPT_BUSES_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "bus_fleet",
    },
    "Transport (buses) - eff": {
        "filepath": IAM_TRSPT_BUSES_VARS,
        "source_attr": "bus_efficiencies",
    },
    "Transport (trucks)": {
        "filepath": IAM_TRSPT_TRUCKS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "road_freight_fleet",
    },
    "Transport (trucks) - eff": {
        "filepath": IAM_TRSPT_TRUCKS_VARS,
        "source_attr": "road_freight_efficiencies",
    },
    "Transport (trains)": {
        "filepath": IAM_TRSPT_TRAINS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "rail_freight_fleet",
    },
    "Transport (trains) - eff": {
        "filepath": IAM_TRSPT_TRAINS_VARS,
        "source_attr": "rail_freight_efficiencies",
    },
    "Transport (ships)": {
        "filepath": IAM_TRSPT_SHIPS_VARS,
        "source_attr": "production_volumes",
        "availability_attr": "sea_freight_fleet",
    },
    "Transport (ships) - eff": {
        "filepath": IAM_TRSPT_SHIPS_VARS,
        "source_attr": "sea_freight_efficiencies",
    },
    "Battery (mobile)": {
        "variables": [
            "NMC111",
            "NMC532",
            "NMC622",
            "NMC811",
            "NMC900",
            "NMC900-Si",
            "LFP",
            "NCA",
            "LSB",
            "SIB",
            "LAB",
            "ASSB (oxidic)",
            "ASSB (polymer)",
            "ASSB (sulfidic)",
        ],
        "source_attr": "battery_mobile_scenarios",
        "rename_dims": {"chemistry": "variables"},
        "group_by": "subscenario",
    },
    "Battery (stationary)": {
        "variables": [
            "NMC111",
            "NMC622",
            "NMC811",
            "LFP",
            "LEAD-ACID",
            "VRFB",
            "NAS",
        ],
        "source_attr": "battery_stationary_scenarios",
        "rename_dims": {"chemistry": "variables"},
        "group_by": "subscenario",
    },
}


def load_report_metadata() -> dict[str, dict[str, Any]]:
    """Load workbook/display metadata for IAM summary sectors."""

    with open(REPORT_METADATA_FILEPATH, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def get_variables(filepath: Path | str) -> list[str]:
    """Return the variable keys from an IAM mapping YAML file."""

    with open(filepath, encoding="utf-8") as stream:
        out = yaml.safe_load(stream)

    return list(out.keys())


def get_sector_specs() -> dict[str, dict[str, Any]]:
    """Return a copy of the sector specification catalog."""

    return deepcopy(SECTOR_SPECS)


def get_sector_variables(sector: str) -> list[str]:
    """Return the configured IAM variables for a sector."""

    spec = SECTOR_SPECS[sector]
    if "variables" in spec:
        variables = list(spec["variables"])
    else:
        variables = get_variables(spec["filepath"])

    if "filter" in spec:
        variables = [
            variable
            for variable in variables
            if any(variable.startswith(prefix) for prefix in spec["filter"])
        ]

    return variables


def get_sector_catalog(
    metadata: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return the explorer/report sector catalog with display metadata."""

    metadata = metadata or load_report_metadata()
    catalog = []
    for sector in SECTOR_SPECS:
        sector_meta = metadata.get(sector, {})
        catalog.append(
            {
                "id": sector,
                "label": sector,
                "unit": sector_meta.get("label"),
                "explanation": sector_meta.get("expl_text", ""),
                "offset": sector_meta.get("offset", 2),
                "group_by": SECTOR_SPECS[sector].get("group_by", "region"),
                "variables": get_sector_variables(sector),
            }
        )
    return catalog


def fetch_data(iam_data: Any, sector: str, variables: list[str] | None = None):
    """Return the relevant IAM DataArray slice for a configured sector."""

    if sector not in SECTOR_SPECS:
        raise KeyError(f"Unknown summary sector: {sector}")

    spec = SECTOR_SPECS[sector]
    availability_attr = spec.get("availability_attr", spec["source_attr"])
    if not hasattr(iam_data, availability_attr):
        return None
    availability = getattr(iam_data, availability_attr)
    if availability is None or availability is False:
        return None
    if not hasattr(iam_data, spec["source_attr"]):
        return None

    sector_data = getattr(iam_data, spec["source_attr"])
    if sector_data is None:
        return None
    rename_dims = spec.get("rename_dims", {})
    if rename_dims:
        sector_data = sector_data.rename(rename_dims)

    requested_variables = variables or get_sector_variables(sector)
    if "variables" not in sector_data.coords:
        return sector_data

    matched_variables = _match_requested_labels(
        list(sector_data.coords["variables"].values),
        requested_variables,
    )
    return sector_data.sel(variables=matched_variables)


def summarize_sector_for_scenario(
    scenario: dict[str, Any],
    sector: str,
    *,
    metadata: dict[str, dict[str, Any]] | None = None,
    default_index: int | None = None,
) -> dict[str, Any] | None:
    """Build a structured summary for one scenario and one sector."""

    metadata = metadata or load_report_metadata()
    variables = get_sector_variables(sector)
    iam_da = fetch_data(scenario.get("iam data"), sector, variables)
    if iam_da is None:
        return None

    if "CCS" in sector:
        try:
            iam_da = iam_da * 100
        except Exception:
            pass

    sector_meta = metadata.get(sector, {})
    summary = {
        "scenario_id": _build_scenario_id(scenario, default_index),
        "model": scenario.get("model", ""),
        "pathway": scenario.get("pathway", ""),
        "label": sector_meta.get("label"),
        "group_by": SECTOR_SPECS[sector].get("group_by", "region"),
        "regions": [],
        "variables": [],
        "years": [],
        "groups": [],
    }

    if sector in BATTERY_SECTORS and "scenario" in iam_da.coords:
        for subscenario in iam_da.coords["scenario"].values:
            selector = {"scenario": subscenario}
            if "year" in iam_da.coords:
                selector["year"] = _allowed_years(iam_da.coords["year"].values)
            group = _summarize_group(
                iam_da.sel(**selector),
                group_name=subscenario,
                group_type="subscenario",
                requested_variables=variables,
                unit=sector_meta.get("label"),
            )
            if group is not None:
                summary["groups"].append(group)
    else:
        regions = (
            list(iam_da.coords["region"].values)
            if "region" in iam_da.coords
            else ["World"]
        )
        for region in regions:
            if sector in WORLD_ONLY_SECTORS and str(region) != "World":
                continue

            selector = {}
            if "region" in iam_da.coords:
                selector["region"] = region
            if "year" in iam_da.coords:
                selector["year"] = _allowed_years(iam_da.coords["year"].values)
            group = _summarize_group(
                iam_da.sel(**selector),
                group_name=region,
                group_type="region",
                requested_variables=variables,
                unit=sector_meta.get("label"),
            )
            if group is not None:
                summary["groups"].append(group)

    if not summary["groups"]:
        return None

    summary["regions"] = sorted(
        {
            group["name"]
            for group in summary["groups"]
            if group["group_type"] == "region"
        }
    )
    summary["subscenarios"] = sorted(
        {
            group["name"]
            for group in summary["groups"]
            if group["group_type"] == "subscenario"
        }
    )
    summary["variables"] = sorted(
        {
            series["variable"]
            for group in summary["groups"]
            for series in group["series"]
        }
    )
    summary["years"] = sorted(
        {
            point["year"]
            for group in summary["groups"]
            for series in group["series"]
            for point in series["points"]
        }
    )
    return summary


def summarize_sector(
    scenarios: list[dict[str, Any]],
    sector: str,
    *,
    metadata: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a structured summary for one sector across scenarios."""

    metadata = metadata or load_report_metadata()
    sector_meta = metadata.get(sector, {})
    summary = {
        "sector": sector,
        "label": sector_meta.get("label"),
        "explanation": sector_meta.get("expl_text", ""),
        "offset": sector_meta.get("offset", 2),
        "group_by": SECTOR_SPECS[sector].get("group_by", "region"),
        "regions": [],
        "subscenarios": [],
        "years": [],
        "variables": get_sector_variables(sector),
        "scenarios": [],
    }

    for index, scenario in enumerate(scenarios):
        scenario_summary = summarize_sector_for_scenario(
            scenario,
            sector,
            metadata=metadata,
            default_index=index,
        )
        if scenario_summary is not None:
            summary["scenarios"].append(scenario_summary)

    summary["regions"] = sorted(
        {
            group["name"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "region"
        }
    )
    summary["subscenarios"] = sorted(
        {
            group["name"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "subscenario"
        }
    )
    summary["years"] = sorted(
        {
            point["year"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
            for point in series["points"]
        }
    )
    summary["variables"] = sorted(
        {
            series["variable"]
            for scenario in summary["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
        }
    )

    return summary


def filter_sector_summary(
    summary: dict[str, Any],
    *,
    scenario_ids: list[str] | None = None,
    group_names: list[str] | None = None,
    regions: list[str] | None = None,
    variables: list[str] | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
) -> dict[str, Any]:
    """Filter a structured sector summary by scenario, region, variable, and year."""

    filtered = {
        key: deepcopy(value)
        for key, value in summary.items()
        if key not in {"scenarios", "years", "regions", "subscenarios", "variables"}
    }
    filtered["scenarios"] = []

    for scenario in summary.get("scenarios", []):
        if scenario_ids and scenario["scenario_id"] not in scenario_ids:
            continue

        filtered_scenario = {
            key: deepcopy(value)
            for key, value in scenario.items()
            if key not in {"groups", "years", "regions", "subscenarios", "variables"}
        }
        filtered_scenario["groups"] = []

        for group in scenario.get("groups", []):
            if group_names and group["name"] not in group_names:
                continue
            if (
                regions
                and group.get("group_type") == "region"
                and group["name"] not in regions
            ):
                continue

            filtered_group = {
                key: deepcopy(value)
                for key, value in group.items()
                if key not in {"series", "years", "variables"}
            }
            filtered_group["series"] = []

            for series in group.get("series", []):
                if variables and series["variable"] not in variables:
                    continue

                points = [
                    deepcopy(point)
                    for point in series.get("points", [])
                    if (year_start is None or point["year"] >= year_start)
                    and (year_end is None or point["year"] <= year_end)
                ]
                if not points:
                    continue

                filtered_series = deepcopy(series)
                filtered_series["points"] = points
                filtered_group["series"].append(filtered_series)

            if not filtered_group["series"]:
                continue

            filtered_group["variables"] = sorted(
                {series["variable"] for series in filtered_group["series"]}
            )
            filtered_group["years"] = sorted(
                {
                    point["year"]
                    for series in filtered_group["series"]
                    for point in series["points"]
                }
            )
            filtered_scenario["groups"].append(filtered_group)

        if not filtered_scenario["groups"]:
            continue

        filtered_scenario["regions"] = sorted(
            {
                group["name"]
                for group in filtered_scenario["groups"]
                if group["group_type"] == "region"
            }
        )
        filtered_scenario["subscenarios"] = sorted(
            {
                group["name"]
                for group in filtered_scenario["groups"]
                if group["group_type"] == "subscenario"
            }
        )
        filtered_scenario["variables"] = sorted(
            {
                series["variable"]
                for group in filtered_scenario["groups"]
                for series in group["series"]
            }
        )
        filtered_scenario["years"] = sorted(
            {
                point["year"]
                for group in filtered_scenario["groups"]
                for series in group["series"]
                for point in series["points"]
            }
        )
        filtered["scenarios"].append(filtered_scenario)

    filtered["regions"] = sorted(
        {
            group["name"]
            for scenario in filtered["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "region"
        }
    )
    filtered["subscenarios"] = sorted(
        {
            group["name"]
            for scenario in filtered["scenarios"]
            for group in scenario["groups"]
            if group["group_type"] == "subscenario"
        }
    )
    filtered["variables"] = sorted(
        {
            series["variable"]
            for scenario in filtered["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
        }
    )
    filtered["years"] = sorted(
        {
            point["year"]
            for scenario in filtered["scenarios"]
            for group in scenario["groups"]
            for series in group["series"]
            for point in series["points"]
        }
    )

    return filtered


def _allowed_years(years: list[Any]) -> list[Any]:
    allowed = []
    for year in years:
        year_value = _coerce_year(year)
        if year_value is not None and year_value <= 2100:
            allowed.append(year)
    return allowed


def _build_scenario_id(scenario: dict[str, Any], default_index: int | None) -> str:
    if scenario.get("scenario_id"):
        return str(scenario["scenario_id"])

    file_stem = None
    for key in ("filepath", "path", "file path"):
        if scenario.get(key):
            file_stem = Path(str(scenario[key])).stem
            break

    parts = [
        str(scenario.get("model", "scenario")),
        str(scenario.get("pathway", "pathway")),
    ]
    if file_stem:
        parts.append(file_stem)
    elif default_index is not None:
        parts.append(str(default_index))
    return "::".join(parts)


def _match_requested_labels(
    available: list[Any], requested: list[Any] | None
) -> list[Any]:
    if not requested:
        return list(available)

    normalized_available = {str(value).strip().lower(): value for value in available}
    matched = []
    for value in requested:
        key = str(value).strip().lower()
        if key in normalized_available:
            matched.append(normalized_available[key])
    return matched


def _summarize_group(
    data_array,
    *,
    group_name: Any,
    group_type: str,
    requested_variables: list[str],
    unit: str | None,
) -> dict[str, Any] | None:
    series = _extract_series(
        data_array, requested_variables=requested_variables, unit=unit
    )
    if not series:
        return None

    years = sorted({point["year"] for entry in series for point in entry["points"]})
    variables = sorted({entry["variable"] for entry in series})
    summary = {
        "name": str(group_name),
        "group_type": group_type,
        "variables": variables,
        "years": years,
        "series": series,
    }
    if group_type == "region":
        summary["region"] = str(group_name)
    if group_type == "subscenario":
        summary["subscenario"] = str(group_name)
    return summary


def _extract_series(
    data_array,
    *,
    requested_variables: list[str],
    unit: str | None,
) -> list[dict[str, Any]]:
    if data_array is None or "year" not in data_array.coords:
        return []

    data_array = data_array.squeeze(drop=True)
    var_dim = None
    if "variables" in data_array.coords:
        var_dim = "variables"
    elif "variable" in data_array.coords:
        var_dim = "variable"

    if var_dim is None:
        points = _extract_points(data_array)
        if not points:
            return []
        variable_name = str(
            data_array.name
            or data_array.attrs.get("variable")
            or data_array.attrs.get("variables")
            or "value"
        )
        return [{"variable": variable_name, "unit": unit, "points": points}]

    available_variables = list(data_array.coords[var_dim].values)
    matched_variables = _match_requested_labels(
        available_variables, requested_variables
    )
    if requested_variables and not matched_variables:
        return []
    if matched_variables:
        data_array = data_array.sel({var_dim: matched_variables})
        available_variables = matched_variables

    series = []
    for variable in available_variables:
        variable_slice = data_array.sel({var_dim: variable})
        points = _extract_points(variable_slice)
        if points:
            series.append({"variable": str(variable), "unit": unit, "points": points})
    return series


def _extract_points(data_array) -> list[dict[str, Any]]:
    points = []
    year_values = list(data_array.coords["year"].values)
    raw_values = list(data_array.values)
    for year, value in zip(year_values, raw_values):
        year_value = _coerce_year(year)
        point_value = _coerce_value(value)
        if year_value is None or point_value is None:
            continue
        points.append({"year": year_value, "value": point_value})
    return points


def _coerce_year(year: Any) -> int | None:
    try:
        if hasattr(year, "item"):
            year = year.item()
        return int(year)
    except Exception:
        return None


def _coerce_value(value: Any) -> float | int | None:
    try:
        if hasattr(value, "item"):
            value = value.item()
        value = float(value)
    except Exception:
        return None

    if value != value:
        return None
    if value.is_integer():
        return int(value)
    return value
