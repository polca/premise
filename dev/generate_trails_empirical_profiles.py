"""Generate reviewed TRAILS pilot profiles from Eurostat and EIA source files.

Example::

    conda run -n premise python dev/generate_trails_empirical_profiles.py \
        --eurostat-directory /path/to/eurostat-json \
        --eia-workbook /path/to/3_1_Generator_Y2025.xlsx \
        --output premise/data/trails/temporal/profiles.yaml

Expected Eurostat filenames are ``road_eqs_{carage,busage,lorroa}_2024.json``.
The script intentionally does not download mutable upstream data.  Callers retain
the exact source snapshots used to produce a reviewed registry revision.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import yaml

from premise.temporal_evidence import (
    eia_operating_fleet_profiles,
    eia_retirement_age_profiles,
    eurostat_age_band_profile,
)

EUROSTAT_FILES = {
    "eu-road-passenger-car-age-2024": (
        "road_eqs_carage_2024.json",
        None,
        "eurostat-road-eqs-carage-2024",
    ),
    "eu-road-bus-age-2024": (
        "road_eqs_busage_2024.json",
        None,
        "eurostat-road-eqs-busage-2024",
    ),
    "eu-road-lorry-over-3.5t-age-2024": (
        "road_eqs_lorroa_2024.json",
        "LOR_GT3P5",
        "eurostat-road-eqs-lorroa-2024",
    ),
    "eu-road-tractor-age-2024": (
        "road_eqs_lorroa_2024.json",
        "TRC",
        "eurostat-road-eqs-lorroa-2024",
    ),
    "eu-road-light-commercial-age-2024": (
        "road_eqs_lorroa_2024.json",
        "VG_LE3P5",
        "eurostat-road-eqs-lorroa-2024",
    ),
}

EIA_TECHNOLOGIES = (
    "Batteries",
    "Conventional Hydroelectric",
    "Conventional Steam Coal",
    "Geothermal",
    "Hydroelectric Pumped Storage",
    "Natural Gas Fired Combined Cycle",
    "Natural Gas Fired Combustion Turbine",
    "Nuclear",
    "Offshore Wind Turbine",
    "Onshore Wind Turbine",
    "Solar Photovoltaic",
    "Wood/Wood Waste Biomass",
)
EIA_CONSTRUCTION_TIMING_PILOTS = {
    "Conventional Hydroelectric": "eia-us-conventional-hydropower-construction-vintage-2025",
    "Hydroelectric Pumped Storage": "eia-us-pumped-storage-construction-vintage-2025",
    "Nuclear": "eia-us-nuclear-construction-vintage-2025",
    "Onshore Wind Turbine": "eia-us-onshore-wind-construction-vintage-2025",
    "Solar Photovoltaic": "eia-us-solar-photovoltaic-construction-vintage-2025",
}

TECHNICAL_LIVES = {
    "nrel-wind-technical-life-30-years": {
        "years": 30,
        "technology": "land-based and offshore wind power plants",
        "activation_status": "active",
        "rationale": "A stable electricity-production selector was audited across ecoinvent 3.8-3.12.",
    },
    "nrel-solar-pv-technical-life-30-years": {
        "years": 30,
        "technology": "utility-scale and distributed solar photovoltaic systems",
        "activation_status": "active",
        "rationale": "A stable photovoltaic electricity-production selector was audited across ecoinvent 3.8-3.12.",
    },
    "nrel-hydropower-technical-life-100-years": {
        "years": 100,
        "technology": "conventional and pumped-storage hydropower projects",
        "activation_status": "active",
        "rationale": "A stable hydroelectricity-production selector was audited across ecoinvent 3.8-3.12.",
    },
    "nrel-nuclear-technical-life-60-years": {
        "years": 60,
        "technology": "nuclear power plants",
        "activation_status": "active",
        "rationale": "A stable nuclear electricity-production selector was audited across ecoinvent 3.8-3.12.",
    },
    "nrel-stationary-battery-technical-life-15-years": {
        "years": 15,
        "technology": "utility-scale, commercial, and residential stationary battery storage",
        "activation_status": "eligible",
        "rationale": "No sufficiently specific cross-version activity selector has yet been established.",
    },
}

EU_LDV_SURVIVAL = {
    "eu-passenger-car-survival-uk-mot": [
        0.96,
        0.96,
        0.95,
        0.94,
        0.94,
        0.92,
        0.92,
        0.90,
        0.87,
        0.83,
        0.78,
        0.70,
        0.59,
        0.48,
        0.27,
        0.20,
        0.12,
        0.06,
        0.04,
    ],
    "eu-light-commercial-vehicle-survival-uk-mot": [
        0.96,
        0.94,
        0.93,
        0.90,
        0.88,
        0.86,
        0.83,
        0.80,
        0.74,
        0.70,
        0.65,
        0.58,
        0.47,
        0.34,
        0.26,
        0.20,
        0.12,
        0.06,
        0.04,
    ],
}


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _round_weights(values: list[float]) -> list[float]:
    rounded = [round(float(value), 12) for value in values]
    rounded[-1] += 1.0 - sum(rounded)
    return rounded


def eurostat_profiles(directory: Path) -> dict[str, dict]:
    profiles = {}
    for profile_id, (filename, vehicle, source_id) in EUROSTAT_FILES.items():
        with (directory / filename).open(encoding="utf-8") as stream:
            result = eurostat_age_band_profile(json.load(stream), vehicle=vehicle)
        bands = []
        for band in result["bands"]:
            bands.append(
                {
                    "source_code": band["code"],
                    "minimum_years": band["minimum_years"],
                    "maximum_years": band["maximum_years"],
                    "weight": round(band["weight"], 12),
                }
            )
        bands[-1]["weight"] += 1.0 - sum(band["weight"] for band in bands)
        profiles[profile_id] = {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "derived",
                "generator": "uniform_within_reported_age_bands",
                "parameters": {"bands": bands},
            },
            "applicability": {
                "geographies": ["Europe"],
                "reference_year": 2024,
            },
            "assumptions": [
                "Aggregate only countries reporting every age band, so every band has identical geographical coverage.",
                "The source's open-ended over-20-years band is retained; no unsupported upper age or annual allocation is introduced.",
                "Vehicle counts represent registered stock, not utilisation-weighted service.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [source_id],
                "population": "Registered end-of-year vehicle stock in EU27 countries reporting every requested age band.",
                "weighting": "Vehicle count.",
                "reference_period": "2024-12-31",
                "censoring": "The oldest age band is open-ended above 20 years.",
                "sample_size": {
                    "count": result["observations"],
                    "unit": "registered vehicles",
                },
                "quality_flags": [
                    "complete-reporting-country-subset",
                    "open-ended-oldest-age-band",
                ],
                "coverage": {
                    "countries": result["countries"],
                    "country_count": result["country_count"],
                    "vehicle_count": result["observations"],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Valid fleet-age evidence, but no reviewed geography-specific caller mapping is active.",
            },
        }
    return profiles


def eia_profiles(workbook: Path) -> dict[str, dict]:
    operating = pd.read_excel(workbook, sheet_name="Operable", header=1)
    retired = pd.read_excel(workbook, sheet_name="Retired and Canceled", header=1)
    fleet = eia_operating_fleet_profiles(
        operating, reference_year=2025, technologies=EIA_TECHNOLOGIES
    )
    retirement = eia_retirement_age_profiles(
        retired, technologies=EIA_TECHNOLOGIES, earliest_retirement_year=2002
    )
    profiles = {}
    for technology, result in fleet.items():
        profile_id = f"us-electricity-{slug(technology)}-age-2025"
        profiles[profile_id] = {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": result["offsets_years"],
                "weights": _round_weights(result["weights"]),
            },
            "applicability": {
                "geographies": ["US"],
                "reference_year": 2025,
                "technology": technology,
            },
            "assumptions": [
                "Includes EIA generators with operating status OP, a valid operating year, and positive reported summer capacity.",
                "Fleet ages are weighted by net summer capacity, not annual generation or utilisation.",
                "Operating age is reference year minus operating year; operating month is not used.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["eia-860-generator-2025"],
                "population": "EIA generators with status OP, a valid operating year, and positive summer capacity.",
                "weighting": "Net summer generating capacity in MW.",
                "reference_period": "2025",
                "censoring": "Operating units are right-censored with respect to eventual retirement.",
                "sample_size": {
                    "count": result["unit_count"],
                    "unit": "generators",
                    "minimum_for_generalisation": 30,
                },
                "quality_flags": [
                    "capacity-weighted",
                    *(["small-sample"] if result["unit_count"] < 30 else []),
                    "surviving-fleet",
                ],
                "coverage": {
                    "technology": technology,
                    "unit_count": result["unit_count"],
                    "summer_capacity_mw": round(result["weight_total_mw"], 6),
                    "mean_age_years": round(result["mean_age_years"], 6),
                    "minimum_age_years": result["minimum_age_years"],
                    "maximum_age_years": result["maximum_age_years"],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Valid U.S. fleet-age evidence; it is not a service-life distribution and lacks reviewed ecoinvent geography mappings.",
            },
        }
        if technology in EIA_CONSTRUCTION_TIMING_PILOTS:
            construction_profile_id = EIA_CONSTRUCTION_TIMING_PILOTS[technology]
            profiles[construction_profile_id] = {
                "quantity": "construction_timing",
                "reference_event": "current_service",
                "distribution": {
                    "kind": "discrete",
                    "offsets_years": result["offsets_years"],
                    "weights": _round_weights(result["weights"]),
                },
                "applicability": {
                    "evidence_geographies": ["US"],
                    "reference_year": 2025,
                    "technology": technology,
                    "proxy_policy": "Use outside the United States only when no higher-precedence geography-specific profile matches.",
                },
                "assumptions": [
                    "The operating-fleet age distribution is used as the construction-vintage distribution of infrastructure serving current electricity production.",
                    "The profile is direct evidence for U.S. callers and an explicit fallback proxy elsewhere.",
                    "Fleet ages are weighted by net summer capacity, not annual generation or utilisation.",
                    "The profile describes existing fleet composition and must not be interpreted as prospective service life.",
                ],
                "evidence": {
                    "status": "empirical",
                    "source_ids": ["eia-860-generator-2025"],
                    "population": "EIA generators with status OP, a valid operating year, and positive summer capacity.",
                    "weighting": "Net summer generating capacity in MW.",
                    "reference_period": "2025",
                    "censoring": "Operating units are right-censored with respect to eventual retirement.",
                    "sample_size": {
                        "count": result["unit_count"],
                        "unit": "generators",
                        "minimum_for_generalisation": 30,
                    },
                    "quality_flags": [
                        "capacity-weighted",
                        *(["small-sample"] if result["unit_count"] < 30 else []),
                        "surviving-fleet",
                        "country-proxy-fallback",
                    ],
                    "coverage": {
                        "technology": technology,
                        "unit_count": result["unit_count"],
                        "summer_capacity_mw": round(result["weight_total_mw"], 6),
                        "mean_age_years": round(result["mean_age_years"], 6),
                        "minimum_age_years": result["minimum_age_years"],
                        "maximum_age_years": result["maximum_age_years"],
                        "derived_from_profile": profile_id,
                    },
                },
                "review": {
                    "activation_status": "active",
                    "rationale": "Caller-and-exchange selectors were audited across ecoinvent 3.8-3.12; the proxy tier is overridden by any direct selector.",
                },
            }
    for technology, result in retirement.items():
        profile_id = f"us-electricity-{slug(technology)}-retirement-age-2002-2025"
        profiles[profile_id] = {
            "quantity": "retirement_timing",
            "reference_event": "asset_commissioning",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-offset for offset in result["offsets_years"]],
                "weights": _round_weights(result["weights"]),
            },
            "applicability": {
                "geographies": ["US"],
                "observation_years": [2002, 2025],
                "technology": technology,
            },
            "assumptions": [
                "Observed retirement ages are not an uncensored survival curve and must not be treated as one without a cohort model.",
                "Includes retired generators from 2002 onward with valid dates and positive reported summer capacity.",
                "Retirement ages are weighted by reported summer capacity.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["eia-860-generator-2025"],
                "population": "EIA generators reported retired from 2002 through 2025 with valid dates and positive summer capacity.",
                "weighting": "Reported summer generating capacity in MW.",
                "reference_period": "2002-2025",
                "censoring": "Conditioned on observed retirement; surviving and pre-2002 retirements are excluded.",
                "sample_size": {
                    "count": result["unit_count"],
                    "unit": "retired generators",
                    "minimum_for_generalisation": 30,
                },
                "quality_flags": [
                    "capacity-weighted",
                    "retirement-conditioned",
                    *(["small-sample"] if result["unit_count"] < 30 else []),
                ],
                "coverage": {
                    "technology": technology,
                    "unit_count": result["unit_count"],
                    "summer_capacity_mw": round(result["weight_total_mw"], 6),
                    "mean_retirement_age_years": round(result["mean_age_years"], 6),
                    "minimum_retirement_age_years": result["minimum_age_years"],
                    "maximum_retirement_age_years": result["maximum_age_years"],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Observed retirement timing is descriptive and must not be used as prospective service life without a cohort survival model.",
            },
        }
    return profiles


def vehicle_survival_profiles() -> dict[str, dict]:
    """Return the right-censored light-duty-vehicle survival evidence."""

    profiles = {}
    for profile_id, survival in EU_LDV_SURVIVAL.items():
        vehicle = (
            "passenger car"
            if "passenger-car" in profile_id
            else "light commercial vehicle"
        )
        profiles[profile_id] = {
            "quantity": "service_life",
            "reference_event": "asset_commissioning",
            "distribution": {
                "kind": "derived",
                "generator": "right_censored_survival_curve",
                "parameters": {
                    "ages_years": list(range(1, 20)),
                    "survival_probabilities": survival,
                    "right_censored_after_years": 19,
                },
            },
            "applicability": {
                "geographies": ["Europe"],
                "vehicle": vehicle,
            },
            "assumptions": [
                "The underlying UK MOT inspection population is used by the source as a proxy for the wider European fleet.",
                "Four percent of vehicles remain at the final reported age; the tail is right-censored and no unsupported retirement age is assigned.",
                "The source does not provide powertrain-specific survival curves.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["ec-light-duty-vehicle-survival-2015"],
                "population": "United Kingdom vehicles represented in MOT periodic-inspection records.",
                "weighting": "Source-reported vehicle survival share by age.",
                "reference_period": "Source evaluation published 2015; underlying observation period not reported in the extracted table.",
                "censoring": "Right-censored after age 19 with four percent surviving.",
                "sample_size": {
                    "not_reported": "The source table does not report the underlying vehicle count.",
                    "unit": "vehicles",
                },
                "quality_flags": ["geographic-proxy", "right-censored"],
                "coverage": {
                    "source_population": "United Kingdom MOT periodic inspection records",
                    "reported_age_years": [1, 19],
                    "last_reported_survival_probability": survival[-1],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "The curve is a UK proxy and cannot produce an uncensored mean lifetime without a reviewed tail model.",
            },
        }
    return profiles


def reviewed_operational_profiles() -> dict[str, dict]:
    """Return behavior-preserving profiles reviewed for runtime activation."""

    return {
        "annual-cereal-preharvest-uptake-one-year": {
            "quantity": "biomass_growth",
            "reference_event": "harvest",
            "distribution": {
                "kind": "uniform",
                "minimum_years": -1,
                "maximum_years": 0,
            },
            "applicability": {
                "crop_type": "annual cereal",
            },
            "assumptions": [
                "Biogenic carbon uptake is allocated uniformly across the year immediately preceding the current harvest.",
                "The profile changes timing metadata only and preserves the existing carbon uptake amount.",
                "This reviewed selector is behavior-equivalent to the resolved legacy mappings it replaces.",
            ],
            "evidence": {
                "status": "assumption",
                "source_ids": [],
                "coverage": {
                    "review_basis": "Resolved TRAILS mappings for annual cereal production",
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Behavior-preserving replacement for reviewed legacy cereal mappings.",
            },
        }
    }


def technical_life_profiles() -> dict[str, dict]:
    """Return source-backed technical-life assumptions used for caller context."""

    profiles = {}
    for profile_id, specification in TECHNICAL_LIVES.items():
        years = specification["years"]
        technology = specification["technology"]
        profiles[profile_id] = {
            "quantity": "service_life",
            "reference_event": "asset_commissioning",
            "service_life_years": years,
            "distribution": {
                "kind": "fixed",
                "offset_years": years,
            },
            "applicability": {"technology": technology},
            "assumptions": [
                "The NREL ATB technical life is used as prospective caller context, not as an observed fleet age.",
                "Component replacements and refurbishment within the project life are not represented by this fixed value.",
                "The profile does not define a construction-vintage distribution for stock-asset exchanges.",
            ],
            "evidence": {
                "status": "literature",
                "source_ids": ["nrel-atb-2024b-technical-lifetimes"],
                "coverage": {
                    "technology": technology,
                    "technical_life_years": years,
                },
            },
            "review": {
                "activation_status": specification["activation_status"],
                "rationale": specification["rationale"],
            },
        }
    return profiles


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eurostat-directory", type=Path, required=True)
    parser.add_argument("--eia-workbook", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    profiles = reviewed_operational_profiles()
    profiles.update(technical_life_profiles())
    profiles.update(vehicle_survival_profiles())
    profiles.update(eurostat_profiles(args.eurostat_directory))
    profiles.update(eia_profiles(args.eia_workbook))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(
            {"schema_version": 1, "profiles": profiles},
            stream,
            sort_keys=False,
            allow_unicode=True,
            width=100,
        )


if __name__ == "__main__":
    main()
