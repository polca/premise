"""Generate reviewed TRAILS pilot profiles from Eurostat and EIA source files.

Example::

    conda run -n premise python dev/generate_trails_empirical_profiles.py \
        --eurostat-directory /path/to/eurostat-json \
        --eurostat-aircraft-file /path/to/avia_eq_arc_age_2024.json \
        --eia-workbook /path/to/3_1_Generator_Y2025.xlsx \
        --moderate-building-workbook /path/to/EU27BuildingStock2020.xlsx \
        --gem-cement-capacity-csv /path/to/gcct-cement-capacity-by-age.csv \
        --gem-steel-assets-directory /path/to/gist-api-pages \
        --gem-coal-mine-assets-directory /path/to/gcmt-api-pages \
        --gem-gas-pipeline-assets-directory /path/to/ggit-api-pages \
        --gem-oil-pipeline-assets-directory /path/to/goit-api-pages \
        --gem-solar-workbook /path/to/Global-Solar-Power-Tracker-February-2026.xlsx \
        --gem-bioenergy-assets-directory /path/to/gbpt-api-pages \
        --gem-chemicals-json /path/to/chemicals-plants-2026.json \
        --gem-wind-geojson /path/to/gwpt_map_2026-02.geojson \
        --igu-lng-report-pdf /path/to/igu-world-lng-report-2026.pdf \
        --dft-vehicle-ods /path/to/veh1111.ods \
        --bnetza-chargers-csv /path/to/Ladesaeulenregister.csv \
        --eia-storage-workbook /path/to/3_4_Energy_Storage_Y2025.xlsx \
        --nbd-factory-workbook /path/to/NBD_Factory_Summary.xlsx \
        --nod-facilities-csv /path/to/nod-facility-fixed-2026.csv \
        --nod-history-csv /path/to/nod-facility-history-2026.csv \
        --output premise/data/trails/temporal/profiles.yaml

Expected road Eurostat filenames are
``road_eqs_{carage,busage,lorroa}_2024.json``.
The script intentionally does not download mutable upstream data.  Callers retain
the exact source snapshots used to produce a reviewed registry revision.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import yaml
from openpyxl import load_workbook

from dev.trails_gap_profiles import (
    bnetza_charger_profiles,
    eia_storage_profiles,
    nbd_factory_profiles,
    remaining_charger_profiles,
    sigeif_low_voltage_profiles,
)
from premise.temporal_evidence import (
    eia_operating_fleet_profiles,
    eia_retirement_age_profiles,
    eurostat_age_band_profile,
    eurostat_aircraft_age_band_profile,
    eurostat_aircraft_representative_age_profile,
    eurostat_representative_age_profile,
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

EUROSTAT_CONSTRUCTION_TIMING = {
    "eu-road-bus-age-2024": {
        "profile_id": "eu-road-bus-construction-vintage-2024",
        "vehicle": "motor coaches, buses, and trolley buses",
        "activation_status": "active",
        "rationale": "The passenger-bus transport inventories expose one stable whole-vehicle exchange per service dataset.",
    },
    "eu-road-lorry-over-3.5t-age-2024": {
        "profile_id": "eu-road-lorry-over-3.5t-construction-vintage-2024",
        "vehicle": "lorries over 3.5 tonnes",
        "activation_status": "active",
        "rationale": "The freight-lorry transport inventories expose one stable whole-vehicle exchange per service dataset.",
    },
    "eu-road-tractor-age-2024": {
        "profile_id": "eu-road-tractor-construction-vintage-2024",
        "vehicle": "road tractors",
        "activation_status": "eligible",
        "rationale": "The evidence is suitable, but no distinct road-tractor service-to-vehicle relationship is present in the installed inventories.",
    },
    "eu-road-light-commercial-age-2024": {
        "profile_id": "eu-road-light-commercial-construction-vintage-2024",
        "vehicle": "light commercial vehicles up to 3.5 tonnes",
        "activation_status": "active",
        "rationale": "The 3.5-tonne freight services expose one stable whole-vehicle exchange per service dataset.",
    },
}

EIA_TECHNOLOGIES = (
    "Batteries",
    "Conventional Hydroelectric",
    "Conventional Steam Coal",
    "Geothermal",
    "Hydroelectric Pumped Storage",
    "Natural Gas Fired Combined Cycle",
    "Natural Gas Fired Combustion Turbine",
    "Natural Gas Steam Turbine",
    "Nuclear",
    "Offshore Wind Turbine",
    "Onshore Wind Turbine",
    "Solar Photovoltaic",
    "Wood/Wood Waste Biomass",
)
EIA_CONSTRUCTION_TIMING_PILOTS = {
    "Conventional Steam Coal": "eia-us-steam-coal-construction-vintage-2025",
    "Conventional Hydroelectric": "eia-us-conventional-hydropower-construction-vintage-2025",
    "Geothermal": "eia-us-geothermal-construction-vintage-2025",
    "Hydroelectric Pumped Storage": "eia-us-pumped-storage-construction-vintage-2025",
    "Natural Gas Fired Combined Cycle": "eia-us-natural-gas-combined-cycle-construction-vintage-2025",
    "Natural Gas Fired Combustion Turbine": "eia-us-natural-gas-combustion-turbine-construction-vintage-2025",
    "Natural Gas Steam Turbine": "eia-us-natural-gas-steam-turbine-construction-vintage-2025",
    "Nuclear": "eia-us-nuclear-construction-vintage-2025",
    "Onshore Wind Turbine": "eia-us-onshore-wind-construction-vintage-2025",
    "Solar Photovoltaic": "eia-us-solar-photovoltaic-construction-vintage-2025",
    "Wood/Wood Waste Biomass": "eia-us-wood-biomass-construction-vintage-2025",
}
EIA_US_ONLY_CONSTRUCTION_TIMING = {"Wood/Wood Waste Biomass"}

UNCTAD_SHIP_DWT_SHARES_2024 = {
    "bulk-carrier": {
        "technology": "bulk carriers",
        "shares_percent_oldest_first": [8, 12, 39, 21, 20],
    },
    "container-ship": {
        "technology": "container ships",
        "shares_percent_oldest_first": [11, 22, 23, 22, 21],
    },
    "oil-tanker": {
        "technology": "oil tankers",
        "shares_percent_oldest_first": [14, 21, 27, 19, 19],
    },
    "other-ship-types": {
        "technology": "other ship types",
        "shares_percent_oldest_first": [30, 17, 17, 19, 17],
    },
}
UNCTAD_SHIP_REPRESENTATIVE_AGES_2024 = [30.214285714, 17, 12, 7, 2]

# Figure 12 of the CCNR 2024 annual market-observation report is a vector line
# chart. Integer vessel counts were recovered from the plotted annual points.
# The first chart category ("Until 1880") is zero for both relevant fleets;
# subsequent points cover 1882-2024. The source separately reports 135 dry-cargo
# vessels with an unknown construction year.
CCNR_RHINE_COMMISSIONING_YEARS_2024 = tuple(range(1882, 2025))
CCNR_RHINE_VESSEL_COUNTS_2024 = {
    "dry-cargo": (
        0,
        0,
        0,
        1,
        1,
        0,
        1,
        1,
        0,
        1,
        1,
        2,
        0,
        3,
        1,
        0,
        1,
        1,
        3,
        4,
        7,
        9,
        2,
        4,
        6,
        5,
        8,
        12,
        13,
        15,
        12,
        13,
        22,
        15,
        7,
        8,
        2,
        1,
        2,
        1,
        13,
        23,
        27,
        24,
        52,
        45,
        51,
        50,
        67,
        80,
        34,
        8,
        8,
        15,
        9,
        24,
        13,
        18,
        32,
        28,
        13,
        21,
        38,
        20,
        6,
        15,
        20,
        51,
        76,
        126,
        81,
        59,
        62,
        94,
        127,
        143,
        212,
        248,
        190,
        162,
        208,
        260,
        274,
        277,
        252,
        173,
        118,
        103,
        106,
        94,
        93,
        101,
        112,
        69,
        40,
        60,
        45,
        50,
        102,
        149,
        112,
        82,
        110,
        121,
        94,
        85,
        127,
        124,
        153,
        82,
        40,
        61,
        63,
        49,
        27,
        28,
        36,
        65,
        70,
        97,
        78,
        88,
        73,
        59,
        52,
        104,
        133,
        171,
        173,
        70,
        50,
        48,
        31,
        8,
        27,
        26,
        35,
        25,
        43,
        51,
        29,
        31,
        32,
    ),
    "tanker": (
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        2,
        0,
        1,
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        1,
        3,
        2,
        2,
        1,
        4,
        4,
        1,
        6,
        2,
        2,
        1,
        0,
        1,
        0,
        1,
        0,
        2,
        0,
        1,
        2,
        2,
        1,
        0,
        0,
        0,
        2,
        3,
        9,
        7,
        4,
        6,
        5,
        18,
        14,
        22,
        19,
        13,
        16,
        16,
        33,
        42,
        32,
        20,
        11,
        13,
        6,
        10,
        18,
        26,
        26,
        19,
        13,
        1,
        3,
        6,
        0,
        1,
        7,
        6,
        5,
        8,
        7,
        9,
        10,
        13,
        12,
        13,
        3,
        20,
        22,
        23,
        8,
        5,
        9,
        13,
        13,
        17,
        18,
        17,
        19,
        38,
        61,
        64,
        36,
        39,
        81,
        127,
        106,
        53,
        43,
        43,
        25,
        22,
        22,
        37,
        34,
        46,
        56,
        58,
        31,
        47,
    ),
}

# Figure 5 of the European Commission/Fraunhofer mapping study reports the
# installed-capacity share of the EU28 building-heating stock in three coarse
# age bands for reference year 2012. Values are rounded whole percentages read
# from the published chart. The open pre-1992 tail is preserved in the raw
# fleet-age profiles; the operational construction profiles make the separate,
# explicit representative-age assumption below.
EU28_HEATING_AGE_SHARES_2012 = {
    "natural-gas-boiler": {
        "technology": "natural-gas building boilers",
        "shares_percent_oldest_first": [22, 34, 44],
        "activation_status": "active",
    },
    "fuel-oil-boiler": {
        "technology": "fuel-oil building boilers",
        "shares_percent_oldest_first": [45, 30, 25],
        "activation_status": "active",
    },
    "coal-boiler-furnace": {
        "technology": "coal boilers and furnaces",
        "shares_percent_oldest_first": [58, 28, 14],
        "activation_status": "hold",
    },
    "biomass-furnace": {
        "technology": "biomass furnaces",
        "shares_percent_oldest_first": [31, 32, 37],
        "activation_status": "active",
    },
    "biomass-stove": {
        "technology": "biomass stoves",
        "shares_percent_oldest_first": [32, 32, 36],
        "activation_status": "active",
    },
    "aerial-heat-pump": {
        "technology": "aerial heat pumps",
        "shares_percent_oldest_first": [0, 1, 99],
        "activation_status": "active",
    },
    "ground-heat-pump": {
        "technology": "ground heat pumps",
        "shares_percent_oldest_first": [0, 8, 92],
        "activation_status": "active",
    },
}
EU28_HEATING_REPRESENTATIVE_AGES_2012 = [30, 15, 4.5]

# Prospective Weibull service-life assumptions in the 2026 European Commission
# market model. These are kept as evidence-only profiles and are not used to
# infer the age composition of the operating stock above.
EU_HEATING_SERVICE_LIVES_2026 = {
    "condensing-gas-boiler": {
        "technology": "condensing gas boilers",
        "shape": 2.5,
        "scale_years": 21.0,
        "mean_years": 18.6,
        "median_years": 18.1,
    },
    "non-condensing-gas-boiler": {
        "technology": "non-condensing gas boilers",
        "shape": 2.5,
        "scale_years": 20.3,
        "mean_years": 18.0,
        "median_years": 17.5,
    },
    "condensing-jet-burner-boiler": {
        "technology": "condensing oil or gas jet-burner boilers",
        "shape": 2.5,
        "scale_years": 28.0,
        "mean_years": 24.8,
        "median_years": 24.2,
    },
    "non-condensing-jet-burner-boiler": {
        "technology": "non-condensing oil or gas jet-burner boilers",
        "shape": 2.5,
        "scale_years": 26.0,
        "mean_years": 23.1,
        "median_years": 22.5,
    },
    "hydronic-heat-pump": {
        "technology": "hydronic ground, outside-air, and exhaust-air heat pumps",
        "shape": 1.84,
        "scale_years": 21.4,
        "mean_years": 19.0,
        "median_years": 17.5,
    },
}

MODERATE_BUILDING_AGES_2020 = [104.541803261, 63, 45.5, 35.5, 25.5, 15, 4.5]
MODERATE_BUILDING_PROFILES = {
    "moderate-eu-nonresidential-building-construction-vintage-2020": {
        "subsector": "Total",
        "technology": "non-residential buildings",
        "mapping": "generic hall and multi-storey building inputs",
    },
    "moderate-eu-hotel-restaurant-building-construction-vintage-2020": {
        "subsector": "Hotels and Restaurants",
        "technology": "hotels and restaurants",
        "mapping": "whole hotel and hostel inputs",
    },
}

GCCT_CEMENT_AGE_COLUMNS_2026 = ["50+", "40-49", "30-39", "20-29", "10-19", "0-9"]
GCCT_CEMENT_REPRESENTATIVE_AGES_2026 = [55, 44.5, 34.5, 24.5, 14.5, 4.5]
GCCT_EUROPE_COUNTRIES = {
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Iceland",
    "Ireland",
    "Italy",
    "Latvia",
    "Liechtenstein",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Norway",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
    "Switzerland",
    "United Kingdom",
}
GCCT_CEMENT_PROFILES = {
    "gem-gcct-brazil-cement-plant-construction-vintage-2026": ["Brazil"],
    "gem-gcct-canada-cement-plant-construction-vintage-2026": ["Canada"],
    "gem-gcct-colombia-cement-plant-construction-vintage-2026": ["Colombia"],
    "gem-gcct-ecuador-cement-plant-construction-vintage-2026": ["Ecuador"],
    "gem-gcct-india-cement-plant-construction-vintage-2026": ["India"],
    "gem-gcct-peru-cement-plant-construction-vintage-2026": ["Peru"],
    "gem-gcct-south-africa-cement-plant-construction-vintage-2026": ["South Africa"],
    "gem-gcct-switzerland-cement-plant-construction-vintage-2026": ["Switzerland"],
    "gem-gcct-tunisia-cement-plant-construction-vintage-2026": ["Tunisia"],
    "gem-gcct-united-states-cement-plant-construction-vintage-2026": ["United States"],
    "gem-gcct-world-cement-plant-construction-vintage-2026": ["World"],
}

GIST_STEEL_PROFILES = {
    "gem-gist-europe-blast-furnace-construction-vintage-2026": {
        "furnace_category": "blast_furnace",
        "geography": "Europe",
        "countries": None,
    },
    "gem-gist-india-blast-furnace-construction-vintage-2026": {
        "furnace_category": "blast_furnace",
        "geography": "India",
        "countries": {"India"},
    },
    "gem-gist-world-blast-furnace-construction-vintage-2026": {
        "furnace_category": "blast_furnace",
        "geography": "World",
        "countries": None,
    },
    "gem-gist-europe-basic-oxygen-furnace-construction-vintage-2026": {
        "furnace_category": "bof",
        "geography": "Europe",
        "countries": None,
    },
    "gem-gist-india-basic-oxygen-furnace-construction-vintage-2026": {
        "furnace_category": "bof",
        "geography": "India",
        "countries": {"India"},
    },
    "gem-gist-world-basic-oxygen-furnace-construction-vintage-2026": {
        "furnace_category": "bof",
        "geography": "World",
        "countries": None,
    },
    "gem-gist-europe-electric-arc-furnace-construction-vintage-2026": {
        "furnace_category": "eaf",
        "geography": "Europe",
        "countries": None,
    },
    "gem-gist-india-electric-arc-furnace-construction-vintage-2026": {
        "furnace_category": "eaf",
        "geography": "India",
        "countries": {"India"},
    },
    "gem-gist-canada-electric-arc-furnace-construction-vintage-2026": {
        "furnace_category": "eaf",
        "geography": "Canada",
        "countries": {"Canada"},
    },
    "gem-gist-world-electric-arc-furnace-construction-vintage-2026": {
        "furnace_category": "eaf",
        "geography": "World",
        "countries": None,
    },
    "gem-gist-india-direct-reduced-iron-furnace-construction-vintage-2026": {
        "furnace_category": "dri",
        "geography": "India",
        "countries": {"India"},
    },
}
GIST_FURNACE_NAMES = {
    "blast_furnace": "blast furnaces",
    "bof": "basic oxygen furnaces",
    "eaf": "electric arc furnaces",
    "dri": "direct reduced iron furnaces",
}

GEM_PIPELINE_PROFILES = {
    "gem-ggit-europe-natural-gas-transmission-pipeline-construction-vintage-2025": {
        "asset_type": "gas-pipeline",
        "geography": "Europe",
        "reference_year": 2025,
        "source_id": "gem-global-gas-infrastructure-tracker-november-2025",
        "technology": "natural gas transmission pipelines",
    },
    "gem-ggit-world-natural-gas-transmission-pipeline-construction-vintage-2025": {
        "asset_type": "gas-pipeline",
        "geography": "World",
        "reference_year": 2025,
        "source_id": "gem-global-gas-infrastructure-tracker-november-2025",
        "technology": "natural gas transmission pipelines",
    },
    "gem-goit-europe-oil-ngl-transmission-pipeline-construction-vintage-2026": {
        "asset_type": "oil-pipeline",
        "geography": "Europe",
        "reference_year": 2026,
        "source_id": "gem-global-oil-infrastructure-tracker-june-2026",
        "technology": "oil and natural-gas-liquids transmission pipelines",
    },
    "gem-goit-world-oil-ngl-transmission-pipeline-construction-vintage-2026": {
        "asset_type": "oil-pipeline",
        "geography": "World",
        "reference_year": 2026,
        "source_id": "gem-global-oil-infrastructure-tracker-june-2026",
        "technology": "oil and natural-gas-liquids transmission pipelines",
    },
}

GEM_COAL_MINE_PROFILES = {
    "gem-gcmt-europe-surface-coal-mine-construction-vintage-2026": {
        "mine_type": "Surface",
        "geography": "Europe",
        "technology": "surface coal mines",
    },
    "gem-gcmt-world-surface-coal-mine-construction-vintage-2026": {
        "mine_type": "Surface",
        "geography": "World",
        "technology": "surface coal mines",
    },
    "gem-gcmt-europe-underground-coal-mine-construction-vintage-2026": {
        "mine_type": "Underground",
        "geography": "Europe",
        "technology": "underground coal mines",
    },
    "gem-gcmt-world-underground-coal-mine-construction-vintage-2026": {
        "mine_type": "Underground",
        "geography": "World",
        "technology": "underground coal mines",
    },
}

# Approximate values digitised from Figure 15 of the European Commission's
# railway-guidelines final report. The figure labels the vertical axis as
# thousands of wagons and reports a separate unknown-manufacturing-year bar.
EU_FREIGHT_WAGON_MANUFACTURING_BANDS_2019 = (
    ("before 1960", 60, None, 10),
    ("1960-1965", 54, 60, 14),
    ("1966-1971", 48, 54, 50),
    ("1972-1977", 42, 48, 86),
    ("1978-1983", 36, 42, 116),
    ("1984-1989", 30, 36, 120),
    ("1990-1995", 24, 30, 56),
    ("1996-2001", 18, 24, 52),
    ("2002-2008", 11, 18, 67),
    ("2009-2013", 6, 11, 44),
    ("2014-2018", 1, 6, 33),
)
EU_FREIGHT_WAGON_REPRESENTATIVE_AGES_2019 = (
    64,
    56.5,
    50.5,
    44.5,
    38.5,
    32.5,
    26.5,
    20.5,
    14,
    8,
    3,
)
EU_FREIGHT_WAGON_UNKNOWN_COUNT_THOUSANDS_2019 = 38

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
    adjustment_index = max(range(len(rounded)), key=rounded.__getitem__)
    rounded[adjustment_index] = round(
        rounded[adjustment_index] + 1.0 - sum(rounded), 12
    )
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
        construction = EUROSTAT_CONSTRUCTION_TIMING.get(profile_id)
        if construction:
            representative = eurostat_representative_age_profile(result)
            profiles[construction["profile_id"]] = {
                "quantity": "construction_timing",
                "reference_event": "current_service",
                "distribution": {
                    "kind": "discrete",
                    "offsets_years": representative["offsets_years"],
                    "weights": _round_weights(representative["weights"]),
                },
                "applicability": {
                    "evidence_geographies": ["Europe"],
                    "reference_year": 2024,
                    "vehicle": construction["vehicle"],
                    "proxy_policy": "Use outside Europe only when no higher-precedence geography-specific profile matches.",
                },
                "assumptions": [
                    "The registered-stock age distribution is used as a proxy for construction vintages of vehicles providing current transport service.",
                    "Closed age bands are placed at their arithmetic midpoints: 1, 3.5, 7.5, and 15 years.",
                    "The open-ended over-20-years band is represented at 22.5 years, following the EEA convention; this is a modelling assumption rather than an observed upper tail.",
                    "Registered vehicle counts are not weighted by distance travelled, passenger-kilometres, or tonne-kilometres.",
                    "The profile describes observed stock composition and must not be interpreted as a prospective lifetime or a fully attribution-based production profile.",
                    "European evidence is a lower-precedence fallback outside Europe until better regional evidence is available.",
                ],
                "evidence": {
                    "status": "empirical",
                    "source_ids": [
                        source_id,
                        "eea-eu-passenger-car-age-method-2022",
                    ],
                    "population": "Registered end-of-year vehicle stock in EU27 countries reporting every requested age band.",
                    "weighting": "Vehicle count.",
                    "reference_period": "2024-12-31",
                    "censoring": "The oldest age band is open-ended above 20 years and represented at 22.5 years for runtime use.",
                    "sample_size": {
                        "count": result["observations"],
                        "unit": "registered vehicles",
                    },
                    "quality_flags": [
                        "complete-reporting-country-subset",
                        "vehicle-count-weighted",
                        "open-ended-oldest-age-band",
                        "representative-age-assumption",
                        "europe-proxy-fallback",
                    ],
                    "coverage": {
                        "countries": result["countries"],
                        "country_count": result["country_count"],
                        "vehicle_count": result["observations"],
                        "mean_representative_age_years": round(
                            representative["mean_age_years"], 6
                        ),
                        "derived_from_profile": profile_id,
                    },
                },
                "review": {
                    "activation_status": construction["activation_status"],
                    "rationale": construction["rationale"],
                },
            }
    return profiles


def eurostat_aircraft_profiles(source_file: Path) -> dict[str, dict]:
    """Return EU27 commercial-aircraft stock ages and a runtime proxy."""

    with source_file.open(encoding="utf-8") as stream:
        result = eurostat_aircraft_age_band_profile(json.load(stream))
    bands = [
        {
            "source_code": band["code"],
            "minimum_years": band["minimum_years"],
            "maximum_years": band["maximum_years"],
            "weight": round(band["weight"], 12),
        }
        for band in result["bands"]
    ]
    bands[-1]["weight"] += 1.0 - sum(band["weight"] for band in bands)
    representative = eurostat_aircraft_representative_age_profile(result)
    shared_coverage = {
        "countries": result["countries"],
        "country_count": result["country_count"],
        "aircraft_count": result["observations"],
        "age_band_counts_youngest_first": [band["count"] for band in result["bands"]],
    }
    raw_id = "eurostat-eu27-commercial-aircraft-age-2024"
    runtime_id = "eurostat-eu27-commercial-passenger-aircraft-construction-vintage-2024"
    return {
        raw_id: {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "derived",
                "generator": "uniform_within_reported_age_bands",
                "parameters": {"bands": bands},
            },
            "applicability": {
                "evidence_geographies": ["EU27"],
                "reference_year": 2024,
                "technology": "commercial aircraft",
            },
            "assumptions": [
                "Only EU27 countries reporting every aircraft age band are aggregated; all 27 countries have complete coverage for 2024.",
                "The open 20-years-or-over band is retained without imposing an unsupported maximum age.",
                "The source combines passenger, cargo, and other covered commercial aircraft categories.",
                "Aircraft counts are not weighted by seats, payload, flights, passenger-kilometres, or tonne-kilometres.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["eurostat-avia-eq-arc-age-2024"],
                "population": "Commercial aircraft in service on 31 December 2024 by country of operator for the complete EU27 reporting set.",
                "weighting": "Aircraft count.",
                "reference_period": "2024-12-31",
                "censoring": "The oldest age band is open at 20 years or over.",
                "sample_size": {
                    "count": result["observations"],
                    "unit": "commercial aircraft",
                },
                "quality_flags": [
                    "aircraft-count-weighted",
                    "complete-eu27-coverage",
                    "open-ended-oldest-age-band",
                    "mixed-commercial-aircraft-categories",
                ],
                "coverage": shared_coverage,
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Preserves Eurostat's reported age bands without introducing a point estimate for the open oldest band.",
            },
        },
        runtime_id: {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": representative["offsets_years"],
                "weights": _round_weights(representative["weights"]),
            },
            "applicability": {
                "evidence_geographies": ["EU27"],
                "reference_year": 2024,
                "technology": "commercial passenger aircraft",
                "proxy_policy": "Use for global passenger-aircraft and belly-freight callers only when no more specific fleet profile matches.",
            },
            "assumptions": [
                "The EU27 commercial-aircraft stock-age distribution is used as a proxy for construction vintages of passenger aircraft providing current transport service.",
                "Closed age bands are represented at ages 2, 7, 12, and 17 years.",
                "The open 20-years-or-over band is represented at age 22.5 years; this yields an 11.9-year mean, close to EASA's independently reported 11.8-year European fleet mean for 2023.",
                "Eurostat combines commercial aircraft categories, so the distribution is not specific to passenger aircraft; dedicated freighters are excluded because EASA reports a much older cargo fleet.",
                "Belly freight uses the passenger-aircraft profile because it is carried in passenger aircraft rather than dedicated freighters.",
                "Aircraft counts are not weighted by seats, payload, flights, passenger-kilometres, or tonne-kilometres, and one common profile is used across haul classes.",
                "All compatible ecoinvent aircraft-transport callers are global, so EU27 evidence is an explicit geographic proxy rather than a direct match.",
                "The profile describes observed stock composition and must not be interpreted as prospective service life or an attribution-based production profile.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [
                    "eurostat-avia-eq-arc-age-2024",
                    "easa-european-aviation-environmental-report-2025",
                ],
                "population": "Commercial aircraft in service on 31 December 2024 by country of operator for the complete EU27 reporting set; applied only to passenger-aircraft and belly-freight inventory relationships.",
                "weighting": "Aircraft count.",
                "reference_period": "2024-12-31; mean-age corroboration uses EASA reference year 2023",
                "censoring": "The open 20-years-or-over band is represented at age 22.5 years for runtime use.",
                "sample_size": {
                    "count": result["observations"],
                    "unit": "commercial aircraft",
                },
                "quality_flags": [
                    "aircraft-count-weighted",
                    "complete-eu27-coverage",
                    "mixed-commercial-aircraft-categories",
                    "representative-age-assumption",
                    "open-tail-modelled",
                    "europe-proxy-fallback",
                    "haul-class-not-distinguished",
                ],
                "coverage": {
                    **shared_coverage,
                    "representative_ages_years_oldest_first": [
                        -offset for offset in representative["offsets_years"]
                    ],
                    "derived_mean_age_years": round(
                        representative["mean_age_years"], 6
                    ),
                    "easa_2023_european_mean_age_years": 11.8,
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Passenger and belly-freight transport activities expose one stable whole-aircraft exchange across ecoinvent 3.8-3.12; dedicated freighters, maintenance, and airports are excluded.",
            },
        },
    }


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
                "rationale": (
                    "Valid U.S. fleet-age evidence retained separately from the reviewed construction-timing mapping; it is not a service-life distribution."
                    if technology == "Wood/Wood Waste Biomass"
                    else "Valid U.S. fleet-age evidence; it is not a service-life distribution and lacks reviewed ecoinvent geography mappings."
                ),
            },
        }
        if technology in EIA_CONSTRUCTION_TIMING_PILOTS:
            construction_profile_id = EIA_CONSTRUCTION_TIMING_PILOTS[technology]
            us_only = technology in EIA_US_ONLY_CONSTRUCTION_TIMING
            applicability = {
                "evidence_geographies": ["US"],
                "reference_year": 2025,
                "technology": technology,
            }
            if not us_only:
                applicability["proxy_policy"] = (
                    "Use outside the United States only when no higher-precedence "
                    "geography-specific profile matches."
                )
            profiles[construction_profile_id] = {
                "quantity": "construction_timing",
                "reference_event": "current_service",
                "distribution": {
                    "kind": "discrete",
                    "offsets_years": result["offsets_years"],
                    "weights": _round_weights(result["weights"]),
                },
                "applicability": applicability,
                "assumptions": [
                    "The operating-fleet age distribution is used as the construction-vintage distribution of infrastructure serving current electricity production.",
                    (
                        "The profile is activated only for U.S. callers; a European profile will provide the lower-precedence fallback elsewhere."
                        if us_only
                        else "The profile is direct evidence for U.S. callers and an explicit fallback proxy elsewhere."
                    ),
                    "Fleet ages are weighted by net summer capacity, not annual generation or utilisation.",
                    "The profile describes existing fleet composition and must not be interpreted as prospective service life.",
                    *(
                        [
                            "The broad EIA wood and wood-waste generator fleet is used for ecoinvent's wood-chip ORC CHP family.",
                            "The furnace and ORC generation unit inherit the plant commissioning vintage; later refurbishment or component replacement is not observed.",
                            "Dust-collection equipment is excluded because it may be retrofitted or replaced on a different schedule.",
                        ]
                        if us_only
                        else []
                    ),
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
                        *([] if us_only else ["country-proxy-fallback"]),
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
                    "rationale": (
                        "The U.S. wood-chip CHP caller and its two core infrastructure exchanges were audited across ecoinvent 3.8-3.12; no non-U.S. proxy is activated."
                        if us_only
                        else "Caller-and-exchange selectors were audited across ecoinvent 3.8-3.12; the proxy tier is overridden by any direct selector."
                    ),
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


def nod_platform_profiles(facilities_csv: Path, history_csv: Path) -> dict[str, dict]:
    """Build original-service vintages for currently operating Norwegian platforms."""

    facilities = pd.read_csv(facilities_csv)
    history = pd.read_csv(history_csv)
    candidates = (
        facilities[
            facilities["fclPhase"].eq("IN SERVICE")
            & facilities["fclSurface"].eq("Y")
            & facilities["fclNationName"].eq("Norway")
            & pd.to_numeric(facilities["fclWaterDepth"], errors="coerce").gt(0)
            & facilities["fclFunctions"]
            .fillna("")
            .str.contains(
                "PROCESSING|DRILLING|WELLHEAD|OIL PRODUCER|GAS PRODUCER", regex=True
            )
            & ~facilities["fclKind"].isin(
                ["ONSHORE FACILITY", "OFFSHORE WIND TURBINE", "FSU", "LOADING SYSTEM"]
            )
        ]
        .drop_duplicates("fclNpdidFacility")
        .copy()
    )
    service = history[history["fclPhaseName"].eq("IN SERVICE")].copy()
    service["date"] = pd.to_datetime(
        service["fclFromDate"], format="%d.%m.%Y", errors="coerce"
    )
    earliest = service.groupby("fclNpdidFacility1")["date"].min()
    candidates["current_start"] = pd.to_datetime(
        candidates["fclStartupDate"], format="%d.%m.%Y", errors="coerce"
    )
    candidates["history_start"] = candidates["fclNpdidFacility"].map(earliest)
    candidates["first_start"] = candidates[["current_start", "history_start"]].min(
        axis=1
    )
    valid = candidates[
        candidates["first_start"].between("1900-01-01", "2026-11-14")
    ].copy()
    if valid.empty:
        raise ValueError("No operating platforms with valid first-service dates.")
    years = valid["first_start"].dt.year.value_counts().sort_index()
    weights = _round_weights((years / years.sum()).tolist())
    offsets = [int(year) - 2026 for year in years.index]
    corrected = valid[valid["first_start"] < valid["current_start"]]
    return {
        "nod-norway-offshore-production-platform-construction-vintage-2026": {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": offsets,
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": ["Norway"],
                "reference_year": 2026,
                "technology": "offshore oil and gas production platforms",
                "proxy_policy": "Lower-precedence Norwegian fallback outside Norway when no better regional evidence matches.",
            },
            "assumptions": [
                "Includes in-service Norwegian surface facilities in positive water depth with production, processing, drilling or wellhead functions.",
                "Subsea structures, wind turbines, land facilities, storage-only vessels, risers, flares and accommodation-only structures are excluded.",
                "Earliest recorded IN SERVICE date across facility-ID history and current startup date approximates original commissioning; later restarts do not reset vintage.",
                "Commissioning year approximates construction timing; fabrication lead time and pre-conversion vessel construction are not observed.",
                "Facilities are count weighted because comparable structural mass, processing capacity and utilisation are unavailable.",
                "One mixed oil/gas profile spans jackets, concrete platforms, semisubmersibles and production vessels; it is a technology-family proxy for both ecoinvent platform types.",
                "Refurbishment and replacement equipment require separate timing; this surviving-stock profile does not establish prospective service life or attribution to current output.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["nod-facility-register-history-2026"],
                "population": "Currently in-service Norwegian offshore surface production and processing platforms with known first-service dates.",
                "weighting": "One equal weight per unique facility ID.",
                "reference_period": "2026-11-14",
                "censoring": "Surviving operating facilities; unknown first-service dates excluded.",
                "sample_size": {"count": len(valid), "unit": "platform facilities"},
                "quality_flags": [
                    "facility-count-weighted",
                    "surviving-fleet",
                    "technology-family-proxy",
                    "commissioning-as-construction-proxy",
                    "restart-history-reviewed",
                    "norway-proxy-fallback",
                ],
                "coverage": {
                    "geography": "Norway",
                    "eligible_facility_count": len(candidates),
                    "known_first_service_facility_count": len(valid),
                    "unknown_first_service_facility_count": len(candidates)
                    - len(valid),
                    "restart_date_corrections": [
                        {
                            "facility": row["fclName"],
                            "first_service_year": int(row["first_start"].year),
                            "current_startup_year": int(row["current_start"].year),
                        }
                        for _, row in corrected.iterrows()
                    ],
                    "derived_mean_age_years": round(
                        -sum(o * w for o, w in zip(offsets, weights, strict=True)), 6
                    ),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Facility-level service history supports a pragmatic whole-platform vintage proxy; only offshore production callers are selected.",
            },
        }
    }


def gem_bioenergy_profiles(source_directory: Path) -> dict[str, dict]:
    """Build dedicated wood and refuse unit vintages, excluding fuel conversions."""
    records = {}
    for path in sorted(source_directory.glob("assets-*.json")):
        with path.open(encoding="utf-8") as stream:
            for record in json.load(stream)["results"]:
                records[record["asset_id"]] = record
    fuels = {
        "wood-biomass": "bioenergy: wood & other biomass (solids)",
        "waste-incineration": "bioenergy: refuse (municipal and industrial wastes)",
    }
    profiles = {}
    for family, fuel in fuels.items():
        for geography in ("Europe", "World"):
            candidates = []
            conversions = []
            for record in records.values():
                fields = record.get("type_fields", {})
                if (
                    record.get("operating_status") != "operating"
                    or fields.get("fuel") != fuel
                ):
                    continue
                if geography == "Europe" and fields.get("region") != "Europe":
                    continue
                if fields.get("unit_conversion_year") or fields.get(
                    "conversion_replacement?"
                ):
                    conversions.append(record)
                    continue
                candidates.append(record)
            by_year = {}
            known = []
            total_capacity = 0.0
            for record in candidates:
                try:
                    capacity = float(record.get("capacity_value"))
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(capacity) or capacity <= 0:
                    continue
                total_capacity += capacity
                try:
                    year = float(record["type_fields"].get("start_year"))
                except (TypeError, ValueError):
                    continue
                if (
                    not math.isfinite(year)
                    or not year.is_integer()
                    or not 1900 <= year <= 2026
                ):
                    continue
                year = int(year)
                by_year[year] = by_year.get(year, 0.0) + capacity
                known.append(record)
            if not known:
                raise ValueError(f"No known-year {geography} {family} units.")
            years = sorted(by_year)
            known_capacity = sum(by_year.values())
            weights = _round_weights([by_year[year] / known_capacity for year in years])
            offsets = [year - 2026 for year in years]
            technology = (
                "dedicated wood biomass power and CHP units"
                if family == "wood-biomass"
                else "municipal and industrial refuse waste-to-energy units"
            )
            profile_id = (
                f"gem-gbpt-{geography.lower()}-{family}-construction-vintage-2026"
            )
            profiles[profile_id] = {
                "quantity": "construction_timing",
                "reference_event": "current_service",
                "distribution": {
                    "kind": "discrete",
                    "offsets_years": offsets,
                    "weights": weights,
                },
                "applicability": {
                    "evidence_geographies": [geography],
                    "reference_year": 2026,
                    "technology": technology,
                    "proxy_policy": "European evidence applies to European callers; world evidence is a lower-priority fallback elsewhere. Existing US EIA wood profiles take precedence for US callers.",
                },
                "assumptions": [
                    "Only operating units with the exact single reported fuel category are included; mixed-fuel, fossil co-firing, landfill gas and sewage-only units are excluded.",
                    "Any unit with a reported conversion year or conversion/replacement flag is excluded to avoid interpreting conversion timing as original construction.",
                    "Valid original start years define vintages; units without a valid year are excluded and known-year capacity is renormalised.",
                    "Reported electric capacity in MW weights the distribution; heat capacity, waste-treatment throughput and utilisation are unavailable.",
                    "The utility-scale source incompletely covers small units; extrapolation to ecoinvent's smaller wood CHP systems is an explicit size and technology proxy.",
                    "The refuse category combines municipal and industrial waste and only represents energy-recovery facilities; hazardous waste and callers without energy recovery are excluded.",
                    "Original generating-unit start years approximate whole-asset construction. Refurbishment, flue-gas equipment retrofits and component replacements need separate timing.",
                    "The surviving-stock distribution does not establish prospective service life or full production attribution to current service.",
                ],
                "evidence": {
                    "status": "empirical",
                    "source_ids": ["gem-bioenergy-power-tracker-api-2026"],
                    "population": f"Operating non-converted {technology} in {geography}, from the complete {len(records):,}-record API snapshot retrieved 14 November 2026.",
                    "weighting": "Reported electrical generating capacity in MW.",
                    "reference_period": "API snapshot 2026-11-14; API records are not asserted identical to the website's latest release.",
                    "censoring": "Operating units only; unknown start-year capacity and reported conversions excluded.",
                    "sample_size": {
                        "count": len(known),
                        "unit": "operating units with known original start year",
                    },
                    "quality_flags": [
                        "electric-capacity-weighted",
                        "surviving-fleet",
                        "conversions-excluded",
                        "single-fuel-only",
                        "small-unit-coverage-incomplete",
                        "capacity-not-service-output-weighted",
                        "technology-family-proxy",
                    ],
                    "coverage": {
                        "geography": geography,
                        "fuel_category": fuel,
                        "operating_unit_count_after_conversion_exclusion": len(
                            candidates
                        ),
                        "excluded_conversion_unit_count": len(conversions),
                        "known_start_year_unit_count": len(known),
                        "operating_capacity_mw": round(total_capacity, 6),
                        "known_start_year_capacity_mw": round(known_capacity, 6),
                        "unknown_start_year_capacity_mw": round(
                            total_capacity - known_capacity, 6
                        ),
                        "known_start_year_capacity_share": round(
                            known_capacity / total_capacity, 6
                        ),
                        "earliest_start_year": min(years),
                        "latest_start_year": max(years),
                        "derived_mean_age_years": round(
                            -sum(o * w for o, w in zip(offsets, weights, strict=True)),
                            6,
                        ),
                    },
                },
                "review": {
                    "activation_status": "active",
                    "rationale": "Pragmatic reviewed size and technology proxy applied only to complete wood CHP core equipment or energy-recovering municipal incineration facilities.",
                },
            }
    return profiles


def _year_capacity_distribution(observations, reference_year=2026):
    by_year = {}
    for year, capacity in observations:
        by_year[year] = by_year.get(year, 0) + capacity
    if not by_year:
        raise ValueError("No valid vintage observations.")
    years = sorted(by_year)
    total = sum(by_year.values())
    weights = _round_weights([by_year[year] / total for year in years])
    distribution = {
        "kind": "discrete",
        "offsets_years": [year - reference_year for year in years],
        "weights": weights,
    }
    mean = round(
        sum(
            (reference_year - year) * weight
            for year, weight in zip(years, weights, strict=True)
        ),
        6,
    )
    return distribution, mean


DFT_CAR_FUELS = {
    "gasoline": "PETROL",
    "diesel": "DIESEL",
    "battery-electric": "BATTERY ELECTRIC",
    "fuel-cell-electric": "FUEL CELL ELECTRIC",
    "gas": "GAS",
    "gasoline-hybrid": "HYBRID ELECTRIC (PETROL)",
    "diesel-hybrid": "HYBRID ELECTRIC (DIESEL)",
    "plugin-gasoline-hybrid": "PLUG-IN HYBRID ELECTRIC (PETROL)",
    "plugin-diesel-hybrid": "PLUG-IN HYBRID ELECTRIC (DIESEL)",
}


def dft_vehicle_rows(source_file: Path) -> list[dict]:
    """Read VEH1111 ODS without an additional spreadsheet dependency."""
    table_ns = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
    office_ns = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    with zipfile.ZipFile(source_file) as archive:
        root = ET.fromstring(archive.read("content.xml"))
    table = next(
        t
        for t in root.iter(f"{{{table_ns}}}table")
        if t.get(f"{{{table_ns}}}name") == "VEH1111"
    )
    header = None
    records = []
    for row in table.findall(f"{{{table_ns}}}table-row"):
        cells = []
        for cell in row:
            value = cell.get(f"{{{office_ns}}}value")
            if value is None:
                value = " ".join(cell.itertext()).strip()
            repeat = int(cell.get(f"{{{table_ns}}}number-columns-repeated", "1"))
            cells.extend([value] * min(repeat, 256))
        if cells and cells[0].startswith("Geography"):
            header = cells
        elif header is not None and cells and cells[0]:
            records.append(dict(zip(header, cells)))
    if not records:
        raise ValueError("VEH1111 has no data rows.")
    return records


def dft_car_profiles(source_file: Path, reference_year=2025) -> dict[str, dict]:
    """Preserve fuel-specific surviving car cohorts, excluding unknown dates."""
    rows = [
        r
        for r in dft_vehicle_rows(source_file)
        if r.get("Geography [note 1]") == "United Kingdom"
        and r.get("Date") == f"{reference_year} Q4 (end December)"
        and r.get("BodyType") == "Cars"
    ]
    profiles = {}
    for technology, fuel in DFT_CAR_FUELS.items():
        matches = [r for r in rows if r["Fuel"] == fuel]
        if len(matches) != 1:
            raise ValueError(f"Expected one UK {reference_year} Cars/{fuel} row.")
        row = matches[0]
        if row["Units"] != "Thousands":
            raise ValueError("Unexpected VEH1111 units.")
        known = []
        suppressed_cells = 0
        for label, value in row.items():
            if not label.isdigit():
                continue
            year = int(label)
            if value in {"[low]", "[z]", ""}:
                suppressed_cells += value == "[low]"
                continue
            count = float(value) * 1000
            if not math.isfinite(count) or count < 0 or year > reference_year:
                raise ValueError(f"Invalid VEH1111 cohort {fuel}/{label}.")
            if count > 0:
                known.append((year, count))
        total = float(row["Total"]) * 1000
        unknown = float(row["Unknown [note 2]"]) * 1000
        known_count = sum(count for _, count in known)
        if (
            not math.isfinite(total)
            or not math.isfinite(unknown)
            or total <= 0
            or known_count > total + 1
            or unknown < 0
            or abs(total - known_count - unknown) > max(1, suppressed_cells + 1)
        ):
            raise ValueError(f"Invalid VEH1111 population for {fuel}.")
        distribution, mean = _year_capacity_distribution(known, reference_year)
        profiles[f"dft-uk-{technology}-car-construction-vintage-{reference_year}"] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": distribution,
            "applicability": {
                "evidence_geographies": ["United Kingdom"],
                "reference_year": reference_year,
                "vehicle": f"passenger cars, {technology}",
                "proxy_policy": "UK fuel-specific evidence is a fallback elsewhere until stronger local evidence is available.",
            },
            "assumptions": [
                "Only licensed UK cars at the end of the reference year are included; SORN vehicles, other body types and other observation dates are excluded.",
                "First-use year approximates construction year; unknown first-use dates and suppressed low cells are excluded and known cohorts renormalised.",
                "Ages are reference year minus first-use year; months within a year are not observed.",
                "Fuel-specific vehicle counts are not weighted by annual mileage, occupancy or lifetime service.",
                "Size and Euro emission-standard labels do not have separate age observations; transfer across these inventory variants is an explicit proxy, not observed vintage eligibility by emission class.",
                "Fuel records describe current fuel classification; conversions do not establish original powertrain commissioning dates.",
                "The GAS category does not distinguish compressed natural gas from liquefied petroleum gas; it supplies both as an explicit fuel-category proxy.",
                "The source is UK evidence; use in continental Europe and elsewhere is a geographic proxy. This is fleet composition, not prospective lifetime or service attribution.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["dft-veh1111-fuel-first-use-2025"],
                "population": f"Licensed UK passenger cars classified as {fuel}, end-{reference_year}.",
                "weighting": "Registered licensed vehicle counts, converted from reported thousands.",
                "reference_period": f"{reference_year}-12-31",
                "censoring": "Surviving licensed stock; unknown dates and suppressed low cells excluded.",
                "sample_size": {
                    "count": round(known_count),
                    "unit": "cars with known first-use year",
                },
                "quality_flags": [
                    "vehicle-count-weighted",
                    "fuel-specific",
                    "first-use-year-proxy",
                    "unknown-age-excluded",
                    "uk-geographic-proxy",
                    "emission-class-not-conditioned",
                    *(["small-fuel-fleet"] if known_count < 1000 else []),
                ],
                "coverage": {
                    "fuel": fuel,
                    "known_age_vehicle_count": round(known_count),
                    "unknown_age_vehicle_count": round(unknown),
                    "reported_total_vehicle_count": round(total),
                    "known_age_vehicle_share": round(known_count / total, 6),
                    "suppressed_low_cohort_cells": suppressed_cells,
                    "total_minus_known_and_unknown": round(
                        total - known_count - unknown, 6
                    ),
                    "derived_mean_age_years": mean,
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Fuel-aware passenger-transport to whole-car assignments replace legacy assumed-age curves; component and manufacturing callers are excluded.",
            },
        }
    return profiles


def construction_event_profiles() -> dict[str, dict]:
    """Construction substeps share their parent event at annual resolution."""
    return {
        "reviewed-new-asset-construction-substep": {
            "quantity": "construction_timing",
            "reference_event": "construction",
            "distribution": {"kind": "fixed", "offset_years": 0},
            "assumptions": [
                "Selected components supplied to explicit new-vehicle assembly, photovoltaic installation, and CHP construction occur in the parent construction year.",
                "Selected markets supplying these same construction products add no further fleet-age offset.",
                "This is an annual-resolution timing convention, not empirical fleet-age evidence; manufacturing lead times within or before the construction year are unavailable.",
                "Whole operating assets receive fleet-age profiles at the service-to-asset link. A component of their construction does not receive a second whole-fleet age shift.",
                "Operating factories, charging infrastructure, replacement and maintenance events are excluded from these assignments.",
                "Exchange amounts and prospective service lives are unchanged.",
            ],
            "evidence": {
                "status": "assumption",
                "source_ids": ["trails-construction-event-convention"],
            },
            "review": {
                "activation_status": "active",
                "rationale": "Relationship-specific construction substeps replace legacy lifetime-based offsets without inventing a component fleet or applying fleet age twice.",
            },
        }
    }


def chemical_site_evidence_profiles(source_file: Path) -> dict[str, dict]:
    """Preserve site-age evidence without assigning it to current process units."""
    with source_file.open(encoding="utf-8") as stream:
        records = json.load(stream)
    profiles = {}
    for chemical in ("methanol", "ammonia"):
        for geography in ("Europe", "World"):
            candidates = [
                r
                for r in records
                if r["status"] in ("operating", "operating pre-retirement")
                and chemical in r["primaryProducts"]
                and "natural gas" in r["feedstock"]
                and "coal" not in r["feedstock"]
                and (geography == "World" or r["region"] == geography)
            ]
            positive = [r for r in candidates if (r["capacity"].get(chemical) or 0) > 0]
            known = [
                r
                for r in positive
                if isinstance(r.get("plantAge"), (int, float))
                and 0 <= r["plantAge"] <= 200
            ]
            distribution, mean = _year_capacity_distribution(
                [(2026 - int(r["plantAge"]), r["capacity"][chemical]) for r in known]
            )
            total = sum(r["capacity"][chemical] for r in positive)
            known_capacity = sum(r["capacity"][chemical] for r in known)
            profiles[f"gem-gcht-{geography.lower()}-{chemical}-site-vintage-2026"] = {
                "quantity": "construction_timing",
                "reference_event": "current_service",
                "distribution": distribution,
                "applicability": {
                    "evidence_geographies": [geography],
                    "reference_year": 2026,
                    "technology": f"industrial sites producing {chemical} using natural gas",
                },
                "assumptions": [
                    "Reported plantAge describes the industrial site and may predate its current ammonia or methanol synthesis units.",
                    "Current capacity of the selected chemical weights site ages; developing capacity is excluded and different chemicals are never summed.",
                    "Natural-gas feedstock is a site attribute and does not identify the age or process route of every production unit.",
                    "The Sluiskil 1929 date refers to ammonium-sulphate production; it cannot establish construction dates for the site's current three ammonia plants.",
                    "No runtime assignment is permitted until process-unit commissioning and expansion dates are resolved.",
                ],
                "evidence": {
                    "status": "empirical",
                    "source_ids": [
                        "gem-chemicals-tracker-dashboard-2026",
                        "yara-sluiskil-site-history-2017",
                    ],
                    "population": f"Operating {geography} industrial sites producing {chemical} and reporting natural gas but not coal among site feedstocks.",
                    "weighting": f"Existing {chemical} capacity in million tonnes per annum from the public dashboard.",
                    "reference_period": "November 2026 release; data current to September 2026",
                    "censoring": "Operating and operating-pre-retirement sites; unknown dates or capacities excluded.",
                    "sample_size": {
                        "count": len(known),
                        "unit": "sites with known age and chemical-specific capacity",
                    },
                    "quality_flags": [
                        "site-age-not-process-unit-age",
                        "expansion-dates-unresolved",
                        "no-runtime-assignment",
                    ],
                    "coverage": {
                        "geography": geography,
                        "candidate_site_count": len(candidates),
                        "known_age_site_count": len(known),
                        "sites_with_unknown_capacity": len(candidates) - len(positive),
                        "known_age_capacity_mtpa": round(known_capacity, 6),
                        "known_age_capacity_share": round(known_capacity / total, 6),
                        "derived_mean_site_age_years": mean,
                    },
                },
                "review": {
                    "activation_status": "evidence_only",
                    "rationale": "Industrial-site start dates cannot be assigned as current process-unit construction vintages; primary operator history confirms the mismatch.",
                },
            }
    return profiles


def offshore_wind_profiles(source_file: Path) -> dict[str, dict]:
    """Capacity-weighted operating fixed-bottom offshore wind phase vintages."""
    with source_file.open(encoding="utf-8") as stream:
        records = [f["properties"] for f in json.load(stream)["features"]]
    records = list({r["unit-id"]: r for r in records}.values())
    profiles = {}
    for geography, countries in {
        "Europe": GCCT_EUROPE_COUNTRIES,
        "World": None,
        "China": {"China"},
        "United Kingdom": {"United Kingdom"},
        "Germany": {"Germany"},
    }.items():
        candidates = [
            r
            for r in records
            if r["status"] == "operating"
            and r["tech-type"] == "Offshore hard mount"
            and (countries is None or r["country-area1"] in countries)
            and float(r["capacity"]) > 0
        ]
        known = []
        for record in candidates:
            try:
                year = float(record["start-year"])
            except (TypeError, ValueError):
                continue
            if math.isfinite(year) and year.is_integer() and 1900 <= year <= 2026:
                known.append((int(year), float(record["capacity"])))
        distribution, mean = _year_capacity_distribution(known)
        capacity = sum(c for _, c in known)
        total = sum(float(r["capacity"]) for r in candidates)
        profiles[
            f"gem-gwpt-{slug(geography)}-offshore-fixed-structure-vintage-2026"
        ] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": distribution,
            "applicability": {
                "evidence_geographies": [geography],
                "reference_year": 2026,
                "technology": "fixed-bottom offshore wind structures",
            },
            "assumptions": [
                "Operating fixed-bottom phases only; floating and unknown-mount phases are excluded.",
                "Phase commissioning year approximates original fixed-structure construction; fabrication lead time is unavailable.",
                "Generating capacity weights phase vintages; foundation mass and construction method are not separately observed.",
                "The modern fleet includes larger turbines than ecoinvent's 2 MW offshore design, so turbine-size transfer is an explicit proxy.",
                "Assignments apply only to fixed parts. Moving parts, replacements, repowering and maintenance are not inferred from original phase age.",
                "Country profiles override European or world fallbacks; this stock composition does not establish prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["gem-wind-power-tracker-february-2026"],
                "population": f"Operating fixed-bottom offshore wind phases in {geography}.",
                "weighting": "Reported electrical capacity in MW.",
                "reference_period": "February 2026 map snapshot",
                "censoring": "Operating phases; unknown start years excluded.",
                "sample_size": {
                    "count": len(known),
                    "unit": "operating wind farm phases",
                },
                "quality_flags": [
                    "capacity-weighted",
                    "fixed-bottom-only",
                    "phase-commissioning-proxy",
                    "turbine-size-proxy",
                ],
                "coverage": {
                    "geography": geography,
                    "operating_phase_count": len(candidates),
                    "known_start_year_phase_count": len(known),
                    "known_start_year_capacity_mw": round(capacity, 6),
                    "known_start_year_capacity_share": round(capacity / total, 6),
                    "derived_mean_age_years": mean,
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Operating-phase evidence supports fixed offshore structures; moving equipment remains outside the mapping.",
            },
        }
    return profiles


def parse_igu_active_fleet_text(text: str) -> list[dict]:
    """Parse every IMO row, failing rather than silently losing a vessel."""
    rows = []
    for block in re.split(r"(?m)(?=^\d{7}\s*$)", text)[1:]:
        match = re.search(
            r"(?m)^(\d{4,6})\s*\n[\s\S]*?\n"
            r"(Conventional|Icebreaker|Q-Max|Q-Flex|FSRU|FSU|Small-scale|Mid-scale|Bunkering\s+vessel)"
            r"\s*\n[\s\S]*?\n((?:19|20)\d{2})\s*\n(\d{1,3})(?=\s|$)",
            block,
        )
        if match is None:
            raise ValueError(f"Cannot parse IGU active-fleet row {block[:7]}.")
        capacity, kind, year, age = (
            int(match[1]),
            re.sub(r"\s+", " ", match[2]),
            int(match[3]),
            int(match[4]),
        )
        if age != 2026 - year or not 1900 <= year <= 2025 or capacity <= 0:
            raise ValueError(f"Inconsistent IGU age or capacity for {block[:7]}.")
        rows.append(
            {
                "imo": block[:7],
                "capacity_m3": capacity,
                "vessel_type": kind,
                "delivery_year": year,
            }
        )
    if not rows or len({r["imo"] for r in rows}) != len(rows):
        raise ValueError("Empty or duplicated IGU active fleet.")
    return rows


def igu_lng_carrier_profiles(source_pdf: Path) -> dict[str, dict]:
    import pymupdf

    with pymupdf.open(source_pdf) as document:
        text = "\n".join(
            page.get_text()
            for page in document
            if "Appendix 3: Table of Global Active LNG Fleet" in page.get_text()
        )
    rows = parse_igu_active_fleet_text(text)
    if len(rows) != 804:
        raise ValueError("IGU 2026 active fleet must contain all 804 reported vessels.")
    carriers = [
        r
        for r in rows
        if r["vessel_type"] in {"Conventional", "Icebreaker", "Q-Max", "Q-Flex"}
    ]
    distribution, mean = _year_capacity_distribution(
        [(r["delivery_year"], r["capacity_m3"]) for r in carriers]
    )
    return {
        "igu-world-lng-carrier-construction-vintage-2026": {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": distribution,
            "applicability": {
                "evidence_geographies": ["World"],
                "reference_year": 2026,
                "technology": "seagoing LNG freight carriers",
            },
            "assumptions": [
                "Appendix 3 active fleet only; the orderbook is excluded. All 804 vessel rows are parsed and reported ages checked against delivery years.",
                "Includes conventional, Q-Flex, Q-Max and icebreaking carriers; excludes FSU, FSRU, small-scale, mid-scale and bunkering vessels.",
                "Cargo volume capacity weights construction vintages; distance, utilisation and vessel construction mass are not observed.",
                "Delivery year approximates original vessel construction; later engine, containment or equipment replacements do not reset hull age.",
                "The source describes the end-2025 fleet and labels ages relative to 2026; runtime offsets follow that reference year.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["igu-world-lng-report-2026"],
                "population": "Active large seagoing LNG cargo carriers listed in IGU Appendix 3 at end-2025.",
                "weighting": "Reported LNG cargo capacity in cubic metres.",
                "reference_period": "End-2025 fleet; 2026 age reference",
                "censoring": "Active surviving fleet; storage/regasification and specialised small ships excluded.",
                "sample_size": {"count": len(carriers), "unit": "active LNG carriers"},
                "quality_flags": [
                    "cargo-volume-capacity-weighted",
                    "surviving-fleet",
                    "delivery-as-construction-proxy",
                ],
                "coverage": {
                    "geography": "World",
                    "parsed_active_vessels": len(rows),
                    "included_carrier_count": len(carriers),
                    "excluded_vessel_count": len(rows) - len(carriers),
                    "cargo_capacity_m3": sum(r["capacity_m3"] for r in carriers),
                    "derived_mean_age_years": mean,
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Dedicated fleet evidence replaces the broad other-ship-types proxy only for LNG freight-to-whole-tanker links.",
            },
        }
    }


def gem_csp_profiles(workbook: Path) -> dict[str, dict]:
    """Return capacity-weighted operating CSP construction vintages."""

    reference_year = 2026
    frame = pd.read_excel(workbook, sheet_name="Utility-Scale (1 MW+)")
    frame = frame[
        frame["Technology Type"].astype(str).str.casefold().eq("solar thermal")
        & frame["Status"].astype(str).str.casefold().eq("operating")
    ].copy()
    frame["Capacity (MW)"] = pd.to_numeric(frame["Capacity (MW)"], errors="coerce")
    frame["Start year"] = pd.to_numeric(frame["Start year"], errors="coerce")
    frame = frame[frame["Capacity (MW)"] > 0]

    specifications = {
        "gem-gspt-spain-csp-construction-vintage-2026": {
            "geography": "Spain",
            "countries": ["Spain"],
        },
        "gem-gspt-united-states-csp-construction-vintage-2026": {
            "geography": "United States",
            "countries": ["United States"],
        },
        "gem-gspt-south-africa-csp-construction-vintage-2026": {
            "geography": "South Africa",
            "countries": ["South Africa"],
        },
        "gem-gspt-world-csp-construction-vintage-2026": {
            "geography": "World",
            "countries": None,
        },
    }
    profiles = {}
    for profile_id, specification in specifications.items():
        subset = frame
        if specification["countries"] is not None:
            subset = subset[subset["Country/Area"].isin(specification["countries"])]
        valid = subset[
            subset["Start year"].between(1900, reference_year, inclusive="both")
        ].copy()
        if valid.empty:
            raise ValueError(f"No valid operating CSP phases for {profile_id}.")
        capacity_by_year = (
            valid.groupby("Start year", as_index=False)["Capacity (MW)"]
            .sum()
            .sort_values("Start year")
        )
        total_capacity = float(subset["Capacity (MW)"].sum())
        known_capacity = float(valid["Capacity (MW)"].sum())
        weights = _round_weights(
            (capacity_by_year["Capacity (MW)"] / known_capacity).tolist()
        )
        start_years = capacity_by_year["Start year"].astype(int).tolist()
        ages = [reference_year - year for year in start_years]
        mean_age = sum(age * weight for age, weight in zip(ages, weights, strict=True))
        geography = specification["geography"]
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-age for age in ages],
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": [geography],
                "reference_year": reference_year,
                "technology": "concentrating solar power plants",
                "proxy_policy": "Use the world profile where no country-specific operating CSP profile matches.",
            },
            "assumptions": [
                "Operating solar-thermal phases are used as the surviving construction-vintage composition of CSP plants providing current electricity.",
                "Start year is treated as commissioning year and phases are weighted by reported nameplate capacity in MW.",
                "The tracker combines parabolic trough, tower, Fresnel, solar chimney, and other solar-thermal designs; it is used as a technology-family proxy for ecoinvent's parabolic-trough plant.",
                "Phases without a valid start year are excluded and known-year capacity is renormalised.",
                "Repowering, storage replacement, and major refurbishment do not reset the original phase start year unless recorded as a new phase.",
                "The profile describes observed operating-stock composition and must not be interpreted as prospective service life or an attribution-based production profile.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["gem-global-solar-power-tracker-february-2026"],
                "population": f"Operating utility-scale solar-thermal phases in {geography} in the February 2026 Global Solar Power Tracker.",
                "weighting": "Reported phase nameplate capacity in MW.",
                "reference_period": "February 2026 release",
                "censoring": "Operating phases are right-censored with respect to eventual retirement; phases without a valid start year are excluded.",
                "sample_size": {
                    "count": len(valid),
                    "unit": "operating solar-thermal phases with known start year",
                },
                "quality_flags": [
                    "capacity-weighted",
                    "surviving-fleet",
                    "technology-family-proxy",
                    *(
                        ["unknown-start-year-capacity-excluded"]
                        if known_capacity < total_capacity
                        else []
                    ),
                ],
                "coverage": {
                    "geography": geography,
                    "operating_phase_count": len(subset),
                    "known_start_year_phase_count": len(valid),
                    "operating_capacity_mw": round(total_capacity, 6),
                    "known_start_year_capacity_mw": round(known_capacity, 6),
                    "known_start_year_capacity_share": round(
                        known_capacity / total_capacity, 6
                    ),
                    "earliest_start_year": min(start_years),
                    "latest_start_year": max(start_years),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Electricity-production datasets expose a stable whole-CSP-plant exchange across ecoinvent 3.8-3.12.",
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


def ship_construction_profiles() -> dict[str, dict]:
    """Return deadweight-tonnage-weighted 2024 global ship-fleet profiles."""

    profiles = {}
    offsets = [-age for age in UNCTAD_SHIP_REPRESENTATIVE_AGES_2024]
    for ship_type, specification in UNCTAD_SHIP_DWT_SHARES_2024.items():
        shares = specification["shares_percent_oldest_first"]
        total = sum(shares)
        weights = [share / total for share in shares]
        mean_age = sum(
            age * weight
            for age, weight in zip(
                UNCTAD_SHIP_REPRESENTATIVE_AGES_2024, weights, strict=True
            )
        )
        profile_id = f"unctad-global-{ship_type}-construction-vintage-2024"
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": offsets,
                "weights": _round_weights(weights),
            },
            "applicability": {
                "evidence_geographies": ["World"],
                "reference_year": 2024,
                "technology": specification["technology"],
            },
            "assumptions": [
                "The operating-fleet age distribution is used as the construction-vintage distribution of vessels serving current freight transport.",
                "Deadweight-tonnage shares are used because ecoinvent marine transport is expressed per tonne-kilometre.",
                "The published age-band percentages are rounded and are normalized to sum to one.",
                "Closed age bands use their midpoints; the open over-20 band uses 30.214 years, calibrated so the published global deadweight-tonnage bands reproduce the reported 12.5-year overall mean.",
                "The common open-tail representative age is applied to each ship type because type-specific within-band ages are not published.",
                "Major retrofits and component replacements are not separated from original vessel construction.",
                "The profile describes existing fleet composition and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["unctad-review-maritime-transport-2024"],
                "population": "Propelled seagoing vessels of 100 gross tons and above in the world fleet on 1 January 2024.",
                "weighting": "Share of deadweight tonnage in each age band.",
                "reference_period": "2024-01-01",
                "censoring": "Operating vessels are right-censored with respect to eventual retirement; the oldest band is open-ended.",
                "sample_size": {
                    "not_reported": "The published figure reports rounded deadweight-tonnage shares rather than vessel counts by type and age band.",
                    "unit": "vessels",
                },
                "quality_flags": [
                    "deadweight-tonnage-weighted",
                    "surviving-fleet",
                    "rounded-age-band-shares",
                    "open-tail-modelled",
                ],
                "coverage": {
                    "technology": specification["technology"],
                    "age_band_shares_percent_oldest_first": shares,
                    "representative_ages_years_oldest_first": UNCTAD_SHIP_REPRESENTATIVE_AGES_2024,
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "The transport-to-whole-vessel relationship was audited across ecoinvent 3.8-3.12.",
            },
        }
    return profiles


def moderate_building_profiles(workbook: Path) -> dict[str, dict]:
    """Return constructed-area-weighted EU27 building-stock profiles."""

    sheet = load_workbook(workbook, read_only=True, data_only=True)["Europe27"]
    rows = list(sheet.iter_rows(values_only=True))
    columns = {value: index for index, value in enumerate(rows[0])}
    profiles = {}
    offsets = [-age for age in MODERATE_BUILDING_AGES_2020]
    for profile_id, specification in MODERATE_BUILDING_PROFILES.items():
        selected = [
            row
            for row in rows[1:]
            if row[columns["sector"]] == "Service sector"
            and row[columns["subsector"]] == specification["subsector"]
        ]
        areas = [float(row[columns["area_Mm2"]]) for row in selected]
        total_area = sum(areas)
        weights = [area / total_area for area in areas]
        mean_age = sum(
            age * weight
            for age, weight in zip(MODERATE_BUILDING_AGES_2020, weights, strict=True)
        )
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": offsets,
                "weights": _round_weights(weights),
            },
            "applicability": {
                "evidence_geographies": ["EU27"],
                "reference_year": 2020,
                "technology": specification["technology"],
                "proxy_policy": "Use outside Europe only when no higher-precedence geography-specific profile matches.",
            },
            "assumptions": [
                "The operating-stock construction-period distribution is used as the construction-vintage distribution of buildings serving current activity.",
                "Construction-period shares are weighted by constructed floor area, matching square-metre and volume-based building inputs better than building counts.",
                "The 2020 EU27 aggregate is direct evidence for European callers and an explicit lower-precedence fallback elsewhere.",
                "Closed construction periods use their midpoints relative to 2020.",
                "The open pre-1945 band is represented at 104.542 years using the 2021 Eurostat occupied-dwelling split between pre-1919 and 1919-1945; 1900 is the explicit representative year for the open pre-1919 tail.",
                "The residential split used to refine the oldest tail is a proxy for non-residential buildings.",
                "Renovation does not reset original construction age; replacement and major refurbishment are not separately observed.",
                f"The reviewed runtime mapping covers {specification['mapping']} and excludes markets, construction activities, and unit-producing asset datasets.",
                "The profile describes existing stock composition and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [
                    "moderate-eu27-building-stock-2020-v1.4",
                    "eurostat-census-2021-dwelling-construction-period",
                ],
                "population": f"EU27 {specification['technology']} represented in the MODERATE 2020 building-stock model.",
                "weighting": "Constructed floor area in million square metres.",
                "reference_period": "2020",
                "censoring": "The oldest construction period is open-ended; surviving buildings are conditioned on presence in the 2020 stock.",
                "sample_size": {
                    "count": 27,
                    "unit": "EU member countries",
                    "minimum_for_generalisation": 27,
                },
                "quality_flags": [
                    "constructed-area-weighted",
                    "surviving-stock",
                    "modelled-gap-filling",
                    "open-tail-modelled",
                    "european-proxy-fallback",
                ],
                "coverage": {
                    "technology": specification["technology"],
                    "construction_periods_oldest_first": [
                        "Before 1945",
                        "1945-1969",
                        "1970-1979",
                        "1980-1989",
                        "1990-1999",
                        "2000-2010",
                        "2011-2020",
                    ],
                    "constructed_area_million_m2_oldest_first": [
                        round(area, 6) for area in areas
                    ],
                    "constructed_area_total_million_m2": round(total_area, 6),
                    "representative_ages_years_oldest_first": MODERATE_BUILDING_AGES_2020,
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Caller exclusions and building relationships were audited across ecoinvent 3.8-3.12.",
            },
        }
    return profiles


def ccnr_inland_vessel_profiles() -> dict[str, dict]:
    """Return count-weighted Rhine cargo-vessel commissioning vintages."""

    if any(
        len(counts) != len(CCNR_RHINE_COMMISSIONING_YEARS_2024)
        for counts in CCNR_RHINE_VESSEL_COUNTS_2024.values()
    ):
        raise ValueError("CCNR commissioning years and vessel counts must align.")
    profiles = {}
    specifications = {
        "dry-cargo": {
            "profile_id": "ccnr-rhine-dry-cargo-vessel-construction-vintage-2024",
            "technology": "Rhine dry-cargo inland vessels",
            "unknown_year_count": 135,
        },
        "tanker": {
            "profile_id": "ccnr-rhine-tanker-vessel-construction-vintage-2024",
            "technology": "Rhine tanker inland vessels",
            "unknown_year_count": None,
        },
    }
    for vessel_type, specification in specifications.items():
        counts = CCNR_RHINE_VESSEL_COUNTS_2024[vessel_type]
        observations = [
            (2024 - year, count, year)
            for year, count in zip(
                CCNR_RHINE_COMMISSIONING_YEARS_2024, counts, strict=True
            )
            if count > 0
        ]
        observations.sort(reverse=True)
        total = sum(count for _, count, _ in observations)
        weights = _round_weights([count / total for _, count, _ in observations])
        ages = [age for age, _, _ in observations]
        years = [year for _, _, year in observations]
        mean_age = sum(age * weight for age, weight in zip(ages, weights, strict=True))
        pre_2000_count = sum(
            count
            for year, count in zip(
                CCNR_RHINE_COMMISSIONING_YEARS_2024, counts, strict=True
            )
            if year < 2000
        )
        profile_id = specification["profile_id"]
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-age for age in ages],
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": ["Rhine countries"],
                "reference_year": 2024,
                "technology": specification["technology"],
                "proxy_policy": "Use outside RER only when no higher-precedence geography-specific inland-vessel profile matches.",
            },
            "assumptions": [
                "Annual vessel counts were recovered from the vector points in Figure 12 of the CCNR report and rounded to the nearest whole vessel.",
                "The IVR population is treated as the surviving Rhine fleet, although the source warns that some inactive vessels may remain, especially among old vintages.",
                "The source's plotted 'Until 1880' category has zero vessels for this fleet type, so no open-tail representative age is required.",
                "Vessel counts are not weighted by loading capacity, payload, distance travelled, or tonne-kilometres.",
                "Construction year is treated as the original vessel vintage; major rebuilds, repowering, and conversion do not reset age unless recorded as a new vessel.",
                "Rhine evidence is direct for RER transport callers and an explicit lower-precedence proxy for RoW callers.",
                "The profile describes observed stock composition and must not be interpreted as prospective service life or an attribution-based production profile.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["ccnr-market-observation-annual-report-2024"],
                "population": f"{specification['technology']} with a plotted commissioning year in the IVR Rhine-fleet population.",
                "weighting": "Vessel count digitised from annual commissioning-year points.",
                "reference_period": "2024 report edition",
                "censoring": "Vessels without a construction year are excluded where reported; the IVR may retain inactive old vessels.",
                "sample_size": {
                    "count": total,
                    "unit": "inland vessels with a plotted commissioning year",
                },
                "quality_flags": [
                    "vessel-count-weighted",
                    "surviving-fleet",
                    "digitized-vector-figure",
                    "possible-inactive-old-vessels",
                    *(
                        ["unknown-construction-year-vessels-excluded"]
                        if specification["unknown_year_count"] is not None
                        else ["unknown-construction-year-count-not-reported"]
                    ),
                    "capacity-and-utilisation-not-observed",
                    "europe-proxy-fallback",
                ],
                "coverage": {
                    "vessel_type": vessel_type,
                    "geography": "Rhine countries",
                    "known_construction_year_vessel_count": total,
                    "unknown_construction_year_vessel_count": specification[
                        "unknown_year_count"
                    ],
                    "positive_count_commissioning_years": len(observations),
                    "earliest_positive_commissioning_year": min(years),
                    "latest_positive_commissioning_year": max(years),
                    "pre_2000_vessel_share": round(pre_2000_count / total, 6),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Inland freight services expose stable whole-barge or whole-barge-tanker exchanges across ecoinvent 3.8-3.12; maintenance and reefer-only services are excluded.",
            },
        }
    return profiles


def european_heating_profiles() -> dict[str, dict]:
    """Return EU28 heating-stock evidence and separate lifetime assumptions."""

    profiles = {}
    source_bands = [
        {
            "source_period": "before 1992",
            "minimum_years": 20,
            "maximum_years": None,
        },
        {
            "source_period": "1992-2002",
            "minimum_years": 10,
            "maximum_years": 20,
        },
        {
            "source_period": "after 2002",
            "minimum_years": 0,
            "maximum_years": 10,
        },
    ]
    for stem, specification in EU28_HEATING_AGE_SHARES_2012.items():
        shares = specification["shares_percent_oldest_first"]
        weights = _round_weights([share / sum(shares) for share in shares])
        raw_id = f"fraunhofer-eu28-{stem}-age-2012"
        construction_id = f"fraunhofer-eu28-{stem}-construction-vintage-2012"
        common_assumptions = [
            "The source reports the installed-capacity composition of the surviving EU28 building-heating stock in reference year 2012.",
            "Rounded whole-percentage shares were transcribed from Figure 5; the study combines empirical inputs with bottom-up modelling where observations are missing.",
            "Installed capacity is not heat-output, utilisation, or unit-count weighting.",
            "The evidence describes operating-stock age and must not be interpreted as prospective service life.",
            "Original installation age is retained because refurbishment and component replacement are not separately observed.",
        ]
        profiles[raw_id] = {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "derived",
                "generator": "uniform_within_reported_age_bands",
                "parameters": {
                    "bands": [
                        {**band, "weight": weight}
                        for band, weight in zip(source_bands, weights, strict=True)
                    ]
                },
            },
            "applicability": {
                "evidence_geographies": ["EU28"],
                "reference_year": 2012,
                "technology": specification["technology"],
            },
            "assumptions": [
                *common_assumptions,
                "The open pre-1992 band is retained without an unsupported upper age.",
            ],
            "evidence": {
                "status": "modelled",
                "source_ids": ["ec-fraunhofer-eu28-heating-cooling-mapping-2016"],
                "quality_flags": [
                    "installed-capacity-weighted",
                    "surviving-stock",
                    "modelled-gap-filling",
                    "digitized-rounded-age-band-shares",
                    "open-ended-oldest-age-band",
                    "stale-reference-year",
                ],
                "coverage": {
                    "technology": specification["technology"],
                    "age_band_shares_percent_oldest_first": shares,
                    "age_bands_oldest_first": [
                        "before 1992",
                        "1992-2002",
                        "after 2002",
                    ],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Preserves the published stock-age resolution without adding a modelled open-tail representative age.",
            },
        }
        status = specification["activation_status"]
        mean_age = sum(
            age * weight
            for age, weight in zip(
                EU28_HEATING_REPRESENTATIVE_AGES_2012, weights, strict=True
            )
        )
        profiles[construction_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [
                    -age for age in EU28_HEATING_REPRESENTATIVE_AGES_2012
                ],
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": ["EU28"],
                "reference_year": 2012,
                "technology": specification["technology"],
                "proxy_policy": "Use outside the EU28 only as a lower-precedence European fallback until suitable regional evidence is available.",
            },
            "assumptions": [
                *common_assumptions,
                "The closed 1992-2002 and after-2002 bands are represented by ages 15 and 4.5 years relative to 2012.",
                "The open pre-1992 tail is represented at age 30 years; this explicit approximation can understate the age of much older equipment.",
                "Runtime use is restricted to reviewed heat-production relationships with a whole heating appliance and excludes condensing, industrial, state-of-the-art, future, and borehole-infrastructure cases.",
                "EU28 evidence is direct for EU callers and an explicitly lower-precedence proxy elsewhere.",
            ],
            "evidence": {
                "status": "modelled",
                "source_ids": ["ec-fraunhofer-eu28-heating-cooling-mapping-2016"],
                "quality_flags": [
                    "installed-capacity-weighted",
                    "surviving-stock",
                    "modelled-gap-filling",
                    "digitized-rounded-age-band-shares",
                    "open-tail-modelled",
                    "stale-reference-year",
                    "european-proxy-fallback",
                ],
                "coverage": {
                    "technology": specification["technology"],
                    "age_band_shares_percent_oldest_first": shares,
                    "representative_ages_years_oldest_first": EU28_HEATING_REPRESENTATIVE_AGES_2012,
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": status,
                "rationale": (
                    "The relationship to a complete building-heating appliance can be audited across ecoinvent 3.8-3.12."
                    if status == "active"
                    else "The source combines coal boilers and furnaces, while the inventory relationship is a coal stove and the open oldest band dominates."
                ),
            },
        }

    for stem, specification in EU_HEATING_SERVICE_LIVES_2026.items():
        profiles[f"ec-eu-{stem}-service-life-2026"] = {
            "quantity": "service_life",
            "reference_event": "asset_commissioning",
            "distribution": {
                "kind": "derived",
                "generator": "weibull_service_life",
                "parameters": {
                    "shape": specification["shape"],
                    "scale_years": specification["scale_years"],
                    "mean_years": specification["mean_years"],
                    "median_years": specification["median_years"],
                },
            },
            "applicability": {
                "geographies": ["Europe"],
                "technology": specification["technology"],
                "publication_year": 2026,
            },
            "assumptions": [
                "The European Commission market model's Weibull retirement parameters are prospective modelling assumptions, not observations of the current operating-stock age composition.",
                "The model groups products and fills evidence gaps with defaults; no runtime construction-timing assignment is inferred from these values.",
                "Component replacements and refurbishment within an appliance life are not represented.",
            ],
            "evidence": {
                "status": "modelled",
                "source_ids": ["ec-vhk-heating-cooling-market-data-2026"],
                "quality_flags": [
                    "prospective-lifetime-model",
                    "weibull-model-assumption",
                    "not-operating-fleet-age",
                ],
                "coverage": {
                    "technology": specification["technology"],
                    "shape": specification["shape"],
                    "scale_years": specification["scale_years"],
                    "mean_years": specification["mean_years"],
                    "median_years": specification["median_years"],
                },
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Useful prospective lifetime evidence, deliberately kept separate from operating-fleet construction vintages.",
            },
        }
    return profiles


def european_grid_evidence_profiles() -> dict[str, dict]:
    """Return unresolved European low-voltage-line stock-age evidence."""

    return {
        "eurelectric-eu27-uk-low-voltage-line-age-ranges-2020": {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "derived",
                "generator": "reported_age_band_share_ranges",
                "parameters": {
                    "bands": [
                        {
                            "source_period": "over 40 years",
                            "minimum_years": 40,
                            "maximum_years": None,
                            "minimum_share": 0.35,
                            "maximum_share": 0.40,
                        },
                        {
                            "source_period": "20-40 years",
                            "minimum_years": 20,
                            "maximum_years": 40,
                            "minimum_share": 0.25,
                            "maximum_share": 0.35,
                        },
                        {
                            "source_period": "under 20 years",
                            "minimum_years": 0,
                            "maximum_years": 20,
                            "minimum_share": 0.30,
                            "maximum_share": 0.35,
                        },
                    ]
                },
            },
            "applicability": {
                "evidence_geographies": ["EU27", "United Kingdom"],
                "reference_year": 2020,
                "technology": "low-voltage distribution power lines",
            },
            "assumptions": [
                "The published ranges are retained instead of choosing midpoints that would create an unsupported point distribution.",
                "The source calls the measure a percentage of power lines but does not document whether aggregation is by route length, circuit length, or line count.",
                "The evidence covers low-voltage lines only and cannot be transferred without qualification to medium- or high-voltage lines, substations, transformers, or digital equipment.",
                "The ecoinvent low-voltage network product bundles infrastructure and is consumed through electricity-market datasets; component replacement and expansion timing are not separately observed.",
                "European evidence may be a fallback outside Europe only after a point distribution and compatible whole-network relationship have been reviewed.",
            ],
            "evidence": {
                "status": "modelled",
                "source_ids": ["eurelectric-edso-low-voltage-grid-age-2020"],
                "quality_flags": [
                    "reported-share-ranges",
                    "open-ended-oldest-age-band",
                    "aggregation-weighting-not-reported",
                    "ten-country-input",
                    "low-voltage-only",
                ],
                "coverage": {
                    "technology": "low-voltage distribution power lines",
                    "input_countries": [
                        "Denmark",
                        "France",
                        "Germany",
                        "Hungary",
                        "Ireland",
                        "Italy",
                        "Poland",
                        "Portugal",
                        "Spain",
                        "Sweden",
                    ],
                    "reported_age_share_ranges_percent": {
                        "under_20_years": [30, 35],
                        "20_to_40_years": [25, 35],
                        "over_40_years": [35, 40],
                    },
                },
            },
            "review": {
                "activation_status": "hold",
                "rationale": "Promising low-voltage evidence, but it supplies overlapping share ranges rather than a point distribution and does not establish length weighting for the bundled ecoinvent network exchange.",
            },
        }
    }


def gem_cement_profiles(source_csv: Path) -> dict[str, dict]:
    """Return capacity-weighted construction vintages for operating cement plants."""

    data = (
        pd.read_csv(source_csv, skiprows=6)
        .drop_duplicates(subset="Country", keep="first")
        .set_index("Country")
    )
    for column in [*GCCT_CEMENT_AGE_COLUMNS_2026, "unknown"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    profile_countries = {
        **GCCT_CEMENT_PROFILES,
        "gem-gcct-europe-cement-plant-construction-vintage-2026": sorted(
            GCCT_EUROPE_COUNTRIES
        ),
        "gem-gcct-europe-without-switzerland-cement-plant-construction-vintage-2026": sorted(
            GCCT_EUROPE_COUNTRIES - {"Switzerland"}
        ),
    }
    profiles = {}
    offsets = [-age for age in GCCT_CEMENT_REPRESENTATIVE_AGES_2026]
    for profile_id, countries in profile_countries.items():
        present = [country for country in countries if country in data.index]
        absent = sorted(set(countries) - set(present))
        capacities = [
            float(data.loc[present, column].fillna(0).sum())
            for column in GCCT_CEMENT_AGE_COLUMNS_2026
        ]
        unknown_capacity = float(data.loc[present, "unknown"].fillna(0).sum())
        known_capacity = sum(capacities)
        weights = [capacity / known_capacity for capacity in capacities]
        mean_age = sum(
            age * weight
            for age, weight in zip(
                GCCT_CEMENT_REPRESENTATIVE_AGES_2026, weights, strict=True
            )
        )
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": offsets,
                "weights": _round_weights(weights),
            },
            "applicability": {
                "evidence_geographies": present,
                "reference_year": 2026,
                "technology": "operating cement plants",
            },
            "assumptions": [
                "The operating cement-capacity age distribution is used as the construction-vintage distribution of cement-plant infrastructure serving current cement and clinker production.",
                "Capacity weighting is used because ecoinvent cement and clinker services are output-based; actual capacity utilisation is not observed.",
                "Closed age bands use their arithmetic midpoints.",
                "The open 50-years-and-over band is represented at 55 years; this is an explicit conservative tail assumption and will understate age where older capacity dominates.",
                "Capacity with an unknown start date is excluded and known-age bands are renormalised.",
                "Plant age is based on the reported plant start date; later kiln additions, grinding-line additions, rebuilds, and refurbishment are not separated.",
                "The profile describes operating stock composition and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["gem-global-cement-concrete-tracker-july-2026"],
                "population": "Operating cement capacity recorded by the Global Cement and Concrete Tracker.",
                "weighting": "Million tonnes per annum of operating cement capacity with a known age band.",
                "reference_period": "July 2026 release",
                "censoring": "Operating plants are conditioned on survival; the oldest band is open-ended and unknown-age capacity is excluded.",
                "sample_size": {
                    "count": round(known_capacity, 6),
                    "unit": "million tonnes annual cement capacity with known age",
                },
                "quality_flags": [
                    "capacity-weighted",
                    "surviving-fleet",
                    "open-tail-modelled",
                    "unknown-age-capacity-excluded",
                    "plant-start-year-not-unit-vintage",
                ],
                "coverage": {
                    "countries_present": present,
                    "countries_without_reported_capacity": absent,
                    "age_bands_oldest_first": GCCT_CEMENT_AGE_COLUMNS_2026,
                    "representative_ages_years_oldest_first": GCCT_CEMENT_REPRESENTATIVE_AGES_2026,
                    "operating_capacity_mtpa_oldest_first": [
                        round(capacity, 6) for capacity in capacities
                    ],
                    "known_age_capacity_mtpa": round(known_capacity, 6),
                    "unknown_age_capacity_mtpa": round(unknown_capacity, 6),
                    "unknown_age_capacity_share": round(
                        unknown_capacity / (known_capacity + unknown_capacity), 6
                    ),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": (
                    "evidence_only"
                    if profile_id
                    == "gem-gcct-europe-cement-plant-construction-vintage-2026"
                    else "active"
                ),
                "rationale": (
                    "Retained as an evidence aggregate; installed inventories use separate Switzerland and Europe-without-Switzerland locations."
                    if profile_id
                    == "gem-gcct-europe-cement-plant-construction-vintage-2026"
                    else "Cement/clinker-operation to whole-factory relationships were audited across ecoinvent 3.8-3.12."
                ),
            },
        }
    return profiles


def gem_steel_profiles(source_directory: Path) -> dict[str, dict]:
    """Return unit-capacity-weighted vintages for operating steel furnaces."""

    assets = []
    pages = sorted(source_directory.glob("assets-*.json"))
    if not pages:
        raise ValueError(f"No assets-*.json files found in {source_directory}")
    for page in pages:
        with page.open(encoding="utf-8") as stream:
            assets.extend(json.load(stream)["results"])
    unique_assets = {asset["asset_id"]: asset for asset in assets}
    profiles = {}
    for profile_id, specification in GIST_STEEL_PROFILES.items():
        category = specification["furnace_category"]
        geography = specification["geography"]
        countries = specification["countries"]
        candidates = []
        for asset in unique_assets.values():
            fields = asset.get("type_fields") or {}
            if asset.get("operating_status") != "operating":
                continue
            if fields.get("furnace_category") != category:
                continue
            if geography == "Europe" and fields.get("region") != "Europe":
                continue
            if countries and asset.get("country") not in countries:
                continue
            try:
                capacity = float(asset["capacity_value"])
            except (KeyError, TypeError, ValueError):
                continue
            if capacity <= 0:
                continue
            candidates.append((asset, capacity))

        known = []
        unknown_capacity = 0.0
        for asset, capacity in candidates:
            try:
                age = float((asset.get("type_fields") or {})["unit_age"])
            except (KeyError, TypeError, ValueError):
                unknown_capacity += capacity
                continue
            if age < 0:
                unknown_capacity += capacity
                continue
            known.append((age, capacity))

        capacity_by_age = {}
        for age, capacity in known:
            capacity_by_age[age] = capacity_by_age.get(age, 0.0) + capacity
        ages = sorted(capacity_by_age, reverse=True)
        capacities = [capacity_by_age[age] for age in ages]
        known_capacity = sum(capacities)
        total_capacity = known_capacity + unknown_capacity
        weights = [capacity / known_capacity for capacity in capacities]
        mean_age = sum(age * weight for age, weight in zip(ages, weights, strict=True))
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-age for age in ages],
                "weights": _round_weights(weights),
            },
            "applicability": {
                "evidence_geographies": [geography],
                "reference_year": 2026,
                "technology": GIST_FURNACE_NAMES[category],
            },
            "assumptions": [
                "The capacity-weighted age distribution of operating furnace units is used as the construction-vintage distribution of furnace infrastructure serving current iron or steel production.",
                "Unit ages, rather than plant ages, are used so later furnace additions are represented separately.",
                "Capacity weighting is used because ecoinvent iron and steel services are output-based; actual capacity utilisation is not observed.",
                "Operating and operating-pre-retirement units are included through the API's operating status; retired, planned, and cancelled units are excluded.",
                "Units with unknown ages are excluded and known-age capacity is renormalised.",
                "Blast-furnace relinings and other major rebuilds do not reset the reported construction age; component replacement and refurbishment timing are not separately represented.",
                "The profile describes operating stock composition and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["gem-global-iron-steel-tracker-september-2026-v1"],
                "population": f"Operating {GIST_FURNACE_NAMES[category]} in the Global Iron and Steel Tracker for {geography}.",
                "weighting": "Reported current furnace capacity in thousand tonnes per annum.",
                "reference_period": "September 2026 V1 API snapshot",
                "censoring": "Operating furnace units are conditioned on survival; units without a numeric age are excluded.",
                "sample_size": {
                    "count": len(known),
                    "unit": "operating furnace units with known age",
                },
                "quality_flags": [
                    "unit-capacity-weighted",
                    "surviving-fleet",
                    "unknown-age-capacity-excluded",
                    "relining-not-age-reset",
                    "capacity-not-utilisation-weighted",
                    *(
                        ["low-known-age-capacity-coverage"]
                        if known_capacity / total_capacity < 0.5
                        else []
                    ),
                    *(["small-sample"] if len(known) < 30 else []),
                ],
                "coverage": {
                    "furnace_category": category,
                    "geography": geography,
                    "countries": sorted(countries) if countries else None,
                    "operating_unit_count": len(candidates),
                    "known_age_unit_count": len(known),
                    "known_age_capacity_ttpa": round(known_capacity, 6),
                    "unknown_age_capacity_ttpa": round(unknown_capacity, 6),
                    "known_age_capacity_share": round(
                        known_capacity / total_capacity, 6
                    ),
                    "minimum_age_years": min(ages),
                    "maximum_age_years": max(ages),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Iron/steel-operation to whole-furnace relationships were audited across ecoinvent 3.8-3.12.",
            },
        }
    return profiles


def gem_pipeline_profiles(
    gas_source_directory: Path, oil_source_directory: Path
) -> dict[str, dict]:
    """Return length-weighted vintages for operating transmission pipelines."""

    source_directories = {
        "gas-pipeline": gas_source_directory,
        "oil-pipeline": oil_source_directory,
    }
    assets_by_type = {}
    for asset_type, source_directory in source_directories.items():
        assets = []
        pages = sorted(source_directory.glob("assets-*.json"))
        if not pages:
            raise ValueError(f"No assets-*.json files found in {source_directory}")
        for page in pages:
            with page.open(encoding="utf-8") as stream:
                assets.extend(json.load(stream)["results"])
        assets_by_type[asset_type] = {
            asset["asset_id"]: asset for asset in assets
        }.values()

    profiles = {}
    for profile_id, specification in GEM_PIPELINE_PROFILES.items():
        asset_type = specification["asset_type"]
        geography = specification["geography"]
        reference_year = specification["reference_year"]
        operating_count = 0
        candidates = []
        omitted_length_count = 0
        for asset in assets_by_type[asset_type]:
            fields = asset.get("type_fields") or {}
            if asset.get("operating_status") != "operating":
                continue
            if geography == "Europe" and fields.get("startregion") != "Europe":
                continue
            operating_count += 1
            try:
                length_km = float(fields["lengthmergedkm"])
            except (KeyError, TypeError, ValueError):
                omitted_length_count += 1
                continue
            if length_km <= 0:
                omitted_length_count += 1
                continue
            candidates.append((asset, length_km))

        known = []
        unknown_start_year_length = 0.0
        multiple_start_year_count = 0
        estimated_length = 0.0
        for asset, length_km in candidates:
            fields = asset.get("type_fields") or {}
            try:
                known_length = float(fields["lengthknownkm"])
            except (KeyError, TypeError, ValueError):
                known_length = 0.0
            if known_length <= 0:
                estimated_length += length_km

            start_years = []
            for field in ("startyear1", "startyear2", "startyear3"):
                try:
                    start_year = int(float(fields[field]))
                except (KeyError, TypeError, ValueError):
                    continue
                if 1800 <= start_year <= reference_year:
                    start_years.append(start_year)
            if not start_years:
                unknown_start_year_length += length_km
                continue
            if len(start_years) > 1:
                multiple_start_year_count += 1
            age = reference_year - min(start_years)
            known.append((age, length_km))

        length_by_age = {}
        for age, length_km in known:
            length_by_age[age] = length_by_age.get(age, 0.0) + length_km
        ages = sorted(length_by_age, reverse=True)
        lengths = [length_by_age[age] for age in ages]
        known_length = sum(lengths)
        total_length = known_length + unknown_start_year_length
        weights = [length_km / known_length for length_km in lengths]
        mean_age = sum(age * weight for age, weight in zip(ages, weights, strict=True))
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-age for age in ages],
                "weights": _round_weights(weights),
            },
            "applicability": {
                "evidence_geographies": [geography],
                "reference_year": reference_year,
                "technology": specification["technology"],
            },
            "assumptions": [
                "The length-weighted age distribution of operating transmission-pipeline phases is used as the construction-vintage distribution of pipeline infrastructure serving current transport.",
                "Merged reported or route-estimated pipeline length is used because the ecoinvent capital exchange is measured in kilometres; throughput and capacity utilisation are not observed.",
                "Only operating phases are included; planned, cancelled, idle, mothballed, and retired phases are excluded.",
                "Where a phase has multiple valid start years, the earliest is used as the original construction year because the source does not allocate phase length among later expansions or recommissioning dates.",
                "Only four-digit start years from 1800 through the source reference year are accepted; phases without a valid start year are excluded and known-age length is renormalised.",
                "The European profile follows the source start-region classification; cross-region length is not split between regions.",
                "The trackers cover transmission pipelines and exclude distribution and gathering networks, matching the activated long-distance transport relationships.",
                "Repairs, replacement sections, reversals, and refurbishment do not reset the reported original start year unless represented as a separate source phase.",
                "The profile describes operating stock composition and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [specification["source_id"]],
                "population": f"Operating {specification['technology']} recorded by Global Energy Monitor for {geography}.",
                "weighting": "Merged reported or route-estimated pipeline length in kilometres for phases with a valid start year.",
                "reference_period": str(reference_year),
                "censoring": "Operating phases are conditioned on survival; phases without a valid start year are excluded.",
                "sample_size": {
                    "count": len(known),
                    "unit": "operating pipeline phases with positive length and known start year",
                },
                "quality_flags": [
                    "length-weighted",
                    "surviving-fleet",
                    "reported-or-estimated-length-weighted",
                    "unknown-start-year-length-excluded",
                    "earliest-reported-start-year",
                    "throughput-not-observed",
                ],
                "coverage": {
                    "asset_type": asset_type,
                    "geography": geography,
                    "operating_phase_count": operating_count,
                    "positive_length_phase_count": len(candidates),
                    "known_start_year_phase_count": len(known),
                    "phases_omitted_for_missing_or_nonpositive_length": omitted_length_count,
                    "phases_with_multiple_valid_start_years": multiple_start_year_count,
                    "known_start_year_length_km": round(known_length, 6),
                    "unknown_start_year_length_km": round(unknown_start_year_length, 6),
                    "known_start_year_length_share": round(
                        known_length / total_length, 6
                    ),
                    "route_estimated_length_km": round(estimated_length, 6),
                    "minimum_age_years": min(ages),
                    "maximum_age_years": max(ages),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Pipeline-transport to whole-transmission-pipeline relationships were audited across ecoinvent 3.8-3.12.",
            },
        }
    return profiles


def british_passenger_rail_profiles() -> dict[str, dict]:
    """Preserve ORR's reported bands and a reviewed construction approximation."""
    shares = [19, 23, 3, 11, 19, 26]
    bounds = [(0, 5), (6, 10), (11, 15), (16, 20), (21, 25), (26, None)]
    weights = _round_weights([share / sum(shares) for share in shares])
    ages = [2.5, 8, 13, 18, 23, 30]
    source_id = "orr-passenger-rolling-stock-maintenance-review-2026"
    evidence = {
        "status": "empirical",
        "source_ids": [source_id],
        "population": "GB mainline passenger rolling-stock vehicles represented in ORR stakeholder submissions, excluding TARA fleets.",
        "weighting": "Reported share of vehicles; percentages normalised from a rounded total of 101%.",
        "reference_period": "2026 review; exact fleet observation date not specified for Figure 3.2",
        "censoring": "Surviving fleets; open 26-plus age band; TARA fleets representing 6% of vehicles are excluded by the source.",
        "sample_size": {
            "not_reported": "Figure 3.2 does not state the number of represented vehicles.",
            "unit": "passenger rail vehicles",
        },
        "quality_flags": [
            "vehicle-count-weighted",
            "rounded-age-band-shares",
            "partial-fleet-coverage",
            "britain-proxy-fallback",
        ],
        "coverage": {
            "geography": "Great Britain",
            "reported_shares_percent_youngest_first": shares,
            "reported_share_total_percent": sum(shares),
            "excluded_tara_fleet_share": 0.06,
        },
    }
    raw_id = "orr-gb-passenger-rail-age-bands-2026"
    raw = {
        "quantity": "fleet_age",
        "reference_event": "current_service",
        "distribution": {
            "kind": "derived",
            "generator": "reported_age_band_shares",
            "parameters": {
                "bands": [
                    {"minimum_years": lo, "maximum_years": hi, "weight": weight}
                    for (lo, hi), weight in zip(bounds, weights, strict=True)
                ]
            },
        },
        "applicability": {
            "evidence_geographies": ["Great Britain"],
            "reference_year": 2026,
            "technology": "mainline passenger rail vehicles",
        },
        "assumptions": [
            "Reported integer-age labels are retained; the open oldest band has no inferred maximum."
        ],
        "evidence": evidence,
        "review": {
            "activation_status": "evidence_only",
            "rationale": "Preserves the observed bands separately from runtime point approximations.",
        },
    }
    runtime = {
        "quantity": "construction_timing",
        "reference_event": "current_service",
        "distribution": {
            "kind": "discrete",
            "offsets_years": [-age for age in reversed(ages)],
            "weights": list(reversed(weights)),
        },
        "applicability": {
            "evidence_geographies": ["Great Britain"],
            "reference_year": 2026,
            "technology": "regional and conventional long-distance passenger trains",
            "proxy_policy": "Use GB mainline evidence outside Britain only while no stronger country-specific profile matches.",
        },
        "assumptions": [
            "The observed mixed GB mainline fleet is a proxy for regional and conventional long-distance whole trains; high-speed, urban, tram and freight callers are excluded.",
            "Closed integer-age bands use label midpoints 2.5, 8, 13, 18 and 23 years; 26-plus is represented at 30 years as an explicit modelling assumption.",
            "The oldest representative age is not an observed tail mean or maximum; using 26 or 40 years changes the overall mean to 15.66 or 19.26 years respectively.",
            "Rounded reported shares sum to 101% and are normalised; the resulting approximate mean at the default tail is 16.69 years.",
            "The review excludes TARA fleets, representing 6% of vehicles, and does not publish separate age profiles by traction or service class.",
            "Vehicle weighting approximates train construction vintages; train lengths, structural masses and passenger-kilometres are not observed.",
            "Refurbishment and maintenance do not reset original vehicle age. The profile represents surviving fleet composition, not prospective lifetime or full attribution to current transport.",
        ],
        "evidence": {
            **evidence,
            "coverage": {
                **evidence["coverage"],
                "derived_from_profile": raw_id,
                "representative_ages_years_youngest_first": ages,
                "derived_mean_age_years": round(
                    sum(a * w for a, w in zip(ages, weights, strict=True)), 6
                ),
            },
        },
        "review": {
            "activation_status": "active",
            "rationale": "Reviewed GB proxy limited to regional and conventional long-distance whole-train service relationships.",
        },
    }
    return {raw_id: raw, "orr-gb-passenger-rail-construction-vintage-2026": runtime}


def european_freight_wagon_profiles() -> dict[str, dict]:
    """Return reported and runtime European freight-wagon stock vintages."""

    known_count = sum(
        count for _, _, _, count in EU_FREIGHT_WAGON_MANUFACTURING_BANDS_2019
    )
    weights = _round_weights(
        [
            count / known_count
            for _, _, _, count in EU_FREIGHT_WAGON_MANUFACTURING_BANDS_2019
        ]
    )
    bands = [
        {
            "source_period": source_period,
            "minimum_years": minimum_years,
            "maximum_years": maximum_years,
            "weight": weight,
        }
        for (source_period, minimum_years, maximum_years, _), weight in zip(
            EU_FREIGHT_WAGON_MANUFACTURING_BANDS_2019, weights, strict=True
        )
    ]
    mean_age = sum(
        age * weight
        for age, weight in zip(
            EU_FREIGHT_WAGON_REPRESENTATIVE_AGES_2019, weights, strict=True
        )
    )
    shared_coverage = {
        "geography": "Europe",
        "reference_year": 2019,
        "known_manufacturing_year_count_thousands_approximate": known_count,
        "unknown_manufacturing_year_count_thousands_approximate": (
            EU_FREIGHT_WAGON_UNKNOWN_COUNT_THOUSANDS_2019
        ),
        "known_manufacturing_year_share": round(
            known_count / (known_count + EU_FREIGHT_WAGON_UNKNOWN_COUNT_THOUSANDS_2019),
            6,
        ),
        "digitized_counts_thousands_oldest_first": [
            count for _, _, _, count in EU_FREIGHT_WAGON_MANUFACTURING_BANDS_2019
        ],
    }
    source_id = "ec-railway-guidelines-final-report-2023"
    return {
        "ec-eu-freight-wagon-manufacturing-year-bands-2019": {
            "quantity": "fleet_age",
            "reference_event": "current_service",
            "distribution": {
                "kind": "derived",
                "generator": "uniform_within_reported_age_bands",
                "parameters": {"bands": bands},
            },
            "applicability": {
                "evidence_geographies": ["Europe"],
                "reference_year": 2019,
                "technology": "freight wagons",
            },
            "assumptions": [
                "The report's manufacturing-year bars were digitised to approximate counts in thousands; small transcription and chart-reading errors are possible.",
                "The unknown-manufacturing-year bar is excluded and the known-year bands are renormalised.",
                "The open pre-1960 band is retained without imposing an unsupported upper age.",
                "Wagon counts are not weighted by payload, distance travelled, or tonne-kilometres.",
                "The evidence describes the surviving registered stock and must not be interpreted as prospective service life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [source_id],
                "population": "Freight wagons in the European Centralised Virtual Vehicle Register sample used by the report.",
                "weighting": "Approximate freight-wagon count digitised from Figure 15.",
                "reference_period": "2019",
                "censoring": "The oldest band is open before 1960; wagons without a manufacturing year are excluded.",
                "sample_size": {
                    "count": known_count * 1000,
                    "unit": "freight wagons with a known manufacturing-year band, approximate",
                },
                "quality_flags": [
                    "vehicle-count-weighted",
                    "surviving-fleet",
                    "digitized-figure",
                    "rounded-thousand-wagon-counts",
                    "open-ended-oldest-age-band",
                    "unknown-manufacturing-year-excluded",
                ],
                "coverage": shared_coverage,
            },
            "review": {
                "activation_status": "evidence_only",
                "rationale": "Preserves the reported age-band resolution and open oldest band without adding a runtime point approximation.",
            },
        },
        "ec-eu-freight-wagon-construction-vintage-2019": {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [
                    -age for age in EU_FREIGHT_WAGON_REPRESENTATIVE_AGES_2019
                ],
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": ["Europe"],
                "reference_year": 2019,
                "technology": "freight wagons",
                "proxy_policy": "Use outside Europe only when no higher-precedence geography-specific profile matches.",
            },
            "assumptions": [
                "The surviving-fleet manufacturing-year distribution is used as the construction-vintage distribution of wagons serving current freight transport.",
                "Closed manufacturing-year bands are represented by their arithmetic midpoint ages relative to 2019.",
                "The open pre-1960 band is represented at age 64 years (manufacturing year 1955); this explicit approximation can understate the age of older wagons.",
                "The unknown-manufacturing-year bar is excluded and the known-year bands are renormalised.",
                "Wagon counts are not weighted by payload, distance travelled, or tonne-kilometres.",
                "The profile describes observed stock composition and must not be interpreted as prospective service life or an attribution-based production profile.",
                "European evidence is a lower-precedence fallback outside Europe until better regional evidence is available.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": [source_id],
                "population": "Freight wagons in the European Centralised Virtual Vehicle Register sample used by the report.",
                "weighting": "Approximate freight-wagon count digitised from Figure 15.",
                "reference_period": "2019",
                "censoring": "The open oldest band is represented at age 64 years; wagons without a manufacturing year are excluded.",
                "sample_size": {
                    "count": known_count * 1000,
                    "unit": "freight wagons with a known manufacturing-year band, approximate",
                },
                "quality_flags": [
                    "vehicle-count-weighted",
                    "surviving-fleet",
                    "digitized-figure",
                    "rounded-thousand-wagon-counts",
                    "representative-age-assumption",
                    "open-tail-modelled",
                    "unknown-manufacturing-year-excluded",
                    "europe-proxy-fallback",
                ],
                "coverage": {
                    **shared_coverage,
                    "representative_ages_years_oldest_first": list(
                        EU_FREIGHT_WAGON_REPRESENTATIVE_AGES_2019
                    ),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Freight-train services expose one stable whole-goods-wagon exchange in ecoinvent 3.8-3.10; maintenance and traction equipment are excluded.",
            },
        },
    }


def gem_coal_mine_profiles(source_directory: Path) -> dict[str, dict]:
    """Return production-weighted vintages for operating coal mines."""

    pages = sorted(source_directory.glob("assets-*.json"))
    if not pages:
        raise ValueError(f"No assets-*.json files found in {source_directory}")
    assets_by_id = {}
    for page in pages:
        with page.open(encoding="utf-8") as stream:
            for asset in json.load(stream)["results"]:
                assets_by_id[asset["asset_id"]] = asset

    profiles = {}
    reference_year = 2026
    for profile_id, specification in GEM_COAL_MINE_PROFILES.items():
        mine_type = specification["mine_type"]
        geography = specification["geography"]
        operating_count = 0
        weighted_candidates = []
        production_weight_count = 0
        capacity_fallback_count = 0
        production_weight_mtpa = 0.0
        capacity_fallback_weight_mtpa = 0.0
        for asset in assets_by_id.values():
            fields = asset.get("type_fields") or {}
            if asset.get("operating_status") != "operating":
                continue
            if fields.get("mine_type") != mine_type:
                continue
            if geography == "Europe" and fields.get("region") != "Europe":
                continue
            operating_count += 1
            weight = None
            weight_source = None
            for field, source in (
                ("production_mtpa", "production"),
                ("capacity_mtpa", "capacity"),
            ):
                try:
                    candidate = float(fields[field])
                except (KeyError, TypeError, ValueError):
                    continue
                if candidate > 0:
                    weight = candidate
                    weight_source = source
                    break
            if weight is None:
                continue
            if weight_source == "production":
                production_weight_count += 1
                production_weight_mtpa += weight
            else:
                capacity_fallback_count += 1
                capacity_fallback_weight_mtpa += weight
            weighted_candidates.append((asset, weight))

        known = []
        unknown_opening_year_weight = 0.0
        for asset, weight in weighted_candidates:
            fields = asset.get("type_fields") or {}
            try:
                opening_year = int(float(fields["opening_year"]))
            except (KeyError, TypeError, ValueError):
                unknown_opening_year_weight += weight
                continue
            if not 1800 <= opening_year <= reference_year:
                unknown_opening_year_weight += weight
                continue
            known.append((reference_year - opening_year, weight))

        weight_by_age = {}
        for age, weight in known:
            weight_by_age[age] = weight_by_age.get(age, 0.0) + weight
        ages = sorted(weight_by_age, reverse=True)
        weights_mtpa = [weight_by_age[age] for age in ages]
        known_weight = sum(weights_mtpa)
        total_weight = known_weight + unknown_opening_year_weight
        if not known_weight:
            raise ValueError(
                f"No operating {geography} {mine_type} coal mines have positive "
                "production/capacity and a valid opening year"
            )
        weights = _round_weights([weight / known_weight for weight in weights_mtpa])
        mean_age = sum(age * weight for age, weight in zip(ages, weights, strict=True))
        profiles[profile_id] = {
            "quantity": "construction_timing",
            "reference_event": "current_service",
            "distribution": {
                "kind": "discrete",
                "offsets_years": [-age for age in ages],
                "weights": weights,
            },
            "applicability": {
                "evidence_geographies": [geography],
                "reference_year": reference_year,
                "technology": specification["technology"],
            },
            "assumptions": [
                "The production-weighted age distribution of operating mines is used as the construction-vintage distribution of mine infrastructure serving current coal production.",
                "Latest reported annual production is preferred; positive mine capacity is used only when production is unavailable or nonpositive.",
                "Only mines classified by the tracker as operating and as the specified surface or underground type are included.",
                "Opening year is treated as original mine-infrastructure construction timing; later expansions, equipment replacement, and major redevelopment do not reset it.",
                "Only opening years from 1800 through 2026 are accepted; mines without a valid opening year are excluded and known-age weight is renormalised.",
                "Current production/capacity weighting approximates the output relevance of mine infrastructure but does not reproduce historical utilisation.",
                "The profile describes operating stock composition and must not be interpreted as prospective mine life.",
            ],
            "evidence": {
                "status": "empirical",
                "source_ids": ["gem-global-coal-mine-tracker-may-2026"],
                "population": f"Operating {specification['technology']} recorded by Global Energy Monitor for {geography}.",
                "weighting": "Latest positive annual production in Mtpa, falling back to positive mine capacity when production is unavailable.",
                "reference_period": "May 2026 tracker release",
                "censoring": "Operating mines are conditioned on survival; mines without valid opening years or positive production/capacity are excluded.",
                "sample_size": {
                    "count": len(known),
                    "unit": "operating mines with positive production/capacity and known opening year",
                },
                "quality_flags": [
                    "production-weighted",
                    "capacity-fallback",
                    "surviving-fleet",
                    "unknown-opening-year-weight-excluded",
                    "redevelopment-not-age-reset",
                ],
                "coverage": {
                    "mine_type": mine_type,
                    "geography": geography,
                    "operating_mine_count": operating_count,
                    "positive_weight_mine_count": len(weighted_candidates),
                    "known_opening_year_mine_count": len(known),
                    "production_weight_mine_count": production_weight_count,
                    "capacity_fallback_mine_count": capacity_fallback_count,
                    "production_weight_mtpa": round(production_weight_mtpa, 6),
                    "capacity_fallback_weight_mtpa": round(
                        capacity_fallback_weight_mtpa, 6
                    ),
                    "known_opening_year_weight_mtpa": round(known_weight, 6),
                    "unknown_opening_year_weight_mtpa": round(
                        unknown_opening_year_weight, 6
                    ),
                    "known_opening_year_weight_share": round(
                        known_weight / total_weight, 6
                    ),
                    "minimum_age_years": min(ages),
                    "maximum_age_years": max(ages),
                    "derived_mean_age_years": round(mean_age, 6),
                },
            },
            "review": {
                "activation_status": "active",
                "rationale": "Coal-mine operations expose stable whole-mine-infrastructure exchanges across ecoinvent 3.8-3.12.",
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
    parser.add_argument("--eurostat-aircraft-file", type=Path, required=True)
    parser.add_argument("--eia-workbook", type=Path, required=True)
    parser.add_argument("--moderate-building-workbook", type=Path, required=True)
    parser.add_argument("--gem-cement-capacity-csv", type=Path, required=True)
    parser.add_argument("--gem-steel-assets-directory", type=Path, required=True)
    parser.add_argument("--gem-coal-mine-assets-directory", type=Path, required=True)
    parser.add_argument("--gem-gas-pipeline-assets-directory", type=Path, required=True)
    parser.add_argument("--gem-oil-pipeline-assets-directory", type=Path, required=True)
    parser.add_argument("--gem-solar-workbook", type=Path, required=True)
    parser.add_argument("--gem-bioenergy-assets-directory", type=Path, required=True)
    parser.add_argument("--gem-chemicals-json", type=Path, required=True)
    parser.add_argument("--gem-wind-geojson", type=Path, required=True)
    parser.add_argument("--igu-lng-report-pdf", type=Path, required=True)
    parser.add_argument("--dft-vehicle-ods", type=Path, required=True)
    parser.add_argument("--bnetza-chargers-csv", type=Path, required=True)
    parser.add_argument("--eia-storage-workbook", type=Path, required=True)
    parser.add_argument("--nbd-factory-workbook", type=Path, required=True)
    parser.add_argument("--nod-facilities-csv", type=Path, required=True)
    parser.add_argument("--nod-history-csv", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    profiles = reviewed_operational_profiles()
    profiles.update(technical_life_profiles())
    profiles.update(vehicle_survival_profiles())
    profiles.update(ship_construction_profiles())
    profiles.update(ccnr_inland_vessel_profiles())
    profiles.update(european_heating_profiles())
    profiles.update(european_grid_evidence_profiles())
    profiles.update(moderate_building_profiles(args.moderate_building_workbook))
    profiles.update(gem_cement_profiles(args.gem_cement_capacity_csv))
    profiles.update(gem_steel_profiles(args.gem_steel_assets_directory))
    profiles.update(european_freight_wagon_profiles())
    profiles.update(british_passenger_rail_profiles())
    profiles.update(gem_coal_mine_profiles(args.gem_coal_mine_assets_directory))
    profiles.update(
        gem_pipeline_profiles(
            args.gem_gas_pipeline_assets_directory,
            args.gem_oil_pipeline_assets_directory,
        )
    )
    profiles.update(gem_csp_profiles(args.gem_solar_workbook))
    profiles.update(gem_bioenergy_profiles(args.gem_bioenergy_assets_directory))
    profiles.update(chemical_site_evidence_profiles(args.gem_chemicals_json))
    profiles.update(offshore_wind_profiles(args.gem_wind_geojson))
    profiles.update(igu_lng_carrier_profiles(args.igu_lng_report_pdf))
    profiles.update(dft_car_profiles(args.dft_vehicle_ods))
    profiles.update(construction_event_profiles())
    profiles.update(bnetza_charger_profiles(args.bnetza_chargers_csv))
    profiles.update(eia_storage_profiles(args.eia_storage_workbook))
    profiles.update(nbd_factory_profiles(args.nbd_factory_workbook))
    profiles.update(sigeif_low_voltage_profiles())
    profiles.update(remaining_charger_profiles())
    profiles.update(
        nod_platform_profiles(args.nod_facilities_csv, args.nod_history_csv)
    )
    profiles.update(eurostat_profiles(args.eurostat_directory))
    profiles.update(eurostat_aircraft_profiles(args.eurostat_aircraft_file))
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
