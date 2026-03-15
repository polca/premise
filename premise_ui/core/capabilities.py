"""Capability helpers for the Premise UI scaffold."""

from __future__ import annotations

from premise_ui.core.premise_metadata import (
    load_premise_constants,
    load_premise_version,
)
from premise_ui.core.scenario_catalog import (
    list_local_iam_scenarios,
    load_iam_scenario_catalog,
)

GUI_EI_VERSIONS = ["3.8", "3.9", "3.10", "3.11", "3.12"]

TRANSFORMATION_CATALOG = [
    {
        "id": "biomass",
        "label": "Biomass",
        "description": "Update biomass supply chains, markets, and regional sourcing assumptions.",
    },
    {
        "id": "electricity",
        "label": "Electricity",
        "description": "Update electricity production mixes, technologies, and regional markets.",
    },
    {
        "id": "cement",
        "label": "Cement",
        "description": "Adjust cement production routes, clinker demand, and fuel use.",
    },
    {
        "id": "steel",
        "label": "Steel",
        "description": "Update steel production routes, energy inputs, and process intensity.",
    },
    {
        "id": "fuels",
        "label": "Fuels",
        "description": "Adjust liquid and gaseous fuel supply chains and conversion efficiencies.",
    },
    {
        "id": "renewable",
        "label": "Renewables",
        "description": "Update dedicated renewable technologies beyond the electricity mix itself.",
    },
    {
        "id": "metals",
        "label": "Metals",
        "description": "Adjust metal production technologies, trade patterns, and intensities.",
    },
    {
        "id": "mining",
        "label": "Mining",
        "description": "Update mining activities, ore grades, and mineral supply assumptions.",
    },
    {
        "id": "heat",
        "label": "Heat",
        "description": "Update industrial and district heat supply technologies and fuels.",
    },
    {
        "id": "cdr",
        "label": "Carbon dioxide removal",
        "description": "Apply carbon dioxide removal technologies such as direct air capture.",
    },
    {
        "id": "battery",
        "label": "Battery",
        "description": "Update battery chemistry, manufacturing energy, and supply chain inputs.",
    },
    {
        "id": "emissions",
        "label": "Emissions",
        "description": "Scale pollutant emission factors using scenario-driven assumptions.",
    },
    {
        "id": "cars",
        "label": "Cars",
        "description": "Update passenger car technologies, fuels, and operating efficiency.",
    },
    {
        "id": "two_wheelers",
        "label": "Two-wheelers",
        "description": "Adjust motorbike and scooter technologies, fuels, and efficiency.",
    },
    {
        "id": "trucks",
        "label": "Trucks",
        "description": "Update freight truck powertrains, fuels, and transport efficiency.",
    },
    {
        "id": "ships",
        "label": "Ships",
        "description": "Adjust shipping technologies, fuels, and maritime transport intensity.",
    },
    {
        "id": "buses",
        "label": "Buses",
        "description": "Update bus technologies, fuels, and public transport assumptions.",
    },
    {
        "id": "trains",
        "label": "Trains",
        "description": "Adjust rail technologies and their electricity or fuel demand.",
    },
    {
        "id": "final energy",
        "label": "Final energy",
        "description": "Propagate final energy demand signals where Premise uses them.",
    },
    {
        "id": "external",
        "label": "External scenarios",
        "description": "Include external scenario data blocks when they are attached to a run.",
    },
]

INCREMENTAL_SECTOR_CATALOG = [
    {
        "id": "electricity",
        "label": "Electricity",
        "description": "Incrementally update electricity production and market structure.",
    },
    {
        "id": "biomass",
        "label": "Biomass",
        "description": "Incrementally update biomass supply chains and sourcing regions.",
    },
    {
        "id": "materials",
        "label": "Materials",
        "description": "Incrementally update cement, steel, metals, and related material sectors.",
    },
    {
        "id": "fuels",
        "label": "Fuels and heat",
        "description": "Incrementally update fuels, heat supply, and conversion technologies.",
    },
    {
        "id": "battery",
        "label": "Battery",
        "description": "Incrementally update battery manufacturing and supply chains.",
    },
    {
        "id": "transport",
        "label": "Transport",
        "description": "Incrementally update road, rail, and maritime transport sectors.",
    },
    {
        "id": "others",
        "label": "Other sectors",
        "description": "Incrementally update the remaining supported transformation sectors.",
    },
    {
        "id": "external",
        "label": "External scenarios",
        "description": "Incrementally include external scenario data blocks when available.",
    },
]

WORKFLOW_EXPORT_TYPES = {
    "new_database": [
        "brightway",
        "matrices",
        "datapackage",
        "simapro",
        "openlca",
        "superstructure",
    ],
    "incremental_database": [
        "brightway",
        "matrices",
        "simapro",
        "openlca",
    ],
    "pathways_datapackage": ["datapackage"],
}


def get_capabilities() -> dict:
    constants = load_premise_constants()
    scenario_catalog = load_iam_scenario_catalog()
    local_scenarios = list_local_iam_scenarios()
    supported_versions = [
        version
        for version in constants["SUPPORTED_EI_VERSIONS"]
        if version in GUI_EI_VERSIONS
    ]
    local_models = sorted({entry["model"] for entry in local_scenarios})
    local_pathways = sorted({entry["pathway"] for entry in local_scenarios})

    return {
        "workflows": ["new_database", "incremental_database", "pathways_datapackage"],
        "source_types": ["brightway", "ecospold"],
        "export_types": WORKFLOW_EXPORT_TYPES["new_database"],
        "workflow_export_types": WORKFLOW_EXPORT_TYPES,
        "ecoinvent_versions": supported_versions,
        "iam_models": local_models,
        "iam_pathways": local_pathways,
        "iam_scenarios": local_scenarios,
        "iam_scenario_catalog": scenario_catalog.get("scenarios", []),
        "transformation_catalog": TRANSFORMATION_CATALOG,
        "incremental_sector_catalog": INCREMENTAL_SECTOR_CATALOG,
        "premise_version": load_premise_version(),
    }
