"""IEA PVPS 2026 photovoltaic inventory integration.

The reference inventories are immutable benchmarks. Country electricity adapters
are generated separately; this module never reinterprets their yields as shares.
"""

from collections import defaultdict
from copy import deepcopy
import math
import re

PV_TECHNOLOGIES = {
    "single-crystalline silicon": "single-Si",
    "multicrystalline silicon": "multi-Si",
    "cadmium telluride": "CdTe",
}


def use_pv_2026(version, system_model):
    """Only select configurations validated against the new cut-off inventory."""
    return version == "3.12" and system_model == "cutoff"


def identity(dataset):
    return (
        dataset.get("name"),
        dataset.get("product", dataset.get("reference product")),
        dataset.get("location"),
        dataset.get("unit"),
    )


def installation_metadata(dataset):
    """Describe the installed system, with MW converted explicitly to kWp."""
    name = dataset["name"].lower()
    if not name.startswith("photovoltaic installation construction"):
        return {}
    match = re.search(r"(\d+(?:\.\d+)?)\s+(kwp|mwp|mw)\b", name)
    technology = next((v for k, v in PV_TECHNOLOGIES.items() if k in name), None)
    if not match or not technology:
        raise ValueError(f"Unknown PV installation: {name}")
    capacity = float(match[1]) * (1000 if match[2].startswith("mw") else 1)
    mounting = (
        "ground"
        if "ground-mounted" in name
        else "facade" if "facade" in name else "roof"
    )
    return {
        "pv technology": technology,
        "pv capacity kwp": capacity,
        "pv mounting": mounting,
        "pv segment": "residential" if capacity <= 10 else "commercial",
        "pv lifetime years": 30,
    }


def module_exchange(exchange):
    """Module area excludes mounting structures, land and building surfaces."""
    name = exchange["name"].lower()
    return (
        exchange["type"] == "technosphere"
        and exchange["unit"] == "square meter"
        and ("photovoltaic panel" in name or "photovoltaic laminate" in name)
    )


def module_area(dataset):
    area = sum(e["amount"] for e in dataset["exchanges"] if module_exchange(e))
    if area <= 0:
        raise ValueError(f"Missing module area: {dataset['name']}")
    return area


def country_adapters(core, parameters, generation):
    """Build country recipes, normalising electricity shares within each segment.

    generation maps ISO2 locations to PV generation in GWh (2023). Missing or
    rounded-zero generation remains zero; it is never used as a donor weight.
    Atlas yields follow the original workbook convention without an additional
    degradation multiplier. The preserved reference activities are not modified.
    """
    plants = {identity(d): d for d in core if installation_metadata(d)}
    for plant in plants.values():
        plant.update(installation_metadata(plant))
    report = {
        d["location"]: d
        for d in core
        if d["name"] == "electricity production mix, photovoltaic"
    }
    yields = parameters["report_yields"]
    lifetime = parameters["lifetime_years"]
    shares = {}
    for country, dataset in report.items():
        country_shares = defaultdict(float)
        for exchange in dataset["exchanges"]:
            if exchange["type"] != "technosphere" or identity(exchange) not in plants:
                continue
            plant = plants[identity(exchange)]
            country_shares[identity(plant)] += (
                exchange["amount"]
                * plant["pv capacity kwp"]
                * lifetime
                * yields[country][plant["pv mounting"]]
            )
        total = sum(country_shares.values())
        if not math.isclose(total, 1, abs_tol=0.005):
            raise ValueError(f"PV mix does not sum to one in {country}: {total}")
        shares[country] = {k: v / total for k, v in country_shares.items() if v > 0}
    total_generation = sum(generation[c] for c in report)
    if total_generation <= 0:
        raise ValueError("PV donor generation must be positive")
    average = defaultdict(float)
    average_yield = {mounting: 0.0 for mounting in ("roof", "facade", "ground")}
    for country, mix in shares.items():
        weight = generation[country] / total_generation
        for key, value in mix.items():
            average[key] += value * weight
        for mounting in average_yield:
            average_yield[mounting] += yields[country][mounting] * weight

    activities = []
    for country in parameters["locations"]:
        iso = country
        atlas_yield = parameters["atlas_ground_yields"].get(
            country,
            parameters["atlas_ground_yields"].get("CN-TW", 0) if country == "TW" else 0,
        )
        mix = shares.get(country, average)
        if country in yields:
            output = yields[country]
            yield_origin = "reported national yield"
        elif atlas_yield > 0:
            output = {
                k: atlas_yield * factor
                for k, factor in parameters["yield_factors"].items()
            }
            yield_origin = (
                "Global Solar Atlas; roof = 0.94 and facade = 0.66 of ground yield"
            )
        else:
            output = average_yield
            yield_origin = (
                "fallback: generation-weighted mean yield of report countries"
            )
        mix_origin = (
            "reported national mix"
            if country in shares
            else "generation-weighted mean mix of report countries"
        )
        template = report.get(country, report["CH"])
        for segment in ("residential", "commercial", "production mix"):
            selected = {
                k: v
                for k, v in mix.items()
                if segment == "production mix" or plants[k]["pv segment"] == segment
            }
            fraction = sum(selected.values())
            # A zero historical segment still needs a prospective supplier.
            if fraction <= 0:
                selected = {
                    k: v
                    for k, v in average.items()
                    if plants[k]["pv segment"] == segment
                }
            subtotal = sum(selected.values())
            product = (
                "electricity, low voltage"
                if segment == "residential"
                else (
                    "electricity, medium voltage"
                    if segment == "commercial"
                    else "electricity, photovoltaic, at plant"
                )
            )
            dataset = {
                "name": f"electricity production, photovoltaic, {segment}",
                "reference product": product,
                "location": country,
                "unit": "kilowatt hour",
                "type": "process",
                "production amount": 1,
                "source": "; ".join(
                    parameters[k]
                    for k in ("source", "generation_source", "atlas_source")
                ),
                "pv mix origin": mix_origin,
                "pv yield origin": yield_origin,
                "pv segment share": fraction,
                "pv generation year": 2023,
                "pv generation status": (
                    "reported (rounded GWh)"
                    if iso in generation
                    else "unavailable; weighting volume set to zero"
                ),
                "pv lifetime years": lifetime,
                "comment": (
                    f"Grid-connected {segment} PV electricity; {mix_origin}; {yield_origin}. "
                    f"Annual yield in kWh/kWp: roof {output['roof']:.6g}, facade {output['facade']:.6g}, "
                    f"ground {output['ground']:.6g}. {lifetime}-year lifetime. "
                    "Installation input = normalised electricity share / (capacity in kWp × annual yield × lifetime). "
                    "No additional degradation multiplier, consistent with the source workbook coefficients. "
                    "Manufacturing suppliers retain their original geography. Cleaning uses 20 litres per square metre "
                    "of module over its lifetime; 90% becomes wastewater and 10% evaporates. "
                    + (
                        "Cleaning water and wastewater suppliers use the Swiss proxy. "
                        if country not in report
                        else ""
                    )
                    + (
                        "Zero historical segment: global segment composition used for prospective supply. "
                        if fraction <= 0
                        else ""
                    )
                    + (
                        "Residential output is assigned to low voltage; commercial and utility output to medium voltage, "
                        "without adding an unreported transformer or distribution loss."
                        if segment != "production mix"
                        else ""
                    )
                ),
                "exchanges": [],
            }
            production = {
                k: dataset[k] for k in ("name", "reference product", "location", "unit")
            }
            production.update(
                type="production",
                amount=1,
                **{"production volume": generation.get(iso, 0) * 1e6 * fraction},
            )
            dataset["exchanges"].append(production)
            area = 0
            for key, share in sorted(selected.items()):
                plant = plants[key]
                amount = (
                    share
                    / subtotal
                    / (
                        plant["pv capacity kwp"]
                        * lifetime
                        * output[plant["pv mounting"]]
                    )
                )
                exchange = {
                    k: plant[k]
                    for k in ("name", "reference product", "location", "unit")
                }
                exchange.update(
                    type="technosphere",
                    amount=amount,
                    comment=f"Electricity share {share / subtotal:.12g} / ({plant['pv capacity kwp']:g} kWp × {output[plant['pv mounting']]:.12g} kWh/kWp/year × {lifetime} years).",
                )
                dataset["exchanges"].append(exchange)
                area += amount * module_area(plant)
            for original in template["exchanges"]:
                if original["type"] == "production" or identity(original) in plants:
                    continue
                exchange = {
                    k: deepcopy(v)
                    for k, v in original.items()
                    if k
                    in (
                        "name",
                        "reference product",
                        "location",
                        "unit",
                        "type",
                        "categories",
                        "amount",
                    )
                    and v is not None
                }
                name = exchange["name"].lower()
                if name == "market for tap water":
                    exchange["amount"] = area * 20
                elif "treatment of wastewater" in name:
                    exchange["amount"] = -area * 20 * 0.9 / 1000
                elif name == "water" and exchange["type"] == "biosphere":
                    exchange["amount"] = area * 20 * 0.1 / 1000
                elif name not in ("heat, waste", "energy, solar, converted"):
                    raise ValueError(f"Unrecognised PV operation input: {name}")
                dataset["exchanges"].append(exchange)
            activities.append(dataset)
    return activities
