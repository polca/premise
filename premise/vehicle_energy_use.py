"""Energy-specific vehicle equipment per MJ of purchased traction energy.

No whole-vehicle or road infrastructure is included. The service inventory is
the source of the normalization; its production link supplies lifetime and
payload/occupancy allocation, so these must never be applied a second time.
"""

from copy import deepcopy
import json
import math
import re

POLICY = "energy-specific equipment; excludes chassis, body, roads and whole-vehicle maintenance"
SOURCE_KEY = "energy use source"


class VehicleLookup:
    """One index per scenario; never scan the full inventory per component."""

    def __init__(self, database):
        self.by_name = {}
        self.by_identity = {}
        for ds in database:
            self.by_name.setdefault(ds["name"], []).append(ds)
            self.by_identity.setdefault(identity(ds), []).append(ds)


def identity(d):
    return (
        d["name"],
        d.get("reference product", d.get("product")),
        d["location"],
        d["unit"],
    )


def source_name(name):
    """Explicit representative classes, shared by both final-energy workbooks."""
    n = name.lower()
    if not ("used in" in n or "burned in" in n):
        return None
    electric = n.startswith("electricity")
    hydrogen = n.startswith("hydrogen")
    gas = n.startswith(("compressed gas", "biomethane"))
    diesel = "diesel" in n
    power = (
        "battery electric"
        if electric
        else (
            "fuel cell electric"
            if hydrogen
            else "compressed gas" if gas else "diesel" if diesel else "gasoline"
        )
    )
    if "passenger train" in n or "freight train" in n or "tram" in n:
        power = (
            "electric"
            if electric
            else "fuel cell, hydrogen" if hydrogen else "diesel-electric"
        )
        return f"transport, freight train, {power}"
    if "passenger car" in n:
        return f"transport, passenger car, {power}, Medium" + (
            "" if electric or hydrogen else ", EURO-6"
        )
    if "light duty vehicle" in n:
        if gas:
            return "transport, passenger car, compressed gas, Van, EURO-6"
        return f"transport, freight, lorry, {power}, 3.5 metric ton"
    if "heavy" in n and "vehicle" in n:
        return f"transport, freight, lorry, {power}, 40 metric ton"
    if "trolleybus" in n:
        return "transport, passenger bus, battery electric - battery-equipped trolleybus, LTO battery, 13m single deck urban bus"
    if "coach" in n or "urban bus" in n:
        power = "battery electric - overnight charging" if electric else power
        kind = "coach" if "dedicated coach" in n else "urban"
        return f"transport, passenger bus, {power}, 13m single deck {kind} bus" + (
            "" if electric or hydrogen else ", EURO-VI"
        )
    if "motorcycle" in n and not hydrogen:
        return f"transport, Motorbike, {power}, 11-35kW" + (
            "" if electric else ", EURO-5"
        )
    if "motorcycle" in n and hydrogen:
        return "transport, passenger car, fuel cell electric, Medium"
    return None


def carrier(e):
    if e.get("type") != "technosphere":
        return None
    p = e.get("product", e.get("reference product", "")) or ""
    n = e["name"].lower()
    u = e["unit"]
    if p.startswith("electricity") and u == "kilowatt hour":
        return "electricity"
    if (
        p == "hydrogen"
        or p.startswith("hydrogen,")
        or n.startswith("market for hydrogen,")
    ) and u == "kilogram":
        return "hydrogen"
    if (
        p.startswith(("natural gas", "biogas", "biomethane", "methane"))
        or n.startswith("market for natural gas")
    ) and u in ("kilogram", "cubic meter"):
        return "gas"
    if (
        p.startswith(("diesel", "biodiesel")) or n.startswith("market for diesel")
    ) and u == "kilogram":
        return "diesel"
    if (
        p.startswith(("petrol", "gasoline", "ethanol"))
        or n == "fuel supply for gasoline vehicles"
    ) and u == "kilogram":
        return "gasoline"
    return None


def fuel_property_key(e):
    """Distinguish ethanol, FAME, FT diesel and delivered CNG conventions."""
    c = carrier(e)
    product = e.get("product", e.get("reference product", "")) or ""
    if c == "gasoline":
        return "ethanol" if product.startswith("ethanol") else "gasoline"
    if c == "diesel":
        if "Fischer" in e["name"] or "synthetic" in e["name"]:
            return "diesel"
        return (
            "biodiesel, from oil crops" if product.startswith("biodiesel") else "diesel"
        )
    if c == "gas":
        return (
            "methane, from biomass"
            if product.startswith(("biogas", "biomethane", "methane"))
            else "natural gas"
        )
    return c


def energy_per_unit(e):
    # Same delivered-fuel conventions as premise car_energy.fuel_spec.
    c = carrier(e)
    if c == "electricity":
        return 3.6
    if c == "hydrogen":
        return 120.0
    if c in ("gasoline", "diesel", "gas"):
        from .car_energy import fuel_properties

        key = fuel_property_key(e)
        if key == "natural gas" and e["unit"] == "kilogram":
            return 46.5
        if c == "gas" and e["unit"] == "cubic meter":
            return float(fuel_properties()["natural gas"]["lhv"]["value"])
        return float(fuel_properties()[key]["lhv"]["value"])
    raise ValueError(f"Unsupported vehicle energy: {identity(e)}")


def equipment(e):
    """Explicit component classes; uncertain mixed components are not guessed."""
    n = e["name"].lower()
    return any(
        t in n
        for t in (
            "battery",
            "electric motor",
            "converter",
            "inverter",
            "power distribution",
            "charger",
            "charging station",
            "powertrain",
            "power electronics",
            "fuel cell",
            "fuel tank",
            "internal combustion engine",
            "exhaust system",
            "gearbox",
            "transmission",
            "retarder",
            "catenary",
            "overhead lines",
            "distribution sheds",
            "transformer",
            "electronic component production, passive",
        )
    ) or n.startswith(
        (
            "polyethylene production, high density",
            "market for waste plastic, industrial electronics",
        )
    )


def scaled(e, factor):
    result = deepcopy(dict(e))
    result["amount"] = float(e["amount"]) * factor
    for field in (
        "input",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "negative",
        "formula",
        "replacement name",
        "replacement product",
        "replacement location",
    ):
        result.pop(field, None)
    result["uncertainty type"] = 0
    return result


def resolve(database, exchange):
    key = identity(exchange)
    matches = (
        database.by_identity.get(key, [])
        if isinstance(database, VehicleLookup)
        else [d for d in database if identity(d) == key]
    )
    if len(matches) != 1:
        raise ValueError(f"Expected one vehicle supplier {key}, got {len(matches)}")
    return matches[0]


def select_source(database, name, location, region=None):
    candidates = (
        database.by_name.get(name, [])
        if isinstance(database, VehicleLookup)
        else [d for d in database if d["name"] == name]
    )
    # Country-specific forwarding datasets are followed below. NEU is supplied
    # explicitly for Switzerland by the calling scenario adapter.
    for loc in dict.fromkeys(
        [location, region, "RER", "Europe without Switzerland", "GLO", "CH"]
    ):
        match = [d for d in candidates if d["location"] == loc]
        if len(match) == 1:
            return match[0]
        if len(match) > 1:
            raise ValueError(f"Ambiguous vehicle source: {name}, {loc}")
    raise ValueError(f"No vehicle source: {name}, {location}, {region}")


def physical_source(source, database):
    seen = set()
    while identity(source) not in seen:
        seen.add(identity(source))
        if any(carrier(e) for e in source["exchanges"]):
            return source
        forwarding = [
            e
            for e in source["exchanges"]
            if e["type"] == "technosphere" and e["unit"] == source["unit"]
        ]
        if len(forwarding) != 1 or not math.isclose(forwarding[0]["amount"], 1):
            break
        source = resolve(database, forwarding[0])
    raise ValueError(f"No physical operating-energy source: {identity(source)}")


def normalize(wrapper, source, database, *, preserve_fuel=True):
    """Replace a wrapper with scoped source exchanges, retaining fuel geography.

    Hydrogen compression electricity is auxiliary: the denominator is delivered
    hydrogen energy, not the sum of hydrogen and compression electricity.
    Alternative-fuel supplier shares and carbon-flow splits are retained, but
    fuel mass is corrected to exactly one MJ using its own heating value.
    """
    if wrapper["unit"] != "megajoule":
        raise ValueError("Final-energy output must be megajoules")
    source = physical_source(source, database)
    main = (
        "electricity"
        if wrapper["name"].startswith("electricity")
        else (
            "hydrogen"
            if wrapper["name"].startswith("hydrogen")
            else (
                "gas"
                if wrapper["name"].startswith(("compressed gas", "biomethane"))
                else "diesel" if "diesel" in wrapper["name"] else "gasoline"
            )
        )
    )
    energies = [e for e in source["exchanges"] if carrier(e) == main]
    q = sum(float(e["amount"]) * energy_per_unit(e) for e in energies)
    if not math.isfinite(q) or q <= 0:
        raise ValueError(f"Missing positive {main} energy in {identity(source)}")
    production = [e for e in wrapper["exchanges"] if e["type"] == "production"]
    if len(production) != 1:
        raise ValueError("Wrapper must have exactly one reference production")
    output = [scaled(production[0], 0)]
    output[0].update(
        amount=1.0,
        name=wrapper["name"],
        location=wrapper["location"],
        unit="megajoule",
        product=wrapper["reference product"],
    )
    retained, excluded = [], []
    old_fuels = [e for e in wrapper["exchanges"] if carrier(e) == main]
    # Preserve alternative suppliers, not inherited gasoline-like heating values.
    alternative = any(
        t in wrapper["name"]
        for t in (
            "bio",
            "synthetic",
            "from coal",
            "from eucalyptus",
            "from residues",
            "from sugar",
        )
    )
    if alternative and preserve_fuel:
        if not old_fuels:
            raise ValueError(f"Missing alternative-fuel purchase: {wrapper['name']}")
        recipe_energy = sum(float(e["amount"]) * energy_per_unit(e) for e in old_fuels)
        if not math.isfinite(recipe_energy) or recipe_energy <= 0:
            raise ValueError("Invalid alternative-fuel energy")
        factor = (
            1 if math.isclose(recipe_energy, 1, rel_tol=1e-12) else 1 / recipe_energy
        )
        output.extend(scaled(e, factor) for e in old_fuels)
    else:
        for e in energies:
            new = scaled(e, 1 / q)
            if len(old_fuels) == 1:
                # Preserve geography but keep the source fuel grade and unit:
                # hydrogen low-pressure supply needs its compression auxiliary.
                if main == "electricity":
                    new["location"] = old_fuels[0]["location"]
                    new.update(
                        {
                            k: old_fuels[0][k]
                            for k in ("name", "product", "unit")
                            if k in old_fuels[0]
                        }
                    )
            output.append(new)
    for e in source["exchanges"]:
        if e["type"] == "production" or carrier(e) == main:
            continue
        if (
            e["type"] == "technosphere"
            and e["unit"] == "unit"
            and any(
                t in e["name"].lower()
                for t in (
                    "passenger car,",
                    "lorry production,",
                    "passenger bus,",
                    "motorbike,",
                    "production of locomotive,",
                )
            )
        ):
            vehicle = resolve(database, e)
            for component in vehicle["exchanges"]:
                if component["type"] == "production":
                    continue
                decision = equipment(component)
                row = {
                    "supplier": (
                        identity(component)
                        if component.get("location")
                        else component["name"]
                    ),
                    "amount per vehicle": component["amount"],
                    "vehicle per source service": e["amount"],
                }
                if decision:
                    if "internal combustion engine, passenger car" in component[
                        "name"
                    ] and any(
                        t in source["name"]
                        for t in ("battery electric", "fuel cell electric")
                    ):
                        row["coefficient limitation"] = (
                            "Native carculator mechanical-powertrain mass uses combustion-engine manufacturing as a material proxy; this is not a combustion engine installed in the electric vehicle."
                        )
                    if (
                        "fuel cell electric" in source["name"]
                        and component.get("product") == "electricity storage capacity"
                    ):
                        row["coefficient limitation"] = (
                            "Native nominal-kWh MIX market is a manufacturing proxy; it does not reproduce the heavier high-power physical battery pack mass."
                        )
                    replacement_factor = 1.0
                    if (
                        source["name"].startswith(
                            "transport, Motorbike, battery electric"
                        )
                        and component.get("product") == "electricity storage capacity"
                    ):
                        comment = source.get("comment", "")
                        capacity = re.search(r"Battery capacity: ([\d.]+) kWh", comment)
                        replacements = re.search(
                            r"Battery replacement throughout lifetime: ([\d.]+)",
                            comment,
                        )
                        if capacity and replacements:
                            nominal = float(capacity.group(1))
                            total = nominal * (1 + float(replacements.group(1)))
                            if math.isclose(component["amount"], nominal, rel_tol=1e-6):
                                replacement_factor = total / nominal
                                row["source correction"] = (
                                    "Include documented replacement pack omitted from source production exchange"
                                )
                            elif not math.isclose(
                                component["amount"], total, rel_tol=1e-6
                            ):
                                raise ValueError(
                                    f"Motorcycle battery quantity {component['amount']} disagrees with nominal {nominal} and replacement total {total}: {identity(source)} / {identity(vehicle)}"
                                )
                    new = scaled(component, float(e["amount"]) * replacement_factor / q)
                    new["comment"] = (
                        str(component.get("comment", ""))
                        + f" Energy-use allocation: {component['amount']} * {e['amount']} / {q} MJ per {source['unit']}."
                    )
                    output.append(new)
                    retained.append(
                        {
                            **row,
                            "replacement correction factor": replacement_factor,
                            "amount per MJ": new["amount"],
                        }
                    )
                else:
                    excluded.append(row)
        elif (
            e["type"] == "biosphere"
            or carrier(e)
            or equipment(e)
            or e["name"].startswith("treatment of ")
            or "refrigerant" in e["name"]
            or "tetrafluoroethane" in e["name"]
        ):
            if (
                alternative
                and e["type"] == "biosphere"
                and e["name"].startswith("Carbon dioxide")
            ):
                continue
            output.append(scaled(e, 1 / q))
        else:
            excluded.append(
                {"supplier": e["name"], "amount per source service": e["amount"]}
            )
    if alternative:
        from .car_energy import fuel_properties

        carbon = [
            e
            for e in wrapper["exchanges"]
            if e["type"] == "biosphere" and e["name"].startswith("Carbon dioxide")
        ]
        old_carbon = sum(float(e["amount"]) for e in carbon)
        target_carbon = sum(
            e["amount"]
            * energy_per_unit(e)
            * float(fuel_properties()[fuel_property_key(e)]["co2"])
            for e in output
            if carrier(e) == main
        )
        if old_carbon <= 0:
            raise ValueError(
                f"Missing alternative-fuel carbon accounting: {wrapper['name']}"
            )
        carbon_factor = (
            1
            if math.isclose(old_carbon, target_carbon, rel_tol=1e-12)
            else target_carbon / old_carbon
        )
        output.extend(scaled(e, carbon_factor) for e in carbon)
    if not retained:
        raise ValueError(
            f"No energy-specific vehicle equipment found: {source['name']}"
        )
    wrapper["exchanges"] = output
    record = {
        "source": identity(source),
        "source unit": source["unit"],
        "purchased MJ per source service": q,
        "boundary": POLICY,
        "retained equipment": retained,
        "excluded": excluded,
        "alternative fuel recipe retained": alternative,
    }
    if alternative:
        record["alternative fuel basis"] = (
            "Supplier mass shares retained; total fuel energy corrected to 1 MJ using premise fuel-specific LHVs. Total combustion CO2 uses the corresponding kg/MJ factor; inherited fossil/biogenic and compartment shares retained. Other exhaust factors retain the source powertrain proxy."
        )
    wrapper[SOURCE_KEY] = json.dumps(record)
    wrapper["comment"] = (
        f"Produces consumption of 1 MJ of purchased energy. Representative powertrain carrier: {main}. {POLICY}. Source {source['name']} [{source['location']}]; {q:.15g} purchased MJ per {source['unit']}. All retained exchanges divided by this energy; lifetime, occupancy/payload and replacement allocation are carried by source production links. Baseline source specifications: {source.get('comment', '')}"
    )
    return record


def refresh_vehicle_energy_emissions(scenario):
    """Propagate final distance-based emissions after GAINS, preserving fuel CO2.

    External fuel-market updates own the wrapper's fuel-derived emissions.
    Reapplying normalization here would undo their fuel mix and supplier links.
    """
    from .inventory_store import get_scenario_inventory, replace_scenario_inventory
    from .vehicle_emissions import fuel_proportional_emission

    database = get_scenario_inventory(scenario)
    wrappers = [d for d in database if SOURCE_KEY in d and not d.get("emptied")]
    if not wrappers:
        return scenario
    lookup = VehicleLookup(database)
    for ds in wrappers:
        record = ds[SOURCE_KEY]
        record = json.loads(record) if isinstance(record, str) else dict(record)
        source = lookup.by_identity.get(tuple(record["source"]), [])
        if len(source) != 1:
            raise ValueError(
                f"Cannot refresh final vehicle emissions: {record['source']}"
            )
        q = record["purchased MJ per source service"]
        retained = [
            e
            for e in ds["exchanges"]
            if e["type"] != "biosphere" or fuel_proportional_emission(e)
        ]
        for e in source[0]["exchanges"]:
            if e["type"] == "biosphere" and not fuel_proportional_emission(e):
                new = scaled(e, 1 / q)
                if "input" in e:
                    new["input"] = e["input"]
                retained.append(new)
        ds["exchanges"] = retained
        record["emissions source stage"] = (
            "after GAINS; fuel-derived emissions retain final fuel-market accounting"
        )
        ds[SOURCE_KEY] = json.dumps(record)
    replace_scenario_inventory(scenario, database)
    return scenario
