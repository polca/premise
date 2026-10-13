"""Explicit energy units and source-carbon properties for fuel markets.

Fuel market outputs are reference-energy equivalents, not a one-kg physical
blend. CO2 factors describe full oxidation; other carbon pollutants and capture
credits are not subtracted here. Compression electricity is not fuel energy.
"""

import math

PROPERTIES_KEY = "premise fuel properties"
CNG_ENERGY_MJ_PER_KG = 46.5


def annotate_cng_properties(database):
    """Carry gas carbon origins through the unchanged compression recipe.

    The documented delivered calorific value is independent of upstream gas
    input energy. Record that boundary difference, without explaining it away
    as measured losses or rescaling the inherited compression recipe.
    """
    product = "natural gas, medium pressure, vehicle grade"
    index = {activity_key(ds): ds for ds in database}
    completed = set()

    def annotate(dataset, seen=frozenset()):
        key = activity_key(dataset)
        if key in completed or key in seen:
            return
        components = []
        carbon = {
            name: 0.0
            for name in (
                "fossil CO2 kg per unit",
                "non-fossil CO2 kg per unit",
                "unresolved CO2 kg per unit",
            )
        }
        input_energy = 0.0
        compression = False
        for exchange in dataset["exchanges"]:
            if exchange.get("type") != "technosphere" or exchange["amount"] <= 0:
                continue
            if exchange.get("product") == product:
                provider = index.get(activity_key(exchange))
                if provider is None:
                    return
                annotate(provider, seen | {key})
            elif (
                exchange.get("product", "").startswith("natural gas")
                and exchange["unit"] == "cubic meter"
            ):
                provider = index.get(activity_key(exchange))
                compression = True
            else:
                continue
            if provider is None or PROPERTIES_KEY not in provider:
                return  # Optional legacy supply: do not invent an origin.
            properties = provider[PROPERTIES_KEY]
            input_energy += exchange["amount"] * properties["energy MJ per unit"]
            for name in carbon:
                carbon[name] += exchange["amount"] * properties[name]
            components.append(
                {
                    **{
                        name: exchange[name]
                        for name in ("name", "product", "location", "unit", "amount")
                    },
                    "energy_MJ_per_unit": properties["energy MJ per unit"],
                }
            )
        if not components:
            return
        total = sum(carbon.values())
        if total <= 0:
            return
        if compression:
            # Compression changes neither the origin nor relative amounts of
            # fossil/non-fossil carbon. Scale oxidation potential to the
            # documented delivered fuel, not the larger upstream energy input.
            carbon = {
                name: value / total * CNG_ENERGY_MJ_PER_KG * 0.06
                for name, value in carbon.items()
            }
        properties = {
            "reference unit": "kilogram",
            "reference energy MJ per unit": CNG_ENERGY_MJ_PER_KG,
            "energy MJ per unit": CNG_ENERGY_MJ_PER_KG,
            **carbon,
            "components": components,
            "carbon convention": "Full oxidation; gas-supply carbon origins preserved through compression",
            "energy source": "ecoinvent vehicle-grade natural gas market comment: 46.5 MJ/kg",
        }
        if compression:
            properties.update(
                {
                    "upstream fuel energy MJ per unit": input_energy,
                    "upstream minus delivered energy MJ per unit": input_energy
                    - CNG_ENERGY_MJ_PER_KG,
                    "energy boundary note": "Inherited source recipe preserved. This boundary difference is not attributed entirely to measured methane losses.",
                }
            )
        dataset[PROPERTIES_KEY] = properties
        completed.add(key)

    for dataset in database:
        if (
            dataset.get("reference product") == product
            and dataset["unit"] == "kilogram"
        ):
            annotate(dataset)


def energy_per_unit(technology, unit, specifications):
    """Return MJ per supplier unit, rejecting an implicit kg/m3 conversion."""
    try:
        spec = specifications[technology]
        value = float(spec["lhv"]["value"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Missing fuel heating value for {technology!r}") from exc
    # The packaged natural-gas value is MJ/m3; liquid fuels, methane and H2
    # use MJ/kg. This explicit boundary corrects the file's generic MJ/kg header.
    basis = spec.get(
        "lhv reference unit",
        "cubic meter" if technology == "natural gas" else "kilogram",
    )
    if unit != basis:
        raise ValueError(f"Fuel {technology!r} requires {basis!r}, received {unit!r}")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"Invalid heating value for {technology!r}: {value}")
    return value


def activity_key(dataset):
    return (
        dataset["name"],
        dataset.get("reference product", dataset.get("product")),
        dataset["location"],
        dataset["unit"],
    )


class CarbonOriginResolver:
    """Trace carbon-bearing inputs, excluding hydrogen and process-energy inputs.

    Retain the capture inventory's explicit biogenic fraction. DAC carbon is
    separately identified as atmospheric. Unknown or ambiguous pathways stay
    unresolved; electrolysis never implies non-fossil carbon by itself.
    """

    def __init__(self, database):
        self.index = {activity_key(ds): ds for ds in database}
        self.cache = {}

    def fuel_origin(self, technology, supplier, specifications):
        spec = specifications[technology]
        if float(spec["co2"]) == 0:
            return 0.0, "Carbon-free fuel"
        if (
            technology == "methane, synthetic"
            or "from electrolysis" in technology
            or technology == "methane, from coal"
        ):
            return self.trace(supplier)
        share = spec.get("biogenic_share")
        if share is None:
            return None, "Unspecified carbon origin"
        share = float(share)
        if not math.isfinite(share) or not 0 <= share <= 1:
            raise ValueError(f"Invalid carbon-origin fraction for {technology!r}")
        return share, f"Packaged fuel properties: {technology}"

    def trace(self, dataset, seen=frozenset()):
        key = activity_key(dataset)
        if key in self.cache:
            return self.cache[key]
        if key in seen:
            return None, f"Cyclic carbon supply: {key}"
        result = self._trace(dataset, seen | {key})
        self.cache[key] = result
        return result

    def _trace(self, dataset, seen):
        product = dataset["reference product"].lower()
        name = dataset["name"]
        if product.startswith("carbon dioxide"):
            if "direct air capture" in name.lower():
                return 1.0, f"Atmospheric carbon: {name}"
            if "share biogenic co2" in dataset:
                share = float(dataset["share biogenic co2"])
                if not math.isfinite(share) or not 0 <= share <= 1:
                    raise ValueError(f"Invalid captured-carbon fraction in {name!r}")
                return share, f"Capture inventory's share biogenic co2: {name}"

        # This packaged intermediate explicitly credits fossil CO2 from coal
        # gasification which is retained in the methane product.
        if name == "methane, from coal-based hydrogen" and any(
            e.get("type") == "biosphere"
            and e.get("name") == "Carbon dioxide, fossil"
            and e.get("amount", 0) < 0
            for e in dataset["exchanges"]
        ):
            return 0.0, "Fossil coal-gasification carbon retained in methane"

        carriers = []
        for exchange in dataset["exchanges"]:
            if (
                exchange.get("type") != "technosphere"
                or exchange.get("amount", 0) <= 0
                or exchange.get("unit") not in ("kilogram", "cubic meter")
            ):
                continue
            carrier = exchange.get("product", "").lower()
            if carrier != product and not carrier.startswith(
                ("carbon dioxide", "carbon monoxide", "syngas", "methanol", "methane")
            ):
                continue
            target = self.index.get(activity_key(exchange))
            if target is None:
                return None, f"Missing carbon supplier: {activity_key(exchange)}"
            fraction, evidence = self.trace(target, seen)
            if fraction is None:
                return None, evidence
            carriers.append((exchange, fraction, evidence))
        if not carriers:
            return None, f"Unresolved carbon source: {name}"
        fractions = {share for _, share, _ in carriers}
        if len(fractions) == 1:
            return carriers[0][1], " | ".join(
                sorted({source for _, _, source in carriers})
            )
        # Mixed CO2 sources can be carbon-weighted directly by their kg inputs.
        # Other mixtures require explicit intermediate carbon contents.
        if all(
            e["unit"] == "kilogram"
            and e["product"].lower().startswith("carbon dioxide")
            for e, _, _ in carriers
        ):
            total = sum(e["amount"] for e, _, _ in carriers)
            share = sum(e["amount"] * fraction for e, fraction, _ in carriers) / total
            return share, "Mixed captured-CO2 inputs: " + " | ".join(
                sorted({source for _, _, source in carriers})
            )
        return None, f"Mixed intermediates need carbon contents: {name}"


def summarize_components(components, reference_energy, unit):
    """Aggregate physical supply energy and oxidation carbon, retaining unknowns."""
    energy = fossil = non_fossil = unresolved = 0.0
    for component in components:
        amount = component["amount"]
        if not math.isfinite(amount):
            raise ValueError("Fuel supplier amount must be finite")
        if (
            not math.isfinite(component["energy_MJ_per_unit"])
            or component["energy_MJ_per_unit"] <= 0
        ):
            raise ValueError("Fuel supplier heating value must be finite and positive")
        if (
            not math.isfinite(component["CO2_kg_per_MJ"])
            or component["CO2_kg_per_MJ"] < 0
        ):
            raise ValueError("Fuel oxidation factor must be finite and nonnegative")
        if amount < 0 and not component["name"].lower().startswith("treatment"):
            raise ValueError(
                "Only waste-treatment fuel suppliers may have negative amounts"
            )
        delivered = abs(amount)
        supplied_energy = delivered * component["energy_MJ_per_unit"]
        co2 = supplied_energy * component["CO2_kg_per_MJ"]
        energy += supplied_energy
        share = component["non_fossil_share"]
        if share is None:
            unresolved += co2
        else:
            if not math.isfinite(share) or not 0 <= share <= 1:
                raise ValueError(
                    "Fuel carbon-origin fraction must be between zero and one"
                )
            non_fossil += co2 * share
            fossil += co2 * (1 - share)
    return {
        "reference unit": unit,
        "reference energy MJ per unit": reference_energy,
        "energy MJ per unit": energy,
        "fossil CO2 kg per unit": fossil,
        "non-fossil CO2 kg per unit": non_fossil,
        "unresolved CO2 kg per unit": unresolved,
        "carbon convention": "Full oxidation; separate carbon pollutants and upstream capture credits unchanged",
        "components": components,
    }


def supplied_energy(dataset):
    """Recalculate energy from current exchanges, not the stored ledger total.

    Multiple IAM technologies can share a provider. Use their ledger-weighted
    heating value after transient technology labels have been removed. Only
    recorded fuel suppliers count; electricity, transport and infrastructure do
    not contribute to the delivered fuel's heating value.
    """
    properties = dataset[PROPERTIES_KEY]
    suppliers = {}
    for component in properties["components"]:
        key = activity_key(component)
        amount, energy = suppliers.get(key, (0.0, 0.0))
        suppliers[key] = (
            amount + abs(component["amount"]),
            energy + abs(component["amount"]) * component["energy_MJ_per_unit"],
        )
    return sum(
        abs(exc["amount"])
        * suppliers[activity_key(exc)][1]
        / suppliers[activity_key(exc)][0]
        for exc in dataset["exchanges"]
        if exc.get("type") == "technosphere"
        and activity_key(exc) in suppliers
        and suppliers[activity_key(exc)][0] > 0
    )
