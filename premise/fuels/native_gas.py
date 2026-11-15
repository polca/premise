"""Conservative origin evidence for unchanged native natural-gas supplies."""

import math
from collections import defaultdict

from .properties import PROPERTIES_KEY, activity_key, energy_per_unit

GASEOUS_NATURAL_GAS_PRODUCTS = {
    "natural gas, high pressure",
    "natural gas, low pressure",
}
NATURAL_GAS_PRODUCTS = GASEOUS_NATURAL_GAS_PRODUCTS | {"natural gas, liquefied"}
EXTRACTION_NAMES = {
    "petroleum and gas production, onshore",
    "petroleum and gas production, offshore",
    "natural gas production",
}


def gas_feed(product):
    return product in NATURAL_GAS_PRODUCTS or any(
        product == name or product.startswith(name + ",")
        for name in ("methane", "biogas", "biomethane")
    )


class NativeGasProperties:
    """Prove exclusively fossil origin; never infer a mixed gas composition.

    Every reachable gas-supply activity must lead to an explicit fossil anchor.
    This admits trade loops connected to extraction, but rejects isolated loops,
    unknown branches and mixed-origin supplies. Process electricity, equipment,
    burned gas and vented gas are outside the delivered gas's feed boundary.
    """

    def __init__(self, index, specifications):
        self.index = index
        self.specifications = specifications
        self.cache = {}

    def properties(self, dataset):
        key = activity_key(dataset)
        if key in self.cache:
            return self.cache[key]
        # LNG can establish an upstream origin but its kg or liquid-volume
        # energy must never be equated to the gaseous 36 MJ/m3 reference.
        if key[1] not in GASEOUS_NATURAL_GAS_PRODUCTS or key[3] != "cubic meter":
            return None
        evidence = self._fossil_evidence(dataset)
        result = None
        if evidence is not None:
            energy = energy_per_unit("natural gas", key[3], self.specifications)
            factor = float(self.specifications["natural gas"]["co2"])
            if not math.isfinite(factor) or factor <= 0:
                raise ValueError(
                    "Natural-gas oxidation factor must be finite and positive"
                )
            result = {
                "reference unit": key[3],
                "energy MJ per unit": energy,
                "fossil CO2 kg per unit": energy * factor,
                "non-fossil CO2 kg per unit": 0.0,
                "unresolved CO2 kg per unit": 0.0,
                "native gas supply evidence": evidence,
            }
        self.cache[key] = result
        return result

    def _fossil_evidence(self, dataset):
        pending = [dataset]
        visited = set()
        anchors = set()
        extractions = set()
        reverse = defaultdict(set)
        while pending:
            current = pending.pop()
            key = activity_key(current)
            if key in visited:
                continue
            visited.add(key)
            properties = current.get(PROPERTIES_KEY)
            if properties is not None:
                try:
                    values = [
                        float(properties[field])
                        for field in (
                            "fossil CO2 kg per unit",
                            "non-fossil CO2 kg per unit",
                            "unresolved CO2 kg per unit",
                        )
                    ]
                except (KeyError, TypeError, ValueError):
                    return None
                if (
                    properties.get("reference unit", key[3]) != key[3]
                    or not all(math.isfinite(value) and value >= 0 for value in values)
                    or values[0] <= 0
                    or values[1:] != [0.0, 0.0]
                ):
                    return None
                anchors.add(key)
                continue
            if (
                current["name"] in EXTRACTION_NAMES
                and current["reference product"] in NATURAL_GAS_PRODUCTS
                and any(
                    exchange.get("type") == "biosphere"
                    and exchange.get("name") == "Gas, natural"
                    and tuple(exchange.get("categories") or ())
                    == ("natural resource", "in ground")
                    and exchange.get("unit") in ("cubic meter", "standard cubic meter")
                    and math.isfinite(exchange["amount"])
                    and exchange["amount"] > 0
                    for exchange in current["exchanges"]
                )
            ):
                anchors.add(key)
                extractions.add(key)
            for exchange in current["exchanges"]:
                if exchange.get("type") != "technosphere" or not gas_feed(
                    exchange.get("product", "")
                ):
                    continue
                amount = float(exchange["amount"])
                if not math.isfinite(amount) or amount < 0:
                    return None
                if amount == 0:
                    continue
                if exchange.get("unit") not in ("cubic meter", "kilogram"):
                    return None
                provider = self.index.get(activity_key(exchange))
                if provider is None:
                    return None
                reverse[activity_key(provider)].add(key)
                pending.append(provider)

        # Reachability to anchors must hold for *every* branch, not merely for
        # one extraction path somewhere in the consumer's supply graph.
        proven = set(anchors)
        pending = list(anchors)
        while pending:
            for consumer in reverse[pending.pop()] - proven:
                proven.add(consumer)
                pending.append(consumer)
        if proven != visited:
            return None
        return {
            "method": "All reachable gas suppliers lead to explicit fossil extraction or fossil-only fuel properties; no IAM blend inferred",
            "activity count": len(visited),
            "fossil extraction activity keys": sorted(extractions),
            "fossil property anchor keys": sorted(anchors - extractions),
            "fuel factor source": "premise/iam_variables_mapping/fuels.yaml#natural gas",
        }
