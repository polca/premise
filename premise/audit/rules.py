"""Independent, explicitly scoped checks of carbon origin and resource coverage."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from .models import AuditFinding, PhysicalContract
from ..validation.validation_framework import ValidationIssue


def default_contracts():
    return (
        PhysicalContract(
            name="premise.iridium_content",
            indicator="metals.Iridium",
            name_pattern=r"iridium production, economic allocation, proxy",
            product_pattern="iridium",
            minimum=1.0,
            maximum=1.0,
            source="Premise product-content attribution approved 2026-11-01; recovery losses unspecified",
        ),
    )


class RuleBook:
    def __init__(self, graph):
        self.graph = graph
        self.findings = []
        self.counts = defaultdict(
            lambda: dict(checked=0, unresolved=0, not_applicable=0, findings=0)
        )

    def status(self, rule, status="checked"):
        self.counts[rule][status] += 1

    def emit(
        self,
        rule,
        i,
        message,
        *,
        category="coverage",
        confidence="medium",
        severity="warning",
        indicator=None,
        expected=None,
        actual=None,
        evidence=None,
        root=None,
    ):
        self.counts[rule]["findings"] += 1
        key = self.graph.keys[i]
        self.findings.append(
            AuditFinding(
                ValidationIssue(
                    rule,
                    severity,
                    message,
                    activity_id=i,
                    activity_key=key[:3],
                    expected=expected,
                    actual=actual,
                ),
                category,
                confidence,
                key,
                indicator,
                evidence or {},
                root or " | ".join(key),
            )
        )

    def coverage(self):
        return tuple(
            dict(rule=rule, **counts) for rule, counts in sorted(self.counts.items())
        )


# Independent composition intervals, not the runtime fuel-carbon dictionary.
# Used as broad elemental-carbon plausibility screens, not measured grades.
# Source: molecular C fractions for ethanol/methanol/methane; hydrocarbon
# intervals intentionally broad. Heating values are diagnostic conversions.
FUEL_CONTENT = {
    "kerosene": (0.82, 0.90, 43),
    "diesel": (0.82, 0.90, 43),
    "petrol": (0.80, 0.90, 43),
    "fuel oil": (0.78, 0.92, 41),
    "natural gas": (0.65, 0.82, 48),
    "coal": (0.10, 0.98, 25),
    "ethanol": (0.50, 0.54, 27),
    "methanol": (0.36, 0.39, 20),
    "biomass": (0.35, 0.60, 18),
    "lpg": (0.78, 0.85, 46),
}


def fuel_family(product):
    p = product.casefold()
    if "burned in" in p:
        return None
    for token, family in (
        ("kerosene", "kerosene"),
        ("biodiesel", "diesel"),
        ("fatty acid methyl ester", "diesel"),
        ("vegetable oil methyl ester", "diesel"),
        ("diesel", "diesel"),
        ("petrol", "petrol"),
        ("gasoline", "petrol"),
        ("fuel oil", "fuel oil"),
        ("heavy fuel oil", "fuel oil"),
        ("natural gas", "natural gas"),
        ("biomethane", "natural gas"),
        ("methane", "natural gas"),
        ("liquefied petroleum gas", "lpg"),
        ("petroleum coke", "coal"),
        ("hard coal", "coal"),
        ("lignite", "coal"),
        ("bioethanol", "ethanol"),
        ("ethanol", "ethanol"),
        ("methanol", "methanol"),
    ):
        if p == token or (
            p.startswith(token) and p[len(token) : len(token) + 1] in {" ", ",", "-"}
        ):
            return family
    if any(
        p.startswith(s)
        for s in ("wood chip", "wood pellet", "biomass", "bagasse", "straw")
    ):
        return "biomass"
    return None


class CarbonOrigin:
    """Trace actual positive carbon-bearing supply, with unknown/cyclic branches visible.

    Fractions describe carbon origin. They never use GWP scores, the runtime
    fossil-variable list, or a stored fossil/non-fossil property as an oracle.
    Reused captured carbon remains unresolved without a declared convention.
    """

    def __init__(self, graph):
        self.graph = graph
        self.cache = {}

    def mass(self, amount, reference_unit, family):
        if reference_unit == "kilogram":
            return amount
        if reference_unit == "megajoule":
            return amount / FUEL_CONTENT[family][2]
        if reference_unit == "kilowatt hour":
            return amount * 3.6 / FUEL_CONTENT[family][2]
        if reference_unit == "cubic meter" and family == "natural gas":
            return amount * 0.78  # diagnostic density; not a strict grade contract
        return None

    def trace(self, i, visiting=frozenset()):
        if i in self.cache:
            return self.cache[i]
        if i in visiting:
            return np.array([0.0, 0.0, 1.0]), ("cycle",)
        key = self.graph.keys[i]
        name, product = key[0].casefold(), key[1].casefold()
        family = fuel_family(product)
        if family is None:
            return np.array([0.0, 0.0, 1.0]), ("unrecognized carbon-bearing supplier",)
        market = (
            name.startswith(("market for ", "market group for ", "fuel supply"))
            or "%" in name
            or "blend" in name
        )
        geological_resource = any(
            amount > 0
            and graph_flow[1][:2] == ("natural resource", "in ground")
            and graph_flow[0]
            .casefold()
            .startswith(("gas, natural", "oil, crude", "coal,", "lignite"))
            for row, amount, exc in self.graph.raw_biosphere[i]
            for graph_flow in (self.graph.flows[row],)
        )
        # A conventional refinery is independent evidence for petroleum carbon.
        if (
            not market
            and ("refinery" in name or "from petroleum" in name)
            and not any(
                s in name
                for s in ("synthetic", "biomass", "co-processing", "co processing")
            )
        ):
            result = np.array([1.0, 0.0, 0.0]), ("conventional petroleum refinery",)
        elif (
            not market
            and family in {"natural gas", "coal", "lpg"}
            and geological_resource
            and any(
                s in name
                for s in ("production", "extraction", "mine operation", "processing")
            )
            and not any(
                s in name
                for s in (
                    "bio",
                    "synthetic",
                    "electrolysis",
                    "hydrogen",
                    "carbon dioxide",
                    "methanation",
                    "sabatier",
                )
            )
        ):
            result = np.array([1.0, 0.0, 0.0]), ("conventional fossil production",)
        elif (
            not market
            and "%" not in name
            and "blend" not in name
            and "methyl ester" not in product
            and "biodiesel" not in product
            and not any(s in name for s in ("co2", "carbon dioxide", "hydrogen from"))
            and (
                family == "biomass"
                or any(
                    s in name
                    for s in (
                        "from biomass",
                        "from wheat",
                        "from maize",
                        "from sugar",
                        "from palm",
                        "from used cooking oil",
                        "from wood",
                    )
                )
            )
        ):
            result = np.array([0.0, 1.0, 0.0]), ("identified biomass feedstock",)
        elif not market and (
            product.startswith("biomethane")
            or "methyl ester" in product
            or "biodiesel" in product
            or any(s in name for s in ("synthetic", "methanation", "electrochemical"))
        ):
            # A fuel used for process heat is not evidence for the carbon
            # origin of a synthesized product. These routes need a declared
            # carbon-feed ledger, including any carbon added by reagents.
            result = np.array([0.0, 0.0, 1.0]), (
                "synthesized/upgraded fuel requires carbon-feed ledger",
            )
        else:
            contributions, reasons = [], []
            for provider, amount, exc in self.graph.inputs[i]:
                if amount <= 0 or provider == i:
                    continue
                target_family = fuel_family(self.graph.keys[provider][1])
                if target_family is None:
                    p = self.graph.keys[provider]
                    if (
                        market
                        and p[3]
                        in {"kilogram", "megajoule", "kilowatt hour", "cubic meter"}
                        and not any(
                            s in p[1].casefold()
                            for s in ("water", "hydrogen", "electricity", "burned in")
                        )
                    ):
                        mass = self.mass(amount, p[3], family)
                        if mass is None:
                            return np.array([0.0, 0.0, 1.0]), (
                                "unsupported market-component unit",
                            )
                        contributions.append(
                            mass
                            * np.mean(FUEL_CONTENT[family][:2])
                            * np.array([0.0, 0.0, 1.0])
                        )
                        reasons.append("unrecognized market component")
                    continue
                # Fuel producers consume utilities too. Only markets/blends
                # can infer origin from arbitrary component fuels; an unknown
                # synthetic producer needs an independently documented ledger.
                if not market and target_family != family:
                    continue
                mass = self.mass(amount, self.graph.keys[provider][3], target_family)
                if mass is None:
                    reasons.append("unsupported supplier unit")
                    return np.array([0.0, 0.0, 1.0]), tuple(sorted(set(reasons)))
                fractions, why = self.trace(provider, visiting | {i})
                contributions.append(
                    mass * np.mean(FUEL_CONTENT[target_family][:2]) * fractions
                )
                reasons.extend(why)
            if contributions:
                carbon = np.sum(contributions, axis=0)
                result = carbon / sum(carbon), tuple(sorted(set(reasons)))
            else:
                result = np.array([0.0, 0.0, 1.0]), ("origin requires source review",)
        # A result obtained through a cycle can depend on the entry point.
        if "cycle" not in result[1]:
            self.cache[i] = result
        return result


def combustion_activity(key):
    n = key[0].casefold()
    return (
        "burned in" in n
        or n.startswith(("transport,", "heat production", "electricity production"))
    ) and not n.startswith(("market", "treatment"))


def check_carbon(graph, book, config):
    origins = CarbonOrigin(graph)
    for i, key in enumerate(graph.keys):
        if not combustion_activity(key):
            book.status("CARBON.ORIGIN", "not_applicable")
            continue
        carbon = np.zeros(3)
        minimum = maximum = captured = 0.0
        sources = []
        unsupported = []
        for provider, amount, exc in graph.inputs[i]:
            if amount <= 0:
                continue
            pkey = graph.keys[provider]
            if (
                pkey[0].casefold().startswith("carbon dioxide, captured")
                and pkey[3] == "kilogram"
            ):
                captured += amount
            family = fuel_family(pkey[1])
            if family is None:
                if any(
                    s in pkey[1].casefold()
                    for s in (
                        "biogas",
                        "biomethane",
                        "blast furnace gas",
                        "coke oven gas",
                        "refinery gas",
                        "municipal solid waste",
                    )
                ):
                    unsupported.append(list(pkey))
                continue
            mass = origins.mass(amount, pkey[3], family)
            if mass is None:
                unsupported.append(list(pkey))
                continue
            fractions, reasons = origins.trace(provider)
            lo, hi = FUEL_CONTENT[family][:2]
            carbon += mass * (lo + hi) / 2 * fractions
            minimum += mass * lo
            maximum += mass * hi
            sources.append(
                dict(
                    provider=list(pkey),
                    amount=amount,
                    origin_fractions=fractions.tolist(),
                    evidence=list(reasons),
                )
            )
        if unsupported:
            book.status("CARBON.ORIGIN", "unresolved")
            book.status("CARBON.UNSUPPORTED_FUEL_BASIS", "unresolved")
            book.emit(
                "CARBON.UNSUPPORTED_FUEL_BASIS",
                i,
                "Carbon origin/closure requires an independently documented fuel basis",
                evidence=dict(unsupported_suppliers=unsupported),
                confidence="low",
            )
            continue
        if not sources:
            book.status("CARBON.ORIGIN", "not_applicable")
            continue
        fossil = bio = other_carbon = 0.0
        for row, amount, exc in graph.raw_biosphere[i]:
            name, cat, u = graph.flows[row]
            if not cat or cat[0] != "air" or u != "kilogram" or amount <= 0:
                continue
            if name == "Carbon dioxide, fossil":
                fossil += amount
            elif name == "Carbon dioxide, non-fossil":
                bio += amount
            elif name.startswith("Carbon monoxide"):
                other_carbon += amount * 12 / 28
            elif name.startswith("Methane"):
                other_carbon += amount * 12 / 16
        emissions = fossil + bio
        evidence = dict(
            suppliers=sources,
            fossil_co2=fossil,
            non_fossil_co2=bio,
            captured_co2=captured,
            unresolved_carbon_fraction=(
                float(carbon[2] / sum(carbon)) if sum(carbon) else 1.0
            ),
        )
        pre = carbon * 44 / 12
        residual = max(0.0, float(sum(pre)) - captured)
        if emissions <= 1e-14 or residual <= 1e-14:
            book.status("CARBON.ORIGIN", "unresolved")
        else:
            lower = max(0.0, float(pre[0]) - captured) / residual
            upper = min(residual, float(pre[0] + pre[2])) / residual
            observed = fossil / emissions
            book.status(
                "CARBON.ORIGIN", "unresolved" if pre[2] > sum(pre) * 0.01 else "checked"
            )
            tol = config.carbon_fraction_tolerance
            if observed < lower - tol or observed > upper + tol:
                definitive = pre[2] <= sum(pre) * 1e-10 and (
                    lower > 1 - tol or upper < tol
                )
                book.emit(
                    "CARBON.ORIGIN",
                    i,
                    "Combustion fossil/non-fossil CO2 split conflicts with traced fuel carbon origin",
                    category="carbon_classification",
                    confidence="high" if definitive else "medium",
                    severity="error" if definitive else "warning",
                    indicator="co2.fossil",
                    expected=[lower, upper],
                    actual=observed,
                    evidence=evidence,
                )
        # Broad independent composition screen; allocation and grades require
        # review, so discrepancies are warnings rather than forced repairs.
        observed_carbon = emissions * 12 / 44 + other_carbon + captured * 12 / 44
        book.status("CARBON.MASS_PLAUSIBILITY")
        if (
            observed_carbon < minimum * 0.8 - 1e-12
            or observed_carbon > maximum * 1.2 + 1e-12
        ):
            book.emit(
                "CARBON.MASS_PLAUSIBILITY",
                i,
                "Fuel carbon and emitted/captured carbon warrant a source-ledger review",
                category="physical_plausibility",
                indicator="co2.fossil",
                actual=observed_carbon,
                expected=[minimum, maximum],
                evidence=evidence,
            )


def check_contracts(graph, book, config, indicators, direct, all_scores):
    index = {s.key: j for j, s in enumerate(indicators)}
    contracts = (
        (*default_contracts(), *config.contracts)
        if config.include_default_contracts
        else config.contracts
    )
    for contract in contracts:
        rule = "CONTRACT." + contract.name
        j = index.get(contract.indicator)
        for i, key in enumerate(graph.keys):
            if not contract.matches(key, config.system_model, config.version):
                book.status(rule, "not_applicable")
                continue
            if j is None:
                book.status(rule, "unresolved")
                book.emit(
                    rule,
                    i,
                    "Contract indicator is unavailable",
                    indicator=contract.indicator,
                    evidence=dict(source=contract.source),
                )
                continue
            value = (
                float(direct[j, i] / graph.production[i])
                if contract.scope == "direct"
                else float(all_scores[j, i])
            )
            book.status(rule)
            low, high = contract.minimum, contract.maximum
            below = (
                low is not None
                and value < low - contract.atol - abs(low) * contract.rtol
            )
            above = (
                high is not None
                and value > high + contract.atol + abs(high) * contract.rtol
            )
            if below or above:
                book.emit(
                    rule,
                    i,
                    "Physical quantity violates an independently declared inventory contract",
                    category="physical_contract",
                    confidence="high",
                    severity="error",
                    indicator=contract.indicator,
                    actual=value,
                    expected=[low, high],
                    evidence=dict(
                        source=contract.source,
                        scope=contract.scope,
                        reference_unit=contract.unit,
                    ),
                )


def resource_target(product, names):
    p = product.casefold()
    for name in names:
        if p == name.casefold():
            return name, 1.0
    if p == "lithium carbonate":
        return "Lithium", 2 * 6.94 / (2 * 6.94 + 12.011 + 3 * 15.999)
    return None


def check_resources_land_water(graph, book, config, indicators, all_scores):
    index = {s.key: j for j, s in enumerate(indicators)}
    elements = [
        s.key.removeprefix("metals.") for s in indicators if s.family == "metals"
    ]
    target_market_available = any(
        k[0] == "market for hydrogen, gaseous, low pressure"
        and k[1] == "hydrogen, gaseous, low pressure"
        and k[3] == "kilogram"
        for k in graph.keys
    )
    for i, key in enumerate(graph.keys):
        name, product, location, u = key
        lower = name.casefold()
        fuel = fuel_family(product)
        fossil_indicator = {
            "natural gas": "resources.natural_gas",
            "coal": "resources.coal",
            "kerosene": "resources.crude_oil",
            "diesel": "resources.crude_oil",
            "petrol": "resources.crude_oil",
            "fuel oil": "resources.crude_oil",
        }.get(fuel)
        conventional = (
            not lower.startswith("market")
            and not any(
                s in lower
                for s in ("bio", "synthetic", "hydrogen", "treatment", "burned in")
            )
            and any(
                s in lower
                for s in (
                    "refinery operation",
                    "mine operation",
                    "gas extraction",
                    "natural gas production",
                )
            )
        )
        if conventional and fossil_indicator in index:
            book.status("RESOURCE.FOSSIL_COVERAGE")
            value = float(all_scores[index[fossil_indicator], i])
            if abs(value) < 1e-14:
                book.emit(
                    "RESOURCE.FOSSIL_COVERAGE",
                    i,
                    "Conventional fuel producer has no represented principal fossil-resource route",
                    indicator=fossil_indicator,
                    actual=value,
                    evidence=dict(
                        qualification="Coverage screen, not a recovery or allocation benchmark"
                    ),
                )
        else:
            book.status("RESOURCE.FOSSIL_COVERAGE", "not_applicable")
        if "market-average hydrogen" in lower:
            target = target_market_available
            book.status(
                "HYDROGEN.SCENARIO_SUPPLY", "checked" if target else "unresolved"
            )
            if target:
                native = [
                    graph.keys[p]
                    for p, amount, exc in graph.inputs[i]
                    if amount > 0
                    and graph.keys[p][0]
                    in {
                        "market for hydrogen, liquid",
                        "market for hydrogen, gaseous",
                        "market for hydrogen, gaseous, medium pressure, merchant",
                    }
                ]
                if native:
                    book.emit(
                        "HYDROGEN.SCENARIO_SUPPLY",
                        i,
                        "Market-average recipe bypasses the available generated hydrogen market",
                        category="supplier_boundary",
                        confidence="high",
                        severity="error",
                        actual=[list(k) for k in native],
                        expected="market for hydrogen, gaseous, low pressure",
                        evidence=dict(
                            boundary="Adopted 30-bar scenario-market delivery; source quantities and process energy retained"
                        ),
                    )
        else:
            book.status("HYDROGEN.SCENARIO_SUPPLY", "not_applicable")
        primary = (
            u == "kilogram"
            and graph.production[i] > 0
            and any(
                s in lower
                for s in ("production", "extraction", "refinery", "mine operation")
            )
            and not any(
                s in lower
                for s in ("recycl", "secondary", "treatment", "scrap", "market for")
            )
        )
        target = resource_target(product, elements)
        if (
            primary
            and target
            and target[0].casefold() not in lower
            and "platinum group metal" not in lower
        ):
            book.status("RESOURCE.PRIMARY_COVERAGE", "unresolved")
        elif primary and target and "metals." + target[0] in index:
            j = index["metals." + target[0]]
            value = float(all_scores[j, i])
            if value < 0 or config.system_model == "consequential":
                book.status("RESOURCE.PRIMARY_COVERAGE", "unresolved")
            else:
                book.status("RESOURCE.PRIMARY_COVERAGE")
                if value < target[1] * 1e-4:
                    book.emit(
                        "RESOURCE.PRIMARY_COVERAGE",
                        i,
                        "Primary material has negligible attributed own-resource extraction; inspect the principal feed boundary",
                        indicator="metals." + target[0],
                        actual=value,
                        expected=dict(
                            product_content_benchmark=target[1], screen_fraction=1e-4
                        ),
                        evidence=dict(
                            qualification="Content benchmark is a completeness screen, not a universal allocation or recovery rule"
                        ),
                    )
        else:
            book.status("RESOURCE.PRIMARY_COVERAGE", "not_applicable")
        if (
            lower.startswith(("farming and supply", "forest establishment", "forestry"))
            and "land.occupation" in index
        ):
            value = float(all_scores[index["land.occupation"], i])
            book.status("LAND.EXPECTED_ROUTE")
            if abs(value) < 1e-14:
                book.emit(
                    "LAND.EXPECTED_ROUTE",
                    i,
                    "Land-dependent activity has no represented land-occupation route",
                    indicator="land.occupation",
                    actual=value,
                )
        else:
            book.status("LAND.EXPECTED_ROUTE", "not_applicable")
        if (
            lower.startswith("tap water production")
            and u == "kilogram"
            and "water.freshwater" in index
        ):
            book.status("WATER.SUPPLY_ROUTE")
            value = float(all_scores[index["water.freshwater"], i])
            if abs(value) < 1e-14:
                book.emit(
                    "WATER.SUPPLY_ROUTE",
                    i,
                    "Water producer has no represented freshwater withdrawal; inspect source-water type",
                    indicator="water.freshwater",
                    actual=value,
                )
        else:
            book.status("WATER.SUPPLY_ROUTE", "not_applicable")
