"""Explicit exchange ledgers for sourced water, material and service budgets."""

from dataclasses import dataclass
import math
import re

from .graph import categories, unit


@dataclass(frozen=True)
class BudgetTerm:
    """Match exchanges and convert their quantity to a budget's common unit.

    The coefficient contains any documented conversion or retained fraction;
    no conversion is inferred from a generic water or material name.
    """

    name_pattern: str
    exchange_type: str
    unit: str
    coefficient: float = 1.0
    categories: tuple[str, ...] | None = None
    required: bool = True

    def __post_init__(self):
        re.compile(self.name_pattern)
        if self.exchange_type not in {"biosphere", "technosphere", "production"}:
            raise ValueError("Unsupported budget exchange type")
        if not self.unit or not math.isfinite(self.coefficient):
            raise ValueError("Budget terms require a unit and finite coefficient")
        if self.categories is not None:
            object.__setattr__(self, "categories", categories(self.categories))

    def matches(self, exchange):
        return (
            exchange.get("type") == self.exchange_type
            and re.fullmatch(self.name_pattern, exchange.get("name", "")) is not None
            and unit(exchange.get("unit")) == unit(self.unit)
            and (
                self.categories is None
                or categories(exchange.get("categories")) == self.categories
            )
        )


@dataclass(frozen=True)
class ExchangeBudget:
    """A declared ledger per reference unit, e.g. water inputs minus outputs."""

    name: str
    source: str
    terms: tuple[BudgetTerm, ...]
    name_pattern: str
    unit: str
    reference_unit: str = "kilogram"
    minimum: float = 0.0
    maximum: float = 0.0
    atol: float = 1e-9
    rtol: float = 1e-6
    system_models: tuple[str, ...] = ()

    def __post_init__(self):
        if not self.name or not self.source or not self.terms or not self.unit:
            raise ValueError("Budgets require a name, source, terms and unit")
        re.compile(self.name_pattern)
        for v in (self.minimum, self.maximum, self.atol, self.rtol):
            if not math.isfinite(v):
                raise ValueError("Budget bounds must be finite")
        if self.minimum > self.maximum or self.atol < 0 or self.rtol < 0:
            raise ValueError("Invalid budget bounds/tolerances")
        object.__setattr__(self, "terms", tuple(self.terms))
        object.__setattr__(self, "system_models", tuple(self.system_models))


def check_budgets(graph, book, config):
    for budget in config.budgets:
        rule = "BUDGET." + budget.name
        for i, key in enumerate(graph.keys):
            if (
                re.fullmatch(budget.name_pattern, key[0]) is None
                or unit(budget.reference_unit) != key[3]
                or (
                    budget.system_models
                    and config.system_model not in budget.system_models
                )
            ):
                book.status(rule, "not_applicable")
                continue
            matched = []
            value = 0.0
            missing = []
            for term in budget.terms:
                exchanges = [
                    e for e in graph.nodes[i].get("exchanges", ()) if term.matches(e)
                ]
                if not exchanges and term.required:
                    missing.append(term.name_pattern)
                subtotal = (
                    sum(float(e["amount"]) * term.coefficient for e in exchanges)
                    / graph.production[i]
                )
                value += subtotal
                matched.append(
                    dict(
                        pattern=term.name_pattern,
                        count=len(exchanges),
                        subtotal=subtotal,
                    )
                )
            book.status(rule)
            lo = budget.minimum - budget.atol - abs(budget.minimum) * budget.rtol
            hi = budget.maximum + budget.atol + abs(budget.maximum) * budget.rtol
            if missing or value < lo or value > hi:
                book.emit(
                    rule,
                    i,
                    "Exchange ledger violates its independently sourced budget",
                    category="physical_contract",
                    confidence="high",
                    severity="error",
                    actual=value,
                    expected=[budget.minimum, budget.maximum],
                    evidence=dict(
                        source=budget.source,
                        unit=budget.unit,
                        terms=matched,
                        missing_required_terms=missing,
                    ),
                )
