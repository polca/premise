"""Annual expected stock and conditional retirement under common amortisation.

Stocks refer to the end of each calendar year. New annual additions enter at
age zero after that year's retirements. Observed initial stocks are already
survivors and are evolved with conditional survival, never weighted by survival
a second time. These numerical models do not supply empirical parameter values.
"""

from dataclasses import dataclass
import math

from .stock_vintage import calendar_year


def _nonnegative(value, label):
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{label} must be finite and nonnegative")
    return value


@dataclass(frozen=True)
class SurvivalLaw:
    """Fixed or Weibull service life; mean is total life, not observed age."""

    family: str
    mean_years: float
    shape: float | None = None

    def __post_init__(self):
        if _nonnegative(self.mean_years, "mean_years") <= 0:
            raise ValueError("Mean lifetime must be positive")
        object.__setattr__(self, "mean_years", float(self.mean_years))
        if self.family not in {"fixed", "weibull"}:
            raise ValueError("Unsupported survival family")
        if self.family == "weibull":
            if self.shape is None or _nonnegative(self.shape, "shape") <= 0:
                raise ValueError("Weibull shape must be positive")
            object.__setattr__(self, "shape", float(self.shape))
        elif self.shape is not None:
            raise ValueError("A fixed lifetime has no shape parameter")

    def conditional(self, age, years):
        """Return P(T > age+years | T > age), using stable log-survival."""
        age, years = _nonnegative(age, "age"), _nonnegative(years, "years")
        if self.family == "fixed":
            if age >= self.mean_years:
                raise ValueError(
                    "Observed survivor is incompatible with fixed lifetime"
                )
            return float(age + years < self.mean_years)
        scale = self.mean_years / math.gamma(1 + 1 / self.shape)
        difference = ((age + years) / scale) ** self.shape - (age / scale) ** self.shape
        return math.exp(-difference)


@dataclass
class StockProjection:
    reference_year: int
    stocks: dict[int, dict[int, float]]
    balances: list[dict]
    survival: SurvivalLaw
    early_exit_kind: str | None


def evolve_stock(
    initial_stock,
    reference_year,
    annual_inputs,
    survival,
    *,
    mode,
    early_exit_kind=None,
):
    """Project stock using either annual gross additions or closing-stock targets.

    A declining target below natural survivors requires an explicit
    ``early_exit_kind`` (physical_retirement or territorial_exit). Excess exits
    are proportional across surviving cohorts, a declared modelling assumption.
    Territorial exits must not later be mistaken for physical disposal.
    """
    reference_year = calendar_year(reference_year)
    if mode not in {"gross_additions", "stock_target"}:
        raise ValueError("Choose gross_additions or stock_target explicitly")
    if early_exit_kind not in {None, "physical_retirement", "territorial_exit"}:
        raise ValueError("Unknown early_exit_kind")
    current = {}
    for cohort, value in initial_stock.items():
        cohort = calendar_year(cohort)
        value = _nonnegative(value, "initial stock")
        if cohort > reference_year:
            raise ValueError("Initial stock contains a future cohort")
        if value:
            survival.conditional(reference_year - cohort, 0)
            current[cohort] = value
    inputs = {
        calendar_year(year): _nonnegative(value, mode)
        for year, value in annual_inputs.items()
    }
    if inputs and sorted(inputs) != list(range(reference_year + 1, max(inputs) + 1)):
        raise ValueError("Future inputs must cover every year after the reference")
    stocks, balances = {reference_year: dict(current)}, []
    for year, value in sorted(inputs.items()):
        opening = math.fsum(current.values())
        survivors = {
            cohort: stock * survival.conditional(year - 1 - cohort, 1)
            for cohort, stock in current.items()
        }
        natural_surviving = math.fsum(survivors.values())
        natural_retirements = opening - natural_surviving
        additions = (
            value if mode == "gross_additions" else max(0, value - natural_surviving)
        )
        early_exits = (
            0.0 if mode == "gross_additions" else max(0, natural_surviving - value)
        )
        if early_exits:
            if early_exit_kind is None:
                raise ValueError(
                    f"Target {year} is below survivors; specify the meaning of excess exits"
                )
            factor = value / natural_surviving
            survivors = {cohort: stock * factor for cohort, stock in survivors.items()}
        current = {cohort: stock for cohort, stock in survivors.items() if stock > 0}
        if additions:
            current[year] = additions
        closing = math.fsum(current.values())
        residual = closing - (opening - natural_retirements - early_exits + additions)
        if not math.isclose(residual, 0, abs_tol=1e-10 * max(1, opening, closing)):
            raise ValueError("Cohort stock balance failed")
        if mode == "stock_target" and not math.isclose(
            closing, value, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError("Cohorts do not reconcile to the closing-stock target")
        stocks[year] = dict(current)
        balances.append(
            {
                "year": year,
                "opening_stock": opening,
                "natural_retirements": natural_retirements,
                "early_exits": early_exits,
                "gross_additions": additions,
                "closing_stock": closing,
                "balance_residual": residual,
                "mode": mode,
                "early_exit_kind": early_exit_kind,
                "early_exit_allocation": (
                    "proportional_to_natural_survivors" if early_exits else None
                ),
            }
        )
    return StockProjection(reference_year, stocks, balances, survival, early_exit_kind)


def service_weights(cohort_stock, utilisation=None):
    """Weight observed stock by declared nonnegative service per stock unit.

    Leaving utilisation unspecified explicitly means equal service per stock
    unit. The returned totals retain that basis; normalisation only creates the
    timing weights, never a replacement capital coefficient.
    """
    if utilisation is not None and set(utilisation) != set(cohort_stock):
        raise ValueError("Utilisation must cover exactly the stock cohorts")
    service = {
        calendar_year(cohort): _nonnegative(stock, "stock")
        * (
            1.0
            if utilisation is None
            else _nonnegative(utilisation[cohort], "utilisation")
        )
        for cohort, stock in cohort_stock.items()
    }
    total = math.fsum(service.values())
    if total <= 0:
        raise ValueError("Positive service is required for timing weights")
    return {
        "service_by_cohort": service,
        "total_service": total,
        "weights": {
            cohort: value / total for cohort, value in service.items() if value > 0
        },
        "utilisation_basis": (
            "equal_per_stock_unit" if utilisation is None else "explicit_by_cohort"
        ),
        "allocation_basis": "common_amortisation",
        "exchange_amount_changed": False,
    }


def retirement_record(
    projection, service_year, weights, *, max_years=500, tail_tolerance=1e-12
):
    """Build retirement timing conditional on observed service-year survivors.

    Use projected cohort declines within the explicit future stock horizon.
    Beyond it, continue natural survival with no additional forced exits.
    Residual mass below ``tail_tolerance`` is retained in the last event year
    and reported, rather than silently discarded and renormalised.
    """
    service_year = calendar_year(service_year)
    if service_year not in projection.stocks:
        raise ValueError("Service year is absent from the stock projection")
    if (
        not 0 < tail_tolerance <= 1e-10
        or not isinstance(max_years, int)
        or max_years <= 0
    ):
        raise ValueError("Require positive horizon and tail tolerance <= 1e-10")
    if projection.early_exit_kind == "territorial_exit" and any(
        row["year"] > service_year and row["early_exits"] > 0
        for row in projection.balances
    ):
        raise ValueError("Territorial exits do not establish physical retirement dates")
    weights = {
        calendar_year(cohort): _nonnegative(value, "weight")
        for cohort, value in weights.items()
    }
    if not math.isclose(math.fsum(weights.values()), 1, rel_tol=0, abs_tol=1e-10):
        raise ValueError("Service weights must sum to one")
    initial = projection.stocks[service_year]
    if any(initial.get(cohort, 0) <= 0 for cohort in weights):
        raise ValueError("Retirement weights include a cohort absent at service time")
    end_year = max(projection.stocks)
    previous = 1.0
    events, amounts = [], []
    for year in range(service_year + 1, service_year + max_years + 1):
        contributions = []
        for cohort, weight in weights.items():
            if year <= end_year:
                ratio = projection.stocks[year].get(cohort, 0) / initial[cohort]
            else:
                stock = projection.stocks[end_year].get(cohort, 0)
                ratio = (
                    0
                    if stock == 0
                    else stock
                    / initial[cohort]
                    * projection.survival.conditional(
                        end_year - cohort, year - end_year
                    )
                )
            contributions.append(weight * ratio)
        remaining = math.fsum(contributions)
        if remaining > previous + 1e-13:
            raise ValueError(
                "Conditional surviving stock increased; inspect transfers or cohort identity"
            )
        mass = max(0.0, previous - remaining)
        if mass:
            events.append(year)
            amounts.append(mass)
        if remaining <= tail_tolerance:
            if not events:
                raise ValueError("No retirement event could be resolved")
            amounts[-1] += remaining
            correction = 1 - math.fsum(amounts)
            if abs(correction) > 1e-10:
                raise ValueError("Retirement mass does not reconcile")
            amounts[-1] += correction
            return {
                "service_year": service_year,
                "event_years": events,
                "weights": amounts,
                "tail_mass_folded_into_last_year": remaining,
                "tail_policy": "fold_below_tolerance_into_terminal_event",
                "projection_end_year": end_year,
                "beyond_projection": "natural_survival_without_further_forced_exits",
            }
        previous = remaining
    raise ValueError(
        "Retirement horizon leaves excessive tail mass; extend it explicitly"
    )
