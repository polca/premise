"""Read-only transport-distance sensitivity helpers for Brightway hydrogen markets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import bw2calc as bc
import numpy as np
import pandas as pd


KG_TO_TONNE = 0.001


@dataclass(frozen=True)
class DistanceMode:
    """Definition of one one-at-a-time distance sensitivity."""

    label: str
    activity_name: str
    baseline_km: float
    higher_distances_km: tuple[float, float, float]
    market_labels: tuple[str, ...]
    kind: str = "tonne-kilometre"

    @property
    def distances_km(self) -> tuple[float, ...]:
        return (self.baseline_km, *self.higher_distances_km)


def _normalized(value) -> str:
    return " ".join(str(value or "").replace(" ,", ",").split()).lower()


def _is_hydrogen_market(activity) -> bool:
    return (
        _normalized(activity.get("name")).startswith(
            "market for hydrogen, gaseous, low pressure"
        )
        and _normalized(activity.get("reference product"))
        == "hydrogen, gaseous, low pressure"
    )


def _reference_output(activity) -> float:
    exchanges = [exc for exc in activity.production() if exc.input.key == activity.key]
    if len(exchanges) != 1:
        raise ValueError(
            f"Expected one reference production exchange for {activity.key}; "
            f"found {len(exchanges)}."
        )
    amount = float(exchanges[0].get("amount", 0.0))
    if amount == 0:
        raise ZeroDivisionError(f"Zero reference production amount for {activity.key}.")
    return amount


def hydrogen_market_chain(activity, demand=1.0, visited=()):
    """Return hydrogen-market activities and their scaled demands, unwrapping pass-throughs."""
    if activity.key in visited:
        raise RuntimeError(f"Hydrogen-market cycle while unwrapping {activity.key}.")

    scaled_demand = float(demand) / _reference_output(activity)
    rows = [(activity, scaled_demand)]
    for exchange in activity.technosphere():
        provider = exchange.input
        if _is_hydrogen_market(provider):
            rows.extend(
                hydrogen_market_chain(
                    provider,
                    scaled_demand * float(exchange.get("amount", 0.0)),
                    (*visited, activity.key),
                )
            )
    return rows


def direct_mode_amount(market, activity_name: str) -> float:
    """Return the scaled direct exchange amount across a pass-through market chain."""
    amount = 0.0
    for activity, scale in hydrogen_market_chain(market):
        for exchange in activity.technosphere():
            if exchange.get("name") == activity_name:
                amount += scale * float(exchange.get("amount", 0.0))
    return amount


def mode_audit(reporting_markets: Mapping[str, object], modes: Iterable[DistanceMode]):
    rows = []
    for mode in modes:
        for market_label in mode.market_labels:
            market = reporting_markets[market_label]
            amount = direct_mode_amount(market, mode.activity_name)
            share = (
                amount
                if mode.kind == "pipeline"
                else amount / (mode.baseline_km * KG_TO_TONNE)
            )
            rows.append(
                {
                    "transport mode": mode.label,
                    "market": market_label,
                    "market location": market.get("location"),
                    "baseline distance (km)": mode.baseline_km,
                    "direct exchange amount": amount,
                    "exchange unit": "kilogram" if mode.kind == "pipeline" else "ton kilometer",
                    "direct transport share": share,
                }
            )
    return pd.DataFrame(rows)


class InMemoryDistanceModel:
    """Apply scenario values to copies of Brightway matrices; never write database exchanges."""

    PIPELINE_INFRASTRUCTURE_NAMES = {
        "pipeline, hydrogen, low pressure distribution network",
        "pipeline, hydrogen, high pressure transmission network",
        "compressor assembly for transmission hydrogen pipeline",
    }
    PIPELINE_ELECTRICITY_NAME = "market group for electricity, low voltage"
    PIPELINE_HYDROGEN_INPUT_PREFIX = "market for hydrogen, gaseous, low pressure"

    # Original 500 km inventory: transmission + distribution electricity are distance-dependent;
    # compression before geological storage is held fixed.
    PIPELINE_STORAGE_ELECTRICITY_KWH = 0.6444251829607142
    PIPELINE_BASE_TRANSMISSION_LEAK_KG = 0.0007125 / 100

    def __init__(self, reporting_markets: Mapping[str, object], method):
        self.reporting_markets = dict(reporting_markets)
        self.method = method
        seed = next(iter(self.reporting_markets.values()))
        self.lca = bc.LCA({seed: 1.0}, method)
        self.lca.lci()
        self.lca.lcia()
        self.base_technosphere = self.lca.technosphere_matrix.copy()
        self.base_biosphere = self.lca.biosphere_matrix.copy()

    def _activity_column(self, activity) -> int:
        return int(self.lca.dicts.activity[int(activity.id)])

    def _product_row(self, activity) -> int:
        return int(self.lca.dicts.product[int(activity.id)])

    def _biosphere_row(self, flow) -> int:
        return int(self.lca.dicts.biosphere[int(flow.id)])

    def _scale_technosphere_cell(self, matrix, provider, consumer, factor, seen):
        position = (self._product_row(provider), self._activity_column(consumer))
        if position in seen:
            return
        matrix[position] = self.base_technosphere[position] * float(factor)
        seen.add(position)

    def _prepare_matrices(self, technosphere, biosphere):
        self.lca.technosphere_matrix = technosphere
        self.lca.biosphere_matrix = biosphere
        if hasattr(self.lca, "solver"):
            del self.lca.solver
        self.lca.decompose_technosphere()

    def _score(self, market) -> float:
        self.lca.redo_lcia({int(market.id): 1.0})
        return float(self.lca.score)

    def restore(self):
        if hasattr(self.lca, "solver"):
            del self.lca.solver
        self.lca.technosphere_matrix = self.base_technosphere.copy()
        self.lca.biosphere_matrix = self.base_biosphere.copy()

    def baseline_scores(self) -> dict[str, float]:
        scores = {}
        try:
            self._prepare_matrices(
                self.base_technosphere.copy(), self.base_biosphere.copy()
            )
            for label, market in self.reporting_markets.items():
                scores[label] = self._score(market)
        finally:
            self.restore()
        return scores

    def _transport_matrices(self, mode: DistanceMode, distance_km: float):
        technosphere = self.base_technosphere.copy()
        biosphere = self.base_biosphere.copy()
        factor = float(distance_km) / mode.baseline_km
        seen = set()

        for market_label in mode.market_labels:
            market = self.reporting_markets[market_label]
            for consumer, _ in hydrogen_market_chain(market):
                for exchange in consumer.technosphere():
                    if exchange.get("name") == mode.activity_name:
                        self._scale_technosphere_cell(
                            technosphere, exchange.input, consumer, factor, seen
                        )
        if not seen:
            raise LookupError(
                f"No exchange named {mode.activity_name!r} was found for {mode.label}."
            )
        return technosphere, biosphere

    def _pipeline_matrices(self, mode: DistanceMode, distance_km: float):
        technosphere = self.base_technosphere.copy()
        biosphere = self.base_biosphere.copy()
        factor = float(distance_km) / mode.baseline_km

        providers = []
        for market_label in mode.market_labels:
            market = self.reporting_markets[market_label]
            for consumer, _ in hydrogen_market_chain(market):
                providers.extend(
                    exchange.input
                    for exchange in consumer.technosphere()
                    if exchange.get("name") == mode.activity_name
                )
        pipeline_activities = {provider.key: provider for provider in providers}
        if not pipeline_activities:
            raise LookupError("No pipeline distribution activity was found in the selected markets.")

        for pipeline in pipeline_activities.values():
            column = self._activity_column(pipeline)
            for exchange in pipeline.technosphere():
                name = exchange.get("name", "")
                row = self._product_row(exchange.input)
                position = (row, column)
                base_amount = abs(float(self.base_technosphere[position]))

                if name in self.PIPELINE_INFRASTRUCTURE_NAMES:
                    technosphere[position] = self.base_technosphere[position] * factor
                elif name == self.PIPELINE_ELECTRICITY_NAME:
                    fixed = self.PIPELINE_STORAGE_ELECTRICITY_KWH
                    if not 0 <= fixed <= base_amount:
                        raise ValueError(
                            "The fixed pipeline-storage electricity exceeds the database total."
                        )
                    new_amount = fixed + (base_amount - fixed) * factor
                    technosphere[position] = self.base_technosphere[position] * (
                        new_amount / base_amount
                    )
                elif _normalized(name).startswith(self.PIPELINE_HYDROGEN_INPUT_PREFIX):
                    base_loss = base_amount
                    fixed_storage_loss = (
                        base_loss - self.PIPELINE_BASE_TRANSMISSION_LEAK_KG
                    )
                    if fixed_storage_loss < 0:
                        raise ValueError("The pipeline transmission-loss split is invalid.")
                    new_loss = (
                        fixed_storage_loss
                        + self.PIPELINE_BASE_TRANSMISSION_LEAK_KG * factor
                    )
                    technosphere[position] = self.base_technosphere[position] * (
                        new_loss / base_loss
                    )

            hydrogen_flows = [
                exchange
                for exchange in pipeline.biosphere()
                if _normalized(exchange.get("name")) == "hydrogen"
            ]
            if len(hydrogen_flows) != 1:
                raise ValueError(
                    f"Expected one direct hydrogen flow in {pipeline.key}; "
                    f"found {len(hydrogen_flows)}."
                )
            hydrogen_exchange = hydrogen_flows[0]
            position = (self._biosphere_row(hydrogen_exchange.input), column)
            base_loss = float(self.base_biosphere[position])
            fixed_storage_loss = (
                base_loss - self.PIPELINE_BASE_TRANSMISSION_LEAK_KG
            )
            new_loss = (
                fixed_storage_loss + self.PIPELINE_BASE_TRANSMISSION_LEAK_KG * factor
            )
            biosphere[position] = self.base_biosphere[position] * (
                new_loss / base_loss
            )

        return technosphere, biosphere

    def scenario_matrices(self, mode: DistanceMode, distance_km: float):
        if mode.kind == "pipeline":
            return self._pipeline_matrices(mode, distance_km)
        return self._transport_matrices(mode, distance_km)

    def run(self, modes: Iterable[DistanceMode], unit: str) -> pd.DataFrame:
        baseline = self.baseline_scores()
        audit = mode_audit(self.reporting_markets, modes)
        share_lookup = audit.set_index(["transport mode", "market"])[
            "direct transport share"
        ].to_dict()
        rows = []

        try:
            for mode in modes:
                for distance in mode.distances_km:
                    technosphere, biosphere = self.scenario_matrices(mode, distance)
                    self._prepare_matrices(technosphere, biosphere)
                    for market_label in mode.market_labels:
                        score = self._score(self.reporting_markets[market_label])
                        baseline_score = baseline[market_label]
                        change = score - baseline_score
                        distance_change = float(distance) - mode.baseline_km
                        rows.append(
                            {
                                "transport mode": mode.label,
                                "market": market_label,
                                "market location": self.reporting_markets[market_label].get(
                                    "location"
                                ),
                                "scenario": (
                                    "Baseline"
                                    if np.isclose(distance, mode.baseline_km)
                                    else f"{distance:g} km"
                                ),
                                "distance (km)": float(distance),
                                "distance multiplier": float(distance) / mode.baseline_km,
                                "direct transport share": share_lookup.get(
                                    (mode.label, market_label), 0.0
                                ),
                                "unit": unit,
                                "score per kg H2": score,
                                "baseline score per kg H2": baseline_score,
                                "absolute change": change,
                                "change (%)": (
                                    100 * change / baseline_score
                                    if baseline_score != 0
                                    else np.nan
                                ),
                                "change per additional 100 km": (
                                    change / distance_change * 100
                                    if distance_change != 0
                                    else np.nan
                                ),
                            }
                        )
        finally:
            self.restore()

        results = pd.DataFrame(rows)
        baseline_rows = results[results["scenario"] == "Baseline"]
        maximum_baseline_error = float(
            (
                baseline_rows["score per kg H2"]
                - baseline_rows["baseline score per kg H2"]
            ).abs().max()
        )
        if maximum_baseline_error > 1e-9:
            raise AssertionError(
                "A baseline matrix override changed the LCIA score by "
                f"{maximum_baseline_error:.3e}."
            )
        return results


def plot_distance_curves(results: pd.DataFrame):
    import matplotlib.pyplot as plt

    modes = results["transport mode"].drop_duplicates().tolist()
    fig, axes = plt.subplots(
        len(modes), 1, figsize=(14, max(5, 4.5 * len(modes))), squeeze=False
    )
    for ax, mode in zip(axes.ravel(), modes):
        subset = results[results["transport mode"] == mode]
        for market, group in subset.groupby("market", sort=False):
            group = group.sort_values("distance (km)")
            ax.plot(
                group["distance (km)"],
                group["score per kg H2"],
                marker="o",
                linewidth=1.8,
                label=market,
            )
        ax.set_title(mode, loc="left")
        ax.set_xlabel("Transport distance (km)")
        ax.set_ylabel(
            f"premise-GWP ({subset['unit'].iloc[0]} / kg H2)"
        )
        ax.axvline(
            subset["distance (km)"].min(), color="#777777", linestyle="--", linewidth=0.9
        )
        ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", frameon=False)
    fig.suptitle("Transport-distance sensitivity of hydrogen-market premise-GWP", y=1.002)
    fig.tight_layout()
    plt.show()


def plot_high_distance_changes(results: pd.DataFrame):
    import matplotlib.pyplot as plt

    highest = (
        results.sort_values("distance (km)")
        .groupby(["transport mode", "market"], as_index=False)
        .tail(1)
    )
    matrix = highest.pivot(
        index="market", columns="transport mode", values="change (%)"
    )
    values = matrix.to_numpy(dtype=float)
    finite = values[np.isfinite(values)]
    limit = max(1.0, float(np.max(np.abs(finite)))) if finite.size else 1.0

    fig, ax = plt.subplots(
        figsize=(max(9, 2.1 * len(matrix.columns)), max(5, 0.7 * len(matrix.index) + 2))
    )
    image = ax.imshow(values, cmap="RdBu_r", vmin=-limit, vmax=limit, aspect="auto")
    ax.set_xticks(np.arange(len(matrix.columns)), matrix.columns, rotation=30, ha="right")
    ax.set_yticks(np.arange(len(matrix.index)), matrix.index)
    for row in range(values.shape[0]):
        for column in range(values.shape[1]):
            value = values[row, column]
            if np.isfinite(value):
                ax.text(
                    column,
                    row,
                    f"{value:+.2f}%",
                    ha="center",
                    va="center",
                    color="white" if abs(value) > 0.55 * limit else "black",
                )
    colorbar = fig.colorbar(image, ax=ax)
    colorbar.set_label("Change from baseline premise-GWP (%)")
    ax.set_title("Effect at the highest tested distance")
    fig.tight_layout()
    plt.show()
