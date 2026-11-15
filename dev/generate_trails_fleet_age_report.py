"""Generate a PDF catalogue of TRAILS fleet-age and construction-vintage profiles.

The report is generated from the packaged YAML registry. It shows every runtime-
assigned fleet construction profile once, plus selected evidence-only profiles
that do not have an active equivalent.

Example::

    PYTHONPATH=. conda run -n premise python \
        dev/generate_trails_fleet_age_report.py
"""

from __future__ import annotations

import argparse
import itertools
import math
import textwrap
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import MaxNLocator, PercentFormatter

from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_specs import TemporalRegistry

DEFAULT_OUTPUT = Path("docs/_static/trails_fleet_age_profiles.pdf")
SUPPLEMENTAL_PROFILE_IDS = {
    "eu-road-passenger-car-age-2024",
    "eu-road-tractor-construction-vintage-2024",
    "us-electricity-batteries-age-2025",
    "us-electricity-offshore-wind-turbine-age-2025",
    "fraunhofer-eu28-coal-boiler-furnace-age-2012",
    "eurelectric-eu27-uk-low-voltage-line-age-ranges-2020",
    "gem-gcht-europe-methanol-site-vintage-2026",
    "gem-gcht-world-methanol-site-vintage-2026",
    "gem-gcht-europe-ammonia-site-vintage-2026",
    "gem-gcht-world-ammonia-site-vintage-2026",
}
CATEGORY_RULES = OrderedDict(
    [
        ("Road vehicles", ("eu-road-", "dft-uk-")),
        ("EV charging infrastructure", ("bnetza-de-", "bernmobil-ch-")),
        ("Commercial aircraft", ("eurostat-eu27-commercial-",)),
        ("Rail freight", ("ec-eu-freight-wagon-",)),
        ("Passenger rail", ("orr-gb-passenger-rail-",)),
        ("Inland freight vessels", ("ccnr-rhine-",)),
        ("Seagoing freight", ("unctad-global-", "igu-world-")),
        (
            "Electricity generation and storage",
            ("eia-us-", "us-electricity-", "gem-gspt-", "gem-gwpt-"),
        ),
        ("Wood biomass CHP", ("gem-gbpt-europe-wood-", "gem-gbpt-world-wood-")),
        (
            "Municipal waste incineration",
            ("gem-gbpt-europe-waste-", "gem-gbpt-world-waste-"),
        ),
        ("Non-residential buildings", ("moderate-eu-",)),
        ("Factory buildings", ("desnz-gb-",)),
        ("Building-heating equipment", ("fraunhofer-eu28-",)),
        ("Electricity distribution grids", ("eurelectric-", "sigeif-fr-")),
        ("Cement plants", ("gem-gcct-",)),
        ("Iron and steel furnaces", ("gem-gist-",)),
        ("Transmission pipelines", ("gem-ggit-", "gem-goit-")),
        ("Coal mines", ("gem-gcmt-",)),
        ("Offshore production platforms", ("nod-norway-",)),
        ("Chemical sites - evidence only", ("gem-gcht-",)),
    ]
)
PROFILE_LABELS = {
    "eu-road-passenger-car-age-2024": "Passenger cars",
    "eu-road-bus-construction-vintage-2024": "Buses and coaches",
    "eu-road-lorry-over-3.5t-construction-vintage-2024": "Lorries over 3.5 t",
    "eu-road-tractor-construction-vintage-2024": "Road tractors",
    "eu-road-light-commercial-construction-vintage-2024": (
        "Light commercial vehicles up to 3.5 t"
    ),
}
STATUS_COLORS = {
    "runtime assigned": "#087E8B",
    "evidence only": "#4C78A8",
    "eligible": "#E68600",
    "hold": "#B33A3A",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def category_for(profile_id: str) -> str:
    for category, prefixes in CATEGORY_RULES.items():
        if profile_id.startswith(prefixes):
            return category
    raise ValueError(f"No report category configured for {profile_id}")


def selected_profiles(
    registry: TemporalRegistry,
) -> OrderedDict[str, list[tuple[str, dict[str, Any], str]]]:
    assigned = {assignment["profile"] for assignment in registry.assignments}
    grouped = OrderedDict((category, []) for category in CATEGORY_RULES)
    for profile_id, profile in registry.profiles.items():
        if profile.get("quantity") not in {"fleet_age", "construction_timing"}:
            continue
        if profile.get("reference_event") != "current_service":
            continue
        if profile_id in assigned:
            status = "runtime assigned"
        elif profile_id in SUPPLEMENTAL_PROFILE_IDS:
            status = (
                profile.get("review", {})
                .get("activation_status", "evidence only")
                .replace("_", " ")
            )
        else:
            continue
        grouped[category_for(profile_id)].append((profile_id, profile, status))
    return grouped


def geography(profile: dict[str, Any]) -> str:
    applicability = profile.get("applicability", {})
    geographies = applicability.get("evidence_geographies") or applicability.get(
        "geographies"
    )
    if geographies:
        return ", ".join(str(item) for item in geographies)
    coverage = profile.get("evidence", {}).get("coverage", {})
    return str(coverage.get("geography", "Unspecified geography"))


def profile_label(profile_id: str, profile: dict[str, Any]) -> str:
    technology = PROFILE_LABELS.get(profile_id) or profile.get("applicability", {}).get(
        "technology"
    )
    technology = technology or profile.get("applicability", {}).get("vehicle")
    if technology is None:
        technology = profile_id.replace("-", " ")
    return f"{str(technology).capitalize()} — {geography(profile)}"


def source_line(profile: dict[str, Any], sources: dict[str, dict[str, Any]]) -> str:
    parts = []
    for source_id in profile.get("evidence", {}).get("source_ids", []):
        source = sources[source_id]
        title = source.get("title", source_id)
        publisher = source.get("publisher")
        parts.append(f"{publisher}: {title}" if publisher else title)
    return "; ".join(parts) if parts else "Documented modelling assumption"


def format_age(value: float) -> str:
    numeric = float(value)
    return f"{numeric:g}"


def band_label(band: dict[str, Any]) -> str:
    minimum = band.get("minimum_years")
    maximum = band.get("maximum_years")
    if minimum is not None and maximum is None:
        return f"{format_age(minimum)}+"
    if minimum == 0 and maximum is not None:
        return f"<{format_age(maximum)}"
    if minimum is not None and maximum is not None:
        return f"{format_age(minimum)}–{format_age(maximum)}"
    return str(band.get("source_period") or band.get("source_code") or "unknown")


def discrete_mean(distribution: dict[str, Any]) -> float:
    ages = [-float(offset) for offset in distribution["offsets_years"]]
    return sum(
        age * float(weight)
        for age, weight in zip(ages, distribution["weights"], strict=True)
    )


def plot_discrete(ax: plt.Axes, distribution: dict[str, Any], color: str) -> None:
    values = sorted(
        (
            -float(offset),
            100 * float(weight),
        )
        for offset, weight in zip(
            distribution["offsets_years"], distribution["weights"], strict=True
        )
    )
    ages = [item[0] for item in values]
    shares = [item[1] for item in values]
    if len(values) > 35:
        ax.vlines(ages, 0, shares, color=color, linewidth=0.8, alpha=0.8)
        ax.plot(ages, shares, color=color, linewidth=0.8, alpha=0.65)
    else:
        distinct = sorted(set(ages))
        spacing = min(
            (right - left for left, right in itertools.pairwise(distinct)),
            default=1,
        )
        ax.bar(
            ages,
            shares,
            width=max(0.35, min(2.5, 0.72 * spacing)),
            color=color,
            alpha=0.82,
            edgecolor="white",
            linewidth=0.35,
        )
    mean = discrete_mean(distribution)
    ax.axvline(mean, color="#333333", linestyle="--", linewidth=0.9)
    ax.text(
        mean,
        0.98,
        f"mean {mean:.1f} y",
        transform=ax.get_xaxis_transform(),
        ha="right" if mean > max(ages, default=0) / 2 else "left",
        va="top",
        fontsize=6,
        color="#333333",
        rotation=90,
    )
    ax.set_xlim(left=0, right=max(ages, default=1) * 1.05 + 1)
    ax.set_xlabel("Asset age (years)", fontsize=7)


def plot_bands(ax: plt.Axes, distribution: dict[str, Any], color: str) -> None:
    bands = sorted(
        distribution["parameters"]["bands"],
        key=lambda item: float(item.get("minimum_years") or 0),
    )
    labels = [band_label(band) for band in bands]
    shares = [100 * float(band["weight"]) for band in bands]
    ax.bar(
        range(len(bands)),
        shares,
        color=color,
        alpha=0.82,
        edgecolor="white",
        linewidth=0.5,
    )
    ax.set_xticks(range(len(bands)), labels, fontsize=6.5)
    ax.set_xlabel("Reported age band (years)", fontsize=7)


def plot_ranges(ax: plt.Axes, distribution: dict[str, Any], color: str) -> None:
    bands = sorted(
        distribution["parameters"]["bands"],
        key=lambda item: float(item.get("minimum_years") or 0),
    )
    labels = [band_label(band) for band in bands]
    lower = [100 * float(band["minimum_share"]) for band in bands]
    upper = [100 * float(band["maximum_share"]) for band in bands]
    middle = [(minimum + maximum) / 2 for minimum, maximum in zip(lower, upper)]
    errors = [
        [middle_value - minimum for middle_value, minimum in zip(middle, lower)],
        [maximum - middle_value for middle_value, maximum in zip(middle, upper)],
    ]
    ax.errorbar(
        range(len(bands)),
        middle,
        yerr=errors,
        fmt="o",
        color=color,
        ecolor=color,
        capsize=5,
        linewidth=2,
    )
    ax.set_xticks(range(len(bands)), labels, fontsize=6.5)
    ax.set_xlabel("Reported age band (years)", fontsize=7)


def plot_profile(
    ax: plt.Axes,
    profile_id: str,
    profile: dict[str, Any],
    status: str,
    sources: dict[str, dict[str, Any]],
) -> None:
    color = STATUS_COLORS[status]
    distribution = profile["distribution"]
    if distribution["kind"] == "discrete":
        plot_discrete(ax, distribution, color)
        if profile_id == "sigeif-fr-low-voltage-network-vintage-2023":
            ax.text(
                0.03,
                0.97,
                "Oldest-band point calibrated from rounded mean;\n"
                "the distribution within that open band is unknown.",
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=6,
                color="#444444",
            )
        if profile_id.startswith("dft-uk-"):
            points = sorted(
                zip(
                    [-float(x) for x in distribution["offsets_years"]],
                    distribution["weights"],
                    strict=True,
                )
            )
            cumulative = 0
            for age, weight in points:
                cumulative += weight
                if cumulative >= 0.995:
                    break
            display_max = max(30, math.ceil(age / 5) * 5)
            tail = sum(w for a, w in points if a > display_max)
            if tail > 0:
                ax.set_xlim(left=-0.5, right=display_max + 0.5)
                ax.text(
                    0.98,
                    0.87,
                    f"{100 * tail:.2f}% older than {display_max} y\n"
                    f"Full data to {max(a for a, _ in points):g} y retained",
                    transform=ax.transAxes,
                    ha="right",
                    va="top",
                    fontsize=6,
                    color="#444444",
                )
    elif distribution.get("generator") == "uniform_within_reported_age_bands":
        plot_bands(ax, distribution, color)
    elif distribution.get("generator") == "reported_age_band_share_ranges":
        plot_ranges(ax, distribution, color)
    else:
        raise ValueError(f"Unsupported distribution for report: {profile_id}")

    ax.yaxis.set_major_locator(MaxNLocator(5))
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    ax.set_ylabel("Fleet share", fontsize=7)
    ax.tick_params(axis="both", labelsize=6.5)
    ax.grid(axis="y", color="#D9D9D9", linewidth=0.55, alpha=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color("#999999")
    ax.spines["bottom"].set_color("#999999")
    ax.set_title(
        textwrap.fill(profile_label(profile_id, profile), 50),
        loc="left",
        fontsize=9,
        fontweight="bold",
        color="#153243",
        pad=8,
    )
    ax.text(
        1,
        1.02,
        status.upper(),
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=6,
        fontweight="bold",
        color=color,
    )
    evidence = profile.get("evidence", {})
    reference_year = profile.get("applicability", {}).get("reference_year")
    detail = " | ".join(
        item
        for item in (
            f"Reference: {reference_year}" if reference_year else None,
            (
                "Modelled proxy: 3 Swiss sites; first-service dates; replacements unobserved"
                if profile_id == "bernmobil-ch-pantograph-site-vintage-2026"
                else None
            ),
            (
                f"Weighting: {evidence.get('weighting')}"
                if evidence.get("weighting")
                else None
            ),
            f"Profile: {profile_id}",
        )
        if item
    )
    source = f"Source: {source_line(profile, sources)}"
    ax.text(
        0,
        -0.25,
        textwrap.fill(detail, 82) + "\n" + textwrap.fill(source, 82),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=5.7,
        color="#444444",
    )


def cover_page(
    pdf: PdfPages,
    grouped: OrderedDict[str, list[tuple[str, dict[str, Any], str]]],
) -> None:
    total = sum(len(items) for items in grouped.values())
    runtime = sum(
        status == "runtime assigned"
        for items in grouped.values()
        for _, _, status in items
    )
    fig = plt.figure(figsize=(11.69, 8.27), facecolor="#F7F4ED")
    fig.text(
        0.075,
        0.78,
        "TRAILS fleet-age distribution profiles",
        fontsize=28,
        fontweight="bold",
        color="#153243",
    )
    fig.text(
        0.078,
        0.71,
        "Runtime construction-vintage profiles and distinct unresolved evidence",
        fontsize=15,
        color="#087E8B",
    )
    fig.text(
        0.078,
        0.61,
        f"{total} profiles shown  •  {runtime} assigned at runtime  •  "
        f"{total - runtime} evidence, eligible, or held",
        fontsize=12,
        color="#333333",
    )
    explanation = (
        "The charts describe the age composition of surviving assets or the "
        "construction-vintage approximations applied to whole-asset exchanges. "
        "They are not prospective lifetime distributions. A current stock profile "
        "also differs from an attribution-based production history when utilisation, "
        "survival, or historical inflow varies by cohort."
    )
    fig.text(
        0.078,
        0.46,
        textwrap.fill(explanation, 100),
        fontsize=11,
        color="#333333",
        linespacing=1.5,
    )
    fig.text(
        0.078,
        0.18,
        "Generated directly from premise/data/trails/temporal/*.yaml\n"
        f"Report date: {date.today().isoformat()}",
        fontsize=9,
        color="#666666",
        linespacing=1.5,
    )
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def overview_page(
    pdf: PdfPages,
    grouped: OrderedDict[str, list[tuple[str, dict[str, Any], str]]],
) -> None:
    fig, ax = plt.subplots(figsize=(11.69, 8.27))
    ax.axis("off")
    fig.suptitle(
        "Coverage by activity type",
        x=0.06,
        y=0.95,
        ha="left",
        fontsize=20,
        fontweight="bold",
        color="#153243",
    )
    rows = []
    for category, items in grouped.items():
        runtime = sum(status == "runtime assigned" for _, _, status in items)
        other = len(items) - runtime
        source_publishers = sorted(
            {
                publisher
                for _, profile, _ in items
                for source_id in profile.get("evidence", {}).get("source_ids", [])
                if (publisher := registry_source_publisher(source_id))
            }
        )
        rows.append(
            [
                category,
                str(runtime),
                str(other),
                ", ".join(source_publishers),
            ]
        )
    table = ax.table(
        cellText=rows,
        colLabels=["Activity type", "Runtime", "Other evidence", "Publishers"],
        cellLoc="left",
        colLoc="left",
        colWidths=[0.31, 0.10, 0.13, 0.46],
        bbox=[0.04, 0.18, 0.92, 0.66],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    for (row, column), cell in table.get_celld().items():
        cell.set_edgecolor("#D9D9D9")
        if row == 0:
            cell.set_facecolor("#153243")
            cell.get_text().set_color("white")
            cell.get_text().set_fontweight("bold")
        elif row % 2:
            cell.set_facecolor("#F3F6F7")
        if column in {1, 2} and row:
            cell.get_text().set_ha("center")
    fig.text(
        0.06,
        0.09,
        "‘Other evidence’ includes profiles retained for transparency but not applied "
        "at runtime. Source details and URLs are listed at the end of this report.",
        fontsize=8.5,
        color="#555555",
    )
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


_REPORT_SOURCES: dict[str, dict[str, Any]] = {}


def registry_source_publisher(source_id: str) -> str | None:
    return _REPORT_SOURCES.get(source_id, {}).get("publisher")


def profile_pages(
    pdf: PdfPages,
    category: str,
    items: list[tuple[str, dict[str, Any], str]],
    sources: dict[str, dict[str, Any]],
) -> None:
    page_count = math.ceil(len(items) / 4)
    for page_index in range(page_count):
        page_items = items[page_index * 4 : (page_index + 1) * 4]
        fig, axes = plt.subplots(2, 2, figsize=(11.69, 8.27))
        fig.patch.set_facecolor("white")
        fig.suptitle(
            category,
            x=0.055,
            y=0.975,
            ha="left",
            fontsize=18,
            fontweight="bold",
            color="#153243",
        )
        if page_count > 1:
            fig.text(
                0.945,
                0.968,
                f"{page_index + 1}/{page_count}",
                ha="right",
                fontsize=8,
                color="#666666",
            )
        for ax, item in zip(axes.flat, page_items, strict=False):
            plot_profile(ax, *item, sources)
        for ax in axes.flat[len(page_items) :]:
            ax.axis("off")
        fig.subplots_adjust(
            left=0.07,
            right=0.97,
            top=0.89,
            bottom=0.13,
            hspace=0.68,
            wspace=0.20,
        )
        pdf.savefig(fig)
        plt.close(fig)


def source_pages(
    pdf: PdfPages,
    source_ids: list[str],
    sources: dict[str, dict[str, Any]],
) -> None:
    per_page = 5
    page_count = math.ceil(len(source_ids) / per_page)
    for page_index in range(page_count):
        fig = plt.figure(figsize=(11.69, 8.27), facecolor="white")
        fig.text(
            0.06,
            0.94,
            "Source catalogue",
            fontsize=20,
            fontweight="bold",
            color="#153243",
        )
        fig.text(
            0.94,
            0.94,
            f"{page_index + 1}/{page_count}",
            ha="right",
            fontsize=8,
            color="#666666",
        )
        y = 0.86
        for source_id in source_ids[
            page_index * per_page : (page_index + 1) * per_page
        ]:
            source = sources[source_id]
            heading = (
                f"{source.get('publisher', '')} — {source.get('title', source_id)}"
            )
            fig.text(
                0.06,
                y,
                textwrap.fill(heading, 105),
                fontsize=9.5,
                fontweight="bold",
                color="#153243",
            )
            y -= 0.05
            metadata = " | ".join(
                str(item)
                for item in (
                    source.get("published"),
                    source.get("kind"),
                    source_id,
                )
                if item
            )
            fig.text(
                0.06, y, textwrap.fill(metadata, 125), fontsize=7.2, color="#555555"
            )
            y -= 0.035
            coverage = source.get("coverage")
            if coverage:
                fig.text(
                    0.06,
                    y,
                    textwrap.fill(f"Coverage: {coverage}", 135),
                    fontsize=7.2,
                    color="#333333",
                )
                y -= 0.05
            uri = source.get("uri") or source.get("project_uri")
            if uri:
                fig.text(
                    0.06,
                    y,
                    textwrap.fill(str(uri), 140, break_long_words=True),
                    fontsize=6.8,
                    color="#087E8B",
                    url=str(uri),
                )
                y -= 0.065
            y -= 0.02
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)


def generate_report(output: Path) -> None:
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    grouped = selected_profiles(registry)
    global _REPORT_SOURCES
    _REPORT_SOURCES = registry.sources
    used_source_ids = sorted(
        {
            source_id
            for items in grouped.values()
            for _, profile, _ in items
            for source_id in profile.get("evidence", {}).get("source_ids", [])
        },
        key=lambda source_id: (
            registry.sources[source_id].get("publisher", ""),
            registry.sources[source_id].get("title", source_id),
        ),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "Title": "TRAILS fleet-age distribution profiles",
        "Author": "premise TRAILS registry",
        "Subject": "Fleet-age evidence and runtime construction-vintage profiles",
        "Keywords": "premise, TRAILS, fleet age, construction vintage, LCA",
        "CreationDate": None,
        "ModDate": None,
    }
    with PdfPages(output, metadata=metadata) as pdf:
        cover_page(pdf, grouped)
        overview_page(pdf, grouped)
        for category, items in grouped.items():
            profile_pages(pdf, category, items, registry.sources)
        source_pages(pdf, used_source_ids, registry.sources)


def main() -> None:
    args = parse_args()
    generate_report(args.output)
    print(args.output.resolve())


if __name__ == "__main__":
    main()
