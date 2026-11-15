"""Reproducible fleet evidence for reviewed gaps in TRAILS asset coverage."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook


def _distribution(ages, amounts):
    if len(ages) != len(amounts) or not ages:
        raise ValueError("Fleet ages and weights must be nonempty and aligned.")
    if any(not math.isfinite(float(x)) or x < 0 for x in [*ages, *amounts]):
        raise ValueError("Fleet observations must be finite and nonnegative.")
    total = sum(amounts)
    if total <= 0:
        raise ValueError("Fleet observations have no positive mass.")
    by_age = {}
    for age, amount in zip(ages, amounts, strict=True):
        by_age[age] = by_age.get(age, 0) + amount
    points = sorted(by_age.items(), reverse=True)
    weights = [round(amount / total, 12) for _, amount in points]
    weights[-1] += 1 - sum(weights)
    return {
        "kind": "discrete",
        "offsets_years": [-float(age) for age, _ in points],
        "weights": weights,
    }


def _profile(
    *,
    year,
    technology,
    geography,
    distribution,
    assumptions,
    source,
    population,
    weighting,
    sample,
    coverage,
    flags,
):
    return {
        "quantity": "construction_timing",
        "reference_event": "current_service",
        "distribution": distribution,
        "applicability": {
            "evidence_geographies": [geography],
            "reference_year": year,
            "technology": technology,
            "proxy_policy": "Use as an explicit geographic or inventory-technology proxy only while no stronger matching evidence is available.",
        },
        "assumptions": [
            *assumptions,
            "The observed age shape is reused for other service years; no scenario-specific future stock, retirement or utilisation model is inferred.",
            "Fleet construction timing does not establish prospective service life or lifetime-service attribution. Exchange amounts remain unchanged.",
        ],
        "evidence": {
            "status": "empirical",
            "source_ids": [source],
            "population": population,
            "weighting": weighting,
            "reference_period": str(year),
            "censoring": "Surviving stock; unknown ages or weights are excluded and known observations renormalised where applicable.",
            "sample_size": sample,
            "quality_flags": flags,
            "coverage": coverage,
        },
        "review": {
            "activation_status": "active",
            "rationale": "Evidence and transfer limitations are explicit; assignments are restricted to reviewed service-to-capital relationships.",
        },
    }


def bnetza_charger_profiles(source_file: Path):
    with source_file.open(encoding="cp1252") as stream:
        header = next(
            (
                i
                for i, line in enumerate(stream)
                if line.startswith("Ladeeinrichtungs-ID;")
            ),
            None,
        )
    if header is None:
        raise ValueError("Missing Bundesnetzagentur charger header.")
    columns = [
        "Ladeeinrichtungs-ID",
        "Status",
        "Art der Ladeeinrichtung",
        "Nennleistung Ladeeinrichtung [kW]",
        "Inbetriebnahmedatum",
    ]
    data = pd.read_csv(
        source_file,
        encoding="cp1252",
        sep=";",
        skiprows=header,
        usecols=columns,
        dtype=str,
    )
    if data[columns[0]].isna().any() or data[columns[0]].duplicated().any():
        raise ValueError("Missing or duplicated charger equipment IDs.")
    power = pd.to_numeric(
        data[columns[3]].str.replace(",", ".", regex=False), errors="coerce"
    )
    selected = data[
        data.Status.eq("In Betrieb")
        & data[columns[2]].eq("Schnellladeeinrichtung")
        & power.between(150, 250)
    ].copy()
    dates = pd.to_datetime(
        selected.Inbetriebnahmedatum, format="%d.%m.%Y", errors="coerce"
    )
    valid = dates.notna() & dates.between("2000-01-01", "2026-11-12")
    counts = dates[valid].dt.year.value_counts().sort_index()
    distribution = _distribution([2026 - int(y) for y in counts.index], counts.tolist())
    mean = sum((2026 - y) * n for y, n in counts.items()) / counts.sum()
    return {
        "bnetza-de-dc-plugin-charger-150-250kw-vintage-2026": _profile(
            year=2026,
            technology="150–250 kW public DC plug-in charging equipment",
            geography="Germany",
            distribution=distribution,
            source="bnetza-public-charger-register-2026-11-12",
            population="German publicly accessible fast-charging equipment in operation, nominal equipment power 150–250 kW, snapshot 12 November 2026.",
            weighting="One count per unique physical charging-equipment ID, not one per connector or vehicle.",
            sample={
                "count": int(valid.sum()),
                "unit": "charging-equipment IDs with known commissioning date",
            },
            assumptions=[
                "Only In Betrieb fast-charging equipment rated 150–250 kW is included; normal charging, maintenance-status equipment, duplicate IDs and future commissioning dates are excluded.",
                "Registered equipment commissioning dates approximate construction; year resolution is used and later component replacement is not observed.",
                "The power band brackets the inventory's 200 kW plug-in charger; nominal equipment power is distinct from simultaneous power per connector.",
                "Public German equipment is an explicit use-class and geographic proxy for private overnight bus-depot chargers. It does not represent pantographs.",
                "Assignments apply only to chargers directly consumed by current road-transport services; chargers embedded in new-truck production remain outside this mapping because their reference event differs.",
            ],
            coverage={
                "register_equipment_count": len(data),
                "operating_power_band_equipment_count": len(selected),
                "known_date_equipment_count": int(valid.sum()),
                "unknown_or_invalid_date_equipment_count": int((~valid).sum()),
                "derived_mean_age_years": round(float(mean), 6),
            },
            flags=[
                "equipment-count-weighted",
                "commissioning-dates",
                "150-250kw-band",
                "public-to-depot-proxy",
                "german-geographic-proxy",
                "replacement-dates-unobserved",
            ],
        )
    }


def remaining_charger_profiles():
    """Date documented bus-charging sites and allocated new-truck equipment.

    BERNMOBIL's route pages identify one operating terminus per route. These
    are site first-service cohorts, not a national equipment register. Elfenau
    on route 19 is still planned and must not enter the operating sample.
    """
    sites = [
        {
            "site": "Köniz, Weiermatt",
            "route": "17",
            "first_service_year": 2019,
            "source_id": "bernmobil-opportunity-charging-line-17",
        },
        {
            "site": "Bremgarten, Stuckishaus",
            "route": "21",
            "first_service_year": 2023,
            "source_id": "bernmobil-opportunity-charging-line-21",
        },
        {
            "site": "Köniz, Blinzern",
            "route": "19",
            "first_service_year": 2025,
            "source_id": "bernmobil-opportunity-charging-line-19",
        },
    ]
    profile = _profile(
        year=2026,
        technology="bus-terminus pantograph opportunity-charging infrastructure",
        geography="Bern area, Switzerland",
        distribution=_distribution(
            [2026 - s["first_service_year"] for s in sites], [1] * 3
        ),
        source="bernmobil-opportunity-charging-system",
        population="Three documented operating pantograph-charging sites serving BERNMOBIL routes 17, 21 and 19; not a national or European fleet census.",
        weighting="Equal weight per documented terminus site, not per bus, charging connector, equipment module, power or service output.",
        sample={
            "count": 3,
            "unit": "documented operating terminus sites",
            "minimum_for_generalisation": 30,
        },
        assumptions=[
            "Use first regular electric-service years as proxies for site construction: Weiermatt 2019, Stuckishaus 2023 and Blinzern 2025. The operator confirms pantograph opportunity charging on all three routes.",
            "ABB dates the line-17 pilot to December 2018 and identifies 450 kW charging; the operator dates regular pilot operation from 2019. The regular-service convention introduces up to one year of dating uncertainty.",
            "The line-19 Elfenau site remains blocked by objections and is excluded; depot overnight chargers, vehicle counts and planned sites are excluded.",
            "The three sites are a small, geographically concentrated convenience sample. Equal site weighting and transfer to all 450 kW pantograph inventory designs are modelling assumptions; the later two sites' ratings and module counts are not established by these sources.",
            "Current project pages establish operating sites but not uninterrupted survival of the original equipment. Site first service does not identify later charger replacement or refurbishment.",
        ],
        coverage={
            "site_cohorts": sites,
            "excluded_planned_sites": ["Bern, Elfenau (line 19)"],
            "derived_mean_age_years": round(11 / 3, 6),
        },
        flags=[
            "small-sample",
            "site-count-weighted",
            "first-service-year-proxy",
            "swiss-geographic-proxy",
            "450kw-rating-transfer",
            "replacement-dates-unobserved",
        ],
    )
    # Construction dates and representative equipment weights are inferred,
    # although the site identities and route-service dates are documented.
    profile["evidence"]["status"] = "modelled"
    profile["evidence"]["source_ids"].extend(
        [s["source_id"] for s in sites] + ["abb-bernmobil-pantograph-pilot-2020"]
    )
    profile["evidence"][
        "censoring"
    ] = "Only documented operating sites are included; planned Elfenau and depot chargers are excluded. Equipment replacements within a site are unobserved."
    profile["review"][
        "rationale"
    ] = "Activate as a transparent small-sample European technology proxy while no representative pantograph equipment-vintage register is available. Stronger local evidence can override this low-priority assignment."
    return {
        "bernmobil-ch-pantograph-site-vintage-2026": profile,
        "reviewed-truck-allocated-charger-construction-event": {
            "quantity": "construction_timing",
            "reference_event": "construction",
            "distribution": {"kind": "fixed", "offset_years": 0},
            "applicability": {
                "technology": "200 kW plug-in charger allocated within new electric/hybrid truck production"
            },
            "assumptions": [
                "The truck workbook allocates fractions of a shared charger to new-truck production rather than directly to current freight service. Preserve that inventory placement and all amounts.",
                "Date the allocated charger in the parent truck construction year, at annual resolution. When a truck fleet-age profile dates vehicle production from freight service, this charger inherits that date without a second age shift.",
                "This is a construction-event convention, not a measured charger fleet age or a claim that truck and shared charger are physically co-manufactured. Independent charger commissioning and replacement cannot be recovered from this allocation.",
                "Direct transport-service charger inputs retain their independent reviewed fleet profiles. Changing the shared-infrastructure service allocation is outside this timing correction.",
                "Prospective service life, exchange amounts and inventory topology are unchanged.",
            ],
            "evidence": {
                "status": "assumption",
                "source_ids": [
                    "trails-truck-charger-inventory-placement",
                    "trails-construction-event-convention",
                ],
                "quality_flags": [
                    "allocated-shared-infrastructure",
                    "construction-year-convention",
                    "independent-charger-vintages-unresolved",
                ],
            },
            "review": {
                "activation_status": "active",
                "rationale": "Replace a legacy lifetime-shaped offset against new-truck construction with an explicit zero additional construction offset; preserve the supplied inventory allocation.",
            },
        },
    }


def sigeif_low_voltage_profiles():
    # SIGEIF 2023 electricity control report, printed p. 19, Table P8.
    lengths = [665788, 600970, 787969, 1174610, 199680, 1980035]
    ages = [5, 15.5, 25.5, 35.5, 45.5]
    reported_mean = 42
    tail_age = (
        reported_mean * sum(lengths) - sum(a * l for a, l in zip(ages, lengths))
    ) / lengths[-1]
    if tail_age < 51:
        raise ValueError("Reported grid mean is inconsistent with the oldest band.")
    ages.append(tail_age)
    return {
        "sigeif-fr-low-voltage-network-vintage-2023": _profile(
            year=2023,
            technology="low-voltage distribution-network infrastructure",
            geography="SIGEIF concession, Île-de-France, France",
            distribution=_distribution(ages, lengths),
            source="sigeif-electricity-control-report-2023",
            population="Low-voltage overhead and underground lines in the 66-commune SIGEIF concession at end-2023, Enedis technical inventory.",
            weighting="Operating line length in metres, aggregated across bare overhead, bundled overhead and underground lines.",
            sample={"count": 66, "unit": "concession municipalities"},
            assumptions=[
                "Table P8 reports six surviving-line age bands and an exact total of 5,409,052 metres; all three construction types are included.",
                "The labels <11, 11–20, 21–30, 31–40 and 1973–1982 are represented by integer-age midpoints 5, 15.5, 25.5, 35.5 and 45.5 years.",
                "The open pre-1973 band is represented at a derived age that reproduces the separately reported rounded 42-year mean. This tail representative is modelled, not an observed oldest-cohort mean or maximum.",
                "Calibration assumes that the reported overall mean describes the same population with the same length weighting as Table P8; a different aggregation would require another tail assumption.",
                "The old-tail age changes by about 1.37 years for a 0.5-year change in the reported overall mean; closed-band and geographic uncertainty are additional.",
                "Local urban and suburban French line ages are a proxy for the bundled low-voltage network consumed by electricity services elsewhere.",
                "Line-length weighting does not date meters, transformers, substations or communications equipment separately. Medium- and high-voltage networks are excluded.",
            ],
            coverage={
                "line_length_m": sum(lengths),
                "line_length_m_youngest_first": lengths,
                "source_age_band_labels_youngest_first": [
                    "<11 years",
                    "11–20 years",
                    "21–30 years",
                    "31–40 years",
                    "1973–1982",
                    "before 1973",
                ],
                "representative_ages_years_youngest_first": ages,
                "reported_mean_age_years_rounded": reported_mean,
                "derived_oldest_band_age_years": round(tail_age, 6),
            },
            flags=[
                "line-length-weighted",
                "subnational-geographic-proxy",
                "open-tail-calibrated-to-rounded-mean",
                "bundled-network-proxy",
                "low-voltage-only",
            ],
        )
    }


def eia_storage_profiles(source_file: Path):
    data = pd.read_excel(source_file, sheet_name="Operable", header=1)
    selected = data[data.Status.eq("OP") & data.Technology.eq("Batteries")].copy()
    if selected.duplicated(["Plant Code", "Generator ID"]).any():
        raise ValueError("Duplicated operating storage-generator IDs.")
    capacity = pd.to_numeric(
        selected["Nameplate Energy Capacity (MWh)"], errors="coerce"
    )
    year = pd.to_numeric(selected["Operating Year"], errors="coerce")
    positive = capacity.gt(0) & capacity.map(math.isfinite)
    valid = positive & year.between(1900, 2025) & year.mod(1).eq(0)
    if not valid.any():
        raise ValueError("No valid operating battery energy capacity.")
    total = float(capacity[positive].sum())
    known = float(capacity[valid].sum())
    lithium_share = float(
        capacity[valid & selected["Storage Technology 1"].eq("LIB")].sum() / known
    )
    mean = float(((2025 - year[valid]) * capacity[valid]).sum() / known)
    return {
        "eia-us-stationary-battery-energy-vintage-2025": _profile(
            year=2025,
            technology="utility-scale stationary battery systems",
            geography="United States",
            distribution=_distribution(
                (2025 - year[valid]).tolist(), capacity[valid].tolist()
            ),
            source="eia-860-energy-storage-2025",
            population="Operating battery generators in EIA-860's 2025 energy-storage schedule; utility-scale survey population.",
            weighting="Nameplate stored-energy capacity in MWh, not discharge power in MW or annual delivered energy.",
            sample={
                "count": int(valid.sum()),
                "unit": "operating battery generators with known year and positive energy capacity",
            },
            assumptions=[
                "Status OP and technology Batteries only; standby, out-of-service, planned units and non-battery storage are excluded.",
                "Operating year approximates whole-system commissioning. Later cell replacement, augmentation and repowering are not separately dated.",
                "Energy-capacity weighting matches a whole storage-capacity exchange better than generator count or discharge-power weighting.",
                "The mixed U.S. utility fleet is an explicit geographic, size and chemistry-mix proxy for the inventory's stationary-battery portfolio. Its chemistry shares are not imposed on the inventory.",
                "The mapping applies once at electricity supply to the complete stationary-capacity portfolio; individual cell, pack, control, mobile-battery and disposal exchanges are excluded.",
            ],
            coverage={
                "operating_battery_count": len(selected),
                "known_year_energy_capacity_count": int(valid.sum()),
                "known_year_energy_capacity_mwh": round(known, 6),
                "known_year_energy_capacity_share": round(known / total, 6),
                "lithium_ion_energy_capacity_share": round(lithium_share, 6),
                "derived_mean_age_years": round(mean, 6),
            },
            flags=[
                "energy-capacity-weighted",
                "utility-scale-only",
                "mixed-chemistry-proxy",
                "us-geographic-proxy",
                "cell-replacements-unobserved",
            ],
        )
    }


def nbd_factory_profiles(source_file: Path):
    workbook = load_workbook(source_file, read_only=True, data_only=True)
    try:
        rows = list(workbook["Table_3_16"].iter_rows(values_only=True))
    finally:
        workbook.close()
    header = next(row for row in rows if row[0] == "CaRBE Activity Grouping")
    matches = [row for row in rows if row[0] == "Factories etc" and row[1] == "FA_B"]
    if len(matches) != 1:
        raise ValueError("Expected exactly one NBD Factories etc age row.")
    labels = [
        "pre-1919",
        "1919-1939",
        "1940-1975",
        "1976-1990",
        "1991-2006",
        "2007-2021",
        "2022 onwards",
    ]
    shares = [float(matches[0][header.index(label)]) for label in labels]
    unknown = float(matches[0][header.index("Age Null")])
    if (
        any(not math.isfinite(x) or x < 0 for x in [*shares, unknown])
        or abs(sum(shares) + unknown - 100) > 0.5
    ):
        raise ValueError("Invalid NBD factory floor-area age shares.")
    ages = [123, 94, 65.5, 40, 24.5, 9, 0.5]
    mean = sum(a * w for a, w in zip(ages, shares, strict=True)) / sum(shares)
    return {
        "desnz-gb-factory-building-vintage-2023": _profile(
            year=2023,
            technology="factory production buildings",
            geography="Great Britain",
            distribution=_distribution(ages, shares),
            source="desnz-nbd-factory-summary-2026",
            population="Factories etc (FA_B) premises in the National Buildings Database's 2023 Great Britain population.",
            weighting="Reported floor-area shares by construction period, Table 3_16; unknown-age floor area excluded.",
            sample={
                "not_reported": "Table 3_16 reports area shares, not the number of premises with a known age.",
                "unit": "factory premises represented by floor area",
            },
            assumptions=[
                "Only the Factories etc subgroup is used; workshops, vehicle repair, food, mineral and other activity groups are not summed or substituted.",
                "Construction-period shares are rounded to 0.1 percentage point; unknown ages are excluded and the reported known shares renormalised.",
                "Closed periods use midpoint construction years; pre-1919 is represented by 1900 and 2022-onwards by mid-2022 to 2023. These tail representatives are modelling assumptions.",
                "The building stock is a proxy for shell-dominated photovoltaic-cell-factory capital and for metal-coating facilities represented by a building hall; it does not date current process machinery.",
                "The photovoltaic-factory inventory also contains machinery. Applying building age to that small separate input is an unresolved whole-facility approximation; no claim about its impact share is made.",
                "Whole chemical plants, batteries, steel furnaces and other process units are excluded. Manufacturing equipment replacement and major refurbishment are not inferred from building age.",
            ],
            coverage={
                "activity_group": "Factories etc",
                "activity_code": "FA_B",
                "age_band_labels_oldest_first": labels,
                "floor_area_shares_percent_oldest_first": shares,
                "unknown_age_floor_area_percent": unknown,
                "known_age_floor_area_share": round(
                    sum(shares) / (sum(shares) + unknown), 6
                ),
                "representative_ages_years_oldest_first": ages,
                "derived_mean_age_years": round(mean, 6),
            },
            flags=[
                "floor-area-weighted",
                "factory-building-proxy",
                "process-equipment-age-unresolved",
                "rounded-shares",
                "open-tail-modelled",
                "british-geographic-proxy",
            ],
        )
    }
