"""Evidence conversion and event boundaries for the remaining asset gaps."""

import pandas as pd
import pytest
from openpyxl import Workbook

from dev.trails_gap_profiles import (
    bnetza_charger_profiles,
    eia_storage_profiles,
    nbd_factory_profiles,
    remaining_charger_profiles,
    sigeif_low_voltage_profiles,
)
from premise import trails
from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_specs import TemporalRegistry


def test_audit_enriches_canonical_reference_product_for_product_selectors():
    from dev.audit_trails_temporal_selectors import exchange_record

    supplier = {
        "name": "market for distribution network, electricity, low voltage",
        "reference product": "distribution network, electricity, low voltage",
        "unit": "kilometer",
        "location": "CH",
    }
    record = exchange_record(
        {"input": ("db", "network")}, {("db", "network"): supplier}
    )
    assert record["reference product"] == supplier["reference product"]
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    result = registry.resolve(
        record,
        target="supplier",
        context={"ecoinvent_version": "3.12"},
        caller_record={
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "unit": "kilowatt hour",
        },
    )
    assert (
        result is not None
        and result.profile_id == "sigeif-fr-low-voltage-network-vintage-2023"
    )


def test_charger_filter_uses_equipment_commissioning_and_matching_power(tmp_path):
    columns = [
        "Ladeeinrichtungs-ID",
        "Status",
        "Art der Ladeeinrichtung",
        "Nennleistung Ladeeinrichtung [kW]",
        "Inbetriebnahmedatum",
    ]
    rows = [
        ["1", "In Betrieb", "Schnellladeeinrichtung", "200,0", "01.01.2016"],
        ["2", "In Betrieb", "Schnellladeeinrichtung", "150", "01.01.2026"],
        ["3", "In Betrieb", "Schnellladeeinrichtung", "400", "01.01.2010"],
        ["4", "In Wartung", "Schnellladeeinrichtung", "200", "01.01.2010"],
        ["5", "In Betrieb", "Normalladeeinrichtung", "200", "01.01.2010"],
        ["6", "In Betrieb", "Schnellladeeinrichtung", "200", "01.01.2027"],
        ["7", "In Betrieb", "Schnellladeeinrichtung", "200", "unknown"],
    ]
    path = tmp_path / "chargers.csv"
    path.write_text(
        "Source notes\n"
        + pd.DataFrame(rows, columns=columns).to_csv(index=False, sep=";"),
        encoding="cp1252",
    )
    profile = next(iter(bnetza_charger_profiles(path).values()))
    assert profile["distribution"] == {
        "kind": "discrete",
        "offsets_years": [-10.0, 0.0],
        "weights": [0.5, 0.5],
    }
    assert profile["evidence"]["coverage"]["operating_power_band_equipment_count"] == 4
    assert profile["evidence"]["coverage"]["known_date_equipment_count"] == 2
    assert "public-to-depot-proxy" in profile["evidence"]["quality_flags"]
    frame = pd.DataFrame(rows + [rows[0]], columns=columns)
    path.write_text(frame.to_csv(index=False, sep=";"), encoding="cp1252")
    with pytest.raises(ValueError, match="duplicated"):
        bnetza_charger_profiles(path)


def test_storage_weights_energy_capacity_and_aggregates_cohorts(tmp_path):
    columns = [
        "Plant Code",
        "Generator ID",
        "Status",
        "Technology",
        "Operating Year",
        "Nameplate Energy Capacity (MWh)",
        "Storage Technology 1",
    ]
    frame = pd.DataFrame(
        [
            [1, "A", "OP", "Batteries", 2015, 40, "FLB"],
            [1, "B", "OP", "Batteries", 2025, 30, "LIB"],
            [2, "A", "OP", "Batteries", 2025, 30, "LIB"],
            [3, "A", "OP", "Batteries", None, 10, "LIB"],
            [4, "A", "SB", "Batteries", 2000, 1000, "LIB"],
            [5, "A", "OP", "Pumped Storage", 2000, 1000, "LIB"],
        ],
        columns=columns,
    )
    path = tmp_path / "storage.xlsx"
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="Operable", index=False, startrow=1)
    profile = next(iter(eia_storage_profiles(path).values()))
    assert profile["distribution"] == {
        "kind": "discrete",
        "offsets_years": [-10.0, 0.0],
        "weights": [0.4, 0.6],
    }
    coverage = profile["evidence"]["coverage"]
    assert coverage["known_year_energy_capacity_count"] == 3
    assert coverage["known_year_energy_capacity_mwh"] == 100
    assert coverage["known_year_energy_capacity_share"] == pytest.approx(
        100 / 110, abs=1e-6
    )
    assert coverage["lithium_ion_energy_capacity_share"] == 0.6
    assert coverage["derived_mean_age_years"] == 4


def test_factory_uses_factory_subgroup_area_and_retains_unknown_share(tmp_path):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Table_3_16"
    sheet.append(
        [
            "CaRBE Activity Grouping",
            "CaRBE Abbreviation",
            "pre-1919",
            "1919-1939",
            "1940-1975",
            "1976-1990",
            "1991-2006",
            "2007-2021",
            "2022 onwards",
            "Age Null",
        ]
    )
    sheet.append(["Workshops etc", "FA_A", 10, 10, 10, 10, 10, 10, 10, 30])
    sheet.append(["Factories etc", "FA_B", 0, 0, 0, 0, 45, 45, 0, 10])
    path = tmp_path / "factory.xlsx"
    workbook.save(path)
    profile = next(iter(nbd_factory_profiles(path).values()))
    distribution = profile["distribution"]
    assert sum(distribution["weights"]) == pytest.approx(1)
    assert (
        dict(zip(distribution["offsets_years"], distribution["weights"], strict=True))[
            -24.5
        ]
        == 0.5
    )
    coverage = profile["evidence"]["coverage"]
    assert coverage["known_age_floor_area_share"] == 0.9
    assert coverage["unknown_age_floor_area_percent"] == 10
    assert coverage["derived_mean_age_years"] == 16.75


def test_grid_preserves_length_and_open_tail_mean_constraint():
    profile = next(iter(sigeif_low_voltage_profiles().values()))
    coverage = profile["evidence"]["coverage"]
    assert coverage["line_length_m"] == 5409052
    distribution = profile["distribution"]
    assert sum(
        -a * w
        for a, w in zip(
            distribution["offsets_years"], distribution["weights"], strict=True
        )
    ) == pytest.approx(42)
    assert distribution["weights"][0] == pytest.approx(1980035 / 5409052)
    assert -distribution["offsets_years"][0] >= 51
    assert (
        "open-tail-calibrated-to-rounded-mean" in profile["evidence"]["quality_flags"]
    )


@pytest.mark.parametrize("version", ["3.8", "3.12"])
def test_gap_assignments_respect_service_and_asset_boundaries(version):
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    rows = [
        (
            "market for electricity, low voltage",
            "electricity, low voltage",
            "kilowatt hour",
            "market for distribution network, electricity, low voltage",
            "distribution network, electricity, low voltage",
            "kilometer",
            "sigeif-fr-",
        ),
        (
            "market for electricity, medium voltage",
            "electricity, medium voltage",
            "kilowatt hour",
            "market for distribution network, electricity, medium voltage",
            "distribution network, electricity, medium voltage",
            "kilometer",
            None,
        ),
        (
            "distribution network construction, electricity, low voltage",
            "distribution network, electricity, low voltage",
            "kilometer",
            "distribution network, electricity, low voltage",
            "distribution network, electricity, low voltage",
            "kilometer",
            None,
        ),
        (
            "electricity supply, from stationary battery (CONT scenario)",
            "electricity, low voltage",
            "kilowatt hour",
            "market for battery capacity, stationary (CONT scenario)",
            "electricity storage capacity",
            "kilowatt hour",
            "eia-us-stationary-",
        ),
        (
            "market for battery capacity, Li-ion, LFP, stationary",
            "electricity storage capacity",
            "kilowatt hour",
            "market for battery cell, lithium-ion, lfp, including end-of-life treatment",
            "battery cell, Li-ion, LFP, including end-of-life treatment",
            "kilogram",
            None,
        ),
        (
            "transport, passenger bus, battery electric - overnight charging, 9m midibus",
            "transport, passenger bus",
            "person kilometer",
            "EV charger, level 3, plugin, 200 kW",
            "EV charger, level 3, plugin, 200 kW",
            "unit",
            "bnetza-de-",
        ),
        (
            "transport, passenger bus, battery electric - opportunity charging, 9m midibus",
            "transport, passenger bus",
            "person kilometer",
            "EV charger, level 3, with pantograph, 450 kW",
            "EV charger, level 3, with pantograph, 450 kW",
            "unit",
            "bernmobil-ch-",
        ),
        (
            "lorry production, battery electric, 26 metric ton",
            "lorry",
            "unit",
            "EV charger, level 3, plugin, 200 kW",
            "EV charger, level 3, plugin, 200 kW",
            "unit",
            "reviewed-truck-allocated-",
        ),
        (
            "photovoltaic cell production, single-Si wafer",
            "photovoltaic cell",
            "square meter",
            "photovoltaic cell factory",
            "photovoltaic cell factory",
            "unit",
            "desnz-gb-",
        ),
        (
            "ammonia production",
            "ammonia",
            "kilogram",
            "chemical plant, organics",
            "chemical plant, organics",
            "unit",
            None,
        ),
        (
            "enamelling",
            "enamel coating",
            "square meter",
            "metal coating facility",
            "metal coating facility",
            "unit",
            "desnz-gb-",
        ),
        (
            "market for photovoltaic cell factory",
            "photovoltaic cell factory",
            "unit",
            "photovoltaic cell factory",
            "photovoltaic cell factory",
            "unit",
            "reviewed-new-asset-",
        ),
    ]
    for caller, product, unit, name, supplier_product, supplier_unit, prefix in rows:
        result = registry.resolve(
            {
                "name": name,
                "reference product": supplier_product,
                "unit": supplier_unit,
            },
            target="supplier",
            context={"ecoinvent_version": version},
            caller_record={
                "name": caller,
                "reference product": product,
                "unit": unit,
                "location": "RoW",
            },
        )
        assert (result is not None) == (prefix is not None), (caller, name)
        if prefix:
            assert result.profile_id.startswith(prefix)


def test_runtime_applies_gap_profiles_once_and_preserves_amounts(monkeypatch, tmp_path):
    from types import SimpleNamespace

    data = [
        (
            "electricity supply, from stationary battery (CONT scenario)",
            "electricity, low voltage",
            "kilowatt hour",
            "market for battery capacity, stationary (CONT scenario)",
            "electricity storage capacity",
            "kilowatt hour",
            "eia-us-stationary-battery-energy-vintage-2025",
        ),
        (
            "market for electricity, low voltage",
            "electricity, low voltage",
            "kilowatt hour",
            "market for distribution network, electricity, low voltage",
            "distribution network, electricity, low voltage",
            "kilometer",
            "sigeif-fr-low-voltage-network-vintage-2023",
        ),
        (
            "photovoltaic cell production, single-Si wafer",
            "photovoltaic cell",
            "square meter",
            "photovoltaic cell factory",
            "photovoltaic cell factory",
            "unit",
            "desnz-gb-factory-building-vintage-2023",
        ),
        (
            "transport, passenger bus, battery electric - overnight charging, 9m midibus",
            "transport, passenger bus",
            "person kilometer",
            "EV charger, level 3, plugin, 200 kW",
            "EV charger, level 3, plugin, 200 kW",
            "unit",
            "bnetza-de-dc-plugin-charger-150-250kw-vintage-2026",
        ),
    ]
    datasets = [
        {
            "name": n,
            "reference product": p,
            "unit": u,
            "location": "RER",
            "exchanges": [
                {
                    "name": sn,
                    "product": sp,
                    "unit": su,
                    "amount": 0.0123,
                    "type": "technosphere",
                }
            ],
        }
        for n, p, u, sn, sp, su, _ in data
    ]
    scenario = {"model": "test", "pathway": "test", "year": 2030, "database": datasets}
    obj = trails.TrailsDataPackage.__new__(trails.TrailsDataPackage)
    obj._load_canonical_temporal_data()
    obj.source_version = "3.12"
    obj.datapackage = SimpleNamespace(scenarios=[scenario], database="db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)
    obj.add_temporal_distributions()
    for dataset, row in zip(datasets, data, strict=True):
        exchange = dataset["exchanges"][0]
        profile = obj.temporal_registry.profiles[row[-1]]
        assert exchange["amount"] == 0.0123
        assert exchange["temporal_offsets"] == profile["distribution"]["offsets_years"]
        assert exchange["temporal_weights"] == profile["distribution"]["weights"]
