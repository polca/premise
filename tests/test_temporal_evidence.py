import json
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd
import pytest

from dev.generate_trails_empirical_profiles import (
    DFT_CAR_FUELS,
    british_passenger_rail_profiles,
    ccnr_inland_vessel_profiles,
    chemical_site_evidence_profiles,
    dft_car_profiles,
    european_freight_wagon_profiles,
    european_grid_evidence_profiles,
    european_heating_profiles,
    eurostat_aircraft_profiles,
    gem_bioenergy_profiles,
    gem_coal_mine_profiles,
    gem_csp_profiles,
    gem_pipeline_profiles,
    nod_platform_profiles,
    offshore_wind_profiles,
    parse_igu_active_fleet_text,
    vehicle_survival_profiles,
)
from premise.temporal_evidence import (
    decode_jsonstat,
    eia_operating_fleet_profiles,
    eia_retirement_age_profiles,
    eurostat_age_band_profile,
    eurostat_aircraft_age_band_profile,
    eurostat_aircraft_representative_age_profile,
    eurostat_representative_age_profile,
)


@pytest.fixture
def dft_vehicle_ods(tmp_path):
    table_ns = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
    office_ns = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    root = ET.Element(f"{{{office_ns}}}document-content")
    table = ET.SubElement(
        root, f"{{{table_ns}}}table", {f"{{{table_ns}}}name": "VEH1111"}
    )
    rows = [
        [
            "Geography [note 1]",
            "Date",
            "Units",
            "BodyType",
            "Fuel",
            "2025",
            "2020",
            "Unknown [note 2]",
            "Total",
        ]
    ]
    rows += [
        [
            "United Kingdom",
            "2025 Q4 (end December)",
            "Thousands",
            "Cars",
            fuel,
            "0.075",
            "0.025",
            "0.01",
            "0.11",
        ]
        for fuel in DFT_CAR_FUELS.values()
    ]
    rows.append(
        [
            "United Kingdom",
            "2025 Q4 (end December)",
            "Thousands",
            "Light goods",
            "PETROL",
            "100",
            "0",
            "0",
            "100",
        ]
    )
    for values in rows:
        row = ET.SubElement(table, f"{{{table_ns}}}table-row")
        for value in values:
            cell = ET.SubElement(row, f"{{{table_ns}}}table-cell")
            cell.text = value
        ET.SubElement(
            row,
            f"{{{table_ns}}}table-cell",
            {f"{{{table_ns}}}number-columns-repeated": "1000"},
        )
    path = tmp_path / "veh1111.ods"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("content.xml", ET.tostring(root))
    return path


def test_dft_profiles_keep_fuels_separate_and_exclude_unknown_ages(dft_vehicle_ods):
    profiles = dft_car_profiles(dft_vehicle_ods)
    assert len(profiles) == 9
    for profile in profiles.values():
        assert profile["distribution"] == {
            "kind": "discrete",
            "offsets_years": [-5, 0],
            "weights": [0.25, 0.75],
        }
        coverage = profile["evidence"]["coverage"]
        assert coverage["known_age_vehicle_count"] == 100
        assert coverage["unknown_age_vehicle_count"] == 10
        assert coverage["known_age_vehicle_share"] == pytest.approx(100 / 110, abs=1e-6)
        assert coverage["derived_mean_age_years"] == 1.25


def test_dft_profiles_reject_inconsistent_populations(dft_vehicle_ods, monkeypatch):
    from dev import generate_trails_empirical_profiles as generator

    rows = generator.dft_vehicle_rows(dft_vehicle_ods)
    rows[0]["Total"] = "0.09"
    monkeypatch.setattr(generator, "dft_vehicle_rows", lambda _: rows)
    with pytest.raises(ValueError, match="Invalid VEH1111 population"):
        dft_car_profiles(dft_vehicle_ods)


def test_chemical_site_ages_stay_evidence_only_and_use_product_capacity(tmp_path):
    records = []
    for chemical in ("ammonia", "methanol"):
        records.append(
            {
                "status": "operating",
                "primaryProducts": [chemical],
                "feedstock": ["natural gas"],
                "region": "Europe",
                "plantAge": 70,
                "capacity": {chemical: 1},
                "totalCapacity": 100,
            }
        )
        records.append(
            {
                "status": "construction",
                "primaryProducts": [chemical],
                "feedstock": ["natural gas"],
                "region": "Europe",
                "plantAge": 1,
                "capacity": {chemical: 100},
            }
        )
        records.append(
            {
                "status": "operating",
                "primaryProducts": [chemical],
                "feedstock": ["natural gas"],
                "region": "Europe",
                "plantAge": None,
                "capacity": {chemical: 1},
            }
        )
    path = tmp_path / "chemicals.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    profiles = chemical_site_evidence_profiles(path)
    assert len(profiles) == 4
    for profile in profiles.values():
        assert profile["review"]["activation_status"] == "evidence_only"
        assert profile["distribution"]["offsets_years"] == [-70]
        assert profile["evidence"]["coverage"]["known_age_capacity_share"] == 0.5


def test_offshore_wind_excludes_floating_retired_and_unknown_year_mass(tmp_path):
    def phase(
        identifier,
        country,
        year,
        capacity,
        kind="Offshore hard mount",
        status="operating",
    ):
        return {
            "properties": {
                "unit-id": identifier,
                "country-area1": country,
                "start-year": year,
                "capacity": capacity,
                "tech-type": kind,
                "status": status,
            }
        }

    features = [
        phase("gb-old", "United Kingdom", "2020", 10),
        phase("gb-new", "United Kingdom", "2024", 30),
        phase("de", "Germany", "2015", 10),
        phase("cn", "China", "2021", 10),
        phase("unknown", "United Kingdom", "", 10),
        phase("floating", "United Kingdom", "2025", 1000, kind="Offshore floating"),
        phase("retired", "Germany", "1990", 1000, status="retired"),
    ]
    path = tmp_path / "wind.geojson"
    path.write_text(json.dumps({"features": features}), encoding="utf-8")
    profiles = offshore_wind_profiles(path)
    eu = profiles["gem-gwpt-europe-offshore-fixed-structure-vintage-2026"]
    assert eu["distribution"]["offsets_years"] == [-11, -6, -2]
    assert eu["distribution"]["weights"] == [0.2, 0.2, 0.6]
    assert eu["evidence"]["coverage"]["derived_mean_age_years"] == 4.6
    assert eu["evidence"]["coverage"][
        "known_start_year_capacity_share"
    ] == pytest.approx(5 / 6, abs=1e-6)


def test_igu_parser_handles_multiline_fields_and_rejects_silent_row_loss():
    text = "Header\n1234567\nShip name\nOwner\nBuilder\n174000\nSelf-\nSupporting\nPrismatic\nConventional\nSteam\nreheat\n2018\n8\n7654321\nOther ship\nOwner\nBuilder\n150000\nMembrane\nFSRU\nSteam\n2000\n26\n"
    rows = parse_igu_active_fleet_text(text)
    assert len(rows) == 2
    assert rows[0]["capacity_m3"] == 174000
    assert rows[1]["vessel_type"] == "FSRU"
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_igu_active_fleet_text(text + "1111111\nIncomplete row\n")
    with pytest.raises(ValueError, match="Inconsistent"):
        parse_igu_active_fleet_text(text.replace("2018\n8", "2018\n7"))


def test_bioenergy_profiles_filter_conversions_mixed_fuels_and_unknown_dates(tmp_path):
    wood = "bioenergy: wood & other biomass (solids)"
    waste = "bioenergy: refuse (municipal and industrial wastes)"

    def asset(
        identifier,
        fuel,
        year,
        capacity,
        region="Europe",
        status="operating",
        conversion=None,
    ):
        return {
            "asset_id": identifier,
            "operating_status": status,
            "capacity_value": capacity,
            "type_fields": {
                "fuel": fuel,
                "region": region,
                "start_year": year,
                "unit_conversion_year": conversion,
            },
        }

    records = [
        asset("old", wood, "2000.0", 10),
        asset("new", wood, "2020", 30),
        asset("unknown", wood, None, 10),
        asset("converted", wood, "1990", 1000, conversion="2015"),
        asset("mixed", wood + ", coal: unknown", "2000", 1000),
        asset("retired", wood, "1980", 1000, status="retired"),
        asset("us-wood", wood, "1990", 20, region="Americas"),
        asset("eu-waste", waste, "2010", 20),
        asset("us-waste", waste, "2018", 20, region="Americas"),
    ]
    (tmp_path / "assets-0.json").write_text(
        json.dumps({"results": records}), encoding="utf-8"
    )
    profiles = gem_bioenergy_profiles(tmp_path)
    eu = profiles["gem-gbpt-europe-wood-biomass-construction-vintage-2026"]
    world = profiles["gem-gbpt-world-waste-incineration-construction-vintage-2026"]
    assert eu["distribution"]["offsets_years"] == [-26, -6]
    assert eu["distribution"]["weights"] == [0.25, 0.75]
    assert eu["evidence"]["coverage"]["excluded_conversion_unit_count"] == 1
    assert eu["evidence"]["coverage"]["known_start_year_capacity_share"] == 0.8
    assert eu["evidence"]["coverage"]["derived_mean_age_years"] == 11
    assert world["distribution"]["offsets_years"] == [-16, -8]
    assert world["distribution"]["weights"] == [0.5, 0.5]


def test_passenger_rail_preserves_open_band_and_rounded_shares():
    profiles = british_passenger_rail_profiles()
    raw = profiles["orr-gb-passenger-rail-age-bands-2026"]
    runtime = profiles["orr-gb-passenger-rail-construction-vintage-2026"]
    assert raw["distribution"]["parameters"]["bands"][-1]["maximum_years"] is None
    assert raw["review"]["activation_status"] == "evidence_only"
    assert runtime["distribution"]["offsets_years"] == [-30, -23, -18, -13, -8, -2.5]
    assert runtime["distribution"]["weights"] == pytest.approx(
        [26 / 101, 19 / 101, 11 / 101, 3 / 101, 23 / 101, 19 / 101]
    )
    coverage = runtime["evidence"]["coverage"]
    assert coverage["excluded_tara_fleet_share"] == 0.06
    assert coverage["derived_mean_age_years"] == pytest.approx(1685.5 / 101, abs=1e-6)


def eurostat_aircraft_fixture():
    return {
        "id": ["age", "geo", "time"],
        "size": [5, 2, 1],
        "dimension": {
            "age": {
                "category": {
                    "index": {
                        "Y_LT5": 0,
                        "Y5-9": 1,
                        "Y10-14": 2,
                        "Y15-19": 3,
                        "Y_GE20": 4,
                    }
                }
            },
            "geo": {"category": {"index": {"AT": 0, "BE": 1}}},
            "time": {"category": {"index": {"2024": 0}}},
        },
        "value": {str(index): value for index, value in enumerate(range(1, 11))},
    }


def eurostat_fixture():
    return {
        "id": ["age", "geo", "time"],
        "size": [5, 3, 1],
        "dimension": {
            "age": {
                "category": {
                    "index": {
                        "Y_LT2": 0,
                        "Y2-5": 1,
                        "Y5-10": 2,
                        "Y10-20": 3,
                        "Y_GT20": 4,
                    }
                }
            },
            "geo": {"category": {"index": {"AT": 0, "BE": 1, "BG": 2}}},
            "time": {"category": {"index": {"2024": 0}}},
        },
        # AT and BE have all five bands; BG is missing the final band.
        "value": {
            **{str(age * 3): age + 1 for age in range(5)},
            **{str(age * 3 + 1): 2 * (age + 1) for age in range(5)},
            **{str(age * 3 + 2): 100 for age in range(4)},
        },
    }


def test_jsonstat_decode_and_eurostat_aggregation_use_common_country_coverage():
    document = eurostat_fixture()
    assert len(decode_jsonstat(document)) == 14
    result = eurostat_age_band_profile(document, countries={"AT", "BE", "BG"})
    assert result["countries"] == ["AT", "BE"]
    assert result["country_count"] == 2
    assert result["observations"] == 45
    assert [band["count"] for band in result["bands"]] == [3, 6, 9, 12, 15]
    assert sum(band["weight"] for band in result["bands"]) == pytest.approx(1)
    assert result["bands"][-1]["maximum_years"] is None


def test_eurostat_representative_ages_preserve_band_mass():
    result = eurostat_age_band_profile(eurostat_fixture(), countries={"AT", "BE", "BG"})
    representative = eurostat_representative_age_profile(result)

    assert representative["offsets_years"] == [-22.5, -15, -7.5, -3.5, -1]
    assert representative["weights"] == pytest.approx(
        [15 / 45, 12 / 45, 9 / 45, 6 / 45, 3 / 45]
    )
    assert representative["mean_age_years"] == pytest.approx(13.5333333333)


def test_eurostat_aircraft_age_bands_and_representative_ages():
    profile = eurostat_aircraft_age_band_profile(
        eurostat_aircraft_fixture(), countries={"AT", "BE"}
    )
    representative = eurostat_aircraft_representative_age_profile(profile)

    assert [band["count"] for band in profile["bands"]] == [3, 7, 11, 15, 19]
    assert profile["observations"] == 55
    assert representative["offsets_years"] == [-22.5, -17, -12, -7, -2]
    assert representative["weights"] == pytest.approx(
        [19 / 55, 15 / 55, 11 / 55, 7 / 55, 3 / 55]
    )


def test_aircraft_profiles_keep_raw_bands_separate_from_runtime_proxy(tmp_path):
    source = tmp_path / "aircraft.json"
    source.write_text(json.dumps(eurostat_aircraft_fixture()), encoding="utf-8")

    profiles = eurostat_aircraft_profiles(source)
    raw = profiles["eurostat-eu27-commercial-aircraft-age-2024"]
    runtime = profiles[
        "eurostat-eu27-commercial-passenger-aircraft-construction-vintage-2024"
    ]

    assert raw["quantity"] == "fleet_age"
    assert raw["distribution"]["parameters"]["bands"][-1]["maximum_years"] is None
    assert raw["review"]["activation_status"] == "evidence_only"
    assert runtime["quantity"] == "construction_timing"
    assert runtime["distribution"]["offsets_years"] == [-22.5, -17, -12, -7, -2]
    assert sum(runtime["distribution"]["weights"]) == pytest.approx(1)
    assert runtime["review"]["activation_status"] == "active"


def test_ccnr_inland_vessel_profiles_reproduce_published_stock_composition():
    profiles = ccnr_inland_vessel_profiles()
    dry = profiles["ccnr-rhine-dry-cargo-vessel-construction-vintage-2024"]
    tanker = profiles["ccnr-rhine-tanker-vessel-construction-vintage-2024"]

    assert sum(dry["distribution"]["weights"]) == pytest.approx(1)
    assert dry["evidence"]["coverage"]["known_construction_year_vessel_count"] == 8319
    assert dry["evidence"]["coverage"]["derived_mean_age_years"] == pytest.approx(
        50.799495
    )
    assert dry["evidence"]["coverage"]["pre_2000_vessel_share"] == pytest.approx(
        0.807188
    )
    assert sum(tanker["distribution"]["weights"]) == pytest.approx(1)
    assert (
        tanker["evidence"]["coverage"]["known_construction_year_vessel_count"] == 1827
    )
    assert tanker["evidence"]["coverage"]["derived_mean_age_years"] == pytest.approx(
        27.566502
    )


def test_platform_vintages_use_first_service_and_exclude_subsea_and_retired(tmp_path):
    facilities = pd.DataFrame(
        [
            [
                1,
                "restart",
                "IN SERVICE",
                "Y",
                "Norway",
                100,
                "PROCESSING",
                "SEMISUB STEEL",
                "01.01.2022",
            ],
            [
                2,
                "new",
                "IN SERVICE",
                "Y",
                "Norway",
                100,
                "WELLHEAD",
                "JACKET 4 LEGS",
                "01.01.2020",
            ],
            [
                3,
                "subsea",
                "IN SERVICE",
                "N",
                "Norway",
                100,
                "OIL PRODUCER",
                "TEMPLATE",
                "01.01.2010",
            ],
            [
                4,
                "retired",
                "REMOVED",
                "Y",
                "Norway",
                100,
                "PROCESSING",
                "JACKET 4 LEGS",
                "01.01.1980",
            ],
        ],
        columns=[
            "fclNpdidFacility",
            "fclName",
            "fclPhase",
            "fclSurface",
            "fclNationName",
            "fclWaterDepth",
            "fclFunctions",
            "fclKind",
            "fclStartupDate",
        ],
    )
    history = pd.DataFrame(
        [
            [1, "IN SERVICE", "01.01.1997"],
            [1, "IN SERVICE", "01.01.2022"],
            [2, "INSTALLATION", "01.01.2018"],
        ],
        columns=["fclNpdidFacility1", "fclPhaseName", "fclFromDate"],
    )
    facilities_path, history_path = (
        tmp_path / "facilities.csv",
        tmp_path / "history.csv",
    )
    facilities.to_csv(facilities_path, index=False)
    history.to_csv(history_path, index=False)
    profile = next(iter(nod_platform_profiles(facilities_path, history_path).values()))
    assert profile["distribution"]["offsets_years"] == [-29, -6]
    assert profile["distribution"]["weights"] == [0.5, 0.5]
    assert profile["evidence"]["coverage"]["known_first_service_facility_count"] == 2
    assert (
        profile["evidence"]["coverage"]["restart_date_corrections"][0][
            "first_service_year"
        ]
        == 1997
    )


def test_gem_csp_profiles_are_capacity_weighted_and_geography_specific(tmp_path):
    source = tmp_path / "solar.xlsx"
    frame = pd.DataFrame(
        [
            ["Spain", "Solar Thermal", "Operating", 50, 2010],
            ["Spain", "Solar Thermal", "Operating", 150, 2020],
            ["United States", "Solar Thermal", "Operating", 100, 2015],
            ["South Africa", "Solar Thermal", "Operating", 100, 2018],
            ["Oman", "Solar Thermal", "Operating", 200, None],
            ["Spain", "PV", "Operating", 1000, 2025],
            ["Spain", "Solar Thermal", "Construction", 1000, 2025],
        ],
        columns=[
            "Country/Area",
            "Technology Type",
            "Status",
            "Capacity (MW)",
            "Start year",
        ],
    )
    with pd.ExcelWriter(source) as writer:
        frame.to_excel(writer, sheet_name="Utility-Scale (1 MW+)", index=False)

    profiles = gem_csp_profiles(source)
    spain = profiles["gem-gspt-spain-csp-construction-vintage-2026"]
    world = profiles["gem-gspt-world-csp-construction-vintage-2026"]

    assert spain["distribution"]["offsets_years"] == [-16, -6]
    assert spain["distribution"]["weights"] == [0.25, 0.75]
    assert spain["evidence"]["coverage"]["derived_mean_age_years"] == 8.5
    assert world["evidence"]["coverage"]["operating_phase_count"] == 5
    assert world["evidence"]["coverage"]["known_start_year_phase_count"] == 4
    assert world["evidence"]["coverage"][
        "known_start_year_capacity_share"
    ] == pytest.approx(2 / 3, abs=1e-6)
    assert world["review"]["activation_status"] == "active"


def test_eia_operating_profiles_are_capacity_weighted_and_status_filtered():
    frame = pd.DataFrame(
        [
            ["OP", "Wind", 2020, 10],
            ["OP", "Wind", 2024, 30],
            ["SB", "Wind", 2000, 1000],
            ["OP", "Solar", 2024, 5],
        ],
        columns=[
            "Status",
            "Technology",
            "Operating Year",
            "Summer Capacity (MW)",
        ],
    )
    profiles = eia_operating_fleet_profiles(frame, reference_year=2025)
    assert profiles["Wind"]["offsets_years"] == [-5, -1]
    assert profiles["Wind"]["weights"] == [0.25, 0.75]
    assert profiles["Wind"]["mean_age_years"] == 2
    assert profiles["Wind"]["weight_total_mw"] == 40


def test_eia_retirement_profiles_exclude_precoverage_and_invalid_rows():
    frame = pd.DataFrame(
        [
            ["RE", "Coal", 1980, 2020, 100],
            ["RE", "Coal", 1990, 2000, 1000],
            ["RE", "Coal", 2020, 2019, 1000],
            ["CN", "Coal", 1980, 2020, 1000],
        ],
        columns=[
            "Status",
            "Technology",
            "Operating Year",
            "Retirement Year",
            "Summer Capacity (MW)",
        ],
    )
    profiles = eia_retirement_age_profiles(frame)
    assert profiles["Coal"]["offsets_years"] == [-40]
    assert profiles["Coal"]["weights"] == [1]
    assert profiles["Coal"]["unit_count"] == 1


def test_vehicle_survival_profiles_preserve_the_censored_tail():
    profiles = vehicle_survival_profiles()
    passenger_cars = profiles["eu-passenger-car-survival-uk-mot"]
    parameters = passenger_cars["distribution"]["parameters"]

    assert passenger_cars["quantity"] == "service_life"
    assert parameters["ages_years"] == list(range(1, 20))
    assert parameters["survival_probabilities"][-1] == 0.04
    assert parameters["right_censored_after_years"] == 19
    assert "service_life_years" not in passenger_cars


def test_european_heating_profiles_separate_stock_age_construction_and_lifetime():
    profiles = european_heating_profiles()
    raw = profiles["fraunhofer-eu28-natural-gas-boiler-age-2012"]
    construction = profiles[
        "fraunhofer-eu28-natural-gas-boiler-construction-vintage-2012"
    ]
    lifetime = profiles["ec-eu-non-condensing-gas-boiler-service-life-2026"]

    assert raw["quantity"] == "fleet_age"
    assert raw["distribution"]["parameters"]["bands"][0]["maximum_years"] is None
    assert construction["quantity"] == "construction_timing"
    assert construction["distribution"]["offsets_years"] == [-30, -15, -4.5]
    assert construction["distribution"]["weights"] == [0.22, 0.34, 0.44]
    assert construction["review"]["activation_status"] == "active"
    assert (
        profiles["fraunhofer-eu28-coal-boiler-furnace-construction-vintage-2012"][
            "review"
        ]["activation_status"]
        == "hold"
    )
    assert lifetime["quantity"] == "service_life"
    assert lifetime["distribution"]["generator"] == "weibull_service_life"
    assert lifetime["distribution"]["parameters"]["mean_years"] == 18.0
    assert lifetime["review"]["activation_status"] == "evidence_only"


def test_european_grid_evidence_preserves_reported_ranges_and_stays_on_hold():
    profile = european_grid_evidence_profiles()[
        "eurelectric-eu27-uk-low-voltage-line-age-ranges-2020"
    ]

    assert profile["quantity"] == "fleet_age"
    assert profile["distribution"]["generator"] == "reported_age_band_share_ranges"
    oldest = profile["distribution"]["parameters"]["bands"][0]
    assert oldest["minimum_years"] == 40
    assert oldest["maximum_years"] is None
    assert (oldest["minimum_share"], oldest["maximum_share"]) == (0.35, 0.40)
    assert profile["review"]["activation_status"] == "hold"


def test_european_freight_wagon_profiles_preserve_bands_and_runtime_approximation():
    profiles = european_freight_wagon_profiles()
    raw = profiles["ec-eu-freight-wagon-manufacturing-year-bands-2019"]
    runtime = profiles["ec-eu-freight-wagon-construction-vintage-2019"]

    assert raw["quantity"] == "fleet_age"
    assert raw["distribution"]["parameters"]["bands"][0]["maximum_years"] is None
    assert runtime["distribution"]["offsets_years"] == [
        -64,
        -56.5,
        -50.5,
        -44.5,
        -38.5,
        -32.5,
        -26.5,
        -20.5,
        -14,
        -8,
        -3,
    ]
    assert sum(runtime["distribution"]["weights"]) == pytest.approx(1)
    coverage = runtime["evidence"]["coverage"]
    assert coverage["derived_mean_age_years"] == 31
    assert coverage["known_manufacturing_year_count_thousands_approximate"] == 648
    assert coverage["unknown_manufacturing_year_count_thousands_approximate"] == 38


def test_coal_mine_profiles_filter_status_and_use_capacity_fallback(tmp_path):
    source_directory = tmp_path / "coal"
    source_directory.mkdir()

    def asset(
        asset_id,
        mine_type,
        opening_year,
        production,
        capacity,
        status="operating",
    ):
        return {
            "asset_id": asset_id,
            "operating_status": status,
            "type_fields": {
                "mine_type": mine_type,
                "region": "Europe",
                "opening_year": opening_year,
                "production_mtpa": production,
                "capacity_mtpa": capacity,
            },
        }

    records = [
        asset("old", "Surface", "2000", "10", "100"),
        asset("new-capacity", "Surface", "2020", None, "30"),
        asset("unknown-year", "Surface", None, "10", "100"),
        asset("retired", "Surface", "2010", "100", "100", status="retired"),
        asset("underground", "Underground", "2016", "5", "10"),
    ]
    (source_directory / "assets-0.json").write_text(
        json.dumps({"results": records}), encoding="utf-8"
    )

    profiles = gem_coal_mine_profiles(source_directory)
    surface = profiles["gem-gcmt-europe-surface-coal-mine-construction-vintage-2026"]
    underground = profiles[
        "gem-gcmt-europe-underground-coal-mine-construction-vintage-2026"
    ]

    assert surface["distribution"]["offsets_years"] == [-26, -6]
    assert surface["distribution"]["weights"] == [0.25, 0.75]
    coverage = surface["evidence"]["coverage"]
    assert coverage["operating_mine_count"] == 3
    assert coverage["production_weight_mine_count"] == 2
    assert coverage["capacity_fallback_mine_count"] == 1
    assert coverage["known_opening_year_weight_share"] == pytest.approx(0.8)
    assert underground["distribution"]["offsets_years"] == [-10]


def test_pipeline_profiles_use_operating_length_and_valid_earliest_year(tmp_path):
    gas_directory = tmp_path / "gas"
    oil_directory = tmp_path / "oil"
    gas_directory.mkdir()
    oil_directory.mkdir()

    def asset(asset_id, status, length, start_year_1, start_year_2=None):
        return {
            "asset_id": asset_id,
            "operating_status": status,
            "type_fields": {
                "startregion": "Europe",
                "lengthmergedkm": length,
                "lengthknownkm": length,
                "startyear1": start_year_1,
                "startyear2": start_year_2,
                "startyear3": None,
            },
        }

    records = [
        asset("old", "operating", "100", "2000"),
        asset("new", "operating", "100", "2020", "5"),
        asset("unknown", "operating", "100", None),
        asset("planned", "planned", "1000", "2010"),
    ]
    document = json.dumps({"results": records})
    (gas_directory / "assets-0.json").write_text(document, encoding="utf-8")
    (oil_directory / "assets-0.json").write_text(document, encoding="utf-8")

    profiles = gem_pipeline_profiles(gas_directory, oil_directory)
    gas = profiles[
        "gem-ggit-europe-natural-gas-transmission-pipeline-construction-vintage-2025"
    ]
    oil = profiles[
        "gem-goit-europe-oil-ngl-transmission-pipeline-construction-vintage-2026"
    ]

    assert gas["distribution"]["offsets_years"] == [-25, -5]
    assert gas["distribution"]["weights"] == [0.5, 0.5]
    assert gas["evidence"]["coverage"][
        "known_start_year_length_share"
    ] == pytest.approx(2 / 3)
    assert oil["distribution"]["offsets_years"] == [-26, -6]
    assert oil["evidence"]["coverage"]["operating_phase_count"] == 3
