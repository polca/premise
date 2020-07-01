from rmnd_lca import DATA_DIR
from rmnd_lca import NewDatabase
from rmnd_lca import RemindDataCollection
from rmnd_lca.cars import Cars

import pytest
import wurst
import brightway2 as bw
import pandas as pd

REGION_MAPPING_FILEPATH = (DATA_DIR / "regionmappingH12.csv")

# for local test runs
BW_PROJECT = "transport_lca"
scenario = "SSP2-PkBudg900"
year = 2049


def get_db():
    remind_regions = list(pd.read_csv(
        REGION_MAPPING_FILEPATH, sep=";").RegionCode.unique())
    db = [
        {
            'name': 'fake activity',
            'reference product': 'fake product',
            'location': 'IAI Area, Africa',
            'unit': 'kilogram',
            'exchanges': [
            {
                'name': 'fake activity',
                'product': 'fake product',
                'amount': 1,
                'type': 'production',
                'unit': 'kilogram',
                'input': ('dummy_db', '6543541'), },
            {
                'name': '1,4-Butanediol',
                'categories': ('air', 'urban air close to ground'),
                'amount': 1,
                'type': 'biosphere',
                'unit': 'kilogram',
                'input': ('dummy_bio', '123'),
            }]
        },
        {
            'name': 'electricity supply for electric vehicles',
            'reference product': 'electricity, low voltage',
            'location': 'RER',
            'unit': 'kilowatt hour',
            'exchanges': [
                {
                    'name': 'electricity supply for electric vehicles',
                    'product': 'fake product',
                    'amount': 1,
                    'type': 'production',
                    'unit': 'kilogram',
                    'input': ('dummy_db', '6543541')},
                {
                    'name': '1,4-Butanediol',
                    'categories': ('air', 'urban air close to ground'),
                    'amount': 1,
                    'type': 'biosphere',
                    'unit': 'kilogram',
                    'input': ('dummy_bio', '123'),
                },
            ]
        },
        {
            'name': 'BEV,',
            'reference product': 'car',
            'location': 'RER',
            'unit': 'car',
            'exchanges': [
                {
                    'name': 'BEV,',
                    'product': 'fake product',
                    'amount': 1,
                    'type': 'production',
                    'unit': 'kilogram',
                    'input': ('dummy_db', '6543541')},
                {
                    'name': 'electricity supply for electric vehicles',
                    'product': 'fake product',
                    'amount': 1,
                    'type': 'technosphere',
                    'unit': 'kilogram',
                    'input': ('dummy_db', '6543541')},
            ]
        },
        *[{
            'name': 'market group for electricity, low voltage',
            'reference product': 'electricity, low voltage',
            'location': region,
            'unit': 'kilowatt hour'
        } for region in remind_regions
        ]
    ]
    version = 3.6
    return db, version


def test_create_evs_mock_db():
    rdc = RemindDataCollection(
        'SSP2-Base', 2012, DATA_DIR / "remind_output_files")
    db, _ = get_db()
    Cars(db, rdc, 'SSP2-Base', 2012).create_local_evs()


@pytest.mark.ecoinvent
def test_create_local_evs():
    bw.projects.set_current(BW_PROJECT)

    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    Cars(ndb.db, ndb.rdc, scenario, year).create_local_evs()


@pytest.mark.ecoinvent
def test_create_local_fcevs():
    bw.projects.set_current(BW_PROJECT)

    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    Cars(ndb.db, ndb.rdc, scenario, year).create_local_fcevs()


@pytest.mark.ecoinvent
def test_full_import():
    bw.projects.set_current(BW_PROJECT)

    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    crs = Cars(ndb.db, ndb.rdc, scenario, year)
    crs.create_local_evs()
    crs.create_local_fcevs()
    crs.create_local_icevs()
    dbname = "test_carculator_complete"
    if dbname in bw.databases:
        del bw.databases[dbname]
    wurst.write_brightway2_database(ndb.db, dbname)
    # del bw.datases[dbname]
    return ndb.db


def test_get_fuel_mix():
    from rmnd_lca import DATA_DIR
    remind_file_path = DATA_DIR / "remind_output_files"
    rdc = RemindDataCollection(scenario, year, remind_file_path)
    data = rdc.get_remind_fuel_mix()
    assert data.shape == (13, 3)
    assert all(data.sum(dim="variables") == 1.)


@pytest.mark.ecoinvent
def test_update_fuel_mix():
    bw.projects.set_current(BW_PROJECT)
    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    crs = Cars(ndb.db, ndb.rdc, scenario, year)
    crs.create_local_icevs()

    dbname = "test_carculator_complete"
    if dbname in bw.databases:
        del bw.databases[dbname]
    wurst.write_brightway2_database(ndb.db, dbname)
    del bw.databases[dbname]
