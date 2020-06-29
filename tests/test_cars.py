from rmnd_lca import NewDatabase
from rmnd_lca.cars import Cars

import wurst
import brightway2 as bw

bw.projects.set_current("transport_lca")
scenario="SSP2-PkBudg900"
year=2050


def test_create_local_evs():
    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    Cars(ndb.db, ndb.rdc, scenario, year).create_local_evs()


def test_create_local_fcevs():
    ndb = NewDatabase(
        scenario=scenario,
        year=year,
        source_db='ecoinvent 3.6 cutoff',
        source_version=3.6)

    ndb.update_electricity_to_remind_data()
    Cars(ndb.db, ndb.rdc, scenario, year).create_local_fcevs()


def test_full_import():
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
    data = ndb.rdc.get_remind_fuel_mix()
    assert data.shape == (13, 3)
    assert all(data.sum(dim="variables") == 1.)

def test_update_fuel_mix():
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
