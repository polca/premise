import brightway2 as bw

from premise import *

bw.projects.set_current("ei39")

scenarios = [
    {"model": "remind", "pathway": "SSP1-Base", "year": 2005},
    {"model": "remind", "pathway": "SSP2-Base", "year": 2010},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2015},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2020},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2025},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2030},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2040},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2050},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2060},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2070},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2080},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2090},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2100},
]

ndb = NewDatabase(
    scenarios=scenarios,
    source_db="ecoinvent 3.9.1 cutoff",
    source_version="3.9",
    key=b"tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
)

ndb.update_all()
ndb.write_superstructure_db_to_brightway(name="my_dp")