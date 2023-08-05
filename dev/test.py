import bw2data

from premise import *

bw2data.projects.set_current("ei39")

scenarios = [
    #{"model": "remind", "pathway": "SSP1-Base", "year": 2005},
    #{"model": "remind", "pathway": "SSP2-Base", "year": 2010},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2050},
]

ndb = NewDatabase(
    scenarios=scenarios,
    source_db="ecoinvent 3.9.1 cutoff",
    key=b"tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
)

ndb.update_all()
ndb.write_db_to_brightway()