import bw2data

from premise import *

bw2data.projects.set_current("ei39")

scenarios = [
    #{"model": "remind", "pathway": "SSP1-Base", "year": 2005},
    #{"model": "remind", "pathway": "SSP2-Base", "year": 2010},
    {"model": "remind", "pathway": "SSP5-Base", "year": 2050},
]

#ndb = NewDatabase(
#    scenarios=scenarios,
#    source_db="ecoinvent 3.9.1 cutoff",
#    key=b"tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
#)

ndb = NewDatabase(
        scenarios = scenarios,
        source_db= "ecoinvent cutoff 3.9.1",
        source_version="3.9.1",
        source_type="ecospold",
        source_file_path=f"/Users/romain/Documents/ecoinvent 3.9.1_cutoff_ecoSpold02/datasets", # <-- this is NEW
        key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=',
        #system_model="cutoff",
        #system_args=combinations[0]
)

#ndb.update_all()
#ndb.write_db_to_brightway()