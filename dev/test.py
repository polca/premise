import bw2data
from datapackage import Package

from premise import *

bw2data.projects.set_current("ecoinvent-3.11-cutoff")

#fp = r"https://raw.githubusercontent.com/premise-community-scenarios/energy-perspective-2050-switzerland/main/datapackage.json"
#ep2050 = Package(fp)
# clear_cache()
scenarios = [
        {"model": "image", "pathway": "SSP2-M", "year": 2050},
        #{"model": "image", "pathway": "SSP2-Base", "year": 2060},
]

ndb = NewDatabase(
        scenarios=scenarios,
        source_db="ecoinvent-3.11-cutoff",
        source_version="3.11",
        key=b'tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=',
)

ndb.update()