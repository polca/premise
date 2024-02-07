import bw2data
from datapackage import Package

from premise import *

bw2data.projects.set_current("ei39")

fp = r"https://raw.githubusercontent.com/premise-community-scenarios/energy-perspective-2050-switzerland/main/datapackage.json"
ep2050 = Package(fp)
# clear_cache()
scenarios = [
        {"model": "image", "pathway": "SSP2-Base", "year": 2050},
        {"model": "image", "pathway": "SSP2-Base", "year": 2060},
]

ndb = NewDatabase(
        scenarios=scenarios,
        source_db="ecoinvent 3.9.1 cutoff",
        source_version="3.9.1",
        key=b'tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=',
        use_multiprocessing=False,
        keep_uncertainty_data=True,
        external_scenarios=[ep2050,],
)

ndb.update()