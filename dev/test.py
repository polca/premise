import time
import bw2data
import numpy as np

from premise import *

bw2data.projects.set_current("ecoinvent-3.12-cutoff")

# start timer
start_time = time.time()

# clear_cache()
scenarios = [
        {"model": "remind", "pathway": "SSP1-NPi", "year": 2050},
        #{"model": "image", "pathway": "SSP2-Base", "year": 2060},
]

ndb = NewDatabase(
        scenarios=scenarios,
        source_db="ecoinvent-3.12-cutoff",
        source_version="3.12",
        key=b'tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=',
)

ndb.update()

end_time = time.time()
print(f"Completed in {end_time - start_time:.2f} seconds.")

ndb.write_db_to_brightway("test", fast=True)

db = bw2data.Database("test")
act = [ds for ds in db if ds["name"] == "market group for electricity, high voltage" and ds["location"] == "EUR"][0]
print(act)

import bw2calc
method = ("EF v3.1", "climate change", "global warming potential (GWP100)")
lca = bw2calc.LCA({act: 1}, method=method)
lca.lci()
lca.lcia()
assert np.isclose(lca.score, 0.018891299651009136)
