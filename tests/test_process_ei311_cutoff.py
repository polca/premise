import gc
import os

import bw2calc
import bw2data
import bw2io
import pytest
from dotenv import load_dotenv

from premise import NewDatabase, clear_inventory_cache
from premise.utils import delete_all_pickles

load_dotenv()

ei_user = os.environ["EI_USERNAME"]
ei_pass = os.environ["EI_PASSWORD"]
key = os.environ["IAM_FILES_KEY"]
# convert to bytes
key = key.encode()

ei_version = "3.11"
system_model = "cutoff"

scenarios = [
    {"model": "remind", "pathway": "SSP3-rollBack", "year": 2050},
    {"model": "image", "pathway": "SSP2-VLHO", "year": 2050},
    # {"model": "tiam-ucl", "pathway": "SSP2-RCP19", "year": 2050},
    # {"model": "gcam", "pathway": "SSP2-RCP26", "year": 2050},
]


def test_brightway():
    bw2data.projects.set_current(f"ecoinvent-{ei_version}-{system_model}")
    clear_inventory_cache()

    if f"ecoinvent-{ei_version}-{system_model}" not in bw2data.databases:

        bw2io.import_ecoinvent_release(
            version=ei_version,
            system_model=system_model,
            username=ei_user,
            password=ei_pass,
            biosphere_name=f"ecoinvent-{ei_version}-biosphere",
        )

    if f"ecoinvent-{ei_version}-biosphere" not in bw2data.databases:
        biosphere_name = "biosphere3"
    else:
        biosphere_name = f"ecoinvent-{ei_version}-biosphere"

    ndb = NewDatabase(
        scenarios=scenarios,
        source_db=f"ecoinvent-{ei_version}-{system_model}",
        source_version=ei_version,
        key=key,
        system_model=system_model,
        biosphere_name=biosphere_name,
    )

    ndb.update()

    if "test1" in bw2data.databases:
        del bw2data.databases["test1"]
    if "test2" in bw2data.databases:
        del bw2data.databases["test2"]
    # if "test3" in bw2data.databases:
    #    del bw2data.databases["test3"]
    # if "test4" in bw2data.databases:
    #    del bw2data.databases["test4"]

    ndb.write_db_to_brightway(
        [
            "test1",
            "test2",
            # "test3",
            # "test4"
        ]
    )

    from bw2data import __version__

    print(f"Using Brightway2 data version: {__version__}")

    print(f"Length of databases: {len(bw2data.Database('test1'))}")

    method = [m for m in bw2data.methods if "IPCC" in str(m)][0]

    lca = bw2calc.LCA({bw2data.Database("test1").random(): 1}, method)
    lca.lci()
    lca.lcia()
    assert isinstance(lca.score, float)
    print(lca.score)

    try:
        # uses BW2
        mclca = bw2calc.MonteCarloLCA({bw2data.Database("test1").random(): 1}, method)
    except AttributeError:
        # uses BW25
        mclca = bw2calc.LCA(
            {bw2data.Database("test1").random(): 1}, method, use_distributions=True
        )

    results = [lca.score for _ in zip(range(10), mclca)]
    assert all(isinstance(result, float) for result in results)
    print(results)
