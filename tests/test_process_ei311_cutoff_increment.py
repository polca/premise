import gc
import os

import bw2calc
import bw2data
import bw2io
import pytest
from dotenv import load_dotenv

from premise import IncrementalDatabase, clear_inventory_cache
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
    {"model": "remind", "pathway": "SSP2-rollBack", "year": 2050},
    {"model": "image", "pathway": "SSP2-VLHO", "year": 2050},
    {"model": "tiam-ucl", "pathway": "SSP2-RCP19", "year": 2050},
]


@pytest.mark.slow
def test_increment():
    bw2data.projects.set_current(f"ecoinvent-{ei_version}-{system_model}")
    clear_inventory_cache()

    if f"ecoinvent-{ei_version}-{system_model}" not in bw2data.databases:
        bw2io.import_ecoinvent_release(
            version=ei_version,
            system_model=system_model,
            username=ei_user,
            password=ei_pass,
        )

    if f"ecoinvent-{ei_version}-biosphere" not in bw2data.databases:
        biosphere_name = "biosphere3"
    else:
        biosphere_name = f"ecoinvent-{ei_version}-biosphere"

    ndb = IncrementalDatabase(
        scenarios=scenarios,
        source_db=f"ecoinvent-{ei_version}-{system_model}",
        source_version=ei_version,
        key=key,
        system_model=system_model,
        biosphere_name=biosphere_name,
    )

    sectors = {
        "electricity": "electricity",
        "steel": "steel",
        "others": ["cement", "cars", "fuels"],
    }

    ndb.update(sectors=sectors)

    if "incremental" in bw2data.databases:
        del bw2data.databases["incremental"]

    ndb.write_increment_db_to_brightway("incremental", file_format="csv")

    method = [m for m in bw2data.methods if "IPCC" in str(m)][0]

    lca = bw2calc.LCA({bw2data.Database("incremental").random(): 1}, method)
    lca.lci()
    lca.lcia()
    assert isinstance(lca.score, float)
    print(lca.score)

    # destroy all objects
    del ndb
    del lca
    gc.collect()
    delete_all_pickles()
