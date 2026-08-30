import gc
import os

import bw2data
import bw2io
import pytest
from dotenv import load_dotenv

from premise import NewDatabase, clear_inventory_cache
from premise.utils import delete_all_pickles

load_dotenv()

ei_user = os.environ.get("EI_USERNAME")
ei_pass = os.environ.get("EI_PASSWORD")
key = os.environ.get("IAM_FILES_KEY", "")
pytestmark = pytest.mark.skipif(
    not (ei_user and ei_pass and key), reason="ecoinvent credentials are unavailable"
)
key = key.encode()

ei_version = "3.12"
system_model = "cutoff"
project_name = f"ecoinvent-{ei_version}-{system_model}"
interpolated_year = 2046


@pytest.mark.slow
def test_update_with_interpolated_iam_year():
    bw2data.projects.set_current(project_name)
    clear_inventory_cache()

    if project_name not in bw2data.databases:
        bw2io.import_ecoinvent_release(
            version=ei_version,
            system_model=system_model,
            username=ei_user,
            password=ei_pass,
            biosphere_name=f"ecoinvent-{ei_version}-biosphere",
        )

    bio_db = next(db for db in bw2data.databases if "biosphere" in db)

    ndb = NewDatabase(
        scenarios=[{"model": "image", "pathway": "SSP2-M", "year": interpolated_year}],
        source_db=project_name,
        source_version=ei_version,
        key=key,
        system_model=system_model,
        biosphere_name=bio_db,
        generate_reports=False,
        inventory_backend="compact",
    )

    try:
        iam_years = ndb.scenarios[0]["iam data"].electricity_mix.coords["year"].values
        assert interpolated_year not in iam_years

        ndb.update(persist=False)

        assert set(ndb.scenarios[0]["applied functions"]) == set(
            ndb.sector_update_methods
        )
    finally:
        del ndb
        gc.collect()
        delete_all_pickles()
