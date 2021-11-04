import cProfile
import pstats
from functools import wraps

import brightway2 as bw

from premise import *

def main():
    NewDatabase(
        scenarios=[
            {
                "model": "remind",
                "pathway": "SSP2-PkBudg900",
                "year": 2044,
                "exclude": ["update_cars", "update_trucks"],
            },
            {
                "model": "image",
                "pathway": "SSP2-RCP26",
                "year": 2050,
                "exclude": ["update_cars", "update_trucks"],
            },
        ],
        source_db="ecoinvent 3.8 cutoff",
        source_version="3.8",
        key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
        use_cached_inventories=True,
        use_cached_database=True
    )


bw.projects.set_current("ei_38")
main()
