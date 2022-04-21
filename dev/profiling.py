import cProfile
import pstats
from functools import wraps

import brightway2 as bw

from premise import *


def main():
    ndb = NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-Base",
                "year": 2050,
                "exclude": ["update_two_wheelers", "update_buses", "update_cars"],
            },
            {
                "model": "image",
                "pathway": "SSP2-RCP26",
                "year": 2030,
                "exclude": ["update_two_wheelers", "update_buses", "update_cars"],
            },
            {
                "model": "image",
                "pathway": "SSP2-RCP26",
                "year": 2020,
                "exclude": ["update_two_wheelers", "update_buses", "update_cars"],
            },
            {
                "model": "remind",
                "pathway": "SSP2-Base",
                "year": 2035,
                "exclude": ["update_two_wheelers", "update_buses", "update_cars"],
            },
        ],
        source_db="ecoinvent 3.8 cutoff",
        source_version="3.8",
        key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
    )

    ndb.update_electricity()


bw.projects.set_current("ei_38")
main()
