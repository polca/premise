import wurst.searching as ws
import pandas as pd

from .geomap import REGION_MAPPING_FILEPATH


class Cars():
    """
    Class that modifies carculator inventories in ecoinvent
    based on REMIND output data.

    :ivar db: ecoinvent database in list-of-dict format
    :ivar scenario: REMIND scenario identifier
    :ivar year: year for the current analysis

    """

    def __init__(self, db, scenario, year):
        self.db = db
        # self.rmd = rmd

        self.scenario = scenario
        self.year = year

    def create_local_evs(self):
        """Create LDV activities for REMIND regions and relink
        existing electricity exchanges for BEVs, FCEVs and PHEVs
        to REMIND-compatible (regional) market groups.
        """
        print("Creating local BEV, PHEV and FCEV activities")
        remind_regions = list(pd.read_csv(
            REGION_MAPPING_FILEPATH, sep=";").RegionCode.unique())
        remind_regions.remove("World")

        def find_evs():
            return list(ws.get_many(
                self.db,
                ws.either(
                    ws.contains("name", "EV,"),
                    ws.contains("name", "PHEV"))))

        bevs = find_evs()
        # any non-global activities found?
        ans = None
        todel = []
        for idx in range(len(bevs)):
            if bevs[idx]["location"] != "GLO":
                print("Found non-global EV activities: {} in {}"
                      .format(bevs[idx]["name"], bevs[idx]["location"]))
                if ans is None:
                    ans = input("Delete existing non-GLO activities? (y/n)")
                if ans == "y":
                    todel.append(idx)
        for i in sorted(todel, reverse=True):
            del bevs[i]

        for region in remind_regions:
            print("Relinking markets for {}".format(region))
            # find markets
            new_market = ws.get_one(self.db,
                ws.startswith("name", "market group for electricity, low voltage"),
                ws.equals("location", region))
            for bev in bevs:
                new_bev = bev.copy()
                new_bev["location"] = region

                # update production exchange
                prod = next(ws.production(bev, ws.equals("name", bev["name"])))
                prod["location"] = region

                # update fuel market
                oldex = list(ws.technosphere(
                    new_bev,
                    ws.equals("name", "market group for electricity, low voltage"),
                    ws.equals("location", "EUR")))
                # should only be one
                if len(oldex) > 1:
                    raise ValueError("More than one electricity market for "
                                     "fuel production found for {}"
                                     .format(new_bev))
                elif len(oldex) == 1:
                    # new exchange
                    oldex[0].update({
                        "name": new_market["name"],
                        "amount": oldex[0]["amount"],
                        "unit": "kilowatt hour",
                        "type": "technosphere",
                        "location": region,
                        "uncertainty type": 1,
                        "reference product": "electricity, low voltage",
                        "product": "electricity, low voltage"
                    })

                    chklst = list(ws.technosphere(
                        new_bev, ws.equals(
                            "name",
                            "market group for electricity, low voltage")))
                    assert len(chklst) == 2, "Deletion failed"
                    self.db.append(new_bev)
