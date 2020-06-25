from .geomap import Geomap

import wurst
import wurst.searching as ws
import pandas as pd

from .geomap import REGION_MAPPING_FILEPATH


class Cars():
    """
    Class that modifies carculator inventories in ecoinvent
    based on REMIND output data.

    :ivar db: ecoinvent database in list-of-dict format
    :ivar rmd: REMIND output data
    :ivar scenario: REMIND scenario identifier
    :ivar year: year for the current analysis

    """

    def __init__(self, db, rmd, scenario, year):
        self.db = db
        self.rmd = rmd
        self.geo = Geomap()

        self.scenario = scenario
        self.year = year

        self.remind_regions = list(pd.read_csv(
            REGION_MAPPING_FILEPATH, sep=";").RegionCode.unique())
        self.remind_regions.remove("World")

    def _delete_non_global(self, acts, proxy_region="RER"):
        # delete any non-global activities?
        ans = None

        # reverse loop, to allow for deletion
        for idx in range(len(acts) - 1, -1, -1):
            if acts[idx]["location"] != proxy_region:
                print("Found non-global EV activities: {} in {}"
                      .format(acts[idx]["name"], acts[idx]["location"]))
                if ans is None:
                    ans = input("Delete existing non-{} activities? (y/n)"
                                .format(proxy_region))
                if ans == "y":
                    del acts[idx]

    def create_local_evs(self):
        """Create LDV activities for REMIND regions and relink
        existing electricity exchanges for BEVs and PHEVs
        to REMIND-compatible (regional) market groups.
        """
        print("Creating local BEV and PHEV activities")

        bevs = list(ws.get_many(
            self.db,
            ws.either(
                ws.contains("name", "BEV,"),
                ws.contains("name", "PHEV"))))

        self._delete_non_global(bevs)

        for region in self.remind_regions:
            print("Relinking electricity markets for BEVs in {}".format(region))
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

    def create_local_fcevs(self):
        """Create LDV activities for REMIND regions and relink
        existing electricity exchanges for FCEVs
        to REMIND-compatible (regional) market groups.
        """
        print("Creating local FCEV activities")

        fcevs = list(ws.get_many(
            self.db,
            ws.contains("name", "FCEV,")))

        self._delete_non_global(fcevs)
        old_supply = ws.get_one(
            self.db,
            ws.startswith(
                "name", "fuel supply for hydrogen vehicles"))

        for region in self.remind_regions:
            if region == "EUR":
                # in this case, the RER activities are just fine
                continue
            print("Relinking hydrogen markets for FCEVs in {}".format(region))
            # create local hydrogen supply
            supply = old_supply.copy()
            supply["location"] = region
            # remove explicit electricity input
            elmark = next(ws.technosphere(supply, ws.equals(
                "name", "market group for electricity, low voltage")))
            elmark["amount"] = 0

            # find hydrogen supply nearby
            ei_locs = self.geo.remind_to_ecoinvent_location(region)
            h2sups = ws.technosphere(
                supply,
                ws.startswith("product", "Hydrogen"))
            for h2sup in h2sups:
                producers = list(ws.get_many(
                    self.db,
                    ws.equals("name", h2sup["name"]),
                    ws.either(*[
                        ws.equals("location", loc) for loc in ei_locs
                    ])))
                if len(producers) >= 1:
                    prod = producers[0]
                    if len(producers) > 1:
                        print(("Multiple producers for {} found in {}, "
                               "using hydrogen from {}").format(
                                   h2sup["product"], region, prod["location"]))
                    h2sup["location"] = prod["location"]
                    h2sup["name"] = prod["name"]
                else:
                    # we can leave things as they are since the existing
                    # supply is the default supply
                    print(("No producers for {} found in {}, "
                           "using hydrogen from {}").format(
                               h2sup["product"], region, h2sup["location"]))

            # create local fcev
            for fcev in fcevs:
                local_fcev = fcev.copy()
                local_fcev["location"] = region
                # link correct market
                fuel_ex = next(ws.technosphere(
                    local_fcev,
                    ws.startswith("name", "fuel supply for hydrogen vehicles")))
                fuel_ex["location"] = region
                self.db.append(local_fcev)
            self.db.append(supply)

    def update_fuel_mix(self):
        data = self.rmd.get_remind_fuel_mix()
        for region in self.remind_regions:
            # find activities
            icevs = ws.get_many(
                self.db,
                ws.either(
                    ws.contains("name", "ICEV-"),
                    ws.contains("name", "HEV-")),
                ws.equals("location", region))

