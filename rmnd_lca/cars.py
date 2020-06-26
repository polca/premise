from .geomap import Geomap

import wurst
import wurst.searching as ws
import pandas as pd
import uuid
import copy

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

    def _create_local_copy(self, old_act, region):
        """
        Create a local copy of an activity.
        Update also the production exchange.
        """
        act = copy.deepcopy(old_act)
        act.update({
            "location": region,
            "code": str(uuid.uuid4().hex)
        })

        # update production exchange
        prods = list(ws.production(
            act, ws.equals("name", act["name"])))
        if len(prods) == 1:
            prods[0]["location"] = region
        else:
            raise ValueError(
                "Multiple or no Production Exchanges found for {}."
                .format(old_act["name"]))
        return act

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

        old_supply = ws.get_one(
            self.db,
            ws.startswith(
                "name", "electricity supply for electric vehicles"))

        for region in self.remind_regions:

            # create local electricity supply
            supply = self._create_local_copy(old_supply, region)
            # replace electricity input
            for sup in ws.technosphere(
                    supply, ws.equals("product", "electricity, low voltage")):
                sup.update({
                    "name": "market group for electricity, low voltage",
                    "location": region
                })
            print("Relinking electricity markets for BEVs in {}".format(region))

            for bev in bevs:
                new_bev = self._create_local_copy(bev, region)
                # update fuel market
                oldex = list(ws.technosphere(
                    new_bev,
                    ws.startswith(
                        "name",
                        "electricity supply for electric vehicles")))
                # should only be one
                if len(oldex) != 1:
                    raise ValueError(
                        "Zero or more than one electricity "
                        "markets for fuel production found for {} in {}"
                        .format(new_bev["name"], new_bev["location"]))
                elif len(oldex) == 1:
                    # reference the new supply
                    oldex[0].update({
                        "location": region
                    })
                    self.db.append(new_bev)
            self.db.append(supply)

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
                print("Assuming default datasets from RER, no updates for EUR.")
                continue
            print("Relinking hydrogen markets for FCEVs in {}".format(region))
            # create local hydrogen supply
            supply = self._create_local_copy(old_supply, region)
            # remove explicit electricity input
            elmark = next(ws.technosphere(supply, ws.startswith(
                "name", "electricity market for fuel preparation")))
            elmark["amount"] = 0
            wurst.delete_zero_amount_exchanges([supply])

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
                # create local fcevs
                local_fcev = self._create_local_copy(fcev, region)
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

