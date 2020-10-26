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
                ws.startswith("name", "Passenger car, BEV"),
                ws.startswith("name", "Passenger car, PHEV"),
            ),
            ws.contains("name", str(self.year))))

        self._delete_non_global(bevs)

        old_supply = ws.get_one(
            self.db,
            ws.equals(
                "name", "electricity supply for electric vehicles, {}"
                .format(self.year)))

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
            ws.startswith("name", "Passenger car, FCEV"),
            ws.contains("name", str(self.year))))

        self._delete_non_global(fcevs)
        old_supply = ws.get_one(
            self.db,
            ws.equals(
                "name", "fuel supply for hydrogen vehicles, {}"
                .format(self.year)))

        for region in self.remind_regions:
            print("Relinking hydrogen markets for FCEVs in {}".format(region))
            # create local hydrogen supply
            supply = self._create_local_copy(old_supply, region)
            # remove explicit electricity input
            elmark = next(ws.technosphere(supply, ws.startswith(
                "name", "electricity market for fuel preparation")))
            elmark["amount"] = 0
            wurst.delete_zero_amount_exchanges([supply])

            # find hydrogen supply nearby
            h2sups = ws.technosphere(
                supply,
                ws.startswith("product", "Hydrogen"))
            for h2sup in h2sups:
                prod = self._find_local_supplier(region, h2sup["name"])
                h2sup["location"] = prod["location"]
                h2sup["name"] = prod["name"]

            # create local fcev
            for fcev in fcevs:
                # create local fcevs
                local_fcev = self._create_local_copy(fcev, region)
                # link correct market
                fuel_ex = next(ws.technosphere(
                    local_fcev,
                    ws.startswith("name", "fuel supply for hydrogen vehicles, {}"
                                  .format(self.year))))
                fuel_ex["location"] = region
                self.db.append(local_fcev)
            self.db.append(supply)

    def _find_local_supplier(self, region, name):
        """
        Use geomatcher to find a supplier with `name` first strictly
        within the region, then in an intersecting region and
        eventually *any* activity with this name.
        """
        def producer_in_locations(locs):
            prod = None
            producers = list(ws.get_many(
                self.db,
                ws.equals("name", name),
                ws.either(*[
                    ws.equals("location", loc) for loc in locs
                ])))
            if len(producers) >= 1:
                prod = producers[0]
                if len(producers) > 1:
                    print(("Multiple producers for {} found in {}, "
                           "using activity from {}").format(
                               name, region, prod["location"]))
            return prod

        ei_locs = self.geo.remind_to_ecoinvent_location(region, contained=True)
        prod = producer_in_locations(ei_locs)
        if prod is None:
            ei_locs = self.geo.remind_to_ecoinvent_location(region)
            prod = producer_in_locations(ei_locs)
            if prod is None:
                # let's use "any" dataset
                producers = list(ws.get_many(
                    self.db,
                    ws.equals("name", name)))
                if len(producers) == 0:
                    raise ValueError("No producers found for {}.")
                prod = producers[0]
                # we can leave things as they are since the existing
                # supply is the default supply
                print(("No producers for {} found in {}\n"
                       "Using activity from {}")
                      .format(name, region, prod["location"]))
        return prod

    def create_local_icevs(self):
        """
        Use REMIND fuel markets to update the mix of bio-, syn-
        and fossil liquids in gasoline and diesel.

        For ICEVs we create also local versions of older cars
        with EURO-X for X < 6d emission standards if required.
        """
        print("Creating local ICEV activities")
        icevs = list(ws.get_many(
            self.db,
            ws.startswith("name", "Passenger car, ICEV-")
        ))

        icevs.extend(list(ws.get_many(
            self.db,
            ws.startswith("name", "Passenger car, HEV-"),
            ws.contains("name", str(self.year))
        )))

        old_suppliers = {
            fuel: ws.get_one(
                self.db,
                ws.equals(
                    "name", "fuel supply for {} vehicles, {}"
                    .format(fuel, self.year)))
            for fuel in ["diesel", "gasoline"]}

        new_producers = {
            "diesel": {
                # biodiesel is only from cooking oil from RER,
                # as this is not the focus for now
                # to be improved!
                "Biomass": ws.get_one(
                    self.db,
                    ws.equals("name", "Biodiesel from cooking oil"))
            },
            "gasoline": {
                # only ethanol from European wheat straw as biofuel
                "Biomass": ws.get_one(
                    self.db,
                    ws.equals("name", "Ethanol from wheat straw pellets"),
                    ws.equals("location", "RER"))
            }
        }

        data = self.rmd.get_remind_fuel_mix_for_ldvs()
        for region in self.remind_regions:
            # two regions for gasoline and diesel production
            if region == "EUR":
                new_producers["gasoline"]["Fossil"] = ws.get_one(
                    self.db,
                    ws.equals("name", "market for petrol, low-sulfur"),
                    ws.equals("location", "Europe without Switzerland"))
                new_producers["diesel"]["Fossil"] = ws.get_one(
                    self.db,
                    ws.equals("name", "market group for diesel"),
                    ws.equals("location", "RER"))
            else:
                new_producers["gasoline"]["Fossil"] = ws.get_one(
                    self.db,
                    ws.equals("name", "market for petrol, low-sulfur"),
                    ws.equals("location", "RoW"))
                new_producers["diesel"]["Fossil"] = ws.get_one(
                    self.db,
                    ws.equals("name", "market group for diesel"),
                    ws.equals("location", "GLO"))

            # local syndiesel
            new_producers["diesel"]["Hydrogen"] = self._find_local_supplier(
                region, "Diesel production, synthetic, Fischer Tropsch process")

            new_producers["gasoline"]["Hydrogen"] = self._find_local_supplier(
                region, "Gasoline production, synthetic, from methanol")

            print("Relinking fuel markets for ICEVs in {}".format(region))
            for ftype in new_producers:
                new_supp = self._create_local_copy(
                    old_suppliers[ftype], region)

                new_supp["exchanges"] = [{
                    "amount": data.loc[region, suptype].values.item(),
                    "name": new_producers[ftype][suptype]["name"],
                    "location": new_producers[ftype][suptype]["location"],
                    "unit": "kilogram",
                    "type": "technosphere",
                    "reference product": new_producers[ftype][suptype]["reference product"],
                    "product": new_producers[ftype][suptype]["reference product"]
                } for suptype in new_producers[ftype]]

                new_supp["exchanges"].append({
                    "amount": 1,
                    "name": new_supp["name"],
                    "location": region,
                    "unit": "kilogram",
                    "type": "production",
                    "reference product": "fuel",
                    "product": "fuel"
                })

                self.db.append(new_supp)

            shortcuts = {
                "diesel": "EV-d",
                "gasoline": "EV-p"
            }

            for ftype in shortcuts:
                # diesel cars
                cars = list(ws.get_many(
                    icevs, ws.contains("name", shortcuts[ftype])))
                for car in cars:
                    # some local activities might already exist
                    local_dcar = self._get_local_act_or_copy(
                        cars, car, region)
                    # replace diesel supplier
                    fuel_ex = next(ws.technosphere(
                        local_dcar,
                        ws.startswith(
                            "name",
                            "fuel supply for {} vehicles".format(ftype))))
                    # point explicitly to the *newest* supply process
                    fuel_ex["name"] = ("fuel supply for {} vehicles, {}"
                                       .format(ftype, self.year))
                    fuel_ex["location"] = region

    def _get_local_act_or_copy(self, db, act, region):
        """
        Find and return a local activity. If it is not found,
        create a local copy, append it to the database and return it.
        If multiple results are found, throw a ValueError.
        """
        local_acts = list(ws.get_many(
            db,
            ws.equals("name", act["name"]),
            ws.equals("location", region)))
        if len(local_acts) == 1:
            return local_acts[0]
        elif len(local_acts) == 0:
            new_act = self._create_local_copy(act, region)
            self.db.append(new_act)
            return new_act
        else:
            raise ValueError("Multiple activities found for {} in {}"
                             .format(act["name"], region))

    def create_icev_fleet_mix(self):
        """
        For historical combustion engine fleets, we assume that
        the fleet is made up from last 15 years of vehicle production,
        distributing production evenly accross 5 year intervals.

        For newer technologies, we assume that the fleet is made up
        solely from new vehicles (this is mainly relevant wrt.
        emission standards).
        """
        techs = ["ICEV-p", "ICEV-d"]
        sizes = ["Mini", "Small", "Lower medium", "Medium", "Large", "SUV", "Van"]
        for region in self.remind_regions:
            for tech in techs:
                for size in sizes:
                    new_dataset = {
                        "location": region,
                        "name": ("Passenger car fleet mix, {}, {}, {}"
                                 .format(tech, size, self.year)),
                        "reference product": "transport, passenger car",
                        "unit": "kilometer",
                        "database": self.db[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": "Fleet mix from historical data for Europe.",
                        "exchanges": [
                            {
                                "uncertainty type": 0,
                                "loc": 1,
                                "amount": 1,
                                "type": "production",
                                "production volume": 0,
                                "product": "transport, passenger car",
                                "name": ("Passenger car fleet mix, {}, {}"
                                         .format(tech, size, self.year)),
                                "unit": "kilometer",
                                "location": region,
                            }]
                    }

                    supplrs = list(ws.get_many(
                        self.db,
                        ws.startswith("name", "Passenger car, {}, {}".format(tech, size)),
                        ws.equals("location", region),
                        ws.exclude(
                            ws.startswith("name", "Passenger car, {}, {}, {}".format(
                                tech, size, self.year))
                    )))

                    # There should be 4 suppliers (last 20 years)
                    if len(supplrs) != 4:
                        raise Exception(
                            "Incorrect number of historical ICEV activities.")

                    new_dataset["exchanges"].extend([{
                        "amount": 0.25,
                        "name": supp["name"],
                        "location": supp["location"],
                        "unit": "kilometer",
                        "type": "technosphere",
                        "reference product": supp["reference product"],
                        "product": supp["reference product"]
                    } for supp in supplrs])
                    self.db.append(new_dataset)


    def update_cars(self):
        self.create_local_evs()
        self.create_local_fcevs()
        self.create_local_icevs()
