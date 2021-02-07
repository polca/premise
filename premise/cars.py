from .geomap import Geomap

import wurst.searching as ws
import uuid
import copy


class Cars:
    """
    Class that modifies carculator inventories in ecoinvent
    based on IAM output data.

    :ivar db: ecoinvent database in list-of-dict format
    :ivar rmd: IAM output data
    :ivar scenario: IAM scenario identifier
    :ivar year: year for the current analysis
    :ivar model: str. "remind" or "image"

    """

    def __init__(self, db, rmd, scenario, year, model):
        self.db = db
        self.rmd = rmd
        self.geo = Geomap(model=model)
        self.scenario = scenario
        self.year = year
        self.model = model

    @staticmethod
    def _create_local_copy(old_act, region):
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

    def link_local_electricity_supply(self):
        """Create LDV activities for REMIND regions and relink
        existing electricity exchanges for BEVs and PHEVs
        to REMIND-compatible (regional) market groups.
        """
        print("Re-linking local electricity supply for all EV and FCEV activities")

        for region in self.rmd.regions:
            try:
                supply = ws.get_one(
                    self.db,
                    ws.equals(
                        "name", "electricity market for fuel preparation, {}"
                        .format(self.year)),
                    ws.equals("location", region))

                # replace electricity input
                supply["exchanges"] = [
                    e for e in supply["exchanges"] if e["type"] == "production"
                ]
                supply["exchanges"].append({
                    "amount": 1.0,
                    "location": region,
                    "name": "market group for electricity, low voltage",
                    "product": "electricity, low voltage",
                    "tag": "energy chain",
                    "type": "technosphere",
                    "uncertainty type": 0,
                    "unit": "kilowatt hour",
                })
            except ws.NoResults:
                pass

    def _find_local_supplier(self, region, name):
        """
        Use geomatcher to find a supplier with `name` first strictly
        within the region, then in an intersecting region and
        eventually *any* activity with this name.
        """
        def producer_in_locations(locs):

            possible_producers = list(ws.get_many(
                self.db,
                ws.equals("name", name),
                ws.either(*[
                    ws.equals("location", loc) for loc in locs
                ])))

            if len(possible_producers) == 1:
                selected_producer = possible_producers[0]

            if len(possible_producers) > 1:
                possible_locations = tuple([p["location"] for p in possible_producers])
                print(("Multiple potential producers for {} found in {}, "
                       "using activity from {}").format(
                           name, region, possible_locations))

                mapping = {
                    ("RAS", "RER"): "RER",
                }
                selected_producer = [p for p in possible_producers if p["location"] == mapping.get(possible_locations, "RER")][0]

                print("We will use the following location: {}".format(selected_producer["location"]))

            if len(possible_producers) == 0:

                selected_producer = None

            return selected_producer

        ei_locs = self.geo.iam_to_ecoinvent_location(region, contained=True)
        prod = producer_in_locations(ei_locs)

        if prod is None:
            ei_locs = self.geo.iam_to_ecoinvent_location(region)
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

    def link_local_liquid_fuel_markets(self):
        """
        Use IAM fuel markets to update the mix of bio-, syn-
        and fossil liquids in gasoline and diesel.

        """
        new_producers = {
            "diesel": {
                # biodiesel is only from cooking oil from RER,
                # as this is not the focus for now
                # to be improved!
                "Biomass": ws.get_one(
                    self.db,
                    ws.equals("name", "Biodiesel, from used cooking oil, at fuelling station"))
            },
            "gasoline": {
                # only ethanol from European wheat straw as biofuel
                "Biomass": ws.get_one(
                    self.db,
                    ws.equals("name", "Ethanol, from wheat straw pellets, at fuelling station"),
                    ws.equals("location", "RER"))
            }
        }

        for region in self.rmd.regions:
            try:
                supply = {
                    ftype: ws.get_one(
                        self.db,
                        ws.equals("location", region),
                        ws.equals(
                            "name", "fuel supply for {} vehicles, {}"
                            .format(ftype, self.year))) for ftype in ["gasoline", "diesel"]
                }

                # two regions for gasoline and diesel production
                if region in ("EUR", "NEU", "WEU", "CEU"):
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
                    region,
                    "Diesel, synthetic, from electrolysis-based hydrogen, energy allocation, at fuelling station")

                new_producers["gasoline"]["Hydrogen"] = self._find_local_supplier(
                    region,
                    "Gasoline, synthetic, from methanol, at fuelling station")

                supply_search = {
                    "gasoline": {
                        "Hydrogen": "gasoline, synthetic, vehicle grade",
                        "Fossil": "petrol, low-sulfur"
                    },
                    "diesel": {
                        "Hydrogen": "diesel, synthetic, vehicle grade",
                        "Fossil": "diesel"
                    }
                }

                print("Relinking fuel markets for ICEVs in {}".format(region))
                for ftype in supply_search:
                    for subtype in supply_search[ftype]:

                        ex = list(ws.technosphere(
                            supply[ftype], ws.equals("product", supply_search[ftype][subtype])))

                        if len(ex) > 0:
                            ex[0].update({
                                "location": new_producers[ftype][subtype]["location"],
                                "name": new_producers[ftype][subtype]["name"],
                            })

            except ws.NoResults:
                pass

    def update_cars(self):
        self.link_local_electricity_supply()
        self.link_local_liquid_fuel_markets()

        return self.db
