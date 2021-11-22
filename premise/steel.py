import copy

import numpy as np
import wurst
from wurst import transformations as wt

from .activity_maps import InventorySet
from .geomap import Geomap
from .utils import *


class Steel:
    """
    Class that modifies steel markets in ecoinvent based on IAM output data.

    :ivar db: database dictionary from :attr:`.NewDatabase.database`
    :vartype database: dict
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :vartype model: str
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :vartype iam_data: xarray.DataArray
    :ivar year: year, from :attr:`.NewDatabase.year`
    :vartype year: int

    """

    def __init__(self, db, model, iam_data, year):
        self.db = db
        self.iam_data = iam_data
        self.year = year
        self.steel_data = self.iam_data.data.interp(year=self.year)
        self.geo = Geomap(
            model=model, current_regions=iam_data.data.coords["region"].values.tolist()
        )
        self.model = model
        mapping = InventorySet(self.db)
        self.emissions_map = mapping.get_gains_to_ecoinvent_emissions()
        self.fuel_map = mapping.generate_fuel_map()
        self.material_map = mapping.generate_material_map()
        self.recycling_rates = get_steel_recycling_rates(year=self.year)
        self.regions = iam_data.data.coords["region"].values.tolist()

    def fetch_proxies(self, name, ref_prod, relink=False):
        """
        Fetch dataset proxies, given a dataset `name` and `ref_prod`.
        Store a copy for each IAM region.
        If an IAM region does not find a fitting ecoinvent location,
        fetch a dataset with a "RoW" location.
        Delete original datasets from the database.

        :return:
        """

        d_map = {
            self.geo.ecoinvent_to_iam_location(d["location"]): d["location"]
            for d in ws.get_many(
                self.db,
                ws.equals("name", name),
                ws.contains("reference product", ref_prod),
            )
        }

        list_iam_regions = [
            c[1]
            for c in self.geo.geo.keys()
            if type(c) == tuple and c[0].lower() == self.model and c[1] != "World"
        ]

        if "market" in name:
            d_iam_to_eco = {r: d_map.get(r, "GLO") for r in list_iam_regions}
        else:
            d_iam_to_eco = {r: d_map.get(r, "RoW") for r in list_iam_regions}

        d_act = {}

        for d in list_iam_regions:
            try:
                ds = ws.get_one(
                    self.db,
                    ws.equals("name", name),
                    ws.contains("reference product", "steel"),
                    ws.equals("location", d_iam_to_eco[d]),
                )

            except ws.NoResults:
                print("No dataset {} found for the IAM region {}".format(name, d))
                continue
            except ws.MultipleResults:
                print(
                    "Multiple results for {} found for the IAM region {}".format(
                        name, d
                    )
                )

                ds = ws.get_many(
                    self.db,
                    ws.equals("name", name),
                    ws.contains("reference product", "steel"),
                    ws.equals("location", d_iam_to_eco[d]),
                )

                for x in ds:
                    print(x["name"], x["location"], x["reference product"])

                raise

            d_act[d] = wt.copy_to_new_location(ds, d)
            d_act[d]["code"] = str(uuid.uuid4().hex)

            if relink:
                relink_technosphere_exchanges(d_act[d], self.db, self.model)

            # Add `production volume` field
            if self.model == "remind":
                if "converter" in name:
                    prod = ["Production|Industry|Steel|Primary"]

                elif "electric" in name:
                    prod = ["Production|Industry|Steel|Secondary"]
                else:
                    prod = [
                        "Production|Industry|Steel|Primary",
                        "Production|Industry|Steel|Secondary",
                    ]

            else:
                prod = ["Production|Steel"]

            prod_vol = (
                self.iam_data.data.sel(region=d, variables=prod)
                .interp(year=self.year)
                .sum(dim="variables")
                .values.item(0)
            )

            if "input" in d_act[d]:
                d_act[d].pop("input")

            for prod in ws.production(d_act[d]):
                prod["location"] = d
                prod["production volume"] = prod_vol

                if "input" in prod:
                    prod.pop("input")

            # remove current steel production inputs
            # if it is a market
            if "market" in name:

                d_name = {
                    "market for steel, low-alloyed": [
                        "steel production, converter, low-alloyed",
                        "steel production, electric, low-alloyed",
                    ],
                    "market for steel, unalloyed": [
                        "steel production, converter, unalloyed"
                    ],
                    "market for steel, chromium steel 18/8": [
                        "steel production, electric, chromium steel 18/8"
                    ],
                }
                d_act[d]["exchanges"] = [
                    e
                    for e in d_act[d]["exchanges"]
                    if e["type"] == "production" or "steel" not in e["product"]
                ]

                if name == "market for steel, low-alloyed" and self.model == "remind":
                    total_production_volume = self.steel_data.sel(
                        region=d, variables="Production|Industry|Steel"
                    )
                    primary_share = (
                        self.steel_data.sel(
                            region=d, variables="Production|Industry|Steel|Primary"
                        )
                        / total_production_volume
                    ).values

                    secondary_share = 1 - primary_share
                    d_act[d]["exchanges"].extend(
                        [
                            {
                                "name": d_name[name][0],
                                "product": ref_prod,
                                "amount": primary_share,
                                "unit": "kilogram",
                                "type": "technosphere",
                                "location": d,
                            },
                            {
                                "name": d_name[name][1],
                                "product": ref_prod,
                                "amount": secondary_share,
                                "unit": "kilogram",
                                "type": "technosphere",
                                "location": d,
                            },
                        ]
                    )
                else:
                    d_act[d]["exchanges"].append(
                        {
                            "name": d_name[name][0],
                            "product": ref_prod,
                            "amount": 1.0,
                            "unit": "kilogram",
                            "type": "technosphere",
                            "location": d,
                        }
                    )

        deleted_markets = [
            (act["name"], act["reference product"], act["location"])
            for act in self.db
            if act["name"] == name and ref_prod in act["reference product"]
        ]

        with open(DATA_DIR / "logs/log deleted steel datasets.csv", "a") as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in deleted_markets:
                writer.writerow(line)

        # Remove old datasets
        self.db = [
            act
            for act in self.db
            if (act["name"], act["reference product"], act["location"])
            not in deleted_markets
        ]

        return d_act

    @staticmethod
    def remove_exchanges(d, list_exc):

        keep = lambda x: {
            k: v for k, v in x.items() if not any(ele in x["name"] for ele in list_exc)
        }

        for r in d:
            d[r]["exchanges"] = [keep(exc) for exc in d[r]["exchanges"]]
            d[r]["exchanges"] = [v for v in d[r]["exchanges"] if v]

        return d

    @staticmethod
    def get_shares_from_production_volume(ds):
        """
        Return shares of supply based on production volumes
        :param ds: list of datasets
        :return: dictionary with (dataset name, dataset location) as keys, shares as values. Shares total 1.
        :rtype: dict
        """
        dict_act = {}
        total_production_volume = 0
        for act in ds:
            for exc in ws.production(act):
                dict_act[
                    (
                        act["name"],
                        act["location"],
                        act["reference product"],
                        act["unit"],
                    )
                ] = float(exc.get("production volume", 1))
                total_production_volume += float(exc.get("production volume", 1))

        for d in dict_act:
            dict_act[d] /= total_production_volume

        return dict_act

    def get_suppliers_of_a_region(
        self, iam_regions, ecoinvent_technologies, reference_product
    ):
        """
        Return a list of datasets which location and name correspond to the region, name and reference product given,
        respectively.

        :param iam_regions: list of REMIND regions
        :type iam_regions: list
        :param ecoinvent_technologies: list of names of ecoinvent dataset
        :type ecoinvent_technologies: list
        :param reference_product: reference product
        :type reference_product: str
        :return: list of wurst datasets
        :rtype: list
        """
        list_regions = [
            self.geo.iam_to_ecoinvent_location(region) for region in iam_regions
        ]
        list_regions = [x for y in list_regions for x in y]

        return ws.get_many(
            self.db,
            *[
                ws.either(
                    *[
                        ws.equals("name", supplier)
                        for supplier in ecoinvent_technologies
                    ]
                ),
                ws.either(*[ws.equals("location", loc) for loc in list_regions]),
                ws.equals("reference product", reference_product),
            ],
        )

    def relink_datasets(self, name, ref_product):
        """
        For a given dataset name, change its location to a REMIND location,
        to effectively link the newly built dataset(s).

        :param ref_product:
        :param name: dataset name
        :type name: str
        """
        list_remind_regions = [
            c[1] for c in self.geo.geo.keys() if type(c) == tuple and c[0] == "REMIND"
        ]

        for act in self.db:
            for exc in act["exchanges"]:
                try:
                    exc["name"]
                except KeyError:
                    print(exc)
                if (exc["name"], exc.get("product")) == (name, ref_product) and exc[
                    "type"
                ] == "technosphere":
                    if act["location"] not in list_remind_regions:
                        if act["location"] == "North America without Quebec":
                            exc["location"] = "USA"
                        else:
                            try:
                                exc["location"] = self.geo.ecoinvent_to_iam_location(
                                    act["location"]
                                )
                            except:
                                print("cannot find for {}".format(act["location"]))
                    else:
                        exc["location"] = act["location"]

    def update_pollutant_emissions(self, ds):
        """
        Update pollutant emissions based on GAINS data.
        We apply a correction factor defined as being equal to
        the emission level in the year in question, compared
        to 2020
        :return:
        """

        # Update biosphere exchanges according to GAINS emission values
        for exc in ws.biosphere(
            ds, ws.either(*[ws.contains("name", x) for x in self.emissions_map])
        ):
            remind_emission_label = self.emissions_map[exc["name"]]

            if (
                ds["location"] in self.iam_data.steel_emissions.region.values
                or ds["location"] == "World"
            ):
                correction_factor = (
                    self.iam_data.steel_emissions.loc[
                        dict(
                            region=ds["location"]
                            if ds["location"] != "World"
                            else "CHA",
                            pollutant=remind_emission_label,
                        )
                    ].interp(year=self.year)
                    / self.iam_data.steel_emissions.loc[
                        dict(
                            region=ds["location"]
                            if ds["location"] != "World"
                            else "CHA",
                            pollutant=remind_emission_label,
                            year=2020,
                        )
                    ]
                ).values.item(0)

            else:

                if self.model == "remind":
                    loc = self.geo.ecoinvent_to_iam_location(ds["location"])
                else:
                    if ds["location"] in self.iam_data.regions:
                        loc = self.geo.iam_to_iam_region(ds["location"])
                    else:
                        loc = self.geo.iam_to_iam_region(
                            self.geo.ecoinvent_to_iam_location(ds["location"])
                        )

                correction_factor = (
                    self.iam_data.steel_emissions.loc[
                        dict(
                            region="CHA" if loc == "World" else loc,
                            pollutant=remind_emission_label,
                        )
                    ].interp(year=self.year)
                    / self.iam_data.steel_emissions.loc[
                        dict(
                            region="CHA" if loc == "World" else loc,
                            pollutant=remind_emission_label,
                            year=2020,
                        )
                    ]
                ).values.item(0)

            if correction_factor != 0 and ~np.isnan(correction_factor):
                if exc["amount"] == 0:
                    wurst.rescale_exchange(
                        exc, correction_factor / 1, remove_uncertainty=True
                    )
                else:
                    wurst.rescale_exchange(exc, correction_factor)

                exc[
                    "comment"
                ] = "This exchange has been modified based on GAINS projections for the steel sector by `premise`."
        return ds

    def fuel_efficiency_factor(self, ds):
        """

        :param ds:
        :return: correction factor
        :rtype: float
        """
        loc = ds["location"]

        if self.model == "remind":
            # REMIND
            if "electric" in ds["name"]:
                final_energy = ["FE|Industry|Steel|Secondary"]
                prod = "Production|Industry|Steel|Secondary"
            else:
                final_energy = ["FE|Industry|Steel|Primary"]
                prod = "Production|Industry|Steel|Primary"
        else:
            # IMAGE
            final_energy = [
                "Final Energy|Industry|Steel|Electricity",
                "Final Energy|Industry|Steel|Gases",
                "Final Energy|Industry|Steel|Heat",
                "Final Energy|Industry|Steel|Hydrogen",
                "Final Energy|Industry|Steel|Liquids",
                "Final Energy|Industry|Steel|Solids",
            ]
            prod = "Production|Steel"

        # sometimes, the energy consumption values are not reported for the region "World"
        # in such case, we then look at teh sum of all the regions
        if (
            self.iam_data.data.loc[dict(region=loc, variables=final_energy)]
            .interp(year=self.year)
            .sum()
            == 0
        ):
            loc = self.iam_data.data.region.values

        eff_factor = (
            (
                self.iam_data.data.loc[
                    dict(
                        region=[loc] if isinstance(loc, str) else loc,
                        variables=final_energy,
                    )
                ]
                .interp(year=self.year)
                .sum(dim=["region", "variables"])
                / self.iam_data.data.loc[
                    dict(
                        region=[loc] if isinstance(loc, str) else loc,
                        variables=prod,
                    )
                ]
                .interp(year=self.year)
                .sum(dim="region")
            )
            / (
                self.iam_data.data.loc[
                    dict(
                        region=[loc] if isinstance(loc, str) else loc,
                        variables=final_energy,
                        year=2020,
                    )
                ].sum(dim=["region", "variables"])
                / self.iam_data.data.loc[
                    dict(
                        region=[loc] if isinstance(loc, str) else loc,
                        variables=prod,
                        year=2020,
                    )
                ].sum(dim="region")
            )
        ).values.item(0)

        # we assume efficiency cannot get worse over time
        if (
            eff_factor == np.nan
            or eff_factor == np.inf
            or eff_factor > 1
            or eff_factor != eff_factor
        ):
            eff_factor = 1

        return eff_factor

    def get_carbon_capture_rate(self, loc):
        """

        :param loc: location of the dataset
        :return: rate of carbon capture
        :rtype: float
        """

        if self.model == "remind":
            if all(
                x in self.iam_data.data.variables.values
                for x in ["Emi|CCO2|FFaI|Industry|Steel", "Emi|CO2|FFaI|Industry|Steel"]
            ):
                rate = (
                    self.iam_data.data.sel(
                        variables="Emi|CCO2|FFaI|Industry|Steel", region=loc
                    ).interp(year=self.year)
                    / self.iam_data.data.sel(
                        variables=[
                            "Emi|CCO2|FFaI|Industry|Steel",
                            "Emi|CO2|FFaI|Industry|Steel",
                        ],
                        region=loc,
                    )
                    .interp(year=self.year)
                    .sum(dim="variables")
                ).values
            else:
                rate = 0
        else:
            if all(
                x in self.iam_data.data.variables.values
                for x in [
                    "Emissions|CO2|Industry|Steel|Gross",
                    "Emissions|CO2|Industry|Steel|Sequestered",
                ]
            ):
                rate = (
                    self.iam_data.data.sel(
                        variables="Emissions|CO2|Industry|Steel|Sequestered", region=loc
                    ).interp(year=self.year)
                    / self.iam_data.data.sel(
                        variables=[
                            "Emissions|CO2|Industry|Steel|Gross",
                            "Emissions|CO2|Industry|Steel|Sequestered",
                        ],
                        region=loc,
                    )
                    .interp(year=self.year)
                    .sum(dim="variables")
                ).values
            else:
                rate = 0

        return rate

    def get_carbon_capture_energy_inputs(self, amount_CO2, loc):
        """
        Returns the additional electricity and heat exchanges to add to the dataset
        associated with the carbon capture

        :param amount_CO2: inital amount of CO2 emitted
        :param loc: location of the steel production dataset
        :return: carbon capture rate, list of exchanges
        :rtype: float, list
        """

        rate = self.get_carbon_capture_rate(loc)

        new_exchanges = list()

        if rate > 0:
            # Electricity: 0.024 kWh/kg CO2 for capture, 0.146 kWh/kg CO2 for compression
            carbon_capture_electricity = (amount_CO2 * rate) * (0.146 + 0.024)

            try:
                new_supplier = ws.get_one(
                    self.db,
                    ws.equals("name", "market group for electricity, medium voltage"),
                    ws.equals("location", loc),
                    ws.equals("reference product", "electricity, medium voltage"),
                )
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 1,
                        "amount": carbon_capture_electricity,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "electricity, medium voltage",
                        "name": "market group for electricity, medium voltage",
                        "unit": "kilowatt hour",
                        "location": new_supplier["location"],
                    }
                )

            except ws.NoResults:
                # maybe update_electricity has not been applied
                try:
                    new_supplier = ws.get_one(
                        self.db,
                        ws.equals(
                            "name", "market group for electricity, medium voltage"
                        ),
                        ws.either(
                            *[
                                ws.equals("location", l[1])
                                if isinstance(l, tuple)
                                else ws.equals("location", l)
                                for l in self.geo.iam_to_ecoinvent_location(loc)
                            ]
                        ),
                        ws.equals("reference product", "electricity, medium voltage"),
                    )
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": carbon_capture_electricity,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": new_supplier["location"],
                        }
                    ]
                except ws.MultipleResults:
                    # We have several potential electricity suppliers
                    # We will look up their respective production volumes
                    # And include them proportionally to it

                    possible_suppliers = ws.get_many(
                        self.db,
                        ws.equals(
                            "name", "market group for electricity, medium voltage"
                        ),
                        ws.either(
                            *[
                                ws.equals("location", l[1])
                                if isinstance(l, tuple)
                                else ws.equals("location", l)
                                for l in self.geo.iam_to_ecoinvent_location(loc)
                            ]
                        ),
                        ws.equals("reference product", "electricity, medium voltage"),
                    )
                    possible_suppliers = self.get_shares_from_production_volume(
                        possible_suppliers
                    )

                    new_exchanges = []
                    for supplier in possible_suppliers:
                        new_exchanges.append(
                            {
                                "uncertainty type": 0,
                                "loc": 1,
                                "amount": carbon_capture_electricity
                                * possible_suppliers[supplier],
                                "type": "technosphere",
                                "production volume": 0,
                                "product": "electricity, medium voltage",
                                "name": "market group for electricity, medium voltage",
                                "unit": "kilowatt hour",
                                "location": supplier[1],
                            }
                        )

                except ws.NoResults:
                    # there's no "market group for electricity" matching the location
                    # we try with "market for electricity"
                    try:
                        new_supplier = ws.get_one(
                            self.db,
                            ws.equals("name", "market for electricity, medium voltage"),
                            ws.either(
                                *[
                                    ws.equals("location", l[1])
                                    if isinstance(l, tuple)
                                    else ws.equals("location", l)
                                    for l in self.geo.iam_to_ecoinvent_location(loc)
                                ]
                            ),
                            ws.equals(
                                "reference product", "electricity, medium voltage"
                            ),
                        )
                        new_exchanges = [
                            {
                                "uncertainty type": 0,
                                "loc": 1,
                                "amount": carbon_capture_electricity,
                                "type": "technosphere",
                                "production volume": 0,
                                "product": "electricity, medium voltage",
                                "name": "market for electricity, medium voltage",
                                "unit": "kilowatt hour",
                                "location": new_supplier["location"],
                            }
                        ]
                    except ws.MultipleResults:
                        # We have several potential electricity suppliers
                        # We will look up their respective production volumes
                        # And include them proportionally to it

                        possible_suppliers = ws.get_many(
                            self.db,
                            ws.equals("name", "market for electricity, medium voltage"),
                            ws.either(
                                *[
                                    ws.equals("location", l[1])
                                    if isinstance(l, tuple)
                                    else ws.equals("location", l)
                                    for l in self.geo.iam_to_ecoinvent_location(loc)
                                ]
                            ),
                            ws.equals(
                                "reference product", "electricity, medium voltage"
                            ),
                        )
                        possible_suppliers = self.get_shares_from_production_volume(
                            possible_suppliers
                        )

                        new_exchanges = []
                        for supplier in possible_suppliers:
                            new_exchanges.append(
                                {
                                    "uncertainty type": 0,
                                    "loc": 1,
                                    "amount": carbon_capture_electricity
                                    * possible_suppliers[supplier],
                                    "type": "technosphere",
                                    "production volume": 0,
                                    "product": "electricity, medium voltage",
                                    "name": "market for electricity, medium voltage",
                                    "unit": "kilowatt hour",
                                    "location": supplier[1],
                                }
                            )

            carbon_capture_heat = (amount_CO2 * rate) * 3.48

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": carbon_capture_heat,
                    "type": "technosphere",
                    "production volume": 0,
                    "product": "heat, from steam, in chemical industry",
                    "name": "steam production, as energy carrier, in chemical industry",
                    "unit": "megajoule",
                    "location": "RoW",
                }
            )

        return rate, new_exchanges

    def generate_activities(self):
        """
        This function generates new activities for primary and secondary steel production and add them to the ecoinvent database.

        :return: Returns a modified database with newly added steel activities for the corresponding year
        """

        print(
            "The validity of the datasets produced from the integration of the steel sector is not yet fully tested. Consider the results with caution."
        )

        print("Log of deleted steel datasets saved in {}".format(DATA_DIR / "logs"))
        print("Log of created steel datasets saved in {}".format(DATA_DIR / "logs"))

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(DATA_DIR / "logs/log deleted steel datasets.csv", "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(["dataset name", "reference product", "location"])

        with open(DATA_DIR / "logs/log created steel datasets.csv", "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(["dataset name", "reference product", "location"])

        print("Create steel markets for different regions")

        created_datasets = list()
        for i in (
            ("market for steel, low-alloyed", "steel, low-alloyed"),
            ("market for steel, unalloyed", "steel, unalloyed"),
            ("market for steel, chromium steel 18/8", "steel, chromium steel 18/8"),
        ):
            act_steel = self.fetch_proxies(i[0], i[1])

            self.db.extend([v for v in act_steel.values()])

            created_datasets.extend(
                [
                    (act["name"], act["reference product"], act["location"])
                    for act in act_steel.values()
                ]
            )

            # Create global market for steel
            ds = ws.get_one(
                self.db,
                ws.equals("name", i[0]),
                ws.contains("reference product", "steel"),
                ws.equals("location", "WEU" if self.model == "image" else "EUR"),
            )
            d = copy.deepcopy(ds)
            d["location"] = "World"
            d["code"] = str(uuid.uuid4().hex)

            if "input" in d:
                d.pop("input")

            for prod in ws.production(d):
                prod["location"] = d["location"]

                if "input" in prod:
                    prod.pop("input")

            d["exchanges"] = [x for x in d["exchanges"] if x["type"] == "production"]

            if self.model == "remind":
                prod = "Production|Industry|Steel"
            else:
                prod = "Production|Steel"

            regions = [r for r in self.iam_data.data.region.values if r != "World"]
            for region in regions:
                share = (
                    self.iam_data.data.sel(variables=prod, region=region).interp(
                        year=self.year
                    )
                    / self.iam_data.data.sel(variables=prod, region=regions)
                    .interp(year=self.year)
                    .sum(dim="region")
                ).values.item(0)

                d["exchanges"].append(
                    {
                        "name": i[0],
                        "product": i[1],
                        "amount": share,
                        "unit": "kilogram",
                        "type": "technosphere",
                        "location": region,
                    }
                )

            self.db.append(d)

            print("Relink new steel markets to steel-consuming activities")
            self.relink_to_new_steel_markets(i[0], i[1])

        # Determine all steel activities in the database. Delete old datasets.
        print("Create new steel production datasets and delete old datasets")
        d_act_primary_steel = {
            mat: self.fetch_proxies(mat[0], mat[1], relink=True)
            for mat in zip(
                self.material_map["steel, primary"],
                ["steel"] * len(self.material_map["steel, primary"]),
            )
        }
        d_act_secondary_steel = {
            mat: self.fetch_proxies(mat[0], mat[1], relink=True)
            for mat in zip(
                self.material_map["steel, secondary"],
                ["steel"] * len(self.material_map["steel, secondary"]),
            )
        }
        d_act_steel = {**d_act_primary_steel, **d_act_secondary_steel}

        # Scale down fuel exchanges, according to efficiency improvement as
        # forecast by the IAM:
        list_fuels = [
            "diesel",
            "coal",
            "lignite",
            "coke",
            "fuel",
            "meat",
            "gas",
            "oil",
            "electricity",
            "natural gas",
        ]

        for steel in d_act_steel:

            for ds in d_act_steel[steel]:
                # the correction factor applied to all fuel/electricity input is
                # equal to the ration fuel/output in the year in question
                # divided by the ratio fuel/output in 2020

                correction_factor = self.fuel_efficiency_factor(d_act_steel[steel][ds])

                # just in case the IAM gives weird stuff
                # we assume that things cannot get worse in the future
                if correction_factor > 1:
                    correction_factor = 1

                for exc in ws.technosphere(
                    d_act_steel[steel][ds],
                    ws.either(*[ws.contains("name", x) for x in list_fuels]),
                ):
                    if correction_factor != 0 and ~np.isnan(correction_factor):
                        if exc["amount"] == 0:
                            wurst.rescale_exchange(
                                exc, correction_factor / 1, remove_uncertainty=True
                            )
                        else:
                            wurst.rescale_exchange(exc, correction_factor)

                        exc[
                            "comment"
                        ] = "This exchange has been modified based on REMIND projections for the steel sector by `premise`."

                for exc in ws.biosphere(
                    d_act_steel[steel][ds],
                    ws.contains("name", "Carbon dioxide, fossil"),
                ):
                    if correction_factor != 0 and ~np.isnan(correction_factor):
                        if exc["amount"] == 0:
                            wurst.rescale_exchange(
                                exc, correction_factor / 1, remove_uncertainty=True
                            )
                        else:
                            wurst.rescale_exchange(exc, correction_factor)

                        exc[
                            "comment"
                        ] = "This exchange has been modified based on REMIND projections for the steel sector by `premise`."

                    # Add carbon capture-related energy exchanges
                    # Carbon capture rate: share of capture of total CO2 emitted
                    # Note: only if variables exist in IAM data

                    (
                        carbon_capture_rate,
                        new_exchanges,
                    ) = self.get_carbon_capture_energy_inputs(exc["amount"], ds)

                    if carbon_capture_rate > 0:
                        exc["amount"] *= 1 - carbon_capture_rate
                        d_act_steel[steel][ds]["exchanges"].extend(new_exchanges)

                # Update hot pollutant emission according to GAINS
                self.update_pollutant_emissions(d_act_steel[steel][ds])

            self.db.extend([v for v in d_act_steel[steel].values()])

            print("Relink new steel production datasets to steel-consuming activities")
            self.relink_to_new_steel_markets(
                d_act_steel[steel][ds]["name"],
                d_act_steel[steel][ds]["reference product"],
            )

        print("Done!")

        return self.db

    def create_new_steel_markets(self):

        d_act = {}
        steel_market_names = ["market for steel, low-alloyed"]

        for steel_market in steel_market_names:

            for loc in self.recycling_rates.region.values:
                ds = ws.get_one(
                    self.db,
                    ws.equals("name", steel_market),
                    ws.contains("reference product", "steel"),
                    ws.equals("location", "GLO"),
                )

                d_act[loc] = copy.deepcopy(ds)

            for d in d_act:
                total_production_share = int(
                    self.recycling_rates.sel(region=d)["world_share"]
                    .sum(dim="type")
                    .values.item(0)
                    * 100
                )
                total_production_bof = int(
                    self.recycling_rates.sel(region=d)["world_share"]
                    .sum(dim="type")
                    .values.item(0)
                    * 100
                )
                total_production_ef = 100 - total_production_bof
                d_act[d]["location"] = d
                d_act[d]["code"] = str(uuid.uuid4().hex)
                d_act[d]["production volume"] = total_production_share
                d_act[d]["comment"] = (
                    f"This market activity has been created by `premise` to represent the steel supply from the region {d}."
                    f"This region supplies the equivalent of {total_production_share} pct. of the world crude steel production ({total_production_bof} pct from Blast oxygen furnace and "
                    f"{total_production_ef} pct from Electric furnace), according to (and extrapolated from)"
                    f"https://www.bir.org/publications/facts-figures/download/643/175/36."
                )

                if "input" in d_act[d]:
                    d_act[d].pop("input")

                for prod in ws.production(d_act[d]):
                    prod["location"] = d

                    if "input" in prod:
                        prod.pop("input")

                d_act[d]["exchanges"] = [
                    exc
                    for exc in d_act[d]["exchanges"]
                    if "steel production" not in exc["name"]
                    or exc["type"] == "production"
                ]

                if d == "GLO":
                    d_act[d]["exchanges"] = [
                        exc
                        for exc in d_act[d]["exchanges"]
                        if "transport" not in exc["name"] or exc["type"] == "production"
                    ]

                if d != "GLO":

                    steel_prod_names = {
                        "market for steel, low-alloyed": [
                            "steel production, converter, low-alloyed",
                            "steel production, electric, low-alloyed",
                        ]
                    }

                    for steel_type in steel_prod_names[steel_market]:
                        try:
                            ds = ws.get_one(
                                self.db,
                                *[
                                    ws.contains("name", steel_type),
                                    ws.equals("location", d),
                                    ws.contains("reference product", "steel"),
                                ],
                            )

                        except ws.NoResults:

                            try:

                                ds = ws.get_one(
                                    self.db,
                                    *[
                                        ws.contains("name", steel_type),
                                        ws.either(
                                            *[
                                                ws.equals("location", l)
                                                for l in self.geo.geo.contained(d)
                                            ]
                                        ),
                                        ws.contains("reference product", "steel"),
                                    ],
                                )

                            except ws.NoResults:
                                ds = ws.get_one(
                                    self.db,
                                    *[
                                        ws.contains("name", steel_type),
                                        ws.equals("location", "RoW"),
                                        ws.contains("reference product", "steel"),
                                    ],
                                )
                            except ws.MultipleResults:

                                ds = ws.get_one(
                                    self.db,
                                    *[
                                        ws.contains("name", steel_type),
                                        ws.equals("location", "CH"),
                                        ws.contains("reference product", "steel"),
                                    ],
                                )

                        except ws.MultipleResults:

                            ds = ws.get_one(
                                self.db,
                                *[
                                    ws.contains("name", steel_type),
                                    ws.equals("location", "CH"),
                                    ws.contains("reference product", "steel"),
                                ],
                            )

                        if steel_type == "steel production, converter, low-alloyed":
                            amount = (
                                self.recycling_rates.sel(region=d, type="BOF")["share"]
                                .sum()
                                .values.item(0)
                            )
                        else:
                            amount = (
                                self.recycling_rates.sel(region=d, type="EF")["share"]
                                .sum()
                                .values.item(0)
                            )

                        d_act[d]["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": amount,
                                "amount": amount,
                                "type": "technosphere",
                                "production volume": 1,
                                "product": ds["reference product"],
                                "name": ds["name"],
                                "unit": ds["unit"],
                                "location": ds["location"],
                            }
                        )

            # Populate the world market
            total_supply_amount = 0
            for loc in self.recycling_rates.region.values:
                if loc != "GLO":
                    amount = (
                        self.recycling_rates.sel(region=loc)["world_share"]
                        .sum(dim="type")
                        .values.item(0)
                    )
                    total_supply_amount += amount
                    d_act["GLO"]["exchanges"].append(
                        {
                            "uncertainty type": 0,
                            "loc": amount,
                            "amount": amount,
                            "type": "technosphere",
                            "production volume": 1,
                            "product": ds["reference product"],
                            "name": "market for steel, low-alloyed",
                            "unit": ds["unit"],
                            "location": loc,
                        }
                    )
            if total_supply_amount < 1:
                d_act["GLO"]["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "loc": 1 - total_supply_amount,
                        "amount": 1 - total_supply_amount,
                        "type": "technosphere",
                        "production volume": 1,
                        "product": ds["reference product"],
                        "name": "market for steel, low-alloyed",
                        "unit": ds["unit"],
                        "location": "GLO",
                    }
                )

            self.db = [act for act in self.db if act["name"] != steel_market]

            self.db.extend([v for v in d_act.values()])

    def relink_to_new_steel_markets(self, name, ref_prod):

        # Loop through datasets that are not steel markets
        for ds in ws.get_many(
            self.db, ws.doesnt_contain_any("name", [name, "market for steel"])
        ):
            # Loop through technosphere exchanges that receive an input from the steel market
            excs = [
                exc
                for exc in ws.technosphere(ds)
                if exc["name"] == name and exc["product"] == ref_prod
            ]

            amount = 0
            for exc in excs:
                amount += exc["amount"]
                ds["exchanges"].remove(exc)

            if amount > 0:
                new_exc = {
                    "name": name,
                    "product": ref_prod,
                    "amount": amount,
                    "type": "technosphere",
                    "unit": "kilogram",
                }

                if ds["location"] in self.regions:
                    new_exc["location"] = ds["location"]

                elif ds["location"] in ["RoW", "GLO"]:
                    new_exc["location"] = "World"

                else:

                    # If the dataset location is an ecoinvent location
                    # Let's try to find a steel market which location
                    # encompasses the location of the dataset

                    new_supplier = ws.get_one(
                        self.db,
                        ws.equals("name", name),
                        ws.equals(
                            "location",
                            self.geo.ecoinvent_to_iam_location(ds["location"]),
                        ),
                        ws.contains("reference product", ref_prod),
                    )
                    new_exc["location"] = new_supplier["location"]

                ds["exchanges"].append(new_exc)
