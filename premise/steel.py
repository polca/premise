import wurst
from .transformation import *
import copy
from .utils import *
import numpy as np


class Steel(BaseTransformation):
    """
    Class that modifies steel markets in ecoinvent based on IAM output data.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :vartype database: dict
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :vartype model: str
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :vartype iam_data: xarray.DataArray
    :ivar year: year, from :attr:`.NewDatabase.year`
    :vartype year: int
    
    """

    def __init__(self, database, iam_data, model, pathway, year, version):
        super().__init__(database, iam_data, model, pathway, year)
        self.version = version
        self.recycling_rates = get_steel_recycling_rates(year=self.year)

    def get_carbon_capture_energy_inputs(self, amount_CO2, loc, sector):
        """
        Returns the additional electricity and heat exchanges to add to the dataset
        associated with the carbon capture

        :param amount_CO2: inital amount of CO2 emitted
        :param loc: location of the steel production dataset
        :return: carbon capture rate, list of exchanges
        :rtype: float, list
        """

        rate = self.get_carbon_capture_rate(loc=loc, sector=sector)

        new_exchanges = list()

        if rate > 0:
            # Electricity: 0.024 kWh/kg CO2 for capture, 0.146 kWh/kg CO2 for compression
            carbon_capture_electricity = (amount_CO2 * rate) * (0.146 + 0.024)

            try:
                new_supplier = ws.get_one(
                    self.database,
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
                        self.database,
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
                        self.database,
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

                    possible_suppliers = get_shares_from_production_volume(
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
                            self.database,
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
                            self.database,
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
                        possible_suppliers = get_shares_from_production_volume(
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

        created_datasets = list()

        print("Create steel markets for different regions")

        for i in (
                ("market for steel, low-alloyed", "steel, low-alloyed"),
                ("market for steel, unalloyed", "steel, unalloyed"),
                ("market for steel, chromium steel 18/8", "steel, chromium steel 18/8"),
        ):
            steel_markets = self.fetch_proxies(
                name=i[0],
                ref_prod=i[1],
                production_variable=["primary steel", "secondary steel"],
                relink=False,
            )

            steel_markets = {r: d for r,d in steel_markets.items() if r!= "World"}

            # adjust share of primary and secondary steel
            if i[0] == "market for steel, low-alloyed":
                for loc, dataset in steel_markets.items():
                    primary_share = self.iam_data.production_volumes.sel(
                        region=loc, variables="primary steel"
                    ).interp(year=self.year).values.item(
                        0
                    ) / self.iam_data.production_volumes.sel(
                        region=loc, variables=["primary steel", "secondary steel"]
                    ).interp(
                        year=self.year
                    ).sum(
                        dim="variables"
                    ).values.item(
                        0
                    )

                    secondary_share = 1 - primary_share

                    new_exc = [
                        {
                            "uncertainty type": 0,
                            "loc": primary_share,
                            "amount": primary_share,
                            "type": "technosphere",
                            "production volume": 1,
                            "product": "steel, low-alloyed",
                            "name": "steel production, converter, low-alloyed",
                            "unit": "kilogram",
                            "location": loc,
                        },
                        {
                            "uncertainty type": 0,
                            "loc": secondary_share,
                            "amount": secondary_share,
                            "type": "technosphere",
                            "production volume": 1,
                            "product": "steel, low-alloyed",
                            "name": "steel production, electric, low-alloyed",
                            "unit": "kilogram",
                            "location": loc,
                        },
                    ]

                    dataset["exchanges"] = [
                        e for e in dataset["exchanges"] if e["type"] == "production"
                                                           or e["unit"] == "ton kilometer"
                    ]
                    dataset["exchanges"].extend(new_exc)

            else:
                for loc, dataset in steel_markets.items():
                    dataset["exchanges"] = [
                        e for e in dataset["exchanges"]
                        if e["type"] == "production" or
                           e["unit"] == "ton kilometer"
                    ]

                    name = i[0].replace("market for ", "").replace("steel,", "steel production,")
                    ref_prod = i[1]

                    dataset["exchanges"].append(
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "technosphere",
                            "production volume": 1,
                            "product": ref_prod,
                            "name": name,
                            "unit": "kilogram",
                            "location": loc,
                        }
                    )

            self.database.extend([v for v in steel_markets.values()])

            created_datasets.extend(
                [
                    (act["name"], act["reference product"], act["location"])
                    for act in steel_markets.values()
                ]
            )

            # Create global market for steel
            region = ws.get_one(
                self.database,
                ws.equals("name", i[0]),
                ws.contains("reference product", "steel"),
                ws.equals("location", "WEU" if self.model == "image" else "EUR"),
            )
            d = copy.deepcopy(region)
            d["location"] = "World"
            d["code"] = str(uuid.uuid4().hex)

            if "input" in d:
                d.pop("input")

            for prod in ws.production(d):
                prod["location"] = d["location"]

                if "input" in prod:
                    prod.pop("input")

            d["exchanges"] = [x for x in d["exchanges"] if x["type"] == "production"]

            regions = [r for r in self.regions if r != "World"]
            for region in regions:
                share = (
                        self.iam_data.production_volumes.sel(
                            variables=["primary steel", "secondary steel"], region=region
                        )
                        .interp(year=self.year)
                        .sum(dim="variables")
                        / self.iam_data.production_volumes.sel(
                    variables=["primary steel", "secondary steel"], region="World"
                )
                        .interp(year=self.year)
                        .sum(dim="variables")
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

            self.database.append(d)

            # Add created datasets to `self.list_datasets`
            self.list_datasets.append(
                (d["name"], d["reference product"], d["location"])
            )

        self.relink_datasets(
            excludes_datasets=[
                "market for steel, low-alloyed",
                "market for steel, unalloyed",
                "market for steel, chromium steel 18/8",
            ]
        )

        # Determine all steel activities in the database. Delete old datasets.
        print("Create new steel production datasets and delete old datasets")
        d_act_primary_steel = {
            mat: self.fetch_proxies(
                name=mat[0],
                ref_prod=mat[1],
                production_variable=["primary steel"],
                relink=True,
            )
            for mat in zip(
                self.material_map["steel, primary"],
                ["steel"] * len(self.material_map["steel, primary"]),
            )
        }
        d_act_secondary_steel = {
            mat: self.fetch_proxies(
                name=mat[0],
                ref_prod=mat[1],
                production_variable=["secondary steel"],
                relink=True,
            )
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

            for region in d_act_steel[steel]:

                # the correction factor applied to all fuel/electricity input is
                # equal to the ration fuel/output in the year in question
                # divided by the ratio fuel/output in 2020

                sector = (
                    "primary steel"
                    if "converter" in d_act_steel[steel][region]["name"]
                    else "secondary steel"
                )
                correction_factor = self.find_iam_efficiency_change(
                    variable=sector, location=d_act_steel[steel][region]["location"],
                )

                # update comments
                text = f"This dataset has been modified by `premise`, according to " \
                    f"the performance for steel production indicated by the IAM model {self.model.upper()} " \
                    f"for the IAM region {region} in {self.year}, following the scenario {self.scenario}. " \
                    f"Among other things, the energy efficiency of the process " \
                    f"has been improved by {int((correction_factor - 1) * 100)}%. "

                d_act_steel[steel][region]["comment"] = text + d_act_steel[steel][region]["comment"]

                for exc in ws.technosphere(
                    d_act_steel[steel][region],
                    ws.either(*[ws.contains("name", x) for x in list_fuels]),
                ):
                    if correction_factor != 0 and ~np.isnan(correction_factor):
                        if exc["amount"] == 0:
                            wurst.rescale_exchange(
                                exc, correction_factor / 1, remove_uncertainty=True
                            )
                        else:
                            wurst.rescale_exchange(exc, 1 / correction_factor)

                        exc[
                            "comment"
                        ] = "This exchange has been modified based on IAM projections " \
                            "for the steel sector by `premise`."

                for exc in ws.biosphere(
                    d_act_steel[steel][region],
                    ws.contains("name", "Carbon dioxide, fossil"),
                ):
                    if correction_factor != 0 and ~np.isnan(correction_factor):
                        if exc["amount"] == 0:
                            wurst.rescale_exchange(
                                exc, correction_factor / 1, remove_uncertainty=True
                            )
                        else:
                            wurst.rescale_exchange(exc, 1 / correction_factor)

                        exc[
                            "comment"
                        ] = "This exchange has been modified based on IAM projections " \
                            "for the steel sector by `premise`."

                    # Add carbon capture-related energy exchanges
                    # Carbon capture rate: share of capture of total CO2 emitted
                    # Note: only if variables exist in IAM data

                    (
                        carbon_capture_rate,
                        new_exchanges,
                    ) = self.get_carbon_capture_energy_inputs(
                        exc["amount"], region, sector=sector
                    )

                    if carbon_capture_rate > 0:
                        exc["amount"] *= 1 - carbon_capture_rate
                        d_act_steel[steel][region]["exchanges"].extend(new_exchanges)

                # Update hot pollutant emission according to GAINS
                self.update_pollutant_emissions(
                    dataset=d_act_steel[steel][region], sector="steel"
                )

            self.database.extend([v for v in d_act_steel[steel].values()])

        print("Relink new steel production datasets to steel-consuming activities")

        self.relink_datasets(
            excludes_datasets=[
                "steel production, electric, chromium steel 18/8",
                "steel production, electric, low-alloyed",
                "steel production, converter, unalloyed",
            ]
        )

        print("Done!")

        return self.database
