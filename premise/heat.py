import os
from . import DATA_DIR
from .activity_maps import InventorySet
from .geomap import Geomap
from wurst import searching as ws
import csv
import numpy as np
import uuid
import wurst
from .utils import get_lower_heating_values
from datetime import date


class Heat:
    """
    Class that modifies heat markets in ecoinvent based on IAM output data.

    :ivar db: database dictionary from :attr:`.NewDatabase.db`
    :vartype db: dict
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :vartype model: str
    :ivar pathway: IAM pathway identifier
    :vartype pathway: str
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :vartype iam_data: xarray.DataArray
    :ivar year: year, from :attr:`.NewDatabase.year`
    :vartype year: int
    
    """
    
    def __init__(self, db, model, pathway, iam_data, year):
        self.db = db
        self.iam_data = iam_data
        self.model = model
        self.geo = Geomap(model=model)
        self.pathway = pathway
        self.year = year
        self.fuels_lhv = get_lower_heating_values()
        mapping = InventorySet(self.db)
        self.emissions_map = mapping.get_remind_to_ecoinvent_emissions()
        self.heatplant_map = mapping.generate_heatplant_map()
        self.fuel_map = mapping.generate_fuel_map()
        self.material_map = mapping.generate_material_map()
        self.heatplant_fuels_map = mapping.generate_heatplant_fuels_map()

    def get_suppliers_of_a_region(self, ecoinvent_regions, ecoinvent_technologies):
        """
        Return a list of heat-producing datasets, for which the location and name correspond to the region and name given,
        respectively.

        :param ecoinvent_regions: an ecoinvent region
        :type ecoinvent_regions: list
        :param ecoinvent_technologies: name of ecoinvent dataset
        :type ecoinvent_technologies: str
        :return: list of wurst datasets
        :rtype: list
        """

        return ws.get_many(
            self.db,
            *[
                ws.either(
                    *[
                        ws.equals("name", supplier)
                        for supplier in ecoinvent_technologies
                    ]
                ),
                ws.either(*[ws.equals("location", loc) for loc in ecoinvent_regions]),
                ws.equals("unit", "megajoule"),
            ]
        )
            
    def create_new_heat_markets(self):
        """
        Create market groups for heat, based on heat mixes given by the IAM.
        Does not return anything. Modifies the database in place.
        """
        # Loop through IAM regions
        gen_region = (
            region for region in self.iam_data.heat_markets.coords["region"].values
        )
        gen_tech = [
            tech for tech in self.iam_data.heat_markets.coords["variables"].values
        ]

        created_markets = []
        for region in gen_region:

            # Fetch ecoinvent regions contained in the IAM region
            ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

            # Create an empty dataset
            new_dataset = {
                "location": region,
                "name": ("market group for heat"),
                "reference product": "heat",
                "unit": "kilowatt hour",
                "database": self.db[1]["database"],
                "code": str(uuid.uuid4().hex),
                "comment": "Dataset produced from IAM scenario output results",
            }

            new_exchanges = [
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": 1,
                    "type": "production",
                    "production volume": 0,
                    "product": "heat",
                    "name": "market group for heat",
                    "unit": "kilowatt hour",
                    "location": region,
                }
            ]

            # Loop through the IAM technologies
            for technology in gen_tech:

                # If the given technology contributes to the mix
                if self.iam_data.heat_markets.loc[region, technology] != 0.0:

                    # Contribution in supply
                    amount = self.iam_data.heat_markets.loc[region, technology].values

                    # Get the possible names of ecoinvent datasets
                    ecoinvent_technologies = self.heatplant_map[
                        self.iam_data.rev_heat_market_labels[technology]
                    ]
                    
                    # Fetch heat-producing technologies contained in the IAM region
                    suppliers = list(
                        self.get_suppliers_of_a_region(
                            ecoinvent_regions, ecoinvent_technologies
                        )
                    )

                    # If no technology is available for the IAM region
                    if len(suppliers) == 0:
                        # We fetch European technologies instead
                        suppliers = list(
                            self.get_suppliers_of_a_region(
                                ["RER"], ecoinvent_technologies
                            )
                        )

                    # If, after looking for European technologies, no technology is available
                    if len(suppliers) == 0:
                        # We fetch RoW technologies instead
                        suppliers = list(
                            self.get_suppliers_of_a_region(
                                ["RoW"], ecoinvent_technologies
                            )
                        )

                    if len(suppliers) == 0:
                        print(
                            "no suppliers for {} in {} with ecoinvent names {}".format(
                                technology, region, ecoinvent_technologies
                            )
                        )

                    for supplier in suppliers:
                        share = 1 / len(suppliers)

                        new_exchanges.append(
                            {
                                "uncertainty type": 0,
                                "loc": float(amount * share),
                                "amount": float(amount),
                                "type": "technosphere",
                                "production volume": 0,
                                "product": supplier["reference product"],
                                "name": supplier["name"],
                                "unit": supplier["unit"],
                                "location": supplier["location"],
                            }
                        )

                        created_markets.append(
                            [
                                "market for heat, "
                                + self.pathway
                                + ", "
                                + str(self.year),
                                technology,
                                region,
                                0.0,
                                0.0,
                                supplier["name"],
                                supplier["location"],
                                share,
                                float(amount * share),
                            ]
                        )
            new_dataset["exchanges"] = new_exchanges

            self.db.append(new_dataset)

        # Writing log of created markets

        with open(
            DATA_DIR
            / "logs/log created markets {} {}-{}.csv".format(
                self.pathway, self.year, date.today()
            ),
            "w",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(
                [
                    "dataset name",
                    "energy type",
                    "IAM location",
                    "Transformation loss",
                    "Distr./Transmission loss",
                    "Supplier name",
                    "Supplier location",
                    "Contribution within energy type",
                    "Final contribution",
                ]
            )
            for line in created_markets:
                writer.writerow(line)

    def relink_activities_to_new_markets(self):
        """
        Links heat input exchanges to new datasets with the appropriate IAM location:
        * "market for heat" --> "market group for heat"
        Does not return anything.
        """

        # Filter all activities that consume heat
        for ds in ws.get_many(
            self.db, ws.exclude(ws.contains("name", "market group for heat"))
        ):

            for exc in ws.get_many(
                ds["exchanges"],
                *[
                    ws.either(
                        *[
                            ws.contains("name", "market for heat,"),
                            ws.contains("name", "generic market for heat"),
                        ]
                    )
                ]
            ):
                if exc["type"] != "production" and exc["unit"] == "megajoule":
                    if 1: ### eventually, we could distinguish between small-scale and large-scale heat production
                        exc["name"] = "market group for heat"
                        exc["product"] = "heat"
                        exc["location"] = self.geo.ecoinvent_to_iam_location(
                            exc["location"]
                        )
                if "input" in exc:
                    exc.pop("input")
                    
                    
    def find_ecoinvent_fuel_efficiency(self, ds, fuel_filters):
        """
        This method calculates the efficiency value set initially, in case it is not specified in the parameter
        field of the dataset. In Carma datasets, fuel inputs are expressed in megajoules instead of kilograms.

        :param ds: a wurst dataset of an electricity-producing technology
        :param fuel_filters: wurst filter to to filter fule input exchanges
        :return: the efficiency value set by ecoinvent
        """

        def calculate_input_energy(fuel_name, fuel_amount, fuel_unit):

            if fuel_unit == "kilogram" or fuel_unit == "cubic meter":

                lhv = [
                    self.fuels_lhv[k] for k in self.fuels_lhv if k in fuel_name.lower()
                ][0]
                return float(lhv) * fuel_amount / 3.6

            if fuel_unit == "megajoule":
                return fuel_amount / 3.6

            if fuel_unit == "kilowatt hour":
                return fuel_amount

        only_allowed = ["thermal"]
        key = list()
        ### Check ds - can we only see "thermal efficiency"?
        if "parameters" in ds:
            key = list(
                key
                for key in ds["parameters"]
                if "efficiency" in key and any(item in key for item in only_allowed)
            )
        if len(key) > 0:
            return ds["parameters"][key[0]]

        else:
            try:
                energy_input = np.sum(
                    np.sum(
                        np.asarray(
                            [
                                calculate_input_energy(
                                    exc["name"], exc["amount"], exc["unit"]
                                )
                                for exc in ds["exchanges"] if exc["name"] in fuel_filters
                            ]
                        )

                    )
                )
            except:
                test = [exc for exc in ds["exchanges"] if exc["name"] in fuel_filters]
                import pdb; pdb.set_trace()

            current_efficiency = (
                float(ws.reference_product(ds)["amount"]) / energy_input
            )

            if "paramters" in ds:
                ds["parameters"]["efficiency"] = current_efficiency
            else:
                ds["parameters"] = {"efficiency": current_efficiency}
            return current_efficiency
            
            
    def find_fuel_efficiency_scaling_factor(self, ds, fuel_filters, technology):
        """
        This method calculates a scaling factor to change the process efficiency set by ecoinvent
        to the efficiency given by the IAM.

        :param ds: wurst dataset of an electricity-producing technology
        :param fuel_filters: wurst filter to filter the fuel input exchanges
        :param technology: label of an electricity-producing technology
        :return: a rescale factor to change from ecoinvent efficiency to the efficiency given by the IAM
        :rtype: float
        """

        ecoinvent_eff = self.find_ecoinvent_fuel_efficiency(ds, fuel_filters)
        ### Check
        # If the current efficiency is too high, there's an issue, and the dataset is skipped.
        if ecoinvent_eff > 1.1:
            print(
                "The current efficiency factor for the dataset {} has not been found."
                "Its current efficiency will remain".format(
                    ds["name"]
                )
            )
            return 1

        # If the current efficiency is precisely 1, it is because it is not the actual heat generation dataset
        # but an additional layer.
        if ecoinvent_eff == 1:
            return 1

        iam_locations = self.geo.ecoinvent_to_iam_location(ds["location"])
        iam_eff = (
            self.iam_data.electricity_efficiencies.loc[
                dict(
                    variables=self.iam_data.electricity_efficiency_labels[technology],
                    region=iam_locations,
                )
            ]
            .mean()
            .values
        )

        # Sometimes, the efficiency factor is set to 1, when no value is available
        # Therefore, we should ignore that
        if iam_eff == 1:
            return 1

        # Sometimes, the efficiency factor from the IAM is not defined
        # Hence, we filter for "nan" and return a scaling factor of 1.
        if np.isnan(iam_eff):
            return 1

        with open(
            DATA_DIR
            / "logs/log power plant efficiencies change {} {} {}-{}.csv".format(
                self.model, self.pathway, self.year, date.today()
            ),
            "a",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")

            writer.writerow([ds["name"], ds["location"], ecoinvent_eff, iam_eff])
        return ecoinvent_eff / iam_eff

    @staticmethod
    def update_ecoinvent_efficiency_parameter(ds, scaling_factor):
        """
        Update the old efficiency value in the ecoinvent dataset by the newly calculated one.
        :param ds: dataset
        :type ds: dict
        :param scaling_factor: scaling factor (new efficiency / old efficiency)
        :type scaling_factor: float
        """
        parameters = ds["parameters"]
        ### Edit possibles
        possibles = ["efficiency", "efficiency_oil_country", "efficiency_electrical"]

        for key in possibles:
            if key in parameters:
                ds["parameters"][key] /= scaling_factor

    def get_remind_mapping(self):
        """
        Define filter functions that decide which wurst datasets to modify.
        :return: dictionary that contains filters and functions
        :rtype: dict
        """

        return {
            tech: {
                "eff_func": self.find_fuel_efficiency_scaling_factor,
                "technology filters": self.heatplant_map[tech],
                "fuel filters": self.heatplant_fuels_map[tech],
            }
            for tech in self.iam_data.heat_efficiency_labels.keys()
        }
        
    def update_heat_efficiency(self):
        """
        This method modifies each ecoinvent coal, gas,
        geothermal and biomass dataset using data from the REMIND model.
        Return a wurst database with modified datasets.

        :return: a wurst database, with rescaled heat-producing datasets.
        :rtype: list
        """

        technologies_map = self.get_remind_mapping()

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(
            DATA_DIR
            / "logs/log efficiencies change {} {}-{}.csv".format(
                self.pathway, self.year, date.today()
            ),
            "w",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(
                ["dataset name", "location", "original efficiency", "new efficiency"]
            )

        print(
            "Log of changes in heat plants efficiencies saved in {}".format(
                DATA_DIR / "logs"
            )
        )

        for iam_technology in technologies_map:
            dict_technology = technologies_map[iam_technology]
            print("Rescale inventories and emissions for", iam_technology)

            datasets = [d for d in self.db
                        if d["name"] in dict_technology["technology filters"]
                        and d["unit"] == "megajoule"
                        ]

            # no activities found? Check filters!
            assert len(datasets) > 0, "No dataset found for {}".format(iam_technology)
            for ds in datasets:
                # Modify using IAM efficiency values:
                scaling_factor = dict_technology["eff_func"](
                    ds, dict_technology["fuel filters"], iam_technology
                )
                self.update_ecoinvent_efficiency_parameter(ds, scaling_factor)
                
                # Rescale all the technosphere exchanges according to IAM efficiency values
                wurst.change_exchanges_by_constant_factor(
                    ds,
                    float(scaling_factor),
                    [],
                    [ws.doesnt_contain_any("name", self.emissions_map)],
                )

                # Update biosphere exchanges according to GAINS emission values
                ### to edit
                for exc in ws.biosphere(
                    ds, ws.either(*[ws.contains("name", x) for x in self.emissions_map])
                ):
                    iam_emission_label = self.emissions_map[exc["name"]]
                    
                    iam_emission = self.iam_data.heat_emissions.loc[
                        dict(
                            region=self.geo.ecoinvent_to_iam_location(
                                ds["location"]
                            ),
                            pollutant=iam_emission_label,
                            sector=self.iam_data.heat_emission_labels[
                                iam_technology
                            ],
                        )
                    ].values.item(0)

                    if exc["amount"] == 0:
                        wurst.rescale_exchange(
                            exc, iam_emission / 1, remove_uncertainty=True
                        )
                    else:
                        wurst.rescale_exchange(exc, iam_emission / exc["amount"])

        return self.db

    def update_heat_markets(self):
        """
        Delete existing heat markets. Create new markets for heat.
        Link heat-consuming datasets to newly created market groups for heat.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for heat
        :rtype: list
        """
        # We first need to delete 'market for heat' and 'market group for heat' datasets
        print("Remove old markets datasets")
        list_to_remove = [
            "market for heat,",
            "generic market for heat",
        ]

        # Writing log of deleted markets
        markets_to_delete = [
            [i["name"], i["location"]]
            for i in self.db
            if any(stop in i["name"] for stop in list_to_remove)
        ]

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(
            DATA_DIR
            / "logs/log deleted markets {} {}-{}.csv".format(
                self.pathway, self.year, date.today()
            ),
            "w",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(["dataset name", "location"])
            for line in markets_to_delete:
                writer.writerow(line)

        self.db = [
            i for i in self.db if not any(stop in i["name"] for stop in list_to_remove)
        ]

        # We then need to create heat markets
        print("Create heat markets.")
        self.create_new_heat_markets()

        # Finally, we need to relink all heat-consuming activities to the new heat markets
        print("Link activities to new heat markets.")
        self.relink_activities_to_new_markets()

        print(
            "Log of deleted heat markets saved in {}".format(DATA_DIR / "logs")
        )
        print(
            "Log of created heat markets saved in {}".format(DATA_DIR / "logs")
        )

        return self.db