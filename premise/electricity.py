"""
electricity.py contains the class `Electricity`, which inherits from `BaseTransformation`.
This class transforms the electricity markets and power plants of the wurst database,
based on projections from the IAM scenario.
It also creates electricity markets which mix is an weighted-average
over a certain period (e.g., 10, 20 years).
It eventually re-links all the electricity-consuming activities of the wurst database to
the newly created electricity markets.

"""

import os
import wurst
from .transformation import *
from .activity_maps import InventorySet

PRODUCTION_PER_TECH = (
    DATA_DIR / "electricity" / "electricity_production_volumes_per_tech.csv"
)
LOSS_PER_COUNTRY = DATA_DIR / "electricity" / "losses_per_country.csv"


def get_losses_per_country_dict():
    """
    Create a dictionary with ISO country codes as keys and loss ratios as values.
    :return: ISO country code (keys) to loss ratio (values) dictionary
    :rtype: dict
    """

    if not LOSS_PER_COUNTRY.is_file():
        raise FileNotFoundError(
            "The production per country dictionary file could not be found."
        )

    with open(LOSS_PER_COUNTRY, encoding="utf-8") as file:
        csv_list = [[val.strip() for val in r.split(";")] for r in file.readlines()]

    (_, *header), *data = csv_list
    csv_dict = {}
    for row in data:
        key, *values = row
        csv_dict[key] = {key: float(value) for key, value in zip(header, values)}

    return csv_dict


def get_production_per_tech_dict():
    """
    Create a dictionary with tuples (technology, country) as keys and production volumes as values.
    :return: technology to production volume dictionary
    :rtype: dict
    """

    if not PRODUCTION_PER_TECH.is_file():
        raise FileNotFoundError(
            "The production per technology dictionary file could not be found."
        )
    csv_dict = {}
    with open(PRODUCTION_PER_TECH, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1])] = row[2]

    return csv_dict


class Electricity(BaseTransformation):
    """
    Class that modifies electricity markets in the database based on IAM output data.
    Inherits from `transformation.BaseTransformation`.

    :ivar database: wurst database, which is a list of dictionaries
    :vartype database: list
    :ivar iam_data: IAM data
    :vartype iam_data: xarray.DataArray
    :ivar model: name of the IAM model (e.g., "remind", "image")
    :vartype model: str
    :ivar pathway: name of the IAM pathway (e.g., "SSP2-Base")
    :vartype pathway: str
    :ivar year: year of the pathway (e.g., 2030)
    :vartype year: int

    """

    def __init__(self, database, iam_data, model, pathway, year):
        super().__init__(database, iam_data, model, pathway, year)
        mapping = InventorySet(self.database)
        self.powerplant_map = mapping.generate_powerplant_map()
        self.powerplant_fuels_map = mapping.generate_powerplant_fuels_map()
        self.losses = get_losses_per_country_dict()
        self.production_per_tech = get_production_per_tech_dict()

    def get_production_weighted_losses(self, voltage, region):
        """
        Return the transformation, transmission and distribution losses at a given voltage level for a given location.
        A weighted average is made of the locations contained in the IAM region.

        :param voltage: voltage level (high, medium or low)
        :type voltage: str
        :param region: IAM region
        :type region: str
        :return: tuple that contains transformation and distribution losses
        :rtype: tuple
        """

        # Fetch locations contained in IAM region
        locations = self.geo.iam_to_ecoinvent_location(region)

        if voltage == "high":

            cumul_prod, transf_loss = 0, 0
            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {"Transformation loss, high voltage": 0, "Production volume": 0},
                )

                transf_loss += (
                    dict_loss["Transformation loss, high voltage"]
                    * dict_loss["Production volume"]
                )
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            return transf_loss

        if voltage == "medium":

            cumul_prod, transf_loss, distr_loss = 0, 0, 0
            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {
                        "Transformation loss, medium voltage": 0,
                        "Transmission loss to medium voltage": 0,
                        "Production volume": 0,
                    },
                )
                transf_loss += (
                    dict_loss["Transformation loss, medium voltage"]
                    * dict_loss["Production volume"]
                )
                distr_loss += (
                    dict_loss["Transmission loss to medium voltage"]
                    * dict_loss["Production volume"]
                )
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            distr_loss /= cumul_prod
            return transf_loss, distr_loss

        if voltage == "low":

            cumul_prod, transf_loss, distr_loss = 0, 0, 0

            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {
                        "Transformation loss, low voltage": 0,
                        "Transmission loss to low voltage": 0,
                        "Production volume": 0,
                    },
                )
                transf_loss += (
                    dict_loss["Transformation loss, low voltage"]
                    * dict_loss["Production volume"]
                )
                distr_loss += (
                    dict_loss["Transmission loss to low voltage"]
                    * dict_loss["Production volume"]
                )
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            distr_loss /= cumul_prod
            return transf_loss, distr_loss

    def create_new_markets_low_voltage(self):
        """
        Create low voltage market groups for electricity, by receiving medium voltage market groups as input
        and adding transformation and distribution losses. Transformation and distribution losses are taken from ei37.
        Contribution from solar power is added here as well, as most is delivered at low voltage,
        although CSP delivers at high voltage.
        Does not return anything. Modifies the database in place.
        """

        # we keep a log of created markets
        log_created_markets = []

        # Loop through IAM regions
        for region in self.regions:

            # `period` is a period of time considered to create time-weighted average mix
            # when `period` == 0, this is a market mix for the year `self.year`
            # when `period` == 10, this is a market mix for the period `self.year` + 10
            # this is useful for systems that consume electricity
            # over a long period of time (e.g., buildings, BEVs, etc.)
            for period in range(0, 60, 10):

                mix = dict(
                    zip(
                        self.iam_data.electricity_markets.variables.values,
                        self.iam_data.electricity_markets.sel(region=region,)
                        .interp(
                            year=np.arange(self.year, self.year + period + 1),
                            kwargs={"fill_value": "extrapolate"},
                        )
                        .mean(dim="year")
                        .values,
                    )
                )

                # Create an empty dataset
                if period == 0:
                    new_dataset = {
                        "location": region,
                        "name": "market group for electricity, low voltage",
                        "reference product": "electricity, low voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario} for the year {self.year}.",
                    }
                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, low voltage",
                            "name": "market group for electricity, low voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                else:
                    new_dataset = {
                        "location": region,
                        "name": f"market group for electricity, low voltage, {period}-year period",
                        "reference product": "electricity, low voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario}. Average electricity mix over a {period}"
                        f"-year period {self.year}-{self.year + period}.",
                    }
                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, low voltage",
                            "name": f"market group for electricity, low voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                # Second, add an input of sulfur hexafluoride (SF6) emission to compensate the transformer's leakage
                # And an emission of a corresponding amount
                # Third, transmission line
                new_exchanges.extend(
                    [
                        {
                            "uncertainty type": 0,
                            "loc": 2.99e-9,
                            "amount": 2.99e-9,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "sulfur hexafluoride, liquid",
                            "name": "market for sulfur hexafluoride, liquid",
                            "unit": "kilogram",
                            "location": "RoW",
                        },
                        {
                            "uncertainty type": 0,
                            "loc": 2.99e-9,
                            "amount": 2.99e-9,
                            "type": "biosphere",
                            "input": (
                                "biosphere3",
                                "35d1dff5-b535-4628-9826-4a8fce08a1f2",
                            ),
                            "name": "Sulfur hexafluoride",
                            "unit": "kilogram",
                            "categories": ("air", "non-urban air or from high stacks"),
                        },
                        {
                            "uncertainty type": 0,
                            "loc": 8.74e-8,
                            "amount": 8.74e-8,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "distribution network, electricity, low voltage",
                            "name": "distribution network construction, electricity, low voltage",
                            "unit": "kilometer",
                            "location": "RoW",
                        },
                    ]
                )

                # Fourth, add the contribution of solar power
                solar_amount = 0

                for technology in (t for t in mix if "solar" in t.lower()):
                    # If the solar power technology contributes to the mix
                    if mix[technology] > 0:
                        # Fetch ecoinvent regions contained in the IAM region
                        ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

                        # Contribution in supply
                        amount = mix[technology]
                        solar_amount += amount

                        # Get the possible names of ecoinvent datasets
                        ecoinvent_technologies = self.powerplant_map[technology]

                        # Fetch electricity-producing technologies contained in the IAM region
                        # if they cannot be found for the ecoinvent locations concerned
                        # we widen the scope to EU-based datasets, and RoW
                        possible_locations = [ecoinvent_regions, ["RER"], ["RoW"]]
                        suppliers, counter = [], 0

                        while len(suppliers) == 0:
                            suppliers = list(
                                get_suppliers_of_a_region(
                                    database=self.database,
                                    locations=possible_locations[counter],
                                    names=ecoinvent_technologies,
                                    reference_product="electricity",
                                    unit="kilowatt hour",
                                )
                            )
                            counter += 1

                        suppliers = get_shares_from_production_volume(suppliers)

                        for supplier, share in suppliers.items():
                            new_exchanges.append(
                                {
                                    "uncertainty type": 0,
                                    "loc": (amount * share),
                                    "amount": (amount * share),
                                    "type": "technosphere",
                                    "production volume": 0,
                                    "product": supplier[2],
                                    "name": supplier[0],
                                    "unit": supplier[-1],
                                    "location": supplier[1],
                                }
                            )

                            if period == 0:
                                log_created_markets.append(
                                    [
                                        f"low voltage, {self.scenario}, {self.year}",
                                        "n/a",
                                        region,
                                        0,
                                        0,
                                        supplier[0],
                                        supplier[1],
                                        share,
                                        (share * amount),
                                    ]
                                )
                            else:
                                log_created_markets.append(
                                    [
                                        f"low voltage, {self.scenario}, {self.year}, {period}-year period",
                                        "n/a",
                                        region,
                                        0,
                                        0,
                                        supplier[0],
                                        supplier[1],
                                        share,
                                        (share * amount),
                                    ]
                                )

                # Fifth, add:
                # * an input from the medium voltage market minus solar contribution, including distribution loss
                # * an self-consuming input for transformation loss

                transf_loss, distr_loss = self.get_production_weighted_losses(
                    "low", region
                )

                if period == 0:
                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": (1 - solar_amount) * (1 + distr_loss),
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, low voltage",
                            "name": "market group for electricity, low voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    log_created_markets.append(
                        [
                            f"low voltage, {self.scenario}, {self.year}",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"low voltage, {self.scenario}, {self.year}",
                            region,
                            1,
                            (1 - solar_amount) * (1 + distr_loss),
                        ]
                    )

                else:
                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": (1 - solar_amount) * (1 + distr_loss),
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": f"market group for electricity, medium voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, low voltage",
                            "name": f"market group for electricity, low voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    log_created_markets.append(
                        [
                            f"low voltage, {self.scenario}, {self.year}, {period}-year period",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"low voltage, {self.scenario}, {self.year}",
                            region,
                            1,
                            (1 - solar_amount) * (1 + distr_loss),
                        ]
                    )

                new_dataset["exchanges"] = new_exchanges
                self.database.append(new_dataset)

        # update `self.list_datasets`
        self.list_datasets.extend(
            [
                (
                    "market group for electricity, low voltage", "electricity, low voltage", dataset[2]
                ) for dataset in log_created_markets
            ]
        )

        with open(
            DATA_DIR
            / f"logs/log created electricity markets {self.scenario} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in log_created_markets:
                writer.writerow(line)

    def create_new_markets_medium_voltage(self):
        """
        Create medium voltage market groups for electricity, by receiving high voltage market groups as inputs
        and adding transformation and distribution losses.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        log_created_markets = []

        for region in self.regions:

            # `period` is a period of time considered to create time-weighted average mix
            # when `period` == 0, this is a market mix for the year `self.year`
            # when `period` == 10, this is a market mix for the period `self.year` + 10
            # this is useful for systems that consume electricity
            # over a long period of time (e.g., buildings, BEVs, etc.)
            for period in range(0, 60, 10):

                # Create an empty dataset

                if period == 0:
                    # this dataset is for year = `self.year`
                    new_dataset = {
                        "location": region,
                        "name": "market group for electricity, medium voltage",
                        "reference product": "electricity, medium voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario} for the year {self.year}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                else:
                    # this dataset is for a period of time
                    new_dataset = {
                        "location": region,
                        "name": f"market group for electricity, medium voltage, {period}-year period",
                        "reference product": "electricity, medium voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario}. Average electricity mix over a {period}"
                        f"-year period {self.year}-{self.year + period}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": f"market group for electricity, medium voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                # Second, add:
                # * an input from the high voltage market, including transmission loss
                # * an self-consuming input for transformation loss

                transf_loss, distr_loss = self.get_production_weighted_losses(
                    "medium", region
                )

                if period == 0:
                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 1 + distr_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": "market group for electricity, high voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                else:

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 1 + distr_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": f"market group for electricity, high voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": f"market group for electricity, medium voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                # Third, add an input to of sulfur hexafluoride emission to compensate the transformer's leakage
                # And an emission of a corresponding amount
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 5.4e-8,
                        "amount": 5.4e-8,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "sulfur hexafluoride, liquid",
                        "name": "market for sulfur hexafluoride, liquid",
                        "unit": "kilogram",
                        "location": "RoW",
                    }
                )
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 5.4e-8,
                        "amount": 5.4e-8,
                        "type": "biosphere",
                        "input": ("biosphere3", "35d1dff5-b535-4628-9826-4a8fce08a1f2"),
                        "name": "Sulfur hexafluoride",
                        "unit": "kilogram",
                        "categories": ("air", "non-urban air or from high stacks"),
                    }
                )

                # Fourth, transmission line
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 1.8628e-8,
                        "amount": 1.8628e-8,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "transmission network, electricity, medium voltage",
                        "name": "transmission network construction, electricity, medium voltage",
                        "unit": "kilometer",
                        "location": "RoW",
                    }
                )

                new_dataset["exchanges"] = new_exchanges

                if period == 0:

                    log_created_markets.append(
                        [
                            f"medium voltage, {self.scenario}, {self.year}",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            "medium voltage, {self.scenario}, {self.year}",
                            region,
                            1,
                            1 + distr_loss,
                        ]
                    )

                else:

                    log_created_markets.append(
                        [
                            f"medium voltage, {self.scenario}, {self.year}, {period}-year period",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"medium voltage, {self.scenario}, {self.year}",
                            region,
                            1,
                            1 + distr_loss,
                        ]
                    )

                self.database.append(new_dataset)

        # update `self.list_datasets`
        self.list_datasets.extend(
            [
                (
                    "market group for electricity, medium voltage", "electricity, medium voltage", dataset[2]
                ) for dataset in log_created_markets
            ]
        )

        with open(
            DATA_DIR
            / f"logs/log created electricity markets {self.scenario} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in log_created_markets:
                writer.writerow(line)

    def create_new_markets_high_voltage(self):
        """
        Create high voltage market groups for electricity, based on electricity mixes given by the IAM scenario.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        log_created_markets = []

        for region in self.regions:

            for period in range(0, 60, 10):

                electriciy_mix = dict(
                    zip(
                        self.iam_data.electricity_markets.variables.values,
                        self.iam_data.electricity_markets.sel(region=region,)
                        .interp(
                            year=np.arange(self.year, self.year + period + 1),
                            kwargs={"fill_value": "extrapolate"},
                        )
                        .mean(dim="year")
                        .values,
                    )
                )

                # Fetch ecoinvent regions contained in the IAM region
                ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

                # Create an empty dataset
                if period == 0:
                    # this dataset is for one year
                    new_dataset = {
                        "location": region,
                        "name": "market group for electricity, high voltage",
                        "reference product": "electricity, high voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario} for the year {self.year}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": "market group for electricity, high voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                else:
                    # this dataset is for a period of time
                    new_dataset = {
                        "location": region,
                        "name": f"market group for electricity, high voltage, {period}-year period",
                        "reference product": "electricity, high voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.scenario}. Average electricity mix over a {period}"
                        f"-year period {self.year}-{self.year + period}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": f"market group for electricity, high voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                # Second, add transformation loss
                transf_loss = self.get_production_weighted_losses("high", region)
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 1,
                        "amount": transf_loss,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "electricity, high voltage",
                        "name": "market group for electricity, high voltage",
                        "unit": "kilowatt hour",
                        "location": region,
                    }
                )

                # Fetch solar contribution in the mix, to subtract it
                # as solar energy is an input of low-voltage markets

                solar_amount = 0
                for tech in electriciy_mix:
                    if "solar" in tech.lower():
                        solar_amount += electriciy_mix[tech]

                # Loop through the technologies
                technologies = (
                    tech for tech in electriciy_mix if "solar" not in tech.lower()
                )
                for technology in technologies:

                    # If the given technology contributes to the mix
                    if electriciy_mix[technology] > 0:

                        # Contribution in supply
                        amount = electriciy_mix[technology]

                        # Get the possible names of ecoinvent datasets
                        ecoinvent_technologies = self.powerplant_map[technology]

                        # Fetch electricity-producing technologies contained in the IAM region
                        # if they cannot be found for the ecoinvent locations concerned
                        # we widen the scope to EU-based datasets, and RoW
                        possible_locations = [ecoinvent_regions, ["RER"], ["RoW"]]
                        suppliers, counter = [], 0

                        while len(suppliers) == 0:
                            suppliers = list(
                                get_suppliers_of_a_region(
                                    database=self.database,
                                    locations=possible_locations[counter],
                                    names=ecoinvent_technologies,
                                    reference_product="electricity",
                                    unit="kilowatt hour",
                                )
                            )
                            counter += 1

                        suppliers = get_shares_from_production_volume(suppliers)

                        for supplier, share in suppliers.items():

                            new_exchanges.append(
                                {
                                    "uncertainty type": 0,
                                    "loc": (amount * share) / (1 - solar_amount),
                                    "amount": (amount * share) / (1 - solar_amount),
                                    "type": "technosphere",
                                    "production volume": 0,
                                    "product": supplier[2],
                                    "name": supplier[0],
                                    "unit": supplier[-1],
                                    "location": supplier[1],
                                }
                            )

                            if period == 0:

                                log_created_markets.append(
                                    [
                                        f"high voltage, {self.scenario}, {self.year}",
                                        technology,
                                        region,
                                        transf_loss,
                                        0.0,
                                        supplier[0],
                                        supplier[1],
                                        share,
                                        (amount * share) / (1 - solar_amount),
                                    ]
                                )
                            else:
                                log_created_markets.append(
                                    [
                                        f"high voltage, {self.scenario}, {self.year}, {period}-year period",
                                        technology,
                                        region,
                                        transf_loss,
                                        0.0,
                                        supplier[0],
                                        supplier[1],
                                        share,
                                        (amount * share) / (1 - solar_amount),
                                    ]
                                )

                new_dataset["exchanges"] = new_exchanges

                self.database.append(new_dataset)



        # update `self.list_datasets`
        self.list_datasets.extend(
            [
                (
                    "market group for electricity, high voltage", "electricity, high voltage", dataset[2]
                ) for dataset in log_created_markets
            ]
        )

        # Writing log of created markets
        with open(
            DATA_DIR
            / f"logs/log created electricity markets {self.scenario} {self.year}-{date.today()}.csv",
            "w",
            encoding="utf-8",
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
            for line in log_created_markets:
                writer.writerow(line)

    def update_electricity_efficiency(self):
        """
        This method modifies each ecoinvent coal, gas,
        oil and biomass dataset using data from the IAM scenario.
        Return a wurst database with modified datasets.

        :return: a wurst database, with rescaled electricity-producing datasets.
        :rtype: list
        """

        print("Adjust efficiency of power plants...")

        eff_labels = self.iam_data.efficiency.variables.values
        all_techs = self.iam_data.electricity_markets.variables.values

        technologies_map = self.get_iam_mapping(
            activity_map=self.powerplant_map,
            fuels_map=self.powerplant_fuels_map,
            technologies=list(set(eff_labels).intersection(all_techs)),
        )

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(
            DATA_DIR
            / f"logs/log power plant efficiencies change {self.model.upper()} {self.scenario} {self.year}-{date.today()}.csv",
            "w",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(
                ["dataset name", "location", "original efficiency", "new efficiency"]
            )

        print(f"Log of changes in power plants efficiencies saved in {DATA_DIR}/logs")

        # to store changes in efficiency
        eff_change_log = []

        for technology in technologies_map:
            dict_technology = technologies_map[technology]
            print("Rescale inventories and emissions for", technology)

            datasets = [
                d
                for d in self.database
                if d["name"] in dict_technology["technology filters"]
                and d["unit"] == "kilowatt hour"
            ]

            # no activities found? Check filters!
            assert len(datasets) > 0, f"No dataset found for {technology}"

            for dataset in datasets:

                # Find current efficiency
                ei_eff = dict_technology["current_eff_func"](
                    dataset, dict_technology["fuel filters"], 3.6
                )

                # Find relative efficiency change indicated by the IAM
                scaling_factor = 1 / dict_technology["IAM_eff_func"](
                    variable=technology,
                    location=self.geo.ecoinvent_to_iam_location(dataset["location"]),
                )

                new_efficiency = ei_eff * scaling_factor

                # we log changes in efficiency
                eff_change_log.append(
                    [dataset["name"], dataset["location"], ei_eff, new_efficiency]
                )

                self.update_ecoinvent_efficiency_parameter(
                    dataset, ei_eff, new_efficiency
                )

                # Rescale all the technosphere exchanges
                # according to the change in efficiency between `year` and 2020
                # from the IAM efficiency values
                wurst.change_exchanges_by_constant_factor(
                    dataset,
                    1 / float(scaling_factor),
                    [],
                    [ws.doesnt_contain_any("name", self.emissions_map)],
                )

                # Update biosphere exchanges according to GAINS emission values
                for exc in ws.biosphere(
                    dataset,
                    ws.either(*[ws.contains("name", x) for x in self.emissions_map]),
                ):
                    pollutant = self.emissions_map[exc["name"]]

                    scaling_factor = self.find_gains_emissions_change(
                        pollutant=pollutant,
                        sector=technology,
                        location=self.geo.iam_to_GAINS_region(
                            self.geo.ecoinvent_to_iam_location(dataset["location"])
                        ),
                    )

                    if exc["amount"] == 0:
                        wurst.rescale_exchange(
                            exc, scaling_factor / 1, remove_uncertainty=True
                        )
                    else:
                        wurst.rescale_exchange(exc, 1 / scaling_factor)

        with open(
            DATA_DIR
            / f"logs/log power plant efficiencies change {self.model.upper()} {self.scenario} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for row in eff_change_log:
                writer.writerow(row)

        print("Done!")

        return self.database

    def update_electricity_markets(self):
        """
        Delete electricity markets. Create high, medium and low voltage market groups for electricity.
        Link electricity-consuming datasets to newly created market groups for electricity.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for electricity
        :rtype: list
        """
        # We first need to delete 'market for electricity' and 'market group for electricity' datasets
        print("Remove old electricity datasets")
        list_to_remove = [
            "market group for electricity",
            "market for electricity",
            "electricity, high voltage, import",
            "electricity, high voltage, production mix",
        ]

        # Writing log of deleted markets
        # We want to preserve special markets for the cobalt industry
        # because if we delete cobalt industry-specific electricity markets
        # the carbon footprint of cobalt mining explodes (as it is currently mostly
        # mined in the RDC with 95% hydro-power)!
        markets_to_delete = [
            [i["name"], i["location"]]
            for i in self.database
            if any(item_to_remove in i["name"] for item_to_remove in list_to_remove)
            and not any(item_to_keep in i["reference product"] for item_to_keep in [
                "cobalt industry",
                "for reuse in municipal waste incineration"
            ])
        ]

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(
            DATA_DIR
            / f"logs/log deleted electricity markets {self.model.upper()} {self.scenario} {self.year}-{date.today()}.csv",
            "w",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(["dataset name", "location"])
            for line in markets_to_delete:
                writer.writerow(line)

        self.database = [
            i
            for i in self.database
            if not any(stop in i["name"] for stop in list_to_remove)
            or "cobalt industry" in i["reference product"]
        ]
        # update `self.list_datasets`
        self.list_datasets = get_tuples_from_database(self.database)

        # We then need to create high voltage IAM electricity markets
        print("Create high voltage markets.")
        self.create_new_markets_high_voltage()
        print("Create medium voltage markets.")
        self.create_new_markets_medium_voltage()
        print("Create low voltage markets.")
        self.create_new_markets_low_voltage()

        # Finally, we need to relink all electricity-consuming activities to the new electricity markets
        print("Link activities to new electricity markets.")

        self.relink_datasets(excludes_datasets=["cobalt industry", "market group for electricity"],
                             alternative_names=[
                                 "market group for electricity, high voltage",
                                 "market group for electricity, medium voltage",
                                 "market group for electricity, low voltage"
                             ]
        )

        print(f"Log of deleted electricity markets saved in {DATA_DIR}/logs")
        print(f"Log of created electricity markets saved in {DATA_DIR}/logs")

        print("Done!")

        return self.database
