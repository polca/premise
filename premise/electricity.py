"""
electricity.py contains the class `Electricity`, which inherits from `BaseTransformation`.
This class transforms the electricity markets and power plants of the wurst database,
based on projections from the IAM pathway.
It also creates electricity markets which mix is an weighted-average
over a certain period (e.g., 10, 20 years).
It eventually re-links all the electricity-consuming activities of the wurst database to
the newly created electricity markets.

"""

import os
from collections import defaultdict

import wurst

from .activity_maps import get_gains_to_ecoinvent_emissions
from .transformation import *
from .utils import c

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

    def __init__(self, database, iam_data, scenarios):
        super().__init__(database, iam_data, scenarios)
        mapping = InventorySet(self.database)
        self.powerplant_map = mapping.generate_powerplant_map()
        self.powerplant_fuels_map = mapping.generate_powerplant_fuels_map()
        self.losses = get_losses_per_country_dict()
        self.production_per_tech = get_production_per_tech_dict()
        self.gains_substances = get_gains_to_ecoinvent_emissions()

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
                        self.iam_data.electricity_markets.sel(
                            region=region,
                        )
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
                        f" using the pathway {self.pathway} for the year {self.year}.",
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
                        f" using the pathway {self.pathway}. Average electricity mix over a {period}"
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
                                        f"low voltage, {self.pathway}, {self.year}",
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
                                        f"low voltage, {self.pathway}, {self.year}, {period}-year period",
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
                            f"low voltage, {self.pathway}, {self.year}",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"low voltage, {self.pathway}, {self.year}",
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
                            f"low voltage, {self.pathway}, {self.year}, {period}-year period",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"low voltage, {self.pathway}, {self.year}",
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
                    "market group for electricity, low voltage",
                    "electricity, low voltage",
                    dataset[2],
                )
                for dataset in log_created_markets
            ]
        )

        with open(
            DATA_DIR
            / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
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
                        f" using the pathway {self.pathway} for the year {self.year}.",
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
                        f" using the pathway {self.pathway}. Average electricity mix over a {period}"
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
                            f"medium voltage, {self.pathway}, {self.year}",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            "medium voltage, {self.pathway}, {self.year}",
                            region,
                            1,
                            1 + distr_loss,
                        ]
                    )

                else:

                    log_created_markets.append(
                        [
                            f"medium voltage, {self.pathway}, {self.year}, {period}-year period",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"medium voltage, {self.pathway}, {self.year}",
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
                    "market group for electricity, medium voltage",
                    "electricity, medium voltage",
                    dataset[2],
                )
                for dataset in log_created_markets
            ]
        )

        with open(
            DATA_DIR
            / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in log_created_markets:
                writer.writerow(line)

    def create_new_markets_high_voltage(self):
        """
        Create high voltage market groups for electricity, based on electricity mixes given by the IAM pathway.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        log_created_markets = []

        for s_pos, scenario in enumerate(self.scenario_labels):
            for region in self.regions:
                model, pathway, year = scenario.split("::")
                for period in range(0, 60, 10):
                    electriciy_mix = dict(
                        zip(
                            self.iam_data.electricity_markets.variables.values,
                            self.iam_data.electricity_markets.sel(
                                region=region, scenario=scenario
                            )
                            .interp(
                                year=np.arange(self.year, self.year + period + 1),
                                kwargs={"fill_value": "extrapolate"},
                            )
                            .mean(dim="year")
                            .values,
                        )
                    )

                    # Fetch ecoinvent regions contained in the IAM region
                    ecoinvent_locations = self.iam_to_ecoinvent_loc[scenario][region]

                    new_exc = []

                    # Create an empty dataset
                    # this dataset is for one year
                    producer = [
                        "market group for electricity, high voltage",
                        "electricity, high voltage",
                        region,
                    ]
                    consumer = [
                        "market group for electricity, high voltage",
                        "electricity, high voltage",
                        region,
                    ]
                    comment = (
                        f"New regional electricity market created by `premise`, for the region"
                        f" {region} in {year}, according to the scenario {scenario}."
                    )

                    if period != 0:
                        producer[0] += f", {period}-year period"
                        comment += f"Average electricity mix over a {period}-year period {self.year}-{self.year + period}."

                    prod_key = create_hash(producer)
                    cons_key = create_hash(consumer)
                    exc_key = create_hash(producer + consumer)

                    prod_vol = (
                        self.iam_data.production_volumes.sel(
                            scenario=scenario,
                            region=region,
                            variables=self.iam_data.electricity_markets.variables.values,
                        )
                        .interp(year=int(scenario.split("::")[-1]))
                        .sum()
                        .values.item(0)
                    )

                    exchanges = [np.nan, np.nan, np.nan, ""] * len(self.scenario_labels)
                    pos = [c[0] for c in self.database.columns].index(scenario)

                    (
                        exchanges[pos],
                        exchanges[pos + 1],
                        exchanges[pos + 2],
                        exchanges[pos + 3],
                    ) = (
                        prod_vol,
                        1,
                        np.nan,
                        comment,
                    )

                    new_exc.append(
                        producer + consumer + [prod_key, cons_key, exc_key] + exchanges
                    )

                    # Second, add transformation loss
                    transf_loss = self.get_production_weighted_losses("high", region)
                    (
                        exchanges[pos],
                        exchanges[pos + 1],
                        exchanges[pos + 2],
                        exchanges[pos + 3],
                    ) = (
                        np.nan,
                        transf_loss,
                        np.nan,
                        "",
                    )
                    new_exc.append(
                        producer + consumer + [prod_key, cons_key, exc_key] + exchanges
                    )

                    # Fetch solar contribution in the mix, to subtract it
                    # as solar energy is an input of low-voltage markets

                    solar_amount = 0
                    for tech in electriciy_mix:
                        if "residential" in tech.lower():
                            solar_amount += electriciy_mix[tech]

                    # Loop through the technologies
                    technologies = (
                        tech
                        for tech in electriciy_mix
                        if "residential" not in tech.lower()
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
                            possible_locations = [
                                ecoinvent_locations,
                                ["RER"],
                                ["RoW"],
                                ["GLO"],
                            ]
                            suppliers, counter = [], 0

                            while len(suppliers) == 0:
                                _filters = (
                                    contains_any_from_list(
                                        (s.exchange, c.cons_loc),
                                        possible_locations[counter],
                                    )
                                    & contains_any_from_list(
                                        (s.exchange, c.cons_name),
                                        ecoinvent_technologies,
                                    )
                                    & contains((s.exchange, c.cons_prod), "electricity")
                                    & equals((s.exchange, c.unit), "kilowatt hour")
                                    & equals((s.exchange, c.type), "production")
                                )
                                suppliers = self.database(_filters(self.database))
                                counter += 1

                            total_production_vol = suppliers[
                                (s.ecoinvent, c.cons_prod_vol)
                            ].sum(axis=0)

                            for _, row in suppliers.iterrows():
                                producer = [
                                    row[(s.exchange, c.cons_name)],
                                    row[(s.exchange, c.cons_prod)],
                                    row[(s.exchange, c.cons_loc)],
                                ]
                                prod_key = create_hash(producer)
                                exc_key = create_hash(producer + consumer)
                                prod_vol = row[(s.ecoinvent, c.cons_prod_vol)]
                                share = prod_vol / total_production_vol
                                exc_amount = (amount * share) / (1 - solar_amount)
                                (
                                    exchanges[pos],
                                    exchanges[pos + 1],
                                    exchanges[pos + 2],
                                    exchanges[pos + 3],
                                ) = (
                                    np.nan,
                                    exc_amount,
                                    np.nan,
                                    "",
                                )

                                new_exc.append(
                                    producer
                                    + consumer
                                    + [prod_key, cons_key, exc_key]
                                    + exchanges
                                )

                                log_created_markets.append(
                                    [
                                        consumer[0],
                                        technology,
                                        region,
                                        transf_loss,
                                        0.0,
                                        producer[0],
                                        producer[1],
                                        share,
                                        (amount * share) / (1 - solar_amount),
                                    ]
                                )

                    self.exchange_stack.append(new_exc)

            # Writing log of created markets
            with open(
                DATA_DIR
                / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
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
        oil and biomass dataset using data from the IAM pathway.
        Return a wurst database with modified datasets.

        :return: a wurst database, with rescaled electricity-producing datasets.
        :rtype: list
        """

        print("Adjust efficiency of power plants...")

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        for scenario in self.scenario_labels:
            model, pathway, year = scenario.split("::")
            year = int(year)

            with open(
                DATA_DIR
                / f"logs/log power plant efficiencies change {model} {pathway} {year}-{date.today()}.csv",
                "w",
                encoding="utf-8",
            ) as csv_file:
                writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
                writer.writerow(
                    [
                        "dataset name",
                        "location",
                        "original efficiency",
                        "new efficiency",
                    ]
                )

            print(
                f"Log of changes in power plants efficiencies saved in {DATA_DIR}/logs"
            )

        all_techs = [
            tech
            for tech in self.iam_data.efficiency.variables.values
            if tech in self.iam_data.electricity_markets.variables.values
        ]

        technologies_map = self.get_iam_mapping(
            activity_map=self.powerplant_map,
            technologies=all_techs,
        )

        list_subsets = []

        for technology in technologies_map:
            # scenarios
            dict_technology = technologies_map[technology]
            print("Rescale inventories and emissions for", technology)

            _filters = contains_any_from_list(
                (s.exchange, c.cons_name),
                list(dict_technology["technology filters"]),
            )

            subset = self.database[_filters(self.database)]
            self.database = self.database[~_filters(self.database)]

            for s_, scenario in enumerate(self.scenario_labels):
                model, pathway, year = scenario.split("::")
                year = int(year)

                if self.scenarios[s_].get(
                    "exclude"
                ) is None or "update_electricity" not in self.scenarios[s_].get(
                    "exclude"
                ):

                    # to store changes in efficiency
                    eff_change_log = []

                    # if tech in scenario
                    if (
                        self.iam_data.efficiency.sel(
                            variables=technology, scenario=scenario
                        ).sum()
                        > 0
                    ):

                        datasets_locs = subset.loc[:, (s.exchange, c.cons_loc)].unique()
                        locs_map = defaultdict(list)

                        for loc in datasets_locs:
                            if self.ecoinvent_to_iam_loc[scenario][loc] in locs_map:
                                locs_map[
                                    self.ecoinvent_to_iam_loc[scenario][loc]
                                ].append(loc)
                            else:
                                locs_map[self.ecoinvent_to_iam_loc[scenario][loc]] = [
                                    loc
                                ]

                        # no activities found? Check filters!
                        assert (
                            len(datasets_locs) > 0
                        ), f"No dataset found for {technology}"

                        for loc in locs_map:

                            # Find relative efficiency change indicated by the IAM
                            scaling_factor = 1 / dict_technology["IAM_eff_func"](
                                variable=technology,
                                location=loc,
                                year=year,
                                scenario=scenario,
                            )

                            assert not np.isnan(scaling_factor), (
                                f"Scaling factor in {loc} for {technology} "
                                f"in scenario {scenario} in {year} is NaN."
                            )

                            __filters = contains_any_from_list(
                                (s.exchange, c.cons_loc), locs_map[loc]
                            )

                            # we log changes in efficiency
                            __filters_prod = __filters & equals(
                                (s.exchange, c.type), "production"
                            )

                            new_eff_list = []
                            new_comment_list = []

                            for _, row in subset.loc[__filters_prod(subset)].iterrows():

                                ei_eff = row[(s.ecoinvent, c.efficiency)]

                                if np.isnan(ei_eff):
                                    continue

                                new_eff = ei_eff * 1 / scaling_factor

                                # log change in efficiency
                                eff_change_log.append(
                                    [
                                        row[(s.exchange, c.cons_name)],
                                        row[(s.exchange, c.cons_loc)],
                                        ei_eff,
                                        new_eff,
                                    ]
                                )

                                # generate text for `comment` field
                                new_text = row[
                                    (s.ecoinvent, c.comment)
                                ] + self.update_new_efficiency_in_comment(
                                    scenario, loc, ei_eff, new_eff
                                )

                                new_eff_list.append(new_eff)
                                new_comment_list.append(new_text)

                            if len(new_eff_list) > 0:
                                subset.loc[
                                    __filters_prod(subset), (scenario, c.efficiency)
                                ] = new_eff_list
                                subset.loc[
                                    __filters_prod(subset), (scenario, c.comment)
                                ] = new_comment_list

                            # Rescale all the technosphere exchanges
                            # according to the change in efficiency
                            # between `year` and 2020
                            # from the IAM efficiency values

                            # filter out the production exchanges
                            # we update technosphere exchanges
                            # as well as emissions except those
                            # covered by GAINS

                            __filters_tech = (
                                __filters
                                & does_not_contain((s.exchange, c.type), "production")
                                & ~contains_any_from_list(
                                    (s.exchange, c.prod_name),
                                    list(self.gains_substances.keys()),
                                )
                            )

                            subset.loc[__filters_tech(subset), (scenario, c.amount)] = (
                                subset.loc[
                                    __filters_tech(subset), (s.ecoinvent, c.amount)
                                ]
                                * scaling_factor
                            )

                            if technology in self.iam_data.emissions.sector:
                                for ei_sub, gains_sub in self.gains_substances.items():

                                    scaling_factor = (
                                        1
                                        / self.find_gains_emissions_change(
                                            pollutant=gains_sub,
                                            sector=technology,
                                            location=self.iam_to_gains[scenario][loc],
                                        )
                                    )

                                    __filters_bio = __filters & equals(
                                        (s.exchange, c.prod_name), ei_sub
                                    )

                                    # update location in the (scenario, c.cons_loc) column
                                    subset.loc[
                                        __filters_bio(subset), (scenario, c.amount)
                                    ] = (
                                        subset.loc[
                                            __filters_bio(subset),
                                            (s.ecoinvent, c.amount),
                                        ]
                                        * scaling_factor
                                    )

                    with open(
                        DATA_DIR
                        / f"logs/log power plant efficiencies change {model.upper()} {pathway} {year}-{date.today()}.csv",
                        "a",
                        encoding="utf-8",
                    ) as csv_file:
                        writer = csv.writer(
                            csv_file, delimiter=";", lineterminator="\n"
                        )
                        for row in eff_change_log:
                            writer.writerow(row)

            # list_subsets.append(subset)

            self.database = pd.concat([self.database, subset])

        print("Done!")

        return self.database

    def create_region_specific_power_plants(self):
        """
        Some power plant inventories are not native to ecoinvent
        but imported. However, they are defined for a specific location
        (mostly European), but are used in many electricity markets
        (non-European). Hence, we create region-specific versions of these datasets,
        to align inputs providers with the geographical scope of the region.
        """

        techs = [
            "Wood chips, burned in power plant",
            #"Natural gas, in ATR ",
            #"100% SNG, burned",
            #"Hard coal, burned",
            #"Lignite, burned",
            #"CO2 storage/",
            #"CO2 capture/",
            #"Biomass CHP CCS",
            #"Biomass ST",
            #"Biomass IGCC CCS",
            #"Biomass IGCC",
            #"Coal IGCC",
            #"Coal PC CCS",
            #"Coal CHP CCS",
            #"Coal IGCC CCS",
            #"Gas CHP CCS",
            #"Gas CC CCS",
            #"Oil CC CCS",
        ]

        for tech in techs:

            __filter_prod = (
                contains((s.exchange, c.cons_name), tech)
                & equals((s.exchange, c.type), "production")
            )(self.database)

            for _, ds in self.database[__filter_prod].iterrows():

                new_plants = self.fetch_proxies(
                    name=ds[(s.exchange, c.cons_name)],
                    ref_prod=ds[(s.exchange, c.cons_prod)],
                    production_variable=tech,
                    relink=True,
                )

                ds_to_add = [list(d.values()) for d in new_plants.values()]
                ds_to_add = [e for v in ds_to_add for e in v]

                self.database = pd.concat(
                    [self.database] + ds_to_add, axis=0, ignore_index=True
                )

    def update_electricity_markets(self):
        """
        Delete electricity markets. Create high, medium and low voltage market groups for electricity.
        Link electricity-consuming datasets to newly created market groups for electricity.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for electricity
        :rtype: list
        """

        # We then need to create high voltage IAM electricity markets
        print("Create high voltage markets.")
        self.create_new_markets_high_voltage()
        print("Create medium voltage markets.")
        self.create_new_markets_medium_voltage()
        print("Create low voltage markets.")
        self.create_new_markets_low_voltage()

        # Finally, we need to relink all electricity-consuming activities to the new electricity markets
        print("Link activities to new electricity markets.")

        self.relink_datasets(
            excludes_datasets=["cobalt industry", "market group for electricity"],
            alternative_names=[
                "market group for electricity, high voltage",
                "market group for electricity, medium voltage",
                "market group for electricity, low voltage",
            ],
        )

        print(f"Log of deleted electricity markets saved in {DATA_DIR}/logs")
        print(f"Log of created electricity markets saved in {DATA_DIR}/logs")

        print("Done!")

        return self.database
