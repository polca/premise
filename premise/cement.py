"""
cement.py contains the class `Cement`, which inherits from `BaseTransformation`.
This class transforms the cement markets and clinker and cement production activities
of the wurst database, based on projections from the IAM scenario.
It also the generic market for cement to reflect the projected clinker-to-cement ratio.
It eventually re-links all the cement-consuming activities (e.g., concrete production)
of the wurst database to the newly created cement markets.

"""

import yaml
import logging.config
import pprint
from collections import defaultdict

from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    List,
    get_shares_from_production_volume,
    get_suppliers_of_a_region,
    np,
    remove_exchanges,
    ws,
)
from .utils import (
    DATA_DIR,
    get_clinker_ratio_ecoinvent,
    get_clinker_ratio,
)

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"

with open(LOG_CONFIG, "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("cement")


class Cement(BaseTransformation):
    """
    Class that modifies clinker and cement production datasets in ecoinvent.
    It creates region-specific new clinker production datasets (and deletes the original ones).
    It adjusts the kiln efficiency based on the improvement indicated in the IAM file, relative to 2020.
    It adjusts non-CO2 pollutants emission, based on improvement indicated by the GAINS file, relative to 2020.
    It adds CCS, if indicated in the IAM file.
    It creates regions-specific cement production datasets (and deletes the original ones).
    It adjust electricity consumption in cement production datasets.
    It creates regions-specific cement market datasets (and deletes the original ones).
    It adjust the clinker-to-cement ratio in the generic cement market dataset.


    :ivar database: wurst database, which is a list of dictionaries
    :ivar iam_data: IAM data
    :ivar model: name of the IAM model (e.g., "remind", "image")
    :ivar pathway: name of the IAM pathway (e.g., "SSP2-Base")
    :ivar year: year of the pathway (e.g., 2030)
    :ivar version: version of ecoinvent database (e.g., "3.7")

    """

    def __init__(
            self,
            database: List[dict],
            iam_data: IAMDataCollection,
            model: str,
            pathway: str,
            year: int,
            version: str,
    ):
        super().__init__(database, iam_data, model, pathway, year)
        self.version = version
        self.clinker_ratio_eco = get_clinker_ratio_ecoinvent(version)
        self.clinker_ratio = get_clinker_ratio(self.year)

    def fetch_current_energy_details(self, dataset):
        """
        Fetches the current energy consumption per ton of clinker

        :param dataset: clinker production dataset
        :return: current energy consumption per ton of clinker, in kj/kg clinker
        """

        # store in a dictionary the amount of fuel and electricity input
        # along with the energy and fossil and biogenic CO2
        # emissions associated with the combustion of each fuel

        _ = lambda x: self.calculate_input_energy(x["name"], x["amount"], x["unit"])

        d_fuels = defaultdict(dict)

        for exc in dataset["exchanges"]:
            if (
                    exc["name"] in self.cement_fuels_map["cement"]
                    and exc["type"] == "technosphere"
            ):

                if exc["name"] not in d_fuels:
                    d_fuels[exc["name"]] = {
                        "amount": exc["amount"],
                        "energy": _(exc) * 1000,
                        "fossil CO2": self.fuels_specs[
                                          self.fuel_map_reverse[exc["name"]]
                                      ]["co2"]
                                      * _(exc)
                                      * (
                                              1
                                              - self.fuels_specs[self.fuel_map_reverse[exc["name"]]][
                                                  "biogenic_share"
                                              ]
                                      ),
                        "biogenic CO2": self.fuels_specs[
                                            self.fuel_map_reverse[exc["name"]]
                                        ]["co2"]
                                        * _(exc)
                                        * self.fuels_specs[self.fuel_map_reverse[exc["name"]]][
                                            "biogenic_share"
                                        ],
                    }
                else:
                    d_fuels[exc["name"]]["amount"] += exc["amount"]
                    d_fuels[exc["name"]]["energy"] += _(exc) * 1000
                    d_fuels[exc["name"]]["fossil CO2"] += (
                            self.fuels_specs[self.fuel_map_reverse[exc["name"]]]["co2"]
                            * _(exc)
                            * (
                                    1
                                    - self.fuels_specs[self.fuel_map_reverse[exc["name"]]][
                                        "biogenic_share"
                                    ]
                            )
                    )
                    d_fuels[exc["name"]]["biogenic CO2"] += (
                            self.fuels_specs[self.fuel_map_reverse[exc["name"]]]["co2"]
                            * _(exc)
                            * self.fuels_specs[self.fuel_map_reverse[exc["name"]]][
                                "biogenic_share"
                            ]
                    )

        return d_fuels

    def fetch_bio_ratio(self, dataset: dict) -> float:
        """
        Return teh ratio between bio and non-bio CO2 emissions
        in the dataset.

        :param dataset: clinker production dataset
        :return: ratio between bio and non-bio CO2 emissions
        """

        bio_co2 = sum(
            [
                e["amount"]
                for e in dataset["exchanges"]
                if e["name"] == "Carbon dioxide, non-fossil"
            ]
        )
        non_bio_co2 = sum(
            [
                e["amount"]
                for e in dataset["exchanges"]
                if e["name"] == "Carbon dioxide, fossil"
            ]
        )

        return bio_co2 / (bio_co2 + non_bio_co2)

    def rescale_fuel_inputs(self, dataset, scaling_factor, energy_details):

        if scaling_factor != 1:
            for exc in dataset["exchanges"]:
                if exc["name"] in self.cement_fuels_map["cement"]:
                    exc["amount"] *= scaling_factor

                    # update energy_details
                    energy_details[exc["name"]]["amount"] *= scaling_factor
                    energy_details[exc["name"]]["energy"] *= scaling_factor
                    energy_details[exc["name"]]["fossil CO2"] *= scaling_factor
                    energy_details[exc["name"]]["biogenic CO2"] *= scaling_factor

        new_energy_input = sum([d["energy"] for d in energy_details.values()])

        dataset["log parameters"].update(
            {
                "new energy input per ton clinker": new_energy_input,
            }
        )

        return dataset

    def rescale_emissions(self, dataset, energy_details, scaling_factor):

        if scaling_factor != 1:
            for exc in dataset["exchanges"]:
                if exc["name"].lower().startswith("carbon dioxide"):

                    if "non-fossil" in exc["name"].lower():
                        dataset["log parameters"].update(
                            {
                                "initial biogenic CO2": exc["amount"],
                            }
                        )
                        # calculate total biogenic CO2 from energy_details
                        total_biogenic_CO2 = sum(
                            [d["biogenic CO2"] for d in energy_details.values()]
                        )

                        exc["amount"] = total_biogenic_CO2
                        dataset["log parameters"].update(
                            {
                                "new biogenic CO2": total_biogenic_CO2,
                            }
                        )
                    else:
                        # calculate total fossil CO2 from energy_details
                        total_fossil_CO2 = sum(
                            [d["fossil CO2"] for d in energy_details.values()]
                        )

                        dataset["log parameters"].update(
                            {
                                "initial fossil CO2": exc["amount"],
                            }
                        )

                        # remove 525 kg for calcination
                        exc["amount"] = 0.525 + total_fossil_CO2
                        dataset["log parameters"].update(
                            {
                                "new fossil CO2": exc["amount"],
                            }
                        )
                else:
                    if exc["type"] == "biosphere":
                        exc["amount"] *= scaling_factor

        else:
            for exc in dataset["exchanges"]:
                if exc["name"].lower().startswith("carbon dioxide"):

                    if "non-fossil" in exc["name"].lower():
                        dataset["log parameters"].update(
                            {
                                "initial biogenic CO2": exc["amount"],
                                "new biogenic CO2": exc["amount"],
                            }
                        )
                    else:
                        dataset["log parameters"].update(
                            {
                                "initial fossil CO2": exc["amount"],
                                "new fossil CO2": exc["amount"],
                            }
                        )

        return dataset

    def build_clinker_production_datasets(self) -> Dict[str, dict]:
        """
        Builds clinker production datasets for each IAM region.
        Adds CO2 capture and Storage, if needed.

        :return: a dictionary with IAM regions as keys and clinker production datasets as values.
        """

        # Fetch clinker production activities
        # and store them in a dictionary
        d_act_clinker = self.fetch_proxies(
            name="clinker production",
            ref_prod="clinker",
            production_variable="cement",
        )

        for region, dataset in d_act_clinker.items():

            # calculate current thermal energy consumption per kg clinker
            energy_details = self.fetch_current_energy_details(dataset)
            current_energy_input_per_ton_clinker = sum(
                [d["energy"] for d in energy_details.values()]
            )

            # fetch the amount of biogenic CO2 emisisons
            bio_CO2 = sum(
                e["amount"]
                for e in dataset["exchanges"]
                if e["name"] == "Carbon dioxide, non-fossil"
            )

            # back-calculate the amount of waste fuel from
            # the biogenic CO2 emissions
            # biogenic CO2 / MJ for waste fuel
            waste_fuel_biogenic_co2_emission_factor = (
                    self.fuels_specs["waste"]["co2"]
                    * self.fuels_specs["waste"]["biogenic_share"]
            )

            waste_fuel_fossil_co2_emission_factor = self.fuels_specs["waste"]["co2"] * (
                    1 - self.fuels_specs["waste"]["biogenic_share"]
            )
            # energy input of waste fuel in MJ
            energy_input_waste_fuel = bio_CO2 / waste_fuel_biogenic_co2_emission_factor
            # amount waste fuel, in kg
            amount_waste_fuel = (
                    energy_input_waste_fuel
                    / self.fuels_specs["waste"]["lhv"]
            )

            # add waste fuel to the energy details
            energy_details["market for waste plastic, mixture"] = {
                "amount": amount_waste_fuel,
                "energy": energy_input_waste_fuel * 1000,
                "fossil CO2": waste_fuel_fossil_co2_emission_factor
                              * energy_input_waste_fuel,
                "biogenic CO2": bio_CO2,
            }

            # add the waste fuel energy input
            # to the total energy input
            current_energy_input_per_ton_clinker += (
                    energy_input_waste_fuel * 1000
            )

            if "log parameters" not in dataset:
                dataset["log parameters"] = {}

            dataset["log parameters"].update(
                {
                    "initial energy input per ton clinker": current_energy_input_per_ton_clinker,
                }
            )

            # calculate the scaling factor
            # the correction factor applied to all fuel/electricity input is
            # equal to the ratio fuel/output in the year in question
            # divided by the ratio fuel/output in 2020

            scaling_factor = 1 / self.find_iam_efficiency_change(
                variable="cement", location=dataset["location"]
            )

            # calculate new thermal energy
            # consumption per kg clinker
            new_energy_input_per_ton_clinker = (
                    current_energy_input_per_ton_clinker
                    * scaling_factor
            )

            # put a floor value of 3000 kj/kg clinker
            if new_energy_input_per_ton_clinker < 3000:
                new_energy_input_per_ton_clinker = 3000

            scaling_factor = (
                    new_energy_input_per_ton_clinker
                    / current_energy_input_per_ton_clinker
            )

            # rescale fuel consumption and emissions
            # rescale the fuel and electricity input
            dataset = self.rescale_fuel_inputs(
                dataset, scaling_factor, energy_details
            )

            # rescale combustion-related CO2 emissions
            dataset = self.rescale_emissions(
                dataset, energy_details, scaling_factor
            )

            # Carbon capture rate: share of capture of total CO2 emitted
            carbon_capture_rate = self.get_carbon_capture_rate(
                loc=dataset["location"], sector="cement"
            )

            dataset["log parameters"].update(
                {
                    "carbon capture rate": carbon_capture_rate,
                }
            )

            # add CCS-related dataset
            if carbon_capture_rate > 0:
                # total CO2 emissions = bio CO2 emissions
                # + fossil CO2 emissions
                # + calcination emissions

                total_co2_emissions = (
                        dataset["log parameters"].get("new fossil CO2", 0)
                        + dataset["log parameters"].get("new biogenic CO2", 0)
                )
                # share bio CO2 stored = sum of biogenic fuel emissions / total CO2 emissions
                bio_co2_stored = (
                        dataset["log parameters"].get("new biogenic CO2", 0) / total_co2_emissions
                )

                # 0.11 kg CO2 leaks per kg captured
                # we need to align the CO2 composition with
                # the CO2 composition of the cement plant
                bio_co2_leaked = bio_co2_stored * 0.11

                # create the CCS dataset to fit this clinker production dataset
                # and add it to the database
                self.create_ccs_dataset(
                    region,
                    bio_co2_stored,
                    bio_co2_leaked,
                )

                # add an input from this CCS dataset in the clinker dataset
                ccs_exc = {
                    "uncertainty type": 0,
                    "loc": 0,
                    "amount": float((total_co2_emissions / 1000) * carbon_capture_rate),
                    "type": "technosphere",
                    "production volume": 0,
                    "name": "carbon dioxide, captured at cement production plant, with underground storage, post, 200 km",
                    "unit": "kilogram",
                    "location": dataset["location"],
                    "product": "carbon dioxide, captured and stored",
                }
                dataset["exchanges"].append(ccs_exc)

                # Update CO2 exchanges
                for exc in dataset["exchanges"]:
                    if (
                            exc["name"].lower().startswith("carbon dioxide")
                            and exc["type"] == "biosphere"
                    ):
                        exc["amount"] *= 1 - carbon_capture_rate

                        if "non-fossil" in exc["name"].lower():
                            dataset["log parameters"].update(
                                {
                                    "new biogenic CO2": exc["amount"],
                                }
                            )
                        else:
                            dataset["log parameters"].update(
                                {
                                    "new fossil CO2": exc["amount"],
                                }
                            )

            dataset["exchanges"] = [v for v in dataset["exchanges"] if v]

            # update comment
            dataset["comment"] = (
                                         "Dataset modified by `premise` based on WBCSD's GNR data and IAM projections "
                                         + " for the cement industry.\n"
                                         + f"Calculated energy input per kg clinker: {np.round(new_energy_input_per_ton_clinker, 1) / 1000}"
                                           f" MJ/kg clinker.\n"
                                         + f"Rate of carbon capture: {int(carbon_capture_rate * 100)} pct.\n"
                                 ) + dataset["comment"]

        print(
            "Adjusting emissions of hot pollutants for clinker production datasets..."
        )
        d_act_clinker = {
            k: self.update_pollutant_emissions(dataset=v, sector="cement")
            for k, v in d_act_clinker.items()
        }

        return d_act_clinker

    def adjust_clinker_ratio(self, d_act: Dict[str, dict]) -> Dict[str, dict]:
        """Adjust the cement suppliers composition for "cement, unspecified", in order to reach
        the average clinker-to-cement ratio given by the IAM.

        The supply of the cement with the highest clinker-to-cement ratio is decreased by 1% to the favor of
        the supply of the cement with the lowest clinker-to-cement ratio, and the average clinker-to-cement ratio
        is calculated.

        This operation is repeated until the average clinker-to-cement ratio aligns with that given by the IAM.
        When the supply of the cement with the highest clinker-to-cement ratio goes below 1%,
        the cement with the second highest clinker-to-cement ratio becomes affected and so forth.

        """

        for region in d_act:
            # we fetch the clinker-to-cement ratio forecast
            # This ratio decreases over time
            # (reducing the amount of clinker in cement), but at
            # different pace across regions.

            clinker_ratio_to_reach = 1
            for r in self.clinker_ratio.region.values:
                if self.geo.ecoinvent_to_iam_location(r) == region:
                    clinker_ratio_to_reach = self.clinker_ratio.sel(region=r).values
                    break

            if clinker_ratio_to_reach == 1:
                raise ValueError(f"clinker-to-cement ratio not found for {region}")

            share, ratio = [], []

            # we loop through the exchange of the dataset
            # if we meet a cement exchange, we store its clinker-to-cement ratio
            # in `ratio`
            for exc in d_act[region]["exchanges"]:
                if "cement" in exc["product"] and exc["type"] == "technosphere":
                    share.append(exc["amount"])
                    try:
                        ratio.append(
                            self.clinker_ratio_eco[(exc["name"], exc["location"])]
                        )
                    except KeyError:
                        print(
                            f"clinker-to-cement ratio not found for {exc['name'], exc['location']}"
                        )

            share = np.array(share)
            ratio = np.array(ratio)

            # once we know what cement exchanges are in the dataset
            # we can calculate the volume-weighted clinker-to-cement ratio
            # of the dataset
            average_ratio = (share * ratio).sum()

            if "log parameters" not in d_act[region]:
                d_act[region]["log parameters"] = {}

            d_act[region]["log parameters"].update(
                {"old clinker-to-cement ratio": average_ratio}
            )

            # As long as we do not reach the clinker-to-cement ratio forecast by REMIND
            # we will decrease the input of the highest clinker-containing cement
            # and increase the input of the lowest clinker-containing cement
            iteration = 0
            while average_ratio > clinker_ratio_to_reach and iteration < 100:
                share[share == 0] = np.nan

                ratio = np.where(share >= 0.001, ratio, np.nan)

                highest_ratio = np.nanargmax(ratio)
                lowest_ratio = np.nanargmin(ratio)

                share[highest_ratio] -= 0.01
                share[lowest_ratio] += 0.01

                average_ratio = (np.nan_to_num(ratio) * np.nan_to_num(share)).sum()
                iteration += 1

            share = np.nan_to_num(share)

            count = 0
            for exc in d_act[region]["exchanges"]:
                if "cement" in exc["product"] and exc["type"] == "technosphere":
                    exc["amount"] = share[count]
                    count += 1

            if "log parameters" not in d_act[region]:
                d_act[region]["log parameters"] = {}

            d_act[region]["log parameters"].update(
                {"new clinker-to-cement ratio": average_ratio}
            )

        return d_act

    def update_electricity_exchanges(self, d_act: Dict[str, dict]) -> Dict[str, dict]:
        """
        Update electricity exchanges in cement production datasets.
        Here, we use data from the GNR database for current consumption, and the 2018 IEA/GNR roadmap publication
        for future consumption.
        Electricity consumption equals electricity use minus on-site electricity generation from excess heat recovery.

        :return:
        """
        d_act = remove_exchanges(d_act, ["electricity"])

        for region in d_act:
            new_exchanges = []

            electricity = (
                    self.iam_data.gnr_data.loc[
                        dict(
                            variables=["Power generation", "Power consumption"],
                            region=self.geo.iam_to_gains_region(region),
                        )
                    ].interp(year=self.year)
                    / 1000
            )
            electricity_needed = (
                    electricity.sel(variables="Power consumption")
                    - electricity.sel(variables="Power generation")
            ).values

            # Fetch electricity-producing technologies contained in the IAM region
            # if they cannot be found for the ecoinvent locations concerned
            # we widen the scope to EU-based datasets, and RoW
            ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)
            possible_locations = [[region], ecoinvent_regions, ["RER"], ["RoW"]]
            suppliers, counter = [], 0

            while len(suppliers) == 0:
                suppliers = list(
                    get_suppliers_of_a_region(
                        database=self.database,
                        locations=possible_locations[counter],
                        names=["electricity, medium voltage"],
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
                        "loc": electricity_needed * share,
                        "amount": electricity_needed * share,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": supplier[2],
                        "name": supplier[0],
                        "unit": supplier[-1],
                        "location": supplier[1],
                    }
                )

            d_act[region]["exchanges"].extend(new_exchanges)
            d_act[region]["exchanges"] = [v for v in d_act[region]["exchanges"] if v]

            txt = (
                "Dataset modified by `premise` based on WBCSD's GNR data and 2018 IEA "
                "roadmap for the cement industry.\n "
                f"Electricity consumption per kg cement: {electricity_needed} kWh."
                f"Of which a part was generated from on-site waste heat recovery.\n"
            )

            if "comment" in d_act[region]:
                d_act[region]["comment"] += txt
            else:
                d_act[region]["comment"] = txt

            if "log parameters" not in d_act[region]:
                d_act[region]["log parameters"] = {}

            d_act[region]["log parameters"].update(
                {
                    "electricity generated": electricity.sel(
                        variables="Power generation"
                    ).values.item(),
                    "electricity consumed": electricity.sel(
                        variables="Power consumption"
                    ).values.item(),
                }
            )

        return d_act

    def add_datasets_to_database(self) -> None:
        """
        Runs a series of methods that create new clinker and cement production datasets
        and new cement market datasets.
        :return: Does not return anything. Modifies in place.
        """

        print("\nStart integration of cement data...\n")

        print("\nCreate new clinker production datasets and delete old datasets")

        clinker_prod_datasets = list(self.build_clinker_production_datasets().values())
        self.database.extend(clinker_prod_datasets)

        # add to log
        for new_dataset in clinker_prod_datasets:
            self.write_log(new_dataset)

        print("\nCreate new clinker market datasets and delete old datasets")
        clinker_market_datasets = list(
            self.fetch_proxies(
                name="market for clinker",
                ref_prod="clinker",
                production_variable="cement",
            ).values()
        )

        self.database.extend(clinker_market_datasets)

        # add to log
        for new_dataset in clinker_market_datasets:
            self.write_log(new_dataset)

        print('Adjust clinker-to-cement ratio in "unspecified cement" datasets')

        if self.version == 3.5:
            name = "market for cement, unspecified"
            ref_prod = "cement, unspecified"

        else:
            name = "cement, all types to generic market for cement, unspecified"
            ref_prod = "cement, unspecified"

        act_cement_unspecified = self.fetch_proxies(
            name=name,
            ref_prod=ref_prod,
            production_variable="cement",
        )
        act_cement_unspecified = self.adjust_clinker_ratio(act_cement_unspecified)
        self.database.extend(list(act_cement_unspecified.values()))

        # add to log
        for new_dataset in list(act_cement_unspecified.values()):
            self.write_log(dataset=new_dataset, status="updated")

        print("\nCreate new cement market datasets")

        # cement markets
        markets = ws.get_many(
            self.database,
            ws.contains("name", "market for cement"),
            ws.contains("reference product", "cement"),
            ws.doesnt_contain_any("name", ["factory", "tile", "sulphate", "plaster"]),
            ws.doesnt_contain_any("location", self.regions),
        )

        markets = {(m["name"], m["reference product"]) for m in markets}

        for dataset in markets:
            new_cement_markets = self.fetch_proxies(
                name=dataset[0],
                ref_prod=dataset[1],
                production_variable="cement",
            )

            self.database.extend(list(new_cement_markets.values()))

            # add to log
            for new_dataset in list(new_cement_markets.values()):
                self.write_log(new_dataset)

        print(
            "\nCreate new cement production datasets and "
            "adjust electricity consumption"
        )
        # cement production
        production = ws.get_many(
            self.database,
            ws.contains("name", "cement production"),
            ws.contains("reference product", "cement"),
            ws.doesnt_contain_any("name", ["factory", "tile", "sulphate", "plaster"]),
        )

        production = {(p["name"], p["reference product"]) for p in production}

        for dataset in production:
            # Fetch proxy datasets (one per IAM region)
            # Delete old datasets
            new_cement_production = self.fetch_proxies(
                name=dataset[0],
                ref_prod=dataset[1],
                production_variable="cement",
            )
            # Update electricity use
            new_cement_production = self.update_electricity_exchanges(
                new_cement_production
            )

            # add them to the wurst database
            self.database.extend(list(new_cement_production.values()))

            # add to log
            for new_dataset in list(new_cement_production.values()):
                self.write_log(dataset=new_dataset, status="updated")

        print("Done!")

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('initial energy input per ton clinker', '')}|"
            f"{dataset.get('log parameters', {}).get('new energy input per ton clinker', '')}|"
            f"{dataset.get('log parameters', {}).get('carbon capture rate', '')}|"
            f"{dataset.get('log parameters', {}).get('initial fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('initial biogenic CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new fossil CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('new biogenic CO2', '')}|"
            f"{dataset.get('log parameters', {}).get('electricity generated', '')}|"
            f"{dataset.get('log parameters', {}).get('electricity consumed', '')}|"
            f"{dataset.get('log parameters', {}).get('old clinker-to-cement ratio', '')}|"
            f"{dataset.get('log parameters', {}).get('new clinker-to-cement ratio', '')}"
        )
