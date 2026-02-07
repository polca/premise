from .utils import fetch_mapping, get_crops_properties
from .config import BIOFUEL_SOURCES, REGION_CLIMATE_MAP

import numpy as np

crops_props = get_crops_properties()


class BiofuelsMixin:

    def generate_biofuel_activities(self):
        """
        Create region-specific biofuel datasets.
        Update the conversion efficiency.
        """
        region_to_climate = fetch_mapping(REGION_CLIMATE_MAP)[self.model]
        crop_types = list(crops_props.keys())
        climates = set(region_to_climate.values())

        climate_to_crop_type = {
            clim: {
                crop_type: crops_props[crop_type]["crop_type"][self.model][clim]
                for crop_type in crop_types
            }
            for clim in climates
        }

        biofuel_activities = fetch_mapping(BIOFUEL_SOURCES)

        # regionalize wood-based biofuels for all regions
        activities = biofuel_activities["wood"]["forest residues"]
        mapping = {
            "forest residues": [
                ds
                for ds in self.database
                if any(ds["name"].startswith(activity) for activity in activities)
            ]
        }

        self.process_and_add_activities(
            mapping=mapping,
            regions=self.regions,
            production_volumes=self.iam_data.production_volumes,
        )

        activities = biofuel_activities["oil"]["used cooking oil"]
        mapping = {
            "used cooking oil": [
                ds
                for ds in self.database
                if any(ds["name"].startswith(activity) for activity in activities)
            ]
        }

        self.process_and_add_activities(
            mapping=mapping,
            regions=self.regions,
            production_volumes=self.iam_data.production_volumes,
        )

        for climate in ["tropical", "temperate"]:
            regions = [k for k, v in region_to_climate.items() if v == climate]
            for crop_type in climate_to_crop_type[climate]:
                specific_crop = climate_to_crop_type[climate][crop_type]

                activities = biofuel_activities[crop_type][specific_crop]

                mapping = {
                    specific_crop: [
                        ds
                        for ds in self.database
                        if any(
                            ds["name"].startswith(activity) for activity in activities
                        )
                    ]
                }

                self.process_and_add_activities(
                    mapping=mapping,
                    regions=regions,
                    efficiency_adjustment_fn=[
                        self.adjust_land_use,
                        self.adjust_land_use_change_emissions,
                    ],
                    production_volumes=self.iam_data.production_volumes,
                )

    def adjust_land_use(self, dataset: dict, crop_type: str) -> dict:
        """
        Adjust land use.

        :param dataset: dataset to adjust
        :param region: region of the dataset
        :param crop_type: crop type of the dataset
        :return: adjusted dataset

        """

        if not self.should_adjust_land_use(dataset, crop_type):
            return dataset

        string = ""
        land_use = 0

        for exc in dataset["exchanges"]:
            # we adjust the land use
            if exc["type"] == "biosphere" and exc["name"].startswith("Occupation"):
                if "LHV [MJ/kg as received]" in dataset:
                    lower_heating_value = dataset["LHV [MJ/kg as received]"]
                else:
                    lower_heating_value = dataset.get("LHV [MJ/kg dry]", 0)

                # Ha/GJ
                if self.year in self.iam_data.land_use.coords["year"].values:
                    land_use = self.iam_data.land_use.sel(
                        region=dataset["location"], variables=crop_type, year=self.year
                    ).values
                else:
                    land_use = (
                        self.iam_data.land_use.sel(
                            region=dataset["location"], variables=crop_type
                        )
                        .interp(year=self.year)
                        .values
                    )

                # replace NA values with 0
                if np.isnan(land_use):
                    land_use = 0

                if land_use > 0:
                    # HA to m2
                    land_use *= 10000
                    # m2/GJ to m2/MJ
                    land_use /= 1000
                    # m2/kg, as received
                    land_use *= lower_heating_value
                    # update exchange value
                    exc["amount"] = float(land_use)

                    string = (
                        f"The land area occupied has been modified to {land_use}, "
                        f"to be in line with the scenario {self.scenario} of {self.model.upper()} "
                        f"in {self.year} in the region {dataset['location']}. "
                    )

        if string and land_use:
            if "comment" in dataset:
                dataset["comment"] += string
            else:
                dataset["comment"] = string

            dataset.setdefault("log parameters", {}).update(
                {
                    "land footprint": land_use,
                }
            )

        return dataset

    def adjust_land_use_change_emissions(
        self,
        dataset: dict,
        crop_type: str,
    ) -> dict:
        """
        Adjust land use change emissions to crop farming dataset
        if the variable is provided by the IAM.

        :param dataset: dataset to adjust
        :param region: region of the dataset
        :param crop_type: crop type of the dataset
        :return: adjusted dataset

        """

        if not self.should_adjust_land_use_change_emissions(dataset, crop_type):
            return dataset

        # then, we should include the Land Use Change-induced CO2 emissions
        # those are given in kg CO2-eq./GJ of primary crop energy

        # kg CO2/GJ
        if self.year in self.iam_data.land_use_change.coords["year"].values:
            land_use_co2 = self.iam_data.land_use_change.sel(
                region=dataset["location"], variables=crop_type, year=self.year
            ).values
        else:
            land_use_co2 = (
                self.iam_data.land_use_change.sel(
                    region=dataset["location"], variables=crop_type
                )
                .interp(year=self.year)
                .values
            )

        # replace NA values with 0
        if np.isnan(land_use_co2):
            land_use_co2 = 0

        if land_use_co2 > 0:
            # lower heating value, as received
            if "LHV [MJ/kg as received]" in dataset:
                lower_heating_value = dataset["LHV [MJ/kg as received]"]
            else:
                lower_heating_value = dataset.get("LHV [MJ/kg dry]", 0)

            # kg CO2/MJ
            land_use_co2 /= 1000
            land_use_co2 *= lower_heating_value

            land_use_co2_exc = {
                "uncertainty type": 0,
                "loc": float(land_use_co2),
                "amount": float(land_use_co2),
                "type": "biosphere",
                "name": "Carbon dioxide, from soil or biomass stock",
                "unit": "kilogram",
                "input": (
                    "biosphere3",
                    self.biosphere_flows[
                        (
                            "Carbon dioxide, from soil or biomass stock",
                            "air",
                            "non-urban air or from high stacks",
                            "kilogram",
                        )
                    ],
                ),
                "categories": (
                    "air",
                    "non-urban air or from high stacks",
                ),
            }
            dataset["exchanges"].append(land_use_co2_exc)

            string = (
                f"{land_use_co2} kg of land use-induced CO2 has been added by premise, "
                f"to be in line with the scenario {self.scenario} of {self.model.upper()} "
                f"in {self.year} in the region {dataset['location']}."
            )

            if "comment" in dataset:
                dataset["comment"] += string
            else:
                dataset["comment"] = string

            dataset.setdefault("log parameters", {}).update(
                {
                    "land use CO2": land_use_co2,
                }
            )

        return dataset

    def should_adjust_land_use(self, dataset: dict, crop_type: str) -> bool:
        """
        Check if the dataset should be adjusted for land use.
        """

        if self.iam_data.land_use is None:
            return False
        return (
            any(i in dataset["name"].lower() for i in ("farming and supply",))
            and crop_type.lower() in self.iam_data.land_use.variables.values
            and not any(
                i in dataset["name"].lower() for i in ["straw", "residue", "stover"]
            )
        )

    def should_adjust_land_use_change_emissions(
        self, dataset: dict, crop_type: str
    ) -> bool:
        """
        Check if the dataset should be adjusted for land use change emissions.
        """
        if self.iam_data.land_use_change is None:
            return False
        return (
            any(i in dataset["name"].lower() for i in ("farming and supply",))
            and crop_type.lower() in self.iam_data.land_use_change.variables.values
            and not any(
                i in dataset["name"].lower() for i in ["straw", "residue", "stover"]
            )
        )
