from collections import defaultdict

from .utils import (
    get_compression_effort,
    get_pre_cooling_energy,
    add_boil_off_losses,
    add_pipeline_losses,
    add_other_losses,
    fetch_mapping,
    adjust_electrolysis_electricity_requirement,
    add_compression_electricity,
    add_hydrogen_regional_storage,
    add_hydrogen_inhibitor,
    add_h2_fuelling_station,
)
from .config import HYDROGEN_SOURCES, HYDROGEN_SUPPLY_LOSSES, SUPPLY_CHAIN_SCENARIOS
from ..transformation import ws, uuid, np


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


class HydrogenMixin:
    def generate_hydrogen_activities(self):
        hydrogen_parameters = fetch_mapping(HYDROGEN_SOURCES)
        losses = fetch_mapping(HYDROGEN_SUPPLY_LOSSES)
        supply_chain_scenarios = fetch_mapping(SUPPLY_CHAIN_SCENARIOS)

        self._adjust_energy_inputs_for_hydrogen(hydrogen_parameters)
        self._generate_supporting_hydrogen_datasets()
        self._generate_supply_chain_variants(losses, supply_chain_scenarios)

    def _adjust_energy_inputs_for_hydrogen(self, hydrogen_parameters):
        new_datasets, seen_datasets = [], []
        for fuel_type, datasets in self.fuel_map.items():

            if not fuel_type.startswith("hydrogen"):
                continue

            params = hydrogen_parameters.get(fuel_type, {})
            feedstock_name = params.get("feedstock name")
            feedstock_unit = params.get("feedstock unit")
            efficiency = params.get("efficiency")
            floor_value = params.get("floor value")

            datasets = group_dicts_by_keys(datasets, ["name", "reference product"])

            for new_dataset in datasets:

                new_dataset = [
                    ds for ds in new_dataset if ds["name"] not in seen_datasets
                ]

                if not new_dataset:
                    continue

                new_ds = self.fetch_proxies(
                    datasets=new_dataset, production_variable=fuel_type
                )

                if params:
                    for region, dataset in new_ds.items():
                        initial_energy_use = sum(
                            exc["amount"]
                            for exc in dataset["exchanges"]
                            if exc["unit"] == feedstock_unit
                            and feedstock_name in exc["name"]
                            and exc["type"] != "production"
                        )

                        dataset.setdefault("log parameters", {}).update(
                            {
                                "initial energy input for hydrogen production": initial_energy_use
                            }
                        )

                        (
                            new_energy_use,
                            min_energy_use,
                            max_energy_use,
                            scaling_factor,
                        ) = (
                            None,
                            None,
                            None,
                            1,
                        )

                        if (
                            fuel_type
                            in self.fuel_efficiencies.variables.values.tolist()
                        ):
                            scaling_factor = 1 / self.find_iam_efficiency_change(
                                data=self.fuel_efficiencies,
                                variable=fuel_type,
                                location=region,
                            )
                            new_energy_use = max(
                                scaling_factor * initial_energy_use, floor_value
                            )

                        if scaling_factor == 1 and "electrolysis" in fuel_type:
                            new_energy_use, min_energy_use, max_energy_use = (
                                adjust_electrolysis_electricity_requirement(
                                    self.year, efficiency
                                )
                            )

                        try:
                            scaling_factor = new_energy_use / initial_energy_use
                        except (ZeroDivisionError, TypeError):
                            scaling_factor = 1

                        if scaling_factor != 1:
                            if min_energy_use is not None:
                                for exc in ws.technosphere(
                                    dataset,
                                    ws.contains("name", feedstock_name),
                                    ws.equals("unit", feedstock_unit),
                                ):
                                    exc["amount"] *= scaling_factor
                                    exc["uncertainty type"] = 5
                                    exc["loc"] = exc["amount"]
                                    exc["minimum"] = exc["amount"] * (
                                        min_energy_use / new_energy_use
                                    )
                                    exc["maximum"] = exc["amount"] * (
                                        max_energy_use / new_energy_use
                                    )

                        dataset.setdefault("log parameters", {}).update(
                            {"new energy input for hydrogen production": new_energy_use}
                        )

                new_datasets.extend(new_ds.values())
                seen_datasets.extend([ds["name"] for ds in new_dataset])

        for dataset in new_datasets:
            self.database.append(dataset)
            self.add_to_index(dataset)

    def _generate_supporting_hydrogen_datasets(self):
        keywords = [
            "hydrogen embrittlement inhibition",
            "geological hydrogen storage",
            "hydrogen refuelling station",
        ]

        datasets = ws.get_many(
            self.database,
            ws.either(*[ws.contains("name", keyword) for keyword in keywords]),
        )
        datasets = group_dicts_by_keys(datasets, ["name", "reference product"])
        new_datasets, seen_datasets = [], []
        for ds in datasets:

            ds = [d for d in ds if d["name"] not in seen_datasets]

            if not ds:
                continue

            new_ds = self.fetch_proxies(
                datasets=ds,
            )

            seen_datasets.append(d["name"] for d in ds)
            new_datasets.extend(new_ds.values())

        for new_d in new_datasets:
            self.database.append(new_d)
            self.add_to_index(new_d)
            self.write_log(new_d)

    def _generate_supply_chain_variants(self, losses, supply_chain_scenarios):
        self.fuel_map = self.mapping.generate_fuel_map()

        for region in self.regions:
            for fuel_type, datasets in self.fuel_map.items():
                if not fuel_type.startswith("hydrogen"):
                    continue

                for vehicle, config in supply_chain_scenarios.items():
                    for state in config["state"]:
                        for distance in config["distance"]:
                            name = f"hydrogen supply, {fuel_type}, by {vehicle}, as {state}, over {distance} km"
                            dataset = {
                                "location": region,
                                "name": name,
                                "reference product": "hydrogen, 700 bar",
                                "unit": "kilogram",
                                "database": self.database[0]["database"],
                                "code": str(uuid.uuid4().hex),
                                "comment": "Dataset representing hydrogen supply, generated by `premise`.",
                                "exchanges": [
                                    {
                                        "uncertainty type": 0,
                                        "loc": 1,
                                        "amount": 1,
                                        "type": "production",
                                        "production volume": 1,
                                        "product": "hydrogen, 700 bar",
                                        "name": name,
                                        "unit": "kilogram",
                                        "location": region,
                                    }
                                ],
                            }

                            subset = [
                                ds
                                for ds in self.database
                                if ds["location"] in (region, "RER", "RoW", "GLO")
                            ]
                            dataset = self.add_hydrogen_transport(
                                dataset, config, region, distance, vehicle, subset
                            )

                            if vehicle == "CNG pipeline":
                                supplier = list(
                                    self.find_suppliers(
                                        name="hydrogen embrittlement inhibition",
                                        ref_prod="hydrogen",
                                        unit="kilogram",
                                        loc=region,
                                        subset=subset,
                                    ).keys()
                                )[0]
                                dataset = add_hydrogen_inhibitor(
                                    dataset, region, supplier
                                )

                            if "regional storage" in config:
                                supplier = list(
                                    self.find_suppliers(
                                        name=config["regional storage"]["name"],
                                        ref_prod=config["regional storage"][
                                            "reference product"
                                        ],
                                        unit=config["regional storage"]["unit"],
                                        loc=region,
                                        subset=subset,
                                    ).keys()
                                )[0]

                                dataset = add_hydrogen_regional_storage(
                                    dataset, region, config, supplier
                                )

                            if state in ["gaseous", "liquid"]:
                                dataset = add_compression_electricity(
                                    state=state,
                                    vehicle=vehicle,
                                    distance=distance,
                                    dataset=dataset,
                                    suppliers=self.find_suppliers(
                                        name="market group for electricity, low voltage",
                                        ref_prod="electricity, low voltage",
                                        unit="kilowatt hour",
                                        loc=region,
                                        exclude=["period"],
                                        subset=subset,
                                    ),
                                    year=self.year,
                                )

                            if state == "liquid organic compound":
                                dataset = self.add_hydrogenation_energy(
                                    region, dataset, subset
                                )

                            dataset = self.add_hydrogen_input_and_losses(
                                hydrogen_datasets=datasets,
                                region=region,
                                losses=losses,
                                vehicle=vehicle,
                                state=state,
                                distance=distance,
                                dataset=dataset,
                            )

                            supplier = list(
                                self.find_suppliers(
                                    name="hydrogen refuelling station",
                                    ref_prod="hydrogen",
                                    unit="unit",
                                    loc=region,
                                    subset=subset,
                                ).keys()
                            )[0]

                            dataset["exchanges"].append(
                                add_h2_fuelling_station(region, supplier)
                            )

                            suppliers = self.find_suppliers(
                                name="market group for electricity, low voltage",
                                ref_prod="electricity, low voltage",
                                unit="kilowatt hour",
                                loc=region,
                                exclude=["period"],
                                subset=subset,
                            )

                            dataset = self.add_pre_cooling_electricity(
                                dataset, suppliers
                            )

                            dataset = self.relink_technosphere_exchanges(dataset)
                            self.database.append(dataset)
                            self.write_log(dataset)
                            self.add_to_index(dataset)

    def add_hydrogen_transport(
        self, dataset, config, region, distance, vehicle, subset
    ):
        for transport in config["vehicle"]:
            suppliers = self.find_suppliers(
                name=transport["name"],
                ref_prod=transport["reference product"],
                unit=transport["unit"],
                loc=region,
                subset=subset,
            )
            for supplier, share in suppliers.items():
                amount = (
                    distance * share / 1000
                    if supplier[-1] == "ton kilometer"
                    else distance * share / 2 * (1 / eval(config["lifetime"]))
                )
                dataset["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "amount": amount,
                        "type": "technosphere",
                        "product": supplier[2],
                        "name": supplier[0],
                        "unit": supplier[-1],
                        "location": supplier[1],
                        "comment": f"Transport over {distance} km by {vehicle}. ",
                    }
                )
        return dataset

    def add_hydrogen_input_and_losses(
        self, hydrogen_datasets, region, losses, vehicle, state, distance, dataset
    ):
        h2_ds = [ds for ds in hydrogen_datasets if ds["location"] == region]

        if not h2_ds:
            h2_ds = [ds for ds in hydrogen_datasets if ds["location"] in ("RER", "RoW")]
        h2_ds = h2_ds[0]

        total_loss = 1
        for loss, val in losses[vehicle][state].items():
            val = float(val)
            if loss == "boil-off":
                total_loss *= add_boil_off_losses(vehicle, distance, val)
            elif loss == "pipeline_leak":
                total_loss *= add_pipeline_losses(distance, val)
            else:
                total_loss *= add_other_losses(val)

        dataset["exchanges"].append(
            {
                "uncertainty type": 0,
                "amount": 1 * total_loss,
                "type": "technosphere",
                "product": h2_ds["reference product"],
                "name": h2_ds["name"],
                "unit": h2_ds["unit"],
                "location": region,
            }
        )

        dataset["exchanges"].append(
            {
                "uncertainty type": 0,
                "amount": total_loss - 1,
                "type": "biosphere",
                "name": "Hydrogen",
                "unit": "kilogram",
                "categories": ("air",),
                "input": (
                    "biosphere3",
                    self.biosphere_flows[
                        ("Hydrogen", "air", "unspecified", "kilogram")
                    ],
                ),
            }
        )

        dataset.setdefault("log parameters", {}).update(
            {"hydrogen distribution losses": total_loss - 1}
        )

        return dataset

    def add_pre_cooling_electricity(self, dataset: dict, suppliers: list) -> dict:
        """
        Add the electricity needed for pre-cooling the hydrogen.

        :param dataset: The dataset to modify.
        :param region: The region for which to add the activity.
        :return: The modified dataset.
        """

        # finally, add pre-cooling
        # is needed before filling vehicle tanks
        # as the hydrogen is pumped, the ambient temperature
        # vaporizes the gas, and because of the Thomson-Joule effect,
        # the gas temperature increases.
        # Hence, a refrigerant is needed to keep the H2 as low as
        # -30 C during pumping.

        # https://www.osti.gov/servlets/purl/1422579 gives us a formula
        # to estimate pre-cooling electricity need
        # it requires a capacity utilization for the fuelling station
        # as well as an ambient temperature
        # we will use a temp of 25 C
        # and a capacity utilization going from 10 kg H2/day in 2020
        # to 150 kg H2/day in 2050

        t_amb = 25
        cap_util = np.interp(self.year, [2020, 2050, 2100], [10, 150, 150])
        el_pre_cooling = get_pre_cooling_energy(t_amb, float(cap_util))

        for supplier, share in suppliers.items():
            dataset["exchanges"].append(
                {
                    "uncertainty type": 0,
                    "amount": el_pre_cooling * share,
                    "type": "technosphere",
                    "product": supplier[2],
                    "name": supplier[0],
                    "unit": supplier[-1],
                    "location": supplier[1],
                }
            )

        string = (
            f"Pre-cooling electricity is considered ({el_pre_cooling}), "
            f"assuming an ambiant temperature of {t_amb}C "
            f"and a capacity utilization for the fuel station of {cap_util} kg/day."
        )
        if "comment" in dataset:
            dataset["comment"] += string
        else:
            dataset["comment"] = string

        dataset.setdefault("log parameters", {}).update(
            {"electricity for hydrogen pre-cooling": el_pre_cooling}
        )

        return dataset
