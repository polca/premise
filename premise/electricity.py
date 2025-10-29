"""
electricity.py contains the class `Electricity`, which inherits from `BaseTransformation`.
This class transforms the electricity markets and power plants of the wurst database,
based on projections from the IAM scenario.
It eventually re-links all the electricity-consuming activities of the wurst database to
the newly created electricity markets.

"""

import copy
import re
from collections import defaultdict
from functools import lru_cache

import yaml

from .export import biosphere_flows_dictionary
from .filesystem_constants import VARIABLES_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    InventorySet,
    List,
    Tuple,
    find_fuel_efficiency,
    get_suppliers_of_a_region,
    np,
    uuid,
    ws,
)
from .utils import (
    get_efficiency_solar_photovoltaics,
    get_water_consumption_factors,
    rescale_exchanges,
)
from .validation import ElectricityValidation

POWERPLANT_TECHS = VARIABLES_DIR / "electricity.yaml"

logger = create_logger("electricity")


def load_electricity_variables() -> dict:
    """
    Load the electricity variables from a YAML file.
    :return: a dictionary with the electricity variables
    :rtype: dict
    """

    with open(POWERPLANT_TECHS, "r", encoding="utf-8") as stream:
        techs = yaml.full_load(stream)

    return techs


def get_losses_per_country(database: list) -> Dict[str, Dict[str, float]]:
    losses = defaultdict(dict)
    for ds in database:
        if ds["name"] in [
            "market for electricity, high voltage",
            "market for electricity, medium voltage",
            "market for electricity, low voltage",
        ]:
            country = ds["location"]
            if "high voltage" in ds["name"]:
                market_type = "high"
            elif "medium voltage" in ds["name"]:
                market_type = "medium"
            else:
                market_type = "low"

            for e in ds["exchanges"]:
                if (
                    e["name"].startswith("market for electricity")
                    and e["type"] == "technosphere"
                ):
                    losses[country].update(
                        {f"Transformation loss {market_type} voltage": e["amount"]}
                    )

            for e in ds["exchanges"]:
                if e["type"] == "production":
                    if "production volume" in e:
                        if "Production volume" not in losses[country]:
                            losses[country].update(
                                {"Production volume": e["production volume"]}
                            )
                        else:
                            if (
                                e["production volume"]
                                > losses[country]["Production volume"]
                            ):
                                losses[country].update(
                                    {"Production volume": e["production volume"]}
                                )

        if (
            ds["name"]
            == "electricity voltage transformation from medium to low voltage"
        ):
            country = ds["location"]
            for e in ds["exchanges"]:
                if (
                    e["name"].startswith("market for electricity")
                    and e["type"] == "technosphere"
                ):
                    losses[country].update(
                        {f"Transmission loss to low voltage": e["amount"] - 1}
                    )

        if (
            ds["name"]
            == "electricity voltage transformation from high to medium voltage"
        ):
            country = ds["location"]
            for e in ds["exchanges"]:
                if (
                    e["name"].startswith("market for electricity")
                    and e["type"] == "technosphere"
                ):
                    losses[country].update(
                        {f"Transmission loss to medium voltage": e["amount"] - 1}
                    )

    return losses


def get_production_weighted_losses(
    losses: Dict[str, Dict[str, float]], locs: List[str]
) -> Dict[str, Dict[str, float]]:
    """
    Return the transformation, transmission and distribution losses at a given voltage level for a given location.
    A weighted average is made of the locations contained in the IAM region.

    """

    # Fetch locations contained in IAM region
    cumul_prod, transf_loss = 0.0, 0.0
    for loc in locs:
        loss = losses.get(loc, {})
        prod = loss.get("Production volume", 0.0)
        transf = loss.get("Transformation loss high voltage", 0.0)

        transf_loss += transf * prod
        cumul_prod += prod

    if cumul_prod == 0:
        return {
            "high": {"transf_loss": 0.0, "distr_loss": 0.0},
            "medium": {"transf_loss": 0.0, "distr_loss": 0.0},
            "low": {"transf_loss": 0.0, "distr_loss": 0.0},
        }

    transf_loss /= cumul_prod

    high = {"transf_loss": transf_loss, "distr_loss": 0.0}

    cumul_prod, transf_loss, distr_loss = 0.0, 0.0, 0.0

    for loc in locs:
        dict_loss = losses.get(
            loc,
            {
                "Transformation loss medium voltage": 0,
                "Transmission loss to medium voltage": 0,
                "Production volume": 0,
            },
        )

        transf_loss += (
            dict_loss.get("Transformation loss medium voltage", 0)
            * dict_loss["Production volume"]
        )
        distr_loss += (
            dict_loss.get("Transmission loss to medium voltage", 0)
            * dict_loss["Production volume"]
        )
        cumul_prod += dict_loss["Production volume"]
    transf_loss /= cumul_prod
    distr_loss /= cumul_prod

    medium = {"transf_loss": transf_loss, "distr_loss": distr_loss}

    cumul_prod, transf_loss, distr_loss = 0.0, 0.0, 0.0

    for loc in locs:
        dict_loss = losses.get(
            loc,
            {
                "Transformation loss low voltage": 0.0,
                "Transmission loss to low voltage": 0.0,
                "Production volume": 0.0,
            },
        )
        transf_loss += (
            dict_loss.get("Transformation loss low voltage", 0)
            * dict_loss["Production volume"]
        )
        distr_loss += (
            dict_loss.get("Transmission loss to low voltage", 0)
            * dict_loss["Production volume"]
        )
        cumul_prod += dict_loss["Production volume"]
    transf_loss /= cumul_prod
    distr_loss /= cumul_prod

    low = {"transf_loss": transf_loss, "distr_loss": distr_loss}

    return {"high": high, "medium": medium, "low": low}


def filter_technology(dataset_names, database, unit="kilowatt hour"):
    return list(
        ws.get_many(
            database,
            ws.either(*[ws.equals("name", name) for name in dataset_names]),
            ws.equals("unit", unit),
        )
    )


def _update_electricity(
    scenario,
    version,
    system_model,
    use_absolute_efficiency,
):

    if scenario["iam data"].electricity_mix is None:
        print("No electricity scenario data available -- skipping")
        return scenario

    electricity = Electricity(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        use_absolute_efficiency=use_absolute_efficiency,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    electricity.create_missing_power_plant_datasets()
    electricity.adjust_coal_power_plant_emissions()
    electricity.update_efficiency_of_solar_pv()
    electricity.correct_hydropower_water_emissions()
    electricity.create_region_specific_power_plants()

    if scenario["year"] >= 2020:
        electricity.adjust_aluminium_electricity_markets()

    if scenario["iam data"].electricity_mix is not None:
        electricity.update_electricity_markets()
    else:
        print("No electricity information found in IAM data. Skipping.")

    if scenario["iam data"].electricity_technology_efficiencies is not None:
        electricity.update_electricity_efficiency()
    else:
        print("No electricity efficiencies found in IAM data. Skipping.")

    electricity.relink_datasets()
    scenario["database"] = electricity.database
    scenario["index"] = electricity.index
    scenario["cache"] = electricity.cache

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["electricity"] = electricity.powerplant_map

    validate = ElectricityValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=electricity.database,
        iam_data=scenario["iam data"],
    )

    validate.run_electricity_checks()

    return scenario


def create_fuel_map(database, version, model) -> tuple[InventorySet, dict, dict]:
    """
    Create a mapping between ecoinvent fuel names and IAM fuel names.
    :param database: ecoinvent database
    :type database: list
    :return: mapping between ecoinvent fuel names and IAM fuel names
    :rtype: dict
    """

    mapping = InventorySet(database=database, version=version, model=model)
    fuel_map = mapping.generate_fuel_map()
    # reverse the fuel map to get a mapping from ecoinvent to premise
    fuel_map_reverse: Dict = {}

    for key, value in fuel_map.items():
        for v in list(value):
            fuel_map_reverse[v["reference product"]] = key

    return mapping, fuel_map, fuel_map_reverse


def select_or_interpolate(data, year, **kwargs):
    """
    Select IAM data at `year` if available, otherwise interpolate.
    kwargs are passed to .sel()
    """
    if year in data.coords["year"].values:
        return data.sel(year=year, **kwargs).values.item(0)
    return data.sel(**kwargs).interp(year=year).values.item(0)


def compute_time_weighted_mix(mix, region, year, period):
    interp_range = np.arange(year, year + period + 1)
    return dict(
        zip(
            mix.variables.values,
            mix.sel(region=region)
            .interp(year=interp_range, kwargs={"fill_value": "extrapolate"})
            .mean(dim="year")
            .values,
        )
    )


def make_generic_market_dataset(
    name, reference_product, region, comment, unit="kilowatt hour"
):
    return {
        "name": name,
        "reference product": reference_product,
        "location": region,
        "unit": unit,
        "code": str(uuid.uuid4().hex),
        "database": "",
        "comment": comment,
        "regionalized": True,
        "exchanges": [],
    }


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
    :vartype pathway: str
    :ivar year: year of the pathway (e.g., 2030)
    :vartype year: int

    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        use_absolute_efficiency: bool = False,
        cache: dict = None,
        index: dict = None,
    ) -> None:
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
        )
        self.mapping, self.fuel_map, self.fuel_map_reverse = create_fuel_map(
            self.database, self.version, self.model
        )
        self.powerplant_map = self.mapping.generate_powerplant_map()
        # reverse dictionary of self.powerplant_map
        self.powerplant_map_rev = {}
        for k, v in self.powerplant_map.items():
            for pp in list(v):
                self.powerplant_map_rev[pp["name"]] = k

        self.powerplant_fuels_map = self.mapping.generate_powerplant_fuels_map()
        self.powerplant_fuels_map = {
            k: [x["name"] for x in v] for k, v in self.powerplant_fuels_map.items()
        }

        self.production_per_tech = self.get_production_per_tech_dict()
        losses = get_losses_per_country(self.database)
        self.network_loss = {
            loc: get_production_weighted_losses(
                losses, self.geo.iam_to_ecoinvent_location(loc)
            )
            for loc in self.regions
        }
        self.system_model = system_model
        self.biosphere_dict = biosphere_flows_dictionary(self.version)
        self.use_absolute_efficiency = use_absolute_efficiency

        self.powerplant_min_efficiency, self.powerplant_max_efficiency = (
            self.mapping.generate_powerplant_efficiency_bounds()
        )

    @lru_cache
    def get_production_per_tech_dict(self) -> Dict[Tuple[str, str], float]:
        """
        Create a dictionary with tuples (technology, country) as keys
        and production volumes as values.
        :return: technology to production volume dictionary
        :rtype: dict
        """

        production_vols = {}

        for technology, datasets in self.powerplant_map.items():
            for dataset in datasets:
                for exc in ws.production(dataset):
                    # even if non-existent, we set a minimum value of 1e-9
                    # because if not, we risk dividing by zero!!!
                    production_vols[(dataset["name"], dataset["location"])] = max(
                        float(exc.get("production volume", 1e-9)), 1e-9
                    )

        return production_vols

    def check_for_production_volume(self, suppliers: List[dict]) -> List[dict]:
        # Remove suppliers that do not have a production volume
        return [
            supplier
            for supplier in suppliers
            if self.get_production_weighted_share(supplier, suppliers) != 0
        ]

    def get_production_weighted_share(
        self, supplier: dict, suppliers: List[dict]
    ) -> float:
        """
        Return the share of production of an electricity-producing dataset in a specific location,
        relative to the summed production of similar technologies in locations contained in
        the same IAM region.
        :param supplier: electricity-producing dataset
        :type supplier: wurst dataset
        :param suppliers: list of electricity-producing datasets
        :type suppliers: list of wurst datasets
        :return: share of production relative to the total population
        :rtype: float
        """

        supplier_key = (supplier["name"], supplier["location"])
        loc_production = float(self.production_per_tech.get(supplier_key, 0.0))

        total_production = sum(
            float(self.production_per_tech.get((s["name"], s["location"]), 0.0))
            for s in suppliers
        )

        # If a corresponding production volume is found.
        if total_production != 0:
            return loc_production / total_production
        # If not, we allocate an equal share of supply
        return 1 / len(suppliers)

    def create_new_markets_low_voltage(self) -> None:
        """
        Create low voltage market groups for electricity, by receiving medium voltage market groups as input
        and adding transformation and distribution losses. Transformation and distribution losses are taken from ei37.
        Contribution from solar power is added here as well, as most is delivered at low voltage,
        although CSP delivers at high voltage.
        Does not return anything. Modifies the database in place.
        """

        # Loop through the technologies
        technologies = [
            tech
            for tech in self.iam_data.electricity_mix.variables.values
            if "solar pv residential" in tech.lower()
        ]

        # Get the possible names of ecoinvent datasets
        ecoinvent_technologies = {
            technology: self.powerplant_map[technology] for technology in technologies
        }

        # Create an empty dataset
        generic_dataset = make_generic_market_dataset(
            name="market group for electricity, low voltage",
            reference_product="electricity, low voltage",
            region="",
            comment=f"Dataset created by `premise` from the IAM model {self.model} "
            f"using the pathway {self.scenario} for the year {self.year}.",
        )

        def generate_regional_markets(region: str, period: int) -> dict:
            new_dataset = copy.deepcopy(generic_dataset)
            new_dataset["location"] = region
            new_dataset["code"] = str(uuid.uuid4().hex)

            transf_loss = self.network_loss[region]["low"]["transf_loss"]
            distr_loss = self.network_loss[region]["low"]["distr_loss"]

            # Fetch ecoinvent regions contained in the IAM region
            ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

            possible_locations = [
                [region],
                ecoinvent_regions,
                ["RER"],
                ["RoW"],
                ["CH"],
            ]

            tech_suppliers = defaultdict(list)

            for technology, datasets in ecoinvent_technologies.items():
                suppliers, counter = [], 0

                try:
                    while len(suppliers) == 0:
                        suppliers = [
                            ds
                            for ds in datasets
                            if any(
                                ds["location"] == x for x in possible_locations[counter]
                            )
                        ]

                        counter += 1

                    for supplier in suppliers:
                        share = self.get_production_weighted_share(supplier, suppliers)
                        tech_suppliers[technology].append((supplier, share))

                    # remove suppliers that have a supply share inferior to 0.1%
                    tech_suppliers[technology] = [
                        supplier
                        for supplier in tech_suppliers[technology]
                        if supplier[1] > 0.001
                    ]
                    # rescale the shares so that they sum to 1
                    total_share = sum(
                        supplier[1] for supplier in tech_suppliers[technology]
                    )
                    tech_suppliers[technology] = [
                        (supplier[0], supplier[1] / total_share)
                        for supplier in tech_suppliers[technology]
                    ]

                except IndexError as exc:
                    if self.system_model == "consequential":
                        continue
                    raise IndexError(
                        f"Couldn't find suppliers for {technology} when looking for {ecoinvent_technologies[technology]}."
                    ) from exc

            # Create a time-weighted average mix
            if self.system_model == "consequential":
                electricity_mix = dict(
                    zip(
                        self.iam_data.electricity_mix.variables.values,
                        self.iam_data.electricity_mix.sel(
                            region=region, year=self.year
                        ).values,
                    )
                )

            else:
                # Create a time-weighted average mix
                electricity_mix = compute_time_weighted_mix(
                    self.iam_data.electricity_mix, region, self.year, period
                )

            production_volume = select_or_interpolate(
                self.iam_data.production_volumes,
                self.year,
                region=region,
                variables=self.iam_data.electricity_mix.variables.values,
            )

            # First, add the reference product exchange
            new_exchanges = [
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": 1,
                    "type": "production",
                    "production volume": production_volume,
                    "product": "electricity, low voltage",
                    "name": "market group for electricity, low voltage",
                    "unit": "kilowatt hour",
                    "location": region,
                    "regionalized": True,
                }
            ]

            if period != 0:
                # this dataset is for a period of time
                new_dataset["name"] += f", {period}-year period"
                new_dataset["comment"] = (
                    f" Average electricity mix over a {period}"
                    f"-year period {self.year}-{self.year + period}."
                )
                new_exchanges[0]["name"] += f", {period}-year period"

            # Second, add an input of sulfur hexafluoride (SF6) emission to compensate the transformer's leakage
            # And an emission of a corresponding amount
            # Third, transmission line and SF6 supply and emission

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 5.4e-8,
                    "amount": 5.4e-8,
                    "type": "technosphere",
                    "product": "sulfur hexafluoride, liquid",
                    "name": "market for sulfur hexafluoride, liquid",
                    "unit": "kilogram",
                    "location": (
                        "RER"
                        if "RER" in self.geo.iam_to_ecoinvent_location(region)
                        else "RoW"
                    ),
                }
            )

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 2.99e-9,
                    "amount": 2.99e-9,
                    "type": "biosphere",
                    "input": (
                        "biosphere3",
                        self.biosphere_dict[
                            (
                                "Sulfur hexafluoride",
                                "air",
                                "non-urban air or from high stacks",
                                "kilogram",
                            )
                        ],
                    ),
                    "name": "Sulfur hexafluoride",
                    "unit": "kilogram",
                    "categories": ("air", "non-urban air or from high stacks"),
                },
            )

            location = None
            for loc in ["CH", "CA-QC"]:
                if loc in self.geo.iam_to_ecoinvent_location(region):
                    location = loc
            if location is None:
                location = "RoW"

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 8.74e-8,
                    "amount": 8.74e-8,
                    "type": "technosphere",
                    "product": "distribution network, electricity, low voltage",
                    "name": "distribution network construction, electricity, low voltage",
                    "unit": "kilometer",
                    "location": location,
                }
            )

            # Fourth, add the contribution of solar power
            solar_amount = 0

            for technology in technologies:
                # If the solar power technology contributes to the mix
                if electricity_mix[technology] > 0:
                    # Contribution in supply
                    amount = electricity_mix[technology]
                    solar_amount += amount

                    for supplier, share in tech_suppliers[technology]:
                        new_exchanges.append(
                            {
                                "uncertainty type": 0,
                                "loc": float(amount * share),
                                "amount": float(amount * share),
                                "type": "technosphere",
                                "product": supplier["reference product"],
                                "name": supplier["name"],
                                "unit": supplier["unit"],
                                "location": supplier["location"],
                            }
                        )

            # Fifth, add:
            # * an input from the medium voltage market minus solar contribution, including distribution loss
            # * a self-consuming input for transformation loss

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 0,
                    "amount": (1 - solar_amount) * (1 + distr_loss),
                    "type": "technosphere",
                    "product": "electricity, medium voltage",
                    "name": (
                        "market group for electricity, medium voltage"
                        if period == 0
                        else f"market group for electricity, medium voltage, {period}-year period"
                    ),
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
                    "product": "electricity, low voltage",
                    "name": (
                        "market group for electricity, low voltage"
                        if period == 0
                        else f"market group for electricity, low voltage, {period}-year period"
                    ),
                    "unit": "kilowatt hour",
                    "location": region,
                }
            )

            new_dataset["exchanges"] = new_exchanges

            new_dataset.setdefault("log parameters", {}).update(
                {
                    "distribution loss": distr_loss,
                    "transformation loss": transf_loss,
                    "renewable share": solar_amount / (1 + distr_loss),
                }
            )
            return new_dataset

        if self.system_model == "consequential":
            periods = [
                0,
            ]
        else:
            periods = [0, 20, 40, 60]

        new_datasets = [
            generate_regional_markets(region, period)
            for region in self.regions
            for period in periods
            if region != "World"
        ]

        self.database.extend(new_datasets)

        for ds in new_datasets:
            self.write_log(ds)
            self.add_to_index(ds)

        new_world_dataset = self.generate_world_market(
            dataset=copy.deepcopy(generic_dataset),
            regions=self.regions,
        )
        self.database.append(new_world_dataset)
        self.write_log(new_world_dataset)

    def create_new_markets_medium_voltage(self) -> None:
        """
        Create medium voltage market groups for electricity, by receiving high voltage market groups as inputs
        and adding transformation and distribution losses.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        # Create an empty dataset
        generic_dataset = make_generic_market_dataset(
            name="market group for electricity, medium voltage",
            reference_product="electricity, medium voltage",
            region="",
            comment=f"Dataset created by `premise` from the IAM model {self.model} "
            f"using the pathway {self.scenario} for the year {self.year}.",
        )

        def generate_regional_markets(region: str, period: int) -> dict:

            new_dataset = copy.deepcopy(generic_dataset)
            new_dataset["location"] = region
            new_dataset["code"] = str(uuid.uuid4().hex)

            transf_loss = self.network_loss[region]["medium"]["transf_loss"]
            distr_loss = self.network_loss[region]["medium"]["distr_loss"]

            # fetch production volume
            production_volume = select_or_interpolate(
                self.iam_data.production_volumes,
                self.year,
                region=region,
                variables=self.iam_data.electricity_mix.variables.values,
            )

            # First, add the reference product exchange
            new_exchanges = [
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": 1,
                    "type": "production",
                    "production volume": production_volume,
                    "product": "electricity, medium voltage",
                    "name": "market group for electricity, medium voltage",
                    "unit": "kilowatt hour",
                    "location": region,
                }
            ]

            # Second, add:
            # * an input from the high voltage market, including transmission loss
            # * a self-consuming input for transformation loss

            if period != 0:
                # this dataset is for a period of time
                new_dataset["name"] += f", {period}-year period"
                new_dataset["comment"] = (
                    f" Average electricity mix over a {period}"
                    f"-year period {self.year}-{self.year + period}."
                )
                new_exchanges[0]["name"] += f", {period}-year period"

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 0,
                    "amount": 1 + distr_loss,
                    "type": "technosphere",
                    "product": "electricity, high voltage",
                    "name": (
                        "market group for electricity, high voltage"
                        if period == 0
                        else f"market group for electricity, high voltage, {period}-year period"
                    ),
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
                    "product": "electricity, medium voltage",
                    "name": (
                        "market group for electricity, medium voltage"
                        if period == 0
                        else f"market group for electricity, medium voltage, {period}-year period"
                    ),
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
                    "product": "sulfur hexafluoride, liquid",
                    "name": "market for sulfur hexafluoride, liquid",
                    "unit": "kilogram",
                    "location": (
                        "RER"
                        if "RER" in self.geo.iam_to_ecoinvent_location(region)
                        else "RoW"
                    ),
                }
            )
            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 5.4e-8,
                    "amount": 5.4e-8,
                    "type": "biosphere",
                    "input": (
                        "biosphere3",
                        self.biosphere_dict[
                            (
                                "Sulfur hexafluoride",
                                "air",
                                "non-urban air or from high stacks",
                                "kilogram",
                            )
                        ],
                    ),
                    "name": "Sulfur hexafluoride",
                    "unit": "kilogram",
                    "categories": ("air", "non-urban air or from high stacks"),
                },
            )

            # Fourth, transmission line
            location = None
            for loc in ["CH", "CA-QC"]:
                if loc in self.geo.iam_to_ecoinvent_location(region):
                    location = loc
            if location is None:
                location = "RoW"

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 1.8628e-8,
                    "amount": 1.8628e-8,
                    "type": "technosphere",
                    "product": "transmission network, electricity, medium voltage",
                    "name": "transmission network construction, electricity, medium voltage",
                    "unit": "kilometer",
                    "location": location,
                }
            )

            new_dataset["exchanges"] = new_exchanges

            new_dataset.setdefault("log parameters", {}).update(
                {
                    "distribution loss": distr_loss,
                    "transformation loss": transf_loss,
                    "renewable share": 0.0,
                }
            )

            return new_dataset

        if self.system_model == "consequential":
            periods = [
                0,
            ]
        else:
            periods = [0, 20, 40, 60]

        new_datasets = [
            generate_regional_markets(region, period)
            for region in self.regions
            for period in periods
            if region != "World"
        ]

        self.database.extend(new_datasets)

        for ds in new_datasets:
            self.write_log(ds)
            self.add_to_index(ds)

        new_world_dataset = self.generate_world_market(
            dataset=copy.deepcopy(generic_dataset),
            regions=self.regions,
        )
        self.database.append(new_world_dataset)
        self.write_log(new_world_dataset)

    def create_new_markets_high_voltage(self) -> None:
        """
        Create high voltage market groups for electricity, based on electricity mixes given by the IAM scenario.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        # Loop through the technologies
        technologies = [
            tech
            for tech in self.iam_data.electricity_mix.variables.values
            if "solar pv residential" not in tech.lower()
        ]

        # Get the possible names of ecoinvent datasets
        ecoinvent_technologies = {
            technology: self.powerplant_map[technology] for technology in technologies
        }

        generic_dataset = make_generic_market_dataset(
            name="market group for electricity, high voltage",
            reference_product="electricity, high voltage",
            region="",
            comment=f"Dataset created by `premise` from the IAM model {self.model} "
            f"using the pathway {self.scenario} for the year {self.year}.",
        )

        def generate_regional_markets(region: str, period: int) -> dict:

            new_dataset = copy.deepcopy(generic_dataset)
            new_dataset["location"] = region
            new_dataset["code"] = str(uuid.uuid4().hex)

            # Fetch ecoinvent regions contained in the IAM region
            ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)
            # Second, add transformation loss
            transf_loss = self.network_loss[region]["high"]["transf_loss"]

            # Fetch electricity-producing technologies contained in the IAM region
            # if they cannot be found for the ecoinvent locations concerned
            # we widen the scope to EU-based datasets, and RoW, and finally Switzerland

            possible_locations = [
                [region],
                ecoinvent_regions,
                ["RER"],
                ["RoW"],
                ["CH"],
                list(self.ecoinvent_to_iam_loc.keys()),
            ]

            tech_suppliers = defaultdict(list)

            for technology, datasets in ecoinvent_technologies.items():
                suppliers = []

                for counter in range(len(possible_locations)):
                    suppliers = [
                        ds
                        for ds in datasets
                        if any(ds["location"] == x for x in possible_locations[counter])
                    ]

                    if len(suppliers) > 0:
                        break

                for supplier in suppliers:
                    share = self.get_production_weighted_share(supplier, suppliers)
                    tech_suppliers[technology].append((supplier, share))

                # remove suppliers that have a supply share inferior to 0.1%
                tech_suppliers[technology] = [
                    supplier
                    for supplier in tech_suppliers[technology]
                    if supplier[1] > 0.001
                ]
                # rescale the shares so that they sum to 1
                total_share = sum(
                    supplier[1] for supplier in tech_suppliers[technology]
                )
                tech_suppliers[technology] = [
                    (supplier[0], supplier[1] / total_share)
                    for supplier in tech_suppliers[technology]
                ]

            if self.system_model == "consequential":
                electricity_mix = dict(
                    zip(
                        self.iam_data.electricity_mix.variables.values,
                        self.iam_data.electricity_mix.sel(
                            region=region, year=self.year
                        ).values,
                    )
                )

            else:
                electricity_mix = compute_time_weighted_mix(
                    self.iam_data.electricity_mix, region, self.year, period
                )

            # remove `solar pv residential` from the mix
            if "Solar PV Residential" in electricity_mix:
                del electricity_mix["Solar PV Residential"]
            # normalize the mix to 1
            total = sum(electricity_mix.values())
            electricity_mix = {
                tech: electricity_mix[tech] / total for tech in electricity_mix
            }

            # fetch production volume
            production_volume = select_or_interpolate(
                self.iam_data.production_volumes,
                self.year,
                region=region,
                variables=self.iam_data.electricity_mix.variables.values,
            )

            # First, add the reference product exchange
            new_exchanges = [
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": 1,
                    "type": "production",
                    "production volume": float(production_volume),
                    "product": "electricity, high voltage",
                    "name": "market group for electricity, high voltage",
                    "unit": "kilowatt hour",
                    "location": region,
                }
            ]

            new_exchanges.append(
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": transf_loss,
                    "type": "technosphere",
                    "product": "electricity, high voltage",
                    "name": "market group for electricity, high voltage",
                    "unit": "kilowatt hour",
                    "location": region,
                }
            )

            if period != 0:
                # this dataset is for a period of time
                new_dataset["name"] += f", {period}-year period"
                new_dataset["comment"] = (
                    f" Average electricity mix over a {period}"
                    f"-year period {self.year}-{self.year + period}."
                )
                new_exchanges[0]["name"] += f", {period}-year period"
                new_exchanges[-1]["name"] += f", {period}-year period"

            # calculate the share of renewable energy in the mix
            renewable_share = 0
            renewable_techs = [
                "solar",
                "wind",
                "geothermal",
                "hydro",
                "biomass",
                "biogas",
                "wave",
            ]
            for tech in electricity_mix:
                if any(x in tech.lower() for x in renewable_techs):
                    renewable_share += electricity_mix[tech]

            for technology in technologies:
                # If the given technology contributes to the mix
                if electricity_mix[technology] > 0:
                    # Contribution in supply
                    amount = electricity_mix[technology]

                    for supplier, share in tech_suppliers[technology]:
                        new_exchanges.append(
                            {
                                "uncertainty type": 0,
                                "loc": float(amount * share),
                                "amount": float(amount * share),
                                "type": "technosphere",
                                "product": supplier["reference product"],
                                "name": supplier["name"],
                                "unit": supplier["unit"],
                                "location": supplier["location"],
                            }
                        )

            new_dataset["exchanges"] = new_exchanges

            new_dataset.setdefault("log parameters", {}).update(
                {
                    "distribution loss": 0.0,
                    "transformation loss": transf_loss,
                    "renewable share": renewable_share / sum(electricity_mix.values()),
                }
            )

            return new_dataset

        if self.system_model == "consequential":
            periods = [
                0,
            ]
        else:
            periods = [0, 20, 40, 60]

        new_datasets = [
            generate_regional_markets(region, period)
            for period in periods
            for region in self.regions
            if region != "World"
        ]

        self.database.extend(new_datasets)

        for ds in new_datasets:
            self.write_log(ds)
            self.add_to_index(ds)

        new_world_dataset = self.generate_world_market(
            dataset=copy.deepcopy(generic_dataset),
            regions=self.regions,
        )
        self.database.append(new_world_dataset)
        self.write_log(new_world_dataset)

    def generate_world_market(
        self,
        dataset: dict,
        regions: List[str],
    ) -> dict:
        """
        Generate the world market for a given dataset and product variables.

        :param dataset: The dataset for which to generate the world market.
        :param regions: A dictionary of activity datasets, keyed by region.

        This function generates the world market exchanges for a given dataset and set of product variables.
        It first filters out non-production exchanges from the dataset, and then calculates the total production
        volume for the world using the given product variables. For each region, it calculates the share of the
        production volume and adds a technosphere exchange to the dataset with the appropriate share.

        """

        # rename location
        dataset["location"] = "World"
        dataset["code"] = str(uuid.uuid4().hex)

        # remove existing production exchange if any
        dataset["exchanges"] = [
            exc for exc in dataset["exchanges"] if exc["type"] != "production"
        ]

        if self.year in self.iam_data.production_volumes.coords["year"].values:
            production_volume = (
                self.iam_data.production_volumes.sel(
                    region=regions,
                    variables=self.iam_data.electricity_mix.variables.values,
                    year=self.year,
                )
                .sum(dim=["region", "variables"])
                .values.item(0)
            )
        else:
            production_volume = (
                self.iam_data.production_volumes.sel(
                    region=regions,
                    variables=self.iam_data.electricity_mix.variables.values,
                )
                .interp(year=self.year)
                .sum(dim=["region", "variables"])
                .values.item(0)
            )

        # add production exchange
        dataset["exchanges"].append(
            {
                "uncertainty type": 0,
                "loc": 1,
                "amount": 1,
                "type": "production",
                "production volume": production_volume,
                "product": dataset["reference product"],
                "name": dataset["name"],
                "unit": dataset["unit"],
                "location": "World",
            }
        )

        # Filter out non-production exchanges
        dataset["exchanges"] = [
            e for e in dataset["exchanges"] if e["type"] == "production"
        ]

        # Calculate share of production volume for each region
        for r in regions:
            if r == "World":
                continue

            if self.year in self.iam_data.production_volumes.coords["year"].values:
                share = (
                    self.iam_data.production_volumes.sel(
                        region=r,
                        variables=self.iam_data.electricity_mix.variables.values,
                        year=self.year,
                    ).sum(dim="variables")
                    / self.iam_data.production_volumes.sel(
                        region=[
                            x
                            for x in self.iam_data.production_volumes.region.values
                            if x != "World"
                        ],
                        variables=self.iam_data.electricity_mix.variables.values,
                        year=self.year,
                    ).sum(dim=["variables", "region"])
                ).values
            else:
                share = (
                    (
                        self.iam_data.production_volumes.sel(
                            region=r,
                            variables=self.iam_data.electricity_mix.variables.values,
                        ).sum(dim="variables")
                        / self.iam_data.production_volumes.sel(
                            region=[
                                x
                                for x in self.iam_data.production_volumes.region.values
                                if x != "World"
                            ],
                            variables=self.iam_data.electricity_mix.variables.values,
                        ).sum(dim=["variables", "region"])
                    )
                    .interp(
                        year=self.year,
                        kwargs={"fill_value": "extrapolate"},
                    )
                    .values
                )

            if np.isnan(share):
                print("Incorrect market share for", dataset["name"], "in", r)

            if share > 0:
                # Add exchange for the region
                exchange = {
                    "uncertainty type": 0,
                    "amount": share,
                    "type": "technosphere",
                    "product": dataset["reference product"],
                    "name": dataset["name"],
                    "unit": dataset["unit"],
                    "location": r,
                }
                dataset["exchanges"].append(exchange)

        return dataset

    def correct_hydropower_water_emissions(self) -> None:
        """
        Correct the emissions of water for hydropower plants.
        In Swiss datasets, water evaporation is too high.
        We use a new factor from Flury and Frischknecht (2021) to correct this.
        https://treeze.ch/fileadmin/user_upload/downloads/Publications/Case_Studies/Energy/flury-2012-hydroelectric-power-generation.pdf
        """

        water_factor = get_water_consumption_factors()

        hydropower_datasets = ws.get_many(
            self.database,
            *[
                ws.contains("name", "electricity production, hydro, reservoir"),
                ws.equals("location", "CH"),
                ws.equals("unit", "kilowatt hour"),
            ],
        )

        for name, flows in water_factor.items():
            for dataset in hydropower_datasets:
                if name in dataset["name"]:
                    for flow in flows:
                        for exc in ws.biosphere(
                            dataset,
                            ws.equals("name", flow["name"]),
                            ws.equals("unit", flow["unit"]),
                            ws.equals("categories", (flow["categories"],)),
                        ):
                            exc["amount"] = flow["amount"]

    def update_efficiency_of_solar_pv(self) -> None:
        """
        Update the efficiency of solar PV modules.
        We look at how many square meters are needed per kilowatt of installed capacity
        to obtain the current efficiency.
        Then we update the surface needed according to the projected efficiency.
        :return:
        """

        possible_techs = [
            "micro-Si",
            "single-Si",
            "multi-Si",
            "CIGS",
            "CIS",
            "CdTe",
            "perovskite",
            "GaAs",
        ]

        # TODO: check if IAM data provides efficiencies for PV panels and use them instead

        # efficiency of modules in the future
        module_eff = get_efficiency_solar_photovoltaics()

        datasets = ws.get_many(
            self.database,
            *[
                ws.contains("name", "photovoltaic"),
                ws.either(
                    ws.contains("name", "installation"),
                    ws.contains("name", "construction"),
                ),
                ws.doesnt_contain_any("name", ["market", "factory", "module"]),
                ws.equals("unit", "unit"),
            ],
        )

        for dataset in datasets:
            numbers = re.findall(r"[-+]?\d*\.\d+|\d+", dataset["name"])
            if not numbers:
                print(f"No numerical value found in dataset name: {dataset['name']}")
                continue

            power = float(numbers[0])

            if "mwp" in dataset["name"].lower():
                power *= 1000

            pv_tech = [
                i for i in possible_techs if i.lower() in dataset["name"].lower()
            ]

            if len(pv_tech) > 0:
                pv_tech = pv_tech[0]

            if pv_tech:
                scaling_factor = None
                for exc in ws.technosphere(
                    dataset,
                    ws.either(
                        ws.contains("name", "photovoltaic"),
                        ws.contains("name", "open ground"),
                    ),
                    ws.equals("unit", "square meter"),
                ):
                    surface = float(exc["amount"])
                    max_power = surface  # in kW, since we assume a constant 1,000W/m^2
                    current_eff = power / max_power

                    if self.year in module_eff.coords["year"].values:
                        new_mean_eff = module_eff.sel(
                            technology=pv_tech, year=self.year, efficiency_type="mean"
                        ).values
                        new_min_eff = module_eff.sel(
                            technology=pv_tech, year=self.year, efficiency_type="min"
                        ).values
                        new_max_eff = module_eff.sel(
                            technology=pv_tech, year=self.year, efficiency_type="max"
                        ).values
                    else:
                        new_mean_eff = (
                            module_eff.sel(technology=pv_tech, efficiency_type="mean")
                            .interp(
                                year=self.year, kwargs={"fill_value": "extrapolate"}
                            )
                            .values
                        )
                        new_min_eff = (
                            module_eff.sel(technology=pv_tech, efficiency_type="min")
                            .interp(
                                year=self.year, kwargs={"fill_value": "extrapolate"}
                            )
                            .values
                        )
                        new_max_eff = (
                            module_eff.sel(technology=pv_tech, efficiency_type="max")
                            .interp(
                                year=self.year, kwargs={"fill_value": "extrapolate"}
                            )
                            .values
                        )

                    # in case self.year <10 or >2050
                    new_mean_eff = np.clip(new_mean_eff, 0.1, 0.30)
                    new_min_eff = np.clip(new_min_eff, 0.1, 0.30)
                    new_max_eff = np.clip(new_max_eff, 0.1, 0.30)

                    # We only update the efficiency if it is higher than the current one.
                    if new_mean_eff.sum() >= current_eff:
                        scaling_factor = float(current_eff / new_mean_eff)
                        exc["amount"] *= scaling_factor
                        exc["uncertainty type"] = 5
                        exc["loc"] = float(exc["amount"])
                        exc["minimum"] = float(
                            exc["amount"] * (new_min_eff / new_mean_eff)
                        )
                        exc["maximum"] = float(
                            exc["amount"] * (new_max_eff / new_mean_eff)
                        )

                        dataset["comment"] = (
                            f"`premise` has changed the efficiency "
                            f"of this photovoltaic installation "
                            f"from {int(current_eff * 100)} pct. to {int(new_mean_eff * 100)} pt."
                        )

                        dataset.setdefault("log parameters", {}).update(
                            {"old efficiency": current_eff}
                        )
                        dataset.setdefault("log parameters", {}).update(
                            {"new efficiency": new_mean_eff}
                        )

                        # add to log
                        self.write_log(dataset=dataset, status="updated")

                # we also want to scale down the EoL dataset
                if scaling_factor:
                    for exc in ws.technosphere(
                        dataset,
                        ws.contains("name", "treatment"),
                        ws.equals("unit", "kilogram"),
                    ):
                        exc["amount"] *= scaling_factor

    def create_region_specific_power_plants(self):
        """
        Some power plant inventories are not native to ecoinvent
        but imported. However, they are defined for a specific location
        (mostly European), but are used in many electricity markets
        (non-European). Hence, we create region-specific versions of these datasets,
        to align inputs providers with the geographical scope of the region.

        """

        all_plants = []

        techs = [
            "Biomass CHP CCS",
            "Biomass ST",
            "Biomass ST CCS",
            "Biomass IGCC CCS",
            "Biomass IGCC",
            "Coal IGCC",
            "Coal PC CCS",
            "Coal CHP CCS",
            "Coal IGCC CCS",
            "Coal SC",
            "Gas CHP CCS",
            "Gas CC CCS",
            "Oil CC CCS",
            # "Oil ST",
            # "Oil CC",
            # "Coal PC",
            "Coal CF 80-20",
            "Coal CF 50-50",
            "Storage, Battery",
            "Storage, Hydrogen",
        ]

        datasets_to_duplicate = [
            dataset
            for technology, datasets in self.powerplant_map.items()
            for dataset in datasets
            if technology in techs
        ]

        datasets_to_duplicate = [
            ds
            for ds in self.database
            if "hydrogen storage, for grid-balancing" in ds["name"]
        ] + datasets_to_duplicate

        datasets_to_duplicate.extend(
            [
                ds
                for ds in self.database
                if any(
                    x in ds["name"]
                    for x in [
                        "carbon dioxide storage from",
                        "carbon dioxide storage at",
                        "carbon dioxide, captured from hard coal",
                        "carbon dioxide, captured from lignite",
                        "carbon dioxide, captured from natural gas",
                        "carbon dioxide, captured at wood burning",
                        "carbon dioxide, captured at hydrogen burning",
                    ]
                )
            ]
        )

        for dataset in datasets_to_duplicate:
            new_plants = self.fetch_proxies(
                datasets=[
                    dataset,
                ],
            )

            for new_plant in new_plants.values():
                self.add_to_index(new_plant)

            # we need to adjust the need for CO2 capture and storage
            # based on the electricity provider in the dataset
            # hence, we want to know how much CO2 is released
            # by each provider, and capture 90% of the amount

            if "CHP CCS" in self.powerplant_map_rev.get(dataset["name"], ""):
                for plant in new_plants.values():
                    co2_amount = 0

                    providers = [
                        e
                        for e in plant["exchanges"]
                        if e["type"] == "technosphere" and e["unit"] == "kilowatt hour"
                    ]

                    for provider in providers:
                        provider_ds = ws.get_one(
                            self.database,
                            ws.equals("name", provider["name"]),
                            ws.equals("location", provider["location"]),
                            ws.equals("reference product", provider["product"]),
                            ws.equals("unit", provider["unit"]),
                        )
                        co2_amount += sum(
                            f["amount"] * provider["amount"]
                            for f in ws.biosphere(
                                provider_ds,
                                ws.contains("name", "Carbon dioxide, "),
                            )
                        )

                    for exc in plant["exchanges"]:
                        if (
                            exc["type"] == "technosphere"
                            and exc["unit"] == "kilogram"
                            and exc["name"].startswith("carbon dioxide, captured")
                        ):
                            exc["amount"] = co2_amount * 0.9

                        if (
                            exc["type"] == "biosphere"
                            and exc["unit"] == "kilogram"
                            and exc["name"].startswith("Carbon dioxide, fossil")
                        ):
                            exc["amount"] = co2_amount * 0.9 * -1

            all_plants.extend(new_plants.values())

        self.database.extend(all_plants)

        for dataset in all_plants:
            self.write_log(dataset=dataset)

        # update self.powerplant_map
        self.powerplant_map = self.mapping.generate_powerplant_map()

    def update_electricity_efficiency(self) -> None:
        """
        This method modifies each ecoinvent coal, gas,
        oil and biomass dataset using data from the IAM scenario.
        Return a wurst database with modified datasets.

        :return: a wurst database, with rescaled electricity-producing datasets.
        :rtype: list
        """

        eff_labels = self.iam_data.electricity_technology_efficiencies.variables.values
        all_techs = self.iam_data.electricity_mix.variables.values

        technologies_map = self.get_iam_mapping(
            activity_map=self.powerplant_map,
            fuels_map=self.powerplant_fuels_map,
            technologies=list(set(eff_labels).intersection(all_techs)),
        )

        for technology in technologies_map:
            dict_technology = technologies_map[technology]

            for dataset in self.powerplant_map[technology]:
                if not self.is_in_index(dataset):
                    continue

                # Find current efficiency
                ei_eff = find_fuel_efficiency(
                    dataset=dataset,
                    fuel_filters=dict_technology["fuel filters"],
                    energy_out=3.6,
                    fuel_specs=self.fuels_specs,
                    fuel_map_reverse=self.fuel_map_reverse,
                )
                new_efficiency = 0

                if not self.use_absolute_efficiency:
                    iam_location = self.geo.ecoinvent_to_iam_location(
                        dataset["location"]
                    )
                    if (
                        iam_location
                        in self.iam_data.electricity_technology_efficiencies.coords[
                            "region"
                        ].values
                    ):
                        # Find relative efficiency change indicated by the IAM
                        scaling_factor = 1 / self.find_iam_efficiency_change(
                            data=self.iam_data.electricity_technology_efficiencies,
                            variable=technology,
                            location=iam_location,
                        )

                        new_efficiency = float(
                            np.clip(
                                ei_eff * 1 / scaling_factor,
                                self.powerplant_min_efficiency.get(technology, 0),
                                self.powerplant_max_efficiency.get(technology, 1.5),
                            )
                        )

                        scaling_factor = ei_eff / new_efficiency

                    else:
                        scaling_factor = 1

                else:
                    new_efficiency = self.find_iam_efficiency_change(
                        data=self.iam_data.electricity_technology_efficiencies,
                        variable=technology,
                        location=self.geo.ecoinvent_to_iam_location(
                            dataset["location"]
                        ),
                    )

                    # if ei_eff is different from 1 and if the new efficiency
                    # is not NaN or zero, we can rescale the exchanges
                    if (
                        ei_eff != 1
                        and new_efficiency != 0
                        and not np.isnan(new_efficiency)
                    ):
                        scaling_factor = ei_eff / new_efficiency
                    else:
                        scaling_factor = 1

                # ensure that the dataset has not already been adjusted
                if (
                    "new efficiency" not in dataset.get("log parameters", {})
                    and scaling_factor != 1
                ):

                    dataset.setdefault("log parameters", {}).update(
                        {
                            "old efficiency": ei_eff,
                            "new efficiency": new_efficiency,
                        }
                    )

                    self.update_ecoinvent_efficiency_parameter(
                        dataset, ei_eff, new_efficiency
                    )

                    # Rescale all the technosphere exchanges
                    # according to the change in efficiency between `year`
                    # and 2020 from the IAM efficiency values
                    rescale_exchanges(dataset, scaling_factor)

                    self.write_log(dataset=dataset, status="updated")

    def adjust_coal_power_plant_emissions(self) -> None:
        """
        Based on:
        Fetch data on coal power plants from external sources.
        Source:
        Oberschelp, C., Pfister, S., Raptis, C.E. et al.
        Global emission hotspots of coal power generation.
        Nat Sustain 2, 113–121 (2019).
        https://doi.org/10.1038/s41893-019-0221-6

        We adjust efficiency and emissions of coal power plants,
        including coal-fired CHPs.
        """

        coal_techs = ["Coal PC", "Coal CHP", "Coal SC", "Coal USC"]

        substances = [
            # ("CO2", "Carbon dioxide, fossil"),
            ("SO2", "Sulfur dioxide"),
            ("CH4", "Methane, fossil"),
            ("NOx", "Nitrogen oxides"),
            ("PM <2.5", "Particulate Matter, < 2.5 um"),
            ("PM 10 - 2.5", "Particulate Matter, > 2.5 um and < 10um"),
            ("PM > 10", "Particulate Matter, > 10 um"),
        ]

        for tech in coal_techs:
            if tech in self.powerplant_map:
                datasets = self.powerplant_map[tech]
                for dataset in datasets:
                    loc = dataset["location"][:2]
                    if loc in self.iam_data.coal_power_plants.country.values:
                        # Find current efficiency
                        ei_eff = find_fuel_efficiency(
                            dataset=dataset,
                            fuel_filters=self.powerplant_fuels_map[tech],
                            energy_out=3.6,
                            fuel_specs=self.fuels_specs,
                            fuel_map_reverse=self.fuel_map_reverse,
                        )

                        new_eff = self.iam_data.coal_power_plants.sel(
                            country=loc,
                            fuel=(
                                "Anthracite coal"
                                if "hard coal" in dataset["name"]
                                else "Lignite coal"
                            ),
                            CHP="co-generation" in dataset["name"],
                            variable="efficiency",
                        )

                        if not np.isnan(new_eff.values.item(0)):
                            # Rescale all the exchanges except for a few biosphere exchanges
                            scaling_factor = ei_eff / new_eff.values.item(0)

                            # limit scaling factor to 1.5
                            scaling_factor = min(scaling_factor, 1.5)
                            # set a floor value of 0.5
                            scaling_factor = max(scaling_factor, 0.5)

                            rescale_exchanges(
                                dataset,
                                scaling_factor,
                                # biosphere_filters=[
                                #    ws.doesnt_contain_any(
                                #        "name", [x[1] for x in substances]
                                #    )
                                # ],
                            )

                            dataset.setdefault("log parameters", {}).update(
                                {
                                    "ecoinvent original efficiency": ei_eff,
                                    "Oberschelp et al. efficiency": new_eff.values.item(
                                        0
                                    ),
                                    "efficiency change": ei_eff
                                    / new_eff.values.item(0),
                                }
                            )

                            if "comment" in dataset:
                                dataset["comment"] += (
                                    f" Efficiency updated from {ei_eff:.2f} to "
                                    f"{new_eff.values.item(0):.2f} "
                                    f"based on Oberschelp et al. (2019)."
                                )
                            else:
                                dataset["comment"] = (
                                    f"Efficiency updated from {ei_eff:.2f} to "
                                    f"{new_eff.values.item(0):.2f} "
                                    f"based on Oberschelp et al. (2019)."
                                )

                            self.update_ecoinvent_efficiency_parameter(
                                dataset, ei_eff, new_eff.values.item(0)
                            )

                        for substance in substances:
                            species, flow = substance

                            emission_factor = self.iam_data.coal_power_plants.sel(
                                country=loc,
                                fuel=(
                                    "Anthracite coal"
                                    if "hard coal" in dataset["name"]
                                    else "Lignite coal"
                                ),
                                CHP="co-generation" in dataset["name"],
                                variable=species,
                            ) / (
                                self.iam_data.coal_power_plants.sel(
                                    country=loc,
                                    fuel=(
                                        "Anthracite coal"
                                        if "hard coal" in dataset["name"]
                                        else "Lignite coal"
                                    ),
                                    CHP="co-generation" in dataset["name"],
                                    variable="generation",
                                )
                                * 1e3
                            )

                            if not np.isnan(emission_factor.values.item(0)):
                                for exc in ws.biosphere(dataset):
                                    if (
                                        exc["name"] == flow
                                        and exc.get(
                                            "categories",
                                            [
                                                None,
                                            ],
                                        )[0]
                                        == "air"
                                    ):
                                        scaling_factor = (
                                            emission_factor.values.item(0)
                                            / exc["amount"]
                                        )
                                        # limit scaling factor to 5
                                        scaling_factor = min(scaling_factor, 5)
                                        exc["amount"] *= scaling_factor

                                        dataset.setdefault("log parameters", {}).update(
                                            {
                                                f"{species} scaling factor": scaling_factor,
                                            }
                                        )

                                        if "comment" in dataset:
                                            dataset["comment"] += (
                                                f" {species} emissions updated from "
                                                f"{exc['amount']:.2e} to "
                                                f"{exc['amount'] * scaling_factor:.2e} "
                                                f"based on Oberschelp et al. (2019)."
                                            )
                                        else:
                                            dataset["comment"] = (
                                                f" {species} emissions updated from "
                                                f"{exc['amount']:.2e} to "
                                                f"{exc['amount'] * scaling_factor:.2e} "
                                                f"based on Oberschelp et al. (2019)."
                                            )

                        self.write_log(dataset=dataset, status="updated")

    def create_missing_power_plant_datasets(self) -> None:
        """
        Create missing power plant datasets.
        We use proxy datasets, copy them and rename them.
        """

        for tech, variable in load_electricity_variables().items():
            if not variable.get("exists in database", True):
                try:
                    original = list(
                        ws.get_many(
                            self.database,
                            ws.equals("name", variable["proxy"]["name"]),
                            ws.equals(
                                "reference product",
                                variable["proxy"]["reference product"],
                            ),
                        )
                    )[0]
                except IndexError:
                    continue

                # make a copy
                new_dataset = copy.deepcopy(original)
                new_dataset["name"] = variable["proxy"]["new name"]
                new_dataset["code"] = str(uuid.uuid4().hex)
                for e in ws.production(new_dataset):
                    e["name"] = variable["proxy"]["new name"]
                    if "input" in e:
                        del e["input"]

                # if `parameters` in dataset, delete them
                if "parameters" in new_dataset:
                    del new_dataset["parameters"]

                new_dataset["comment"] = (
                    "This dataset is a proxy dataset for a power plant. "
                    "It is used to create missing power plant datasets."
                )

                # update efficiency
                if "new efficiency" in variable["proxy"]:
                    new_eff = variable["proxy"]["new efficiency"]
                    ei_eff = find_fuel_efficiency(
                        dataset=new_dataset,
                        fuel_filters=self.powerplant_fuels_map[tech],
                        energy_out=3.6,
                        fuel_specs=self.fuels_specs,
                        fuel_map_reverse=self.fuel_map_reverse,
                    )
                    rescale_exchanges(new_dataset, ei_eff / new_eff)

                self.database.append(new_dataset)

                new_datasets = self.fetch_proxies(
                    datasets=[new_dataset],
                )

                for ds in new_datasets.values():
                    ds["name"] = variable["proxy"]["new name"]
                    ds["code"] = str(uuid.uuid4().hex)
                    for e in ws.production(ds):
                        e["name"] = variable["proxy"]["new name"]
                        if "input" in e:
                            del e["input"]

                    ds["comment"] = (
                        "This dataset is a proxy dataset for a power plant. "
                        "It is used to create missing power plant datasets."
                    )

                    self.add_to_index(ds)
                self.database.extend(new_datasets.values())

        mapping = InventorySet(self.database, model=self.model)
        self.powerplant_map = mapping.generate_powerplant_map()

        # reverse dictionary of self.powerplant_map
        self.powerplant_map_rev = {}
        for k, v in self.powerplant_map.items():
            for pp in list(v):
                self.powerplant_map_rev[pp["name"]] = k

    def adjust_aluminium_electricity_markets(self) -> None:
        """
        Aluminium production is a major electricity consumer.
        In Ecoinvent, aluminium producers have their own electricity markets.
        In the IAM, aluminium production is part of the electricity market.
        We need to adjust the electricity markets of aluminium producers by linking
        them to the regional electricity markets. However, some aluminium electricity
        markets are already deeply decarbonized because some smelters are powered by
        hydroelectricity.
        Hence, we choose to link aluminium producers to the regional electricity markets
        but only those that are not already decarbonized, meaning from those regions:

        * RoW
        * IAI Area, Africa
        * CN
        * IAI Area, South America
        * UN-OCEANIA
        * IAI Area, Asia, without China and GCC
        * IAI Area, Gulf Cooperation Council

        while we leave untouched the following regions:

        * IAI Area, Russia & RER w/o EU27 & EFTA
        * CA
        * IAI Area, EU27 & EFTA

        """

        LIST_AL_REGIONS = [
            "RoW",
            "IAI Area, Africa",
            "CN",
            "IAI Area, South America",
            "UN-OCEANIA",
            "IAI Area, Asia, without China and GCC",
            "IAI Area, Gulf Cooperation Council",
        ]

        for dataset in ws.get_many(
            self.database,
            *[
                ws.contains(
                    "name", "market for electricity, high voltage, aluminium industry"
                ),
                ws.equals("unit", "kilowatt hour"),
            ],
        ):

            if dataset["location"] in LIST_AL_REGIONS:
                # empty exchanges
                dataset["exchanges"] = [
                    e for e in dataset["exchanges"] if e["type"] == "production"
                ]

                # add the new electricity market
                dataset["exchanges"].append(
                    {
                        "name": f"market group for electricity, high voltage",
                        "product": f"electricity, high voltage",
                        "amount": 1,
                        "uncertainty type": 0,
                        "location": self.geo.ecoinvent_to_iam_location(
                            dataset["location"]
                        ),
                        "type": "technosphere",
                        "unit": "kilowatt hour",
                    }
                )

                self.write_log(dataset=dataset, status="updated")

    def update_electricity_markets(self) -> None:
        """
        Delete electricity markets. Create high, medium and low voltage market groups for electricity.
        Link electricity-consuming datasets to newly created market groups for electricity.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for electricity
        :rtype: list
        """

        list_to_empty = [
            "market group for electricity",
            "market for electricity",
            "electricity, high voltage, import",
            "electricity, high voltage, production mix",
        ]

        # we want to preserve some electricity-related datasets
        list_to_preserve = [
            "cobalt industry",
            "aluminium industry",
            "coal mining",
            "label-certified",
            "renewable energy products",
            "for reuse in municipal waste incineration",
            "Swiss Federal Railways",
            "to generic market for electricity",
        ]

        # We first need to empty 'market for electricity'
        # and 'market group for electricity' datasets

        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.contains("name", n) for n in list_to_empty]),
            ws.equals("unit", "kilowatt hour"),
            ws.doesnt_contain_any("name", list_to_preserve),
        ):
            # add tag
            dataset["has_downstream_consumer"] = False
            dataset["exchanges"] = [
                e for e in dataset["exchanges"] if e["type"] == "production"
            ]

            if "high voltage" in dataset["name"]:
                voltage = "high voltage"
            elif "medium voltage" in dataset["name"]:
                voltage = "medium voltage"
            else:
                voltage = "low voltage"

            dataset["exchanges"].append(
                {
                    "name": f"market group for electricity, {voltage}",
                    "product": f"electricity, {voltage}",
                    "amount": 1,
                    "uncertainty type": 0,
                    "location": self.ecoinvent_to_iam_loc[dataset["location"]],
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                }
            )

            self.write_log(dataset=dataset, status="updated")

            # list `market group for electricity` as "emptied"
            self.remove_from_index(dataset)

            # add new regional datasets to cache
            self.add_new_entry_to_cache(
                location=dataset["location"],
                exchange=dataset,
                allocated=[
                    {
                        "name": f"market group for electricity, {voltage}",
                        "reference product": f"electricity, {voltage}",
                        "unit": "kilowatt hour",
                        "location": self.ecoinvent_to_iam_loc[dataset["location"]],
                    },
                ],
                shares=[
                    1.0,
                ],
            )

        # We then need to create high voltage IAM electricity markets
        self.create_new_markets_high_voltage()
        self.create_new_markets_medium_voltage()
        self.create_new_markets_low_voltage()

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('old efficiency', '')}|"
            f"{dataset.get('log parameters', {}).get('new efficiency', '')}|"
            f"{dataset.get('log parameters', {}).get('transformation loss', '')}|"
            f"{dataset.get('log parameters', {}).get('distribution loss', '')}|"
            f"{dataset.get('log parameters', {}).get('renewable share', '')}|"
            f"{dataset.get('log parameters', {}).get('ecoinvent original efficiency', '')}|"
            f"{dataset.get('log parameters', {}).get('Oberschelp et al. efficiency', '')}|"
            f"{dataset.get('log parameters', {}).get('efficiency change', '')}|"
            f"{dataset.get('log parameters', {}).get('CO2 scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('SO2 scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('CH4 scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('NOx scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('PM <2.5 scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('PM 10 - 2.5 scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('PM > 10 scaling factor', '')}"
        )
