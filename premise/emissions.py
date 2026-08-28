"""
Integrates projections regarding emissions of hot pollutants
from GAINS.
"""

import copy
import math
from collections import defaultdict
from functools import lru_cache
from typing import Union

import numpy as np
import xarray as xr
import yaml
from numpy import ndarray

from .activity_maps import GAINS_MAPPING, get_mapping
from .filesystem_constants import DATA_DIR
from .geomap import Geomap
from .logger import create_logger
from .provenance import record_change_event
from .inventory_store import (
    CompactInventoryStore,
    get_scenario_inventory,
    replace_scenario_inventory,
)
from .utils import rescale_exchange
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    InventorySet,
    List,
)

logger = create_logger("emissions")

EI_POLLUTANTS = DATA_DIR / "GAINS_emission_factors" / "GAINS_ei_pollutants.yaml"


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def _update_emissions(scenario, version, system_model, gains_scenario):

    if scenario["iam data"].gains_data_IAM is None:
        print("No pollutant emissions scenario data available -- skipping")
        return scenario

    store = scenario.get("_inventory_store")
    if isinstance(store, CompactInventoryStore):
        emissions = Emissions.from_inventory_store(
            store=store,
            year=scenario["year"],
            model=scenario["model"],
            pathway=scenario["pathway"],
            iam_data=scenario["iam data"],
            version=version,
            system_model=system_model,
            gains_scenario=gains_scenario,
        )
        affected_activity_keys = emissions.update_emissions_in_store(store)
        scenario.setdefault("_validation_direct_targets", {})["emissions"] = {
            "activity_keys": sorted(affected_activity_keys, key=repr)
        }
        return scenario

    emissions = Emissions(
        database=get_scenario_inventory(scenario),
        year=scenario["year"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        version=version,
        system_model=system_model,
        gains_scenario=gains_scenario,
    )

    emissions.update_emissions_in_database()
    replace_scenario_inventory(scenario, emissions.database)

    return scenario


class Emissions(BaseTransformation):
    """
    Class that modifies emissions of hot pollutants
    according to GAINS projections.
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
        gains_scenario: str,
    ):
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
        )

        self.version = version
        self.gains_IAM = self.prepare_data(iam_data.gains_data_IAM)
        self._compile_gains_factor_lookup()
        self.ei_pollutants = fetch_mapping(EI_POLLUTANTS)
        self.gains_pollutant = {v: k for k, v in self.ei_pollutants.items()}
        self.gains_scenario = gains_scenario

        mapping = InventorySet(self.database)
        self.gains_map = mapping.generate_gains_mapping()
        self.rev_gains_map = {}

        for s in self.gains_map:
            for t in self.gains_map[s]:
                self.rev_gains_map[t["name"]] = s

    @classmethod
    def from_inventory_store(
        cls,
        *,
        store: CompactInventoryStore,
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        gains_scenario: str,
    ) -> "Emissions":
        """Create an updater without materialising the compact graph."""

        updater = object.__new__(cls)
        updater.database = None
        updater.iam_data = iam_data
        updater.model = model
        updater.regions = iam_data.regions
        updater.geo = Geomap(model=model)
        updater.scenario = pathway
        updater.year = year
        updater.version = version
        updater.system_model = system_model
        updater.cache = {}
        updater.index = {}
        updater.gains_IAM = updater.prepare_data(iam_data.gains_data_IAM)
        updater._compile_gains_factor_lookup()
        updater.ei_pollutants = fetch_mapping(EI_POLLUTANTS)
        updater.gains_pollutant = {
            value: key for key, value in updater.ei_pollutants.items()
        }
        updater.gains_scenario = gains_scenario

        locations = {
            activity.get("location")
            for _, activity, _ in store._iter_storage_activities()
            if activity.get("location") is not None
        }
        locations.update(("LA", "FJ", "GN"))
        updater.ecoinvent_to_iam_loc = {
            location: updater.geo.ecoinvent_to_iam_location(location)
            for location in locations
        }
        updater.iam_to_ecoinvent_loc = defaultdict(list)
        for location, region in updater.ecoinvent_to_iam_loc.items():
            updater.iam_to_ecoinvent_loc[region].append(location)
        updater.rev_gains_map = updater._compile_store_gains_mapping(store)
        return updater

    @staticmethod
    def _compile_store_gains_mapping(
        store: CompactInventoryStore,
    ) -> dict[str, str]:
        """Compile legacy GAINS contains/mask filters against store metadata."""

        activities = {
            activity_id: activity
            for activity_id, activity, _ in store._iter_storage_activities()
        }
        strings: dict[str, dict[str, set[int]]] = {
            "name": defaultdict(set),
            "reference product": defaultdict(set),
        }
        for activity_id, activity in activities.items():
            for field_name in strings:
                value = activity.get(field_name)
                if isinstance(value, str):
                    strings[field_name][value].add(activity_id)

        predicate_cache: dict[tuple[str, str], set[int]] = {}

        def containing(field_name: str, term: str) -> set[int]:
            key = (field_name, term)
            if key not in predicate_cache:
                predicate_cache[key] = {
                    activity_id
                    for value, activity_ids in strings.get(field_name, {}).items()
                    if term in value
                    for activity_id in activity_ids
                }
            return predicate_cache[key]

        def normalise(filters, *, default_field="name") -> dict[str, list[str]]:
            if isinstance(filters, str):
                return {default_field: [filters]}
            if isinstance(filters, list):
                return {default_field: filters}
            return {
                field_name: values if isinstance(values, list) else [values]
                for field_name, values in (filters or {}).items()
            }

        reverse: dict[str, str] = {}
        mappings = get_mapping(filepath=GAINS_MAPPING, var="ecoinvent_aliases")
        all_activity_ids = set(activities)
        for sector, specification in mappings.items():
            candidates = all_activity_ids
            for field_name, terms in normalise(specification.get("fltr")).items():
                field_matches: set[int] = set()
                for term in terms:
                    field_matches.update(containing(field_name, term))
                candidates = candidates.intersection(field_matches)
            excluded: set[int] = set()
            for field_name, terms in normalise(specification.get("mask")).items():
                for term in terms:
                    excluded.update(containing(field_name, term))
            for activity_id in candidates.difference(excluded):
                reverse[activities[activity_id]["name"]] = sector
        return reverse

    @staticmethod
    def add_world_region(data: xr.DataArray) -> xr.DataArray:
        """
        Add a ``World`` region by summing absolute GAINS emissions.

        The emissions update later converts absolute emissions to scaling factors
        relative to 2020. Summing before that conversion gives ``World`` datasets
        an emissions-weighted global correction factor instead of leaving them
        unchanged or averaging regional ratios.
        """

        if "region" not in data.dims:
            return data

        has_world = "World" in data.coords["region"].values
        regional_data = data.drop_sel(region="World") if has_world else data
        world_data = regional_data.sum(dim="region", skipna=True).expand_dims(
            region=["World"]
        )

        if has_world:
            data = data.drop_sel(region="World")

        return xr.concat([data, world_data], dim="region")

    def prepare_data(self, data):

        def _safe_divide(x):
            return xr.where((np.isnan(x)) | (x == 0), 1, x)

        data = self.add_world_region(data)
        base = data.sel(year=2020, method="nearest")

        if self.year in data.coords["year"]:
            year_slice = data.sel(year=self.year)
        else:
            year_slice = data.interp(year=self.year)

        data = year_slice / _safe_divide(base)

        # replace 0 values with 1
        data = xr.where((np.isnan(data)) | (data == 0), 1, data)

        return data

    def _compile_gains_factor_lookup(self) -> None:
        """Keep GAINS selections in NumPy instead of rebuilding xarray indexes."""

        ordered = self.gains_IAM.transpose("region", "pollutant", "sector")
        self._gains_factor_values = np.asarray(ordered.values)
        self._gains_region_index = {
            value: position
            for position, value in enumerate(ordered.coords["region"].values)
        }
        self._gains_pollutant_index = {
            value: position
            for position, value in enumerate(ordered.coords["pollutant"].values)
        }
        self._gains_sector_index = {
            value: position
            for position, value in enumerate(ordered.coords["sector"].values)
        }

    @staticmethod
    def _rescaled_exchange_updates(exchange, scaling_factor: float) -> dict:
        """Return the exact field changes made by Wurst's uncertainty rescaler."""

        updates = {"amount": exchange["amount"] * scaling_factor}
        uncertainty_type = exchange.get("uncertainty type")
        if uncertainty_type not in {1, 2, 3, 4, 5}:
            return updates

        if "loc" in exchange:
            updates["loc"] = (
                exchange["loc"] + math.log(scaling_factor)
                if uncertainty_type == 2
                else exchange["loc"] * scaling_factor
            )
        if "scale" in exchange and uncertainty_type != 2:
            updates["scale"] = exchange["scale"] * abs(scaling_factor)
        for bound in ("minimum", "maximum"):
            if bound in exchange:
                updates[bound] = exchange[bound] * scaling_factor
        return updates

    def update_emissions_in_database(self):
        for ds in self.database:
            name = ds["name"]
            loc = ds["location"]

            if name in self.rev_gains_map:
                iam_loc = self.ecoinvent_to_iam_loc.get(loc)
                if iam_loc and iam_loc in self.gains_IAM.coords["region"]:
                    sector = self.rev_gains_map[name]
                    self.update_pollutant_emissions(
                        ds,
                        sector,
                        regions=self.gains_IAM.region.values,
                    )
                    self.write_log(ds, status="updated")

    def update_emissions_in_store(
        self, store: CompactInventoryStore
    ) -> frozenset[tuple[str, str, str]]:
        """Patch hot-pollutant exchanges directly in a compact transaction."""

        relevant = set(self.ei_pollutants)
        regions = self.gains_IAM.region.values
        affected_activity_keys = set()
        with store.transaction("sector:emissions") as transaction:
            for activity_id, dataset, exchange_ids in store._iter_storage_activities():
                name = dataset["name"]
                location = dataset["location"]
                sector = self.rev_gains_map.get(name)
                iam_location = self.ecoinvent_to_iam_loc.get(location)
                if (
                    sector is None
                    or not iam_location
                    or iam_location not in self.gains_IAM.coords["region"]
                ):
                    continue

                log_parameters = copy.deepcopy(dataset.get("log parameters", {}))
                log_changed = False
                for exchange_id in exchange_ids:
                    exchange = store._storage_exchange(exchange_id)
                    if (
                        exchange.get("type") != "biosphere"
                        or exchange.get("name") not in relevant
                    ):
                        continue
                    gains_pollutant = self.ei_pollutants[exchange["name"]]
                    scaling_factor = self.find_gains_emissions_change(
                        pollutant=gains_pollutant,
                        location=(location if location in regions else iam_location),
                        sector=sector,
                    )
                    if not 1 > scaling_factor > 0:
                        continue
                    if gains_pollutant in log_parameters:
                        continue

                    transaction.patch_exchange(
                        exchange_id,
                        self._rescaled_exchange_updates(
                            exchange,
                            scaling_factor,
                        ),
                        activity_id=activity_id,
                    )
                    if "GAINS sector" not in log_parameters:
                        log_parameters["GAINS sector"] = sector
                    log_parameters[gains_pollutant] = scaling_factor
                    log_changed = True

                if log_changed:
                    transaction.patch_activity(
                        activity_id,
                        {"log parameters": log_parameters},
                    )
                    affected_activity_keys.add(
                        (
                            dataset.get("name"),
                            dataset.get("reference product"),
                            dataset.get("location"),
                        )
                    )
                log_dataset = dict(dataset)
                if log_changed:
                    log_dataset["log parameters"] = log_parameters
                self.write_log(log_dataset, status="updated")
        return frozenset(affected_activity_keys)

    def update_pollutant_emissions(
        self, dataset: dict, sector: str, regions: list
    ) -> dict:
        """
        Update pollutant emissions based on GAINS data.
        We apply a correction factor equal to the relative
        change in emissions compared to 2020

        :param dataset: dataset to adjust non-CO2 emission for
        :param sector: GAINS industrial sector to look up
        :return: Does not return anything. Modified in place.
        """

        # Update biosphere exchanges according to GAINS emission values
        relevant = set(self.ei_pollutants)
        biosphere_excs = [
            exc
            for exc in dataset["exchanges"]
            if exc["type"] == "biosphere" and exc["name"] in relevant
        ]

        for exc in biosphere_excs:
            gains_pollutant = self.ei_pollutants[exc["name"]]
            scaling_factor = self.find_gains_emissions_change(
                pollutant=gains_pollutant,
                location=(
                    dataset["location"]
                    if dataset["location"] in regions
                    else self.ecoinvent_to_iam_loc[dataset["location"]]
                ),
                sector=sector,
            )

            if 1 > scaling_factor > 0:
                if gains_pollutant not in dataset.get("log parameters", {}):
                    rescale_exchange(exc, scaling_factor, remove_uncertainty=False)

                    logp = dataset.setdefault("log parameters", {})
                    if "GAINS sector" not in logp:
                        logp["GAINS sector"] = sector
                    logp[gains_pollutant] = scaling_factor

        return dataset

    @lru_cache
    def find_gains_emissions_change(
        self, pollutant: str, location: str, sector: str
    ) -> Union[ndarray, float]:
        """
        Return the relative change in emissions compared to 2020
        for a given pollutant, location and sector.
        :param pollutant: name of pollutant
        :param sector: name of technology/sector
        :param location: location of emitting dataset
        :model: GAINS model
        :return: a
        """

        if not hasattr(self, "_gains_factor_values"):
            self._compile_gains_factor_lookup()

        sf = self._gains_factor_values[
            self._gains_region_index[location],
            self._gains_pollutant_index[pollutant],
            self._gains_sector_index[sector],
        ].item()

        if np.isnan(sf) or sf == 0.0:
            return 1.0

        return float(sf)

    def write_log(self, dataset, status="created"):
        """Record a structured emissions provenance event."""

        record_change_event(self, dataset, status, sector="emissions")
