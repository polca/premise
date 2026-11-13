"""
module to create a variant of the onshore and offshore wind
turbine datasets to represent the direct-drive technology.

"""

import copy
import uuid
import warnings

from ..inventory.activity_maps import InventorySet, get_mapping
from ..core.utils import DATA_DIR
from ..core.logger import create_logger
from ..reporting.provenance import record_change_event
from ..inventory.inventory_store import (
    get_scenario_inventory,
    replace_scenario_inventory,
)
from ..core.transformation import BaseTransformation, IAMDataCollection, List, np, ws

logger = create_logger("wind_turbine")


def _update_wind_turbines(
    scenario, version, system_model, family="ecoinvent", biosphere_version=None
):
    wind_turbine = WindTurbine(
        database=get_scenario_inventory(scenario),
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
        family=family,
        biosphere_version=biosphere_version,
    )

    wind_turbine.create_direct_drive_turbines()

    replace_scenario_inventory(scenario, wind_turbine.database)
    scenario["index"] = wind_turbine.index
    scenario["cache"] = wind_turbine.cache

    return scenario


def relink(dataset, suppliers=None) -> dict:
    """
    Relink technosphere exchanges to the new datasets.
    """

    for exc in ws.technosphere(
        dataset,
        ws.equals("unit", "unit"),
        ws.exclude(ws.contains("name", "connection")),
    ):
        if (
            suppliers is not None
            and (
                exc["name"],
                exc.get("product", exc.get("reference product")),
                exc["location"],
                exc["unit"],
            )
            not in suppliers
        ):
            continue
        exc["name"] += ", direct drive"
        # Explicit native input keys still identify the original supplier.
        # Resolve the renamed purchase by its new identity during normal linking.
        exc.pop("input", None)

    return dataset


class WindTurbine(BaseTransformation):
    """
    Class that create additional wind turbine datasets
    to represent the direct-drive technology.

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
        cache: dict = None,
        index: dict = None,
        family: str = "ecoinvent",
        biosphere_version: str = None,
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
            family=family,
            biosphere_version=biosphere_version,
        )
        self.system_model = system_model
        mapping = InventorySet(
            database=database, version=version, model=model, family=family
        )
        self.powerplant_map = mapping.generate_powerplant_map()
        self.variant_mapping = get_mapping(
            DATA_DIR / "renewables" / "wind_turbines.yaml", f"{family}_aliases"
        ).get("direct drive")

    def create_direct_drive_turbines(self):
        """
        Create direct-drive wind turbine datasets.
        """

        datasets_terms = [
            "electricity production, wind, <1MW turbine, onshore",
            "electricity production, wind, >3MW turbine, onshore",
            "electricity production, wind, 1-3MW turbine, onshore",
            "electricity production, wind, 1-3MW turbine, offshore",
            "wind power plant construction",
            "wind turbine construction",
            "market for wind power plant",
            "market for wind turbine",
        ]

        candidates = ws.get_many(
            self.database,
            ws.either(*[ws.contains("name", tech) for tech in datasets_terms]),
            ws.exclude(ws.contains("name", "direct drive")),
        )
        suppliers = None
        if self.variant_mapping is not None:
            mapping = InventorySet(self.database, family=self.family)
            candidates = mapping.generate_sets_from_filters(
                {"direct drive": self.variant_mapping}
            )["direct drive"]
            for identity in self.variant_mapping["exact_identities"]:
                if not any(
                    all(ds.get(key) == value for key, value in identity.items())
                    for ds in candidates
                ):
                    raise ValueError(
                        f"Missing native wind variant template: {identity!r}."
                    )
            for dataset in candidates:
                references = list(ws.production(dataset))
                if (
                    len(references) != 1
                    or references[0]["amount"]
                    != self.variant_mapping["reference_amount"]
                    or isinstance(references[0]["amount"], (bool, np.bool_))
                    or references[0].get("uncertainty type", 0) != 0
                    or references[0].get("formula")
                    or references[0].get("unit", dataset["unit"]) != dataset["unit"]
                ):
                    raise ValueError(
                        "Native wind variant requires a deterministic +1 reference."
                    )
            suppliers = {
                (ds["name"], ds["reference product"], ds["location"], ds["unit"])
                for ds in candidates
            }
            warnings.warn(
                self.variant_mapping["limitations"], UserWarning, stacklevel=2
            )

        new_datasets, processed = [], []
        for dataset in candidates:
            key = (dataset["name"], dataset["reference product"], dataset["location"])
            if key not in processed:
                dataset_copy = self.create_dataset_copy(dataset, "direct drive")
                dataset_copy = relink(dataset_copy, suppliers)
                if self.variant_mapping is not None:
                    dataset_copy["comment"] += (
                        "\n" + self.variant_mapping["limitations"]
                    )
                new_datasets.append(dataset_copy)
                processed.append(key)

        self.database.extend(new_datasets)

    def create_dataset_copy(self, dataset, suffix):
        dataset_copy = copy.deepcopy(dataset)
        dataset_copy["name"] += f", {suffix}"
        dataset_copy["code"] = str(uuid.uuid4().hex)

        if "comment" not in dataset_copy:
            dataset_copy["comment"] = ""

        dataset_copy["comment"] += (
            f" This dataset represents the {suffix} technology "
            f"variant of the {self.family} dataset."
        )

        for exc in ws.production(dataset_copy):
            exc["name"] = dataset_copy["name"]
            if "input" in exc:
                del exc["input"]

        self.write_log(dataset_copy)
        return dataset_copy

    def write_log(self, dataset, status="created"):
        """Record a structured renewable-technology provenance event."""

        record_change_event(self, dataset, status, sector="renewable")
