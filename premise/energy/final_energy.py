"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from ..core.declarative import canonical_provider_kind_operation
from ..core.transformation import BaseTransformation, InventorySet
from ..factorization import register_factorization_shared_operation
from ..iam.data_collection import IAMDataCollection
from ..inventory.inventory_store import (
    get_scenario_inventory,
    replace_scenario_inventory,
)
from ..transport.battery_mass import convert_battery_mass_inputs


def _declare_provider_kind_policies() -> None:
    """Declare final-energy relinking policies without inspecting live providers."""

    families = (
        ("natural gas, high pressure", "cubic meter"),
        ("heat, district or industrial, natural gas", "megajoule"),
    )
    for family, unit in families:
        operation = canonical_provider_kind_operation(
            source_name=f"market for {family}",
            target_names=(f"market group for {family}",),
            product=family,
            unit=unit,
        )
        if operation is not None:
            register_factorization_shared_operation(operation.to_dict())


def _update_final_energy(
    scenario,
    version,
    system_model,
):

    if scenario["iam data"].final_energy_use is None:
        print("No final energy scenario data available -- skipping")
        return scenario

    _declare_provider_kind_policies()

    final_energy = FinalEnergy(
        database=get_scenario_inventory(scenario),
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
    )

    # A final-energy-only update still reconstructs equipment from the vehicle
    # sources. Restore their declared capacity route if transport was skipped.
    convert_battery_mass_inputs(final_energy.database, transformation=final_energy)
    final_energy.regionalize_heating_datasets()

    # Reconstruct scoped transport wrappers from the already updated regional
    # vehicle service and production inventories. This propagates scenario
    # energy and battery sizing once, before final supplier relinking.
    from ..transport.vehicle_energy_use import (
        SOURCE_KEY,
        VehicleLookup,
        normalize,
        select_source,
        source_name,
    )

    vehicles = VehicleLookup(final_energy.database)
    for dataset in final_energy.database:
        name = source_name(dataset["name"])
        if name and SOURCE_KEY in dataset and dataset.get("unit") == "megajoule":
            if dataset.get("emptied"):
                continue
            source = select_source(vehicles, name, dataset["location"])
            normalize(dataset, source, vehicles)

    final_energy.relink_datasets()
    replace_scenario_inventory(scenario, final_energy.database)
    scenario["index"] = final_energy.index
    scenario["cache"] = final_energy.cache

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["final energy"] = final_energy.final_energy_map

    return scenario


class FinalEnergy(BaseTransformation):
    """
    Class that creates heating datasets based on IAM output data.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :ivar year: year, from :attr:`.NewDatabase.year`

    """

    def __init__(
        self,
        database: list[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict | None = None,
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
        )
        self.version = version

        mapping = InventorySet(database=database, version=version, model=model)
        self.final_energy_map = mapping.generate_final_energy_map()

    def regionalize_heating_datasets(self):

        self.process_and_add_activities(
            mapping=self.final_energy_map,
            production_volumes=self.iam_data.production_volumes,
        )
