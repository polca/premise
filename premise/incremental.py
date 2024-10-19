from datetime import datetime

from .new_database import (
    NewDatabase,
    _update_biomass,
    _update_electricity,
    _update_dac,
    _update_cement,
    _update_steel,
    _update_fuels,
    _update_heat,
    _update_battery,
    _update_emissions,
    _update_vehicles,
    _update_external_scenarios,
)
from .utils import dump_database
from copy import copy
from tqdm import tqdm

import pickle

SECTORS = {
    "electricity": "electricity",
    "biomass": "biomass",
    "materials": ["cement", "steel"],
    "fuels": ["fuels", "heat"],
    "battery": "battery",
    "transport": ["cars", "two_wheelers", "trucks", "buses", "trains"],
    "others": ["emissions", "dac"],
    "external": "external",
}


class IncrementalDatabase(NewDatabase):
    """
    Class for creating an incremental database. Incremental databases allow measuring the
    effects of sectoral updates. The class inherits from the NewDatabase class.
    """
    def update(self, sectors: [str, list, None] = None) -> None:
        """
        Update the database with the specified sectors.

        :param sectors: A list of sectors to update. If None, all sectors will be updated incrementally.
        :type

        """

        sector_update_methods = {
            "biomass": {
                "func": _update_biomass,
                "args": (self.version, self.system_model),
            },
            "electricity": {
                "func": _update_electricity,
                "args": (self.version, self.system_model, self.use_absolute_efficiency),
            },
            "dac": {"func": _update_dac, "args": (self.version, self.system_model)},
            "cement": {
                "func": _update_cement,
                "args": (self.version, self.system_model),
            },
            "steel": {"func": _update_steel, "args": (self.version, self.system_model)},
            "fuels": {"func": _update_fuels, "args": (self.version, self.system_model)},
            "heat": {"func": _update_heat, "args": (self.version, self.system_model)},
            "battery": {
                "func": _update_battery,
                "args": (self.version, self.system_model),
            },
            "emissions": {
                "func": _update_emissions,
                "args": (self.version, self.system_model, self.gains_scenario),
            },
            "cars": {
                "func": _update_vehicles,
                "args": ("car", self.version, self.system_model),
            },
            "two_wheelers": {
                "func": _update_vehicles,
                "args": ("two-wheeler", self.version, self.system_model),
            },
            "trucks": {
                "func": _update_vehicles,
                "args": ("truck", self.version, self.system_model),
            },
            "buses": {
                "func": _update_vehicles,
                "args": ("bus", self.version, self.system_model),
            },
            "trains": {
                "func": _update_vehicles,
                "args": ("train", self.version, self.system_model),
            },
            "external": {
                "func": _update_external_scenarios,
                "args": (
                    self.version,
                    self.system_model,
                ),
            },
        }

        if sectors is None:
            sectors = list(SECTORS.keys())

        new_scenarios = []

        applied_sectors = []
        for scenario in self.scenarios:
            scenario["database"] = pickle.loads(pickle.dumps(self.database, -1))
            scenario_sectors = []
            for sector in sectors:
                label = f"(... + {sector})"
                scenario_sectors.append(sector)

                if sector == "external" and "external" not in scenario["pathway"]:
                    continue

                new_scenario = scenario.copy()
                new_scenario["pathway"] += label
                new_scenarios.append(new_scenario)

                applied_sectors.append(copy(scenario_sectors))

        self.scenarios = new_scenarios

        with tqdm(total=len(self.scenarios), ncols=70) as pbar_outer:
            for s, scenario in enumerate(self.scenarios):
                scenario["database"] = pickle.loads(pickle.dumps(self.database, -1))

                for sector in applied_sectors[s]:
                    func_names = SECTORS[sector]

                    if isinstance(func_names, str):
                        func_names = [func_names]

                    for func_name in func_names:
                        # Prepare the function and arguments
                        update_func = sector_update_methods[func_name]["func"]
                        fixed_args = sector_update_methods[func_name]["args"]
                        scenario = update_func(scenario, *fixed_args)
                dump_database(scenario)
                pbar_outer.update()

        print("Done!\n")

    def write_increment_db_to_brightway(
        self,
        name: str = f"super_db_{datetime.now().strftime('%d-%m-%Y')}",
        filepath: str = None,
        file_format: str = "excel",
    ) -> None:
        """
        Write the superstructure database to a Brightway2 database.
        """

        self.write_superstructure_db_to_brightway(
            name, filepath, file_format, preserve_original_column=True
        )
