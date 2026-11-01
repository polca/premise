"""Legacy module imports for public callers and existing serialized caches."""

import importlib
import importlib.abc
import importlib.util
import sys
import types

MODULE_ALIASES = {
    "premise.new_database": "premise.database.new_database",
    "premise.incremental": "premise.database.incremental",
    "premise.data_collection": "premise.iam.data_collection",
    "premise.geomap": "premise.iam.geomap",
    "premise.marginal_mixes": "premise.iam.marginal_mixes",
    "premise.scenario_downloader": "premise.iam.scenario_downloader",
    "premise.external": "premise.iam.external",
    "premise.external_data_validation": "premise.iam.external_data_validation",
    "premise.pathways": "premise.iam.pathways",
    "premise.system_models": "premise.iam.system_models",
    "premise.activity_maps": "premise.inventory.activity_maps",
    "premise.clean_datasets": "premise.inventory.clean_datasets",
    "premise.inventory_copy": "premise.inventory.inventory_copy",
    "premise.inventory_imports": "premise.inventory.inventory_imports",
    "premise.inventory_normalization": "premise.inventory.inventory_normalization",
    "premise.inventory_store": "premise.inventory.inventory_store",
    "premise.filesystem_constants": "premise.core.filesystem_constants",
    "premise.utils": "premise.core.utils",
    "premise.efficiency_bounds": "premise.core.efficiency_bounds",
    "premise.transformation": "premise.core.transformation",
    "premise.runtime_cache": "premise.core.runtime_cache",
    "premise.cache_cleanup": "premise.core.cache_cleanup",
    "premise.logger": "premise.core.logger",
    "premise._bw2_backend_compat": "premise.core._bw2_backend_compat",
    "premise._fast_sqlite": "premise.core._fast_sqlite",
    "premise.change_report": "premise.reporting.change_report",
    "premise.provenance": "premise.reporting.provenance",
    "premise.report": "premise.reporting.report",
    "premise._report_inputs": "premise.reporting._report_inputs",
    "premise._report_parallel": "premise.reporting._report_parallel",
    "premise.score_comparison": "premise.reporting.score_comparison",
    "premise.metal_input_comparison": "premise.reporting.metal_input_comparison",
    "premise.export_payload": "premise.export.export_payload",
    "premise.brightway2": "premise.export.brightway2",
    "premise.brightway25": "premise.export.brightway25",
    "premise.olca_export": "premise.export.olca_export",
    "premise.simapro_export": "premise.export.simapro_export",
    "premise.scenario_array": "premise.export.scenario_array",
    "premise.validation_framework": "premise.validation.validation_framework",
    "premise.sector_validation": "premise.validation.sector_validation",
    "premise.battery_mass": "premise.transport.battery_mass",
    "premise.car_energy": "premise.transport.car_energy",
    "premise.vehicle_battery": "premise.transport.vehicle_battery",
    "premise.vehicle_emissions": "premise.transport.vehicle_emissions",
    "premise.vehicle_energy": "premise.transport.vehicle_energy",
    "premise.vehicle_energy_use": "premise.transport.vehicle_energy_use",
    "premise.vehicle_hydrogen": "premise.transport.vehicle_hydrogen",
    "premise.vehicle_lpg": "premise.transport.vehicle_lpg",
    "premise.battery": "premise.batteries.battery",
    "premise.cement": "premise.materials.cement",
    "premise.steel": "premise.materials.steel",
    "premise.metals": "premise.materials.metals",
    "premise.mining": "premise.materials.mining",
    "premise.metals_rules": "premise.materials.metals_rules",
    "premise.metals_resource_boundaries": "premise.materials.metals_resource_boundaries",
    "premise.fkm_gasket": "premise.materials.fkm_gasket",
    "premise.ptfe_gasket": "premise.materials.ptfe_gasket",
    "premise.transformer_core": "premise.materials.transformer_core",
    "premise.pipeline_coating": "premise.materials.pipeline_coating",
    "premise.photovoltaic": "premise.electricity.photovoltaic",
    "premise.renewables": "premise.electricity.renewables",
    "premise.heat": "premise.energy.heat",
    "premise.heat_data": "premise.energy.heat_data",
    "premise.biomass": "premise.energy.biomass",
    "premise.final_energy": "premise.energy.final_energy",
    "premise.carbon_dioxide_removal": "premise.emissions.carbon_dioxide_removal",
    "premise.iam_variables_mapping.extract_mapping": "premise.iam.extract_mapping",
}


class _AliasLoader(importlib.abc.Loader):
    def __init__(self, target):
        self.target = target

    def create_module(self, spec):
        module = importlib.import_module(self.target)
        self.canonical_spec = module.__spec__
        self.canonical_loader = module.__loader__
        return module

    def exec_module(self, module):
        # Import machinery assigns the alias spec to the returned module.
        # Keep canonical relative imports, reloads and serialization coherent.
        module.__spec__ = self.canonical_spec
        module.__loader__ = self.canonical_loader

    def get_code(self, fullname):
        # Support legacy `python -m premise.<module>` worker invocations too.
        return compile(
            "import runpy; runpy.run_module("
            + repr(self.target)
            + ", run_name='__main__', alter_sys=True)",
            "<premise module compatibility>",
            "exec",
        )


class _AliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        target = MODULE_ALIASES.get(fullname)
        if target:
            return importlib.util.spec_from_loader(fullname, _AliasLoader(target))


def install():
    if not any(isinstance(finder, _AliasFinder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _AliasFinder())


class _PublicModule(types.ModuleType):
    """Keep assignment to established package APIs visible to their implementation."""

    def __setattr__(self, name, value):
        implementation = sys.modules.get(self.__dict__.get("_premise_implementation"))
        if (
            not name.startswith("__")
            and implementation is not None
            and name in vars(implementation)
        ):
            setattr(implementation, name, value)
        super().__setattr__(name, value)

    def __delattr__(self, name):
        implementation = sys.modules.get(self.__dict__.get("_premise_implementation"))
        if (
            not name.startswith("__")
            and implementation is not None
            and name in vars(implementation)
        ):
            delattr(implementation, name)
            if name not in self.__dict__:
                return
        super().__delattr__(name)


def install_public_module(name, implementation):
    module = sys.modules[name]
    module.__dict__["_premise_implementation"] = implementation
    module.__class__ = _PublicModule
