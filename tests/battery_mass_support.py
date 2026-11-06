"""Exercise the runtime representation with actual bundled capacity suppliers."""

import json

from bw2io import ExcelImporter

from premise.battery_mass import SPEC_KEY, convert_battery_mass_inputs
from premise.filesystem_constants import INVENTORY_DIR


def with_capacity_suppliers(datasets):
    marked = [d for d in datasets if SPEC_KEY in d]
    if not marked:
        return datasets
    wanted = set()
    for dataset in marked:
        value = dataset[SPEC_KEY]
        spec = json.loads(value) if isinstance(value, str) else value
        supplier = spec["capacity supplier"]
        wanted.add(
            tuple(
                supplier[k] for k in ("name", "reference product", "location", "unit")
            )
        )
    providers = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data
    selected = [
        d
        for d in providers
        if tuple(d.get(k) for k in ("name", "reference product", "location", "unit"))
        in wanted
    ]
    return datasets + selected


def activate_static_vehicle_batteries(datasets, family=None):
    return convert_battery_mass_inputs(with_capacity_suppliers(datasets), family)
