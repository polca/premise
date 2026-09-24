# Metadata for Brightpath SimaPro exports

Apply these helpers to the detached, prepared export payload before constructing
the Brightpath inventory:

```python
from premise.simapro_export import (
    assign_simapro_category_paths,
    assign_simapro_provenance,
)

assign_simapro_category_paths(data)
metadata = assign_simapro_provenance(data, scenario, version, system_model)
inventory = SimaProInventory.from_data(
    data,
    background_profile=profile,
    database_name=database_name,
    metadata=metadata,
)
```

The folder helper uses the same ISIC hierarchy and fallbacks as openLCA without
changing product/waste-treatment status. Waste categories must still be resolved
by the chosen export policy.

The provenance helper supplies missing Generator, System description and External
documents fields. It records the running Premise and Brightpath versions, exact
ecoinvent version and system model, scenario model/pathway/year, and the Premise
documentation link. The returned document metadata includes the referenced system
description record, which must be passed to `from_data` as shown above.

Existing non-empty process metadata and source comments are preserved. Helpers
modify only the detached payload and are idempotent for the same export context.
Brightpath already supports these metadata fields; no Premise-specific defaults
are added to its generic renderer.

The local comparison workflow `export_isic_folders.py` applies both helpers.
The default legacy exporter is unchanged. Previously generated CSVs do not
retroactively acquire these metadata corrections.
