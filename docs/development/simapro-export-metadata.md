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

The public `NewDatabase.write_db_to_simapro()` Brightpath writer applies both
helpers to its detached payload. Previously generated CSVs do not acquire these
metadata corrections. See the [SimaPro export guide](../user_guide/export/simapro.rst)
for setup, outputs, and the classification/coverage report.

## Integration verification, 28 September 2026

The integrated writer was run on the reviewed ecoinvent 3.12 cutoff snapshot
containing 42,770 processes and 1,386,053 source exchanges. All inventory and
parameter rows match the earlier Brightpath comparison CSV exactly; only export
dates differ. All 490,895 emitted technosphere references resolve uniquely, and
process identifiers are nonempty and unique.

The export report records 1,961 classification fallbacks, 4,351 inventory
indicators excluded during preparation, and 5,881 Brightpath-blacklisted biosphere
exchanges. This reproduces the reviewed conversion behavior; it does not validate
native SimaPro import, LCIA equivalence, or the historical scenario build.

The integration requires the accompanying Brightpath unit-label additions and
the `fossil well` resource-subcompartment mapping. These are ordinary library
mapping resources; the production adapter does not patch Brightpath functions.
