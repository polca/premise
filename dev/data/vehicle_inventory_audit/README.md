# Vehicle inventory audit artifacts

The CSV files preserve direct noise exchanges generated with the October 2026
passenger-car and truck inventory refreshes. Premise does not import these
exchanges.

The JSON files preserve source revisions, generation metadata, and hashes for
generation inputs and excluded audit artifacts. Regression tests verify that
their generation metadata matches the provenance embedded in the vehicle
activities, but Premise does not read the JSON files at runtime.

None of the files in this directory are runtime package data.

## Shared component reuse

`component-reuse-plan.json` pins the reviewed source workbooks and 28 explicit
model-specific aliases for 15 existing canonical suppliers.
`shared-component-reuse.json` records the applied cleanup, original component
metadata, source hashes, canonical recipe fingerprints, and consumer counts.
Canonical amounts and uncertainty remain unchanged; model-specific metadata is
retained here and references remain traceable in consumer exchange comments.

`python scripts/deduplicate_vehicle_components.py` checks the reviewed source
state without writing; `--apply` applies that exact campaign's cleanup. Repeated
execution on the unchanged result is a no-op. A regenerated or otherwise changed
source must be reviewed again rather than bypassing the checksum guards. The
component-supplier regression tests reject reintroduced aliases or dangling
references, including final-energy and rail consumers.
