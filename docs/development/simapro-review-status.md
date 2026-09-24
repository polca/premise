# SimaPro export review status

Review decisions as of 2026-09-25. The other SimaPro comparison reports in this
directory are chronological snapshots, not statements of the latest code behavior.

## Accepted decisions

- Retain Brightpath's classifications for the 260 reviewed disagreements against
  the original legacy export: recovered fertiliser and energy, the reviewed ordinary
  products/services, negative waste references, and the confirmed effluent treatment.
- Preserve uncertainty, source numeric precision, parameters, paragraph formatting,
  and the separate Electricity/heat input section.
- Preserve complete supplier names and reject ambiguous serialized supplier labels.
- Use the openLCA ISIC hierarchy for folders independently of waste identification.
- Fill Geography from location, supply export provenance from Premise, and align
  default ecoinvent process display names with legacy conventions and system model.
- Keep dedicated SimaPro process identifiers; do not append UUIDs to comments.
- Apply explicit waste categories to the 30 confirmed negative-reference processes.
  The earlier count of 51 sign differences was corrected: 21 other cases have
  positive, non-unit production quantities and remain ordinary products.

## Implemented scope

Brightpath implements the naming, sign/uncertainty, water-unit, classification,
folder-path and display-metadata behavior with regression tests. Premise has corrected
CPC and reviewed SimaPro mappings plus preparation helpers for folder paths and
provenance. The local comparison workflow uses those helpers; the default public
Premise SimaPro export still uses the legacy exporter. A production Brightpath backend
has not yet replaced it.

The last full CSV comparison predates the folder, Geography, provenance, display-name
and final reviewed-reference corrections. Those later changes were verified through
tests and focused real-renderer checks, but require another full export to include
them in a scenario CSV. Supplier labels in the last generated CSV were unique and
every technosphere label matched one included process.

## Deferred work

- Formate/formic-acid/thallium correspondence requires a trusted SimaPro reference.
- The blacklist and unmatched/excluded biosphere flows were explicitly deferred.
- Multiple source flows mapped to one emitted label were also deferred.
- Remaining amount differences need attribution; comparison counts overlap with
  accepted classification and naming changes and are not standalone error counts.
- Unresolved classification cases still receive explicit legacy categories in the
  comparison harness. Generic Brightpath inference rejects unresolved cases.
- Native SimaPro import and LCIA equivalence have not been validated.

Licensed inventory snapshots, generated CSVs and detailed per-dataset audit files
remain local and are excluded from commits.
