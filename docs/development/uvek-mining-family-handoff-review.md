# Mining: explicit background handoff and treatment-role mappings

The real UVEK 2026 / ecoinvent 3.10 biosphere mining diagnostic now passes
inventory preparation, the shared update and exhaustive graph validation for
encrypted REMIND SSP2-NPi 2050. This resolves the electricity-label cycle error;
it does not complete the prospective tailings-market integration.

## Why the small Python change is necessary

`NewDatabase` already forwards its explicit source family and biosphere version
to biomass, electricity, heat and other adapted sectors. Mining did not receive
them, so its base transformation and inventory filters defaulted to ecoinvent.
Its validation baseline used UVEK electricity product aliases while the updated
graph did not. The apparent new 4,156-node cycle differed only in 232 electricity
product labels. Normalizing a private copy reproduced all 57 baseline cycles
without changing quantities, uncertainty or supplier keys.

The runtime fix is limited to two files: one entry in `NewDatabase`'s existing
handoff list, and forwarding the same arguments through the mining wrapper and
constructor to `BaseTransformation` and `InventorySet`. YAML cannot supply a
missing Python argument. Defaults preserve the original ecoinvent call. No new
sector engine, public argument or validation suppression is introduced.

`Mining.update_tailings_treatment` is unchanged, including its algorithm and
ordering. Its AST hash matches the pre-change branch. All original treatment
shares and ecoinvent filters also remain unchanged. The adjacent receipt records
these comparisons and the real-run results.

## Mapping and role ledger

The existing tailings YAML now supplies four explicit UVEK aliases:

- The complete native `Disposal, sulfidic tailings, off-site` service is the
  impoundment role, matched by exact name, product and kg unit. Its documentation
  describes a mechanically stable impoundment, short-/long-term groundwater
  leachate, land use and spontaneous renaturation. It retains reference +1 kg.
  Its generic composition/geography and long-term model remain screening limits;
  regional clones do not invent ore- or climate-specific parameters.
- Backfilling, flocculation/flotation and roasting/leaching use their three
  reviewed published CLIC slugs and explicit role-binding keys. They retain
  their negative waste references, rather than borrowing native name matches.

The retained-workbook identity ledger now assigns those three exact roots to
`tailings_activities.yaml` roles. The maintainer generator reads that existing
YAML as well as IAM-variable files, so future regeneration retains the entries.
No coefficients, source identities, other role bindings or workbook hashes were
changed. Existing CLIC eligibility and unit checks remain active: renamed
published records can retain their role, but a wrong unit or unrelated slug
cannot claim it.

## Verification and remaining work

The real run retains every one of the 11,947 native source codes, with source
database metadata unchanged. All four roles match one original provider and
create 12 regional clones each: 48 in total. Native impoundment references stay
+1; the three CLIC treatment references stay -1. Quantities, uncertainty fields
and formulas are unchanged for the 57 original native impoundment consumers.
The source database is read only.

Seventeen focused mapping/requirements checks, 115 mapping/validation
regressions and 48 database/handoff checks pass (the groups overlap). Black
passes; the new test file passes Ruff, and existing Ruff findings in the changed
legacy files are unchanged. The ordinary ecoinvent wrapper call is covered.

The public mining gate remains closed. This diagnostic bypasses only that gate;
it does not disable validation and does not export a database. The result still
has zero sulfidic-tailings market datasets. Native positive-reference consumers
must reach the configured regional fate mix, including the three negative-waste
CLIC treatments, without duplicating the native impoundment inventory or turning
treatment into a credit. That market/consumer handoff, its geographic coverage,
signed LCIs and validated export are the next work. Passing graph validation or
creating regional treatment clones alone does not qualify mining or finish the
remaining-sector goal.
