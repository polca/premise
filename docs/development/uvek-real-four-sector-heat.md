# Clean actual-scenario heat verification

The standard NewDatabase workflow successfully updates biomass, electricity, heat
and cement together for encrypted REMIND SSP2-NPi 2050, then writes a Brightway
database. The source is BAFU:2026-reimported in the regular Brightway project
bafu-2026-ecospold2-biosphere-310, with ecoinvent-3.10-biosphere.

This repeat used a detached clean worktree whose tracked contents match Premise
bc1bfb10, and installed Brightpath 587bfd9. Unrelated local GAINS review changes
were excluded. The encrypted IAM files were accessed through a private symlink;
no key or licensed inventory was committed. No IAM values were replaced with
controlled test arrays.

The export has 13,891 activities, including 1,944 new activities. Independent
Brightway calculations for positive and negative unit demand for every new
activity give 3,888 finite LCIs, with exact sign reversal. All technosphere links
resolve inside the exported database; biosphere links use the selected database.
There are 13 markets each for secondary, building and industrial heat, including
EUR and NEU. All heat-market supplier coefficients sum to one.

Exhaustive graph validation reports no issues under ruleset 9. The 108 electricity
time-period markets retain positive self-loss coefficients, up to 0.0414263.
Convergence is explicitly proved for 109 feedback components, with maximum
absolute convergence upper bound 0.991963. No loss quantity was removed to pass
validation. Zero-scaled clinker inputs have deterministic uncertainty.

Three small generic corrections were necessary: normalize transformed inventory
before store certification; allow the existing strict convergence proof to assess
fully declared new feedback components; and include capitalized native heat names
in heat-efficiency validation. Their focused test groups passed (155 graph/store/
database tests and 59 heat/filter tests; overlapping groups are not additive).
The independent name-filter audit expands eligible activities from 97 to 265 and
finds no heat-efficiency issues. The actual ecoinvent 3.12 five-sector regression
also passed before the capitalization-only heat-filter correction.

The machine-readable receipt is `uvek-real-four-sector-heat-verification.json`.
Private full reports and inspection scripts remain in the task workspace. The
mapping-specific screening warnings, allocation boundaries, geographical proxies,
CSP reuse-credit omission and uncertainty limitations still apply. A finite LCI
and graph validation do not establish scientific equivalence to ecoinvent.

This qualifies one real four-sector scenario. Other real IAM scenarios, the
combined fuel update with end-use carbon checks, and the remaining registered
sectors are unfinished. Controlled individual-role tests remain separate evidence.
