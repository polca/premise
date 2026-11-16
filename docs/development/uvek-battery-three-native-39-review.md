# Three native supply decisions — 16 November 2026

The current required source-3.9 import uses lithium hydroxide, PVC-product
waste disposal and tetraethyl orthosilicate. These decisions add no helper,
foreground omission or production/generator Python change. Exact references
were checked in ecoinvent3.9cut-off and both native releases.

- Orthosilicate now uses the existing named native `Tetraethylorthosilicat`
  instead of generic organic chemicals. Its published inventory has three
  inputs: silicon, ethanol and electricity. Reaction route, catalyst, grade,
  coproduct and emissions coverage are poorly documented. This is a specific
  chemical screening proxy, not a reconstruction of the source alcoholysis
  process. No extra reaction burden, acid credit or yield factor is added.
- Lithium hydroxide uses the complete published native supply per nominal kg.
  Native versus source lithium-carbonate demand is materially different
  (0.52631 versus1.6227970530005518kg/kg). Co-product allocation, yield, hydration
  state, purity and battery-grade performance remain explicit uncertainties.
  No guessed concentration, monohydrate or yield multiplier is applied.
- PVC uses the native complete Swiss building-product disposal service, with
  the existing factor minus one from the source negative waste reference to
  the native positive treatment reference. Native demolition, route mix,
  moisture, allocation and geography differ. No extra demolition or recovery
  credit is added around the complete native inventory.

Only the actually required source3.9cut-off contexts gain approval. An examined
source3.8reference does not gain permission without a demonstrated requirement.
Warnings remain in fresh and cached migration provenance; withdrawing a decision
also prevents a previously cached conversion.

The existing override ledger generates one changed orthosilicate rule and its
review. Existing recipes and both biosphere resources remain unchanged. The
integrity manifest is refreshed. Twenty-five focused Brightpath tests, eight
Premise tests and the migration-manifest test pass; six signed native-target
inventory solves are finite without a new database.

Actual battery preparation changes13to10pending first-cohort correspondences;
final energy changes21to19. Across the two review groups, the changes are27to10
and29to19 respectively. These are first-cohort counts, not whole-sector
qualification. See [verification](uvek-battery-three-native-39-verification.json).

The updated wheel also passes four fresh/cache/withdrawal checks using
GitHub-installed Brightpath134eb47 and installed Premise. Every loaded Premise
and Brightpath module was checked to be inside the isolated environment.
Thirty-six historical-override regressions pass after correcting an existing
test fixture; the original conflict reproduces on baseline35307d9. This changes
the test's authoring-stage input, without changing the production generator.
