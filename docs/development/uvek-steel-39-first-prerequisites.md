# First steel prerequisite review

Four exact ecoinvent 3.9 cut-off correspondences are now reviewed for native UVEK
2026 with ecoinvent 3.10 biosphere. This resolves the first failing steel source
cohort, not the complete steel transformation.

- Global high-pressure gas: replace the incorrect venting match with complete
  native delivered fuel supply. The helper supplies 36 MJ per original m3 using
  Premise's documented nominal gas reference. The actual global source's net
  calorific property is unavailable; this is an explicit energy-functional
  screening assumption, not a measured source property or equal-volume/state/
  composition proof. Retain complete native upstream supply and network losses
  once, preserve caller volume and separate stack emissions, and add no burner.
- Waste zeolite: use generic inert-landfill operation and facility, matching the
  actual source treatment's two components. The old suggested native construction-
  waste landfill adds waste-specific leaching that the source does not model, so
  it is not used. Retain all nine original collection-freight rows and their
  uncertainty, without merging or renormalization. The negative source reference
  becomes a positive physical treatment service with factor -1; both treatment
  inputs remain, including 1.48e-9 facility units per kg. Native geography, fleets,
  background and landfill-model differences remain screening limitations.
- Liquid ammonia: retain kg of anhydrous NH3, without nitrogen-content or solution
  scaling. The complete native 85% reforming/15% partial-oxidation supply and its
  delivery replace the source market, allocated minor co-product and freight.
- Iron pellets: retain complete native pellet production per kg. Do not substitute
  ore or pig iron, add a second sintering burden or infer a contained-metal factor.
  The source Quebec/RoW mix is replaced by native global production.

These decisions use standing autonomous approval for similar cases. Technology,
geography, allocation, vintage, internal uncertainty and unquantified score
limitations remain explicit. Only the inspected 3.9 cut-off scope is added;
other releases, inverse and cross-system-model routes do not inherit approval.

Brightpath commit 014c1b3 contains two changed mapping rules, three derived native
recipes, pinned scientific evidence and a replayable generator step. Every prior
helper/review is unchanged. Eighty-five focused Brightpath tests pass, covering
signs, caller uncertainty/formulae, foreground preservation, unreviewed-profile
refusal, atomic replay and related regressions. Fourteen signed native-supplier
LCIs are finite with exact sign reversal; the 11,947-activity source is unchanged.

Premise's strict fresh/cached harmonization test passes for all four inputs,
including all declared helper quantities/uncertainty and approval withdrawal
after warming the cache. All 758 exact approval fingerprints validate, preserving
the previous 754 scopes. The regular conda environment installs the pushed GitHub
commit, and the colleague guide/requirements pin is updated. No runtime Python
module changes in either repository are needed. The broader Premise approval
suite subsequently passed 834 compatibility/approval and fuel-carbon tests.

The actual encrypted REMIND steel diagnostic now advances to retained-workbook
source-biosphere validation: 141 unresolved exchanges spanning 13 legacy flow
identities. Parent-only air/water compartments are valid in the catalog; the
flow names differ. Publisher UUID or reviewed migration evidence is needed before
canonicalizing metal names with oxidation states. No emissions are dropped and
no validation is bypassed. Steel remains publicly gated, with native filters,
transformation, physical validation and export still unfinished.

The biosphere-name frontier has since been resolved through the existing
correspondences; see `uvek-retained-biosphere-name-normalization.md`. The retained
workbook now passes all exact source-link checks and stops at 52 unreviewed
technosphere correspondences. See `uvek-steel-retained-technosphere-frontier.json`.
