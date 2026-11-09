# Required wastewater markets in the UVEK electricity pilot

Four ecoinvent 3.9 cut-off wastewater-average markets are required: RoW,
Canada–Québec, Switzerland and Europe without Switzerland. Each has two sewer
collection inputs, one treatment input and 43 direct elementary exchanges for
untreated wastewater. Replacing each entire market with a fully treated sewage
service would remove those untreated emissions and change the treated fraction.

The reviewed composition preserves all original market exchanges separately,
including signed amounts and marginal uncertainty. Negative waste references
become positive services through factor −1. Treatment demand changes sign once;
positive collection demands and the original direct emissions retain their
quantities. No market share is aggregated, normalized or omitted.

The original treated fractions are approximately 41.27% for RoW, 81.32% for
Québec, 98.11% for Switzerland and 87.30% for Europe without Switzerland. All
172 direct untreated-emission entries remain in the four helpers. Their 43
publisher UUIDs, units, complete receiving compartments and emission/resource
roles are checked against the installed ecoinvent 3.10 biosphere. Label or CAS
changes remain warnings, without molecular or speciation factors.

## Collection per original cubic metre

Both source sewer-construction services explicitly deliver collection for 1 m³
of wastewater. Their declared network lifetime is 100 years, including a
renovation after 70 years. Their references are throughput services, not cubic
metres of pipe material or pipe length.

Native class-3 sewage treatment already contains a municipal sewer-grid input
of 2.1782 × 10⁻⁷ km per treated m³. Its supplier explicitly describes a municipal
network of about 109.4 km and 24,864 population equivalents, with the same
100-year lifetime and renovation after 70 years. Preserve that native per-m³
collection coefficient and its uncertainty as the screening basis for each
original source rural/urban collection provider.

Copy the installed native class-3 treatment inventory into one shared foreground
plant-only helper, removing only this municipal sewer-grid exchange. Retain all
other 29 technosphere and 105 biosphere entries and their uncertainty. Four
source-region treatment wrappers reuse that single helper. The copied inventory
plus its isolated grid exchange reconstructs the original native service exactly
per m³; no negative avoided-treatment inventory or LCI subtraction is introduced.
The installed native database itself remains unchanged.

Apply collection to the original collection volume and plant-only treatment to
the original treated fraction. Each market therefore retains one m³ of collection
and its independently specified treated volume. Municipal collection is counted
once, including for the original untreated fraction; it is not duplicated inside
the treatment helper or scaled by the treatment fraction.

A separate comparison of native class-2 residential and treatment-only records
found an additional private residential grid at 2.8713 × 10⁻⁷ km/m³. Its metadata
explicitly describes private building-area infrastructure. That additional
network is excluded from this municipal collection proxy.

Under the user's standing automatic approval, the complete native municipal
network is a screening proxy for both rural and urban source providers. Keep
the two original source entries and uncertainty separately. Rural/urban geometry,
material mix, construction and end-of-life, energy/machinery, geography, vintage
and internal uncertainty are not reconstructed. Shared native upstream
uncertainty correlation can differ from the two original source networks.

This is not a general m³-to-km conversion. The coefficient is taken from the
native service's own per-m³ inventory. No arbitrary pipe length, lifetime,
capacity or density multiplier is introduced, and no private residential network
or extra treatment service is added.

## Treatment load and capacity proxy

Use the copied native Swiss class-3 plant-only inventory for each original
treated m³, with municipal collection separated as described above. It represents a medium plant of 24,900 population equivalents, with
mechanical, biological and chemical treatment and sludge digestion.

The source average wastewater contains COD of 0.42534 kg/m³, TOC of
0.1091 kg/m³ and BOD of 0.20572 kg/m³. Native sewage documentation specifies COD
of 0.1556 kg/m³. The source generic treatment model is therefore not established
as equivalent in organic loading, removal efficiency or plant capacity.
Climate, chemistry, sludge/energy/water balances, infrastructure, allocation,
vintage, geography and internal uncertainty also differ materially.

Retain hydraulic volume with an explicit load/capacity screening warning. Do not
multiply the complete native process by the COD ratio, which would also multiply
its water and infrastructure volumes. Do not invent load-specific additions or
change original untreated emissions to balance native effluent.

Source internal collection and treatment inventories, including their materials,
sludge routes, emissions and infrastructure recovery handovers, are replaced by
the complete native component models. They are not added again, and no avoided
recovery or energy credit is introduced. These are explicit component-boundary
substitutions, not a claim of equal treatment performance or impact scores.

## Scope and evidence

Private read-only source/native inventories and full fingerprints support the
public review's derived coefficients and limitations. Native import revision
`794d90f2c2dbb90f0c56ba8cc527fa1494aa8ef6` and its documented 2,991 upstream
biosphere exclusions remain explicit. Score effects of the screening proxies
are unquantified.

Only these four exact 3.9 cut-off parents qualify. Standalone component mappings,
other releases, reverse routes and cross-system-model conversion are not
approved. Caller ownership, signed uncertainty, formulas, foreground links and
separately listed purchases and emissions remain intact. Full electricity
scenario, certification and export verification remain separate completion
gates.

## Verification checkpoint

All 37 focused Brightpath tests and 33 Premise tests pass. They cover signed
caller demand, normal/lognormal uncertainty and formulas, caller non-mutation,
unsupported releases, atomic authoring replay, original untreated fractions,
municipal/private network separation, cold/warm caches and portable limitations.
Lint passes for the new Python files; the generator's existing import-order
exception is unchanged.

An independent comparison verifies all 172 original untreated-emission terms,
eight collection inputs and four treated-volume inputs, including their
uncertainty and reference signs. It verifies every remaining native plant-only
coordinate against the installed inventory: 29 technosphere and 105 biosphere
entries. Only the municipal sewer-grid input is removed and relocated to the
collection providers; no private residential network is introduced. Original
collection volume and treated fraction remain separate, without omissions or
renormalization.

The original 3,495 recipes, 728 shared reviews and 405 Premise fingerprints are
unchanged. The review adds eleven recipes, one shared review and four exact
compatibility scopes. The complete generator reproduces the migration resource
byte for byte. A built wheel loads its own package data and converts all four
roots through the public Brightpath facade.

Each migrated graph has six foreground records and 148 direct biosphere entries
(43 original untreated releases and 105 copied native plant emissions/resources).
All four produce finite inventories on the installed native background.
Independent native and augmented solves, exact rational foreground equations
and floating-point balance bounds pass, with iterative refinement and residuals
reported. Native technosphere and biosphere scientific signatures are unchanged.
These are numerical/linking checks, not a validation of proxy load, capacity or
impact equivalence.

Superseded build, wheel and harmonization payloads are removed after source
comparison, retaining current audits and lockfiles. The required nitrate supplier
set now has 121 of 125 exact scopes. Broader pilot mapping, the encrypted-IAM
scenario, certification and Brightway export remain outstanding.
