# Required hydrogen-train suppliers: exact source 3.12 review

Thirteen direct correspondences needed by the six actual CLIC hydrogen-freight
train foregrounds have independent primary checks in ecoinvent 3.12 cut-off,
UVEK 2025 and native UVEK 2026. All have positive one references and matching
units. Signed caller quantities, formulas and uncertainty retain factor +1.
These decisions follow the standing instruction to approve and log modelling
choices autonomously. They do not approve consequential or reverse conversion.

| Input | Native screening supplier and boundary |
| --- | --- |
| PPS | Solid native PPS; incomplete process emissions and missing solid waste remain explicit. |
| Locomotive | Complete 84-tonne conventional RE460 manufacturing proxy; retain separately specified hydrogen drivetrain. |
| Goods wagon | Complete native average wagon, per unit; preserve caller lifetime usage. |
| Phenolic resin | Pure native resin, per kg; no solution or dry-mass multiplier. |
| Gold and palladium | Complete native European storage mixes; warn about production, recovery, allocation and delivery differences. |
| White liquid phosphorus | Native elemental P4; its erroneous “liquid ozone” comment remains a documented source defect, not a chemical conversion. |
| Through-hole lead-free board | Unmounted two-layer FR4 board per m²; retain separate components and warn about conflicting generic surface-mount wording. |
| Ferrite | Native MnZn representative; composition/performance differences and absent native waste/direct emissions are limitations. |
| SMD resistor | Complete native resistor, per kg, with the same documented representative component mass. |
| European medium-voltage grid | Complete ENTSO-E grid, with native losses and infrastructure; no extra loss multiplier. |
| Steel sheet rolling | Cold-rolling service only; keep the caller steel bill and the native process-loss replacement steel. |
| Brass | Complete 70/30 copper/zinc brass; native theoretical model has documented poor data quality. |

Replacing a source market by a native producer or storage mix does not preserve
its exact final delivery, supplier mix or allocation. These differences, native
biosphere exclusions, internal uncertainty and unquantified score effects are
retained as warnings and portable provenance. No extra freight recipe is invented
merely to clear a correspondence, and no claim of LCIA equivalence is made.

Brightpath packages bounded identities, reference checks, private-primary record
hashes and the scientific boundary review in
`export/uvek_hydrogen_train_direct_312_review.json`. Full licensed inventories stay
private. Two existing rules, PPS and medium-voltage electricity, also need an
explicit source-3.12 reference-sign decision. A maintainer replay hook preserves
those mapping-data extensions during regeneration; it changes no runtime sector
algorithm. Their old source-profile decisions are compared before rebinding
existing approval fingerprints and remain identical.

Premise adds thirteen exact-profile scopes bound to the packaged evidence and
rule fingerprints. A subsequent independent approval of the production-only
aluminium-scrap handover is recorded in `uvek-hydrogen-train-cutoff-stock.md`.
The actual first source cohort now refuses five composed supply/treatment rules;
later source cohorts and the full train update remain unfinished. The rejected
barium-carbonate/lithium-carbonate candidate gains no scope; the separate
caller-specific omission is explained in `uvek-hydrogen-train-barium-omission.md`.
