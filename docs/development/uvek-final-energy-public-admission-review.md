# Approved public admission of final energy

The shared sixteen-sector REMIND SSP2-NPi 2050 run now has suppliers for all
203 positive final-energy roles and passes exhaustive graph validation with
zero errors. All 876 activities added by final energy, plus a representative
of each positive role, pass 1,894 positive/negative inventory calculations in
memory. The four missing biofuel templates use the existing normalization
method and complete native biodiesel, with explicit screening limitations.

The remaining public API barrier is its literal support list. The approved
one-file patch adds `final energy` and updates the matching error message.
It changes no transformation, formula, dataset filter, cache, validation rule
or public constructor. The no-argument default stays biomass/electricity/heat;
battery, emissions and metals remain refused.

Use final energy after the ordinary preceding sector updates, in the existing
sector order. Its electricity, biomass, heat and fuel market selectors refer
to the shared markets those updates create; country grids are not cloned again
merely to fill a missing prerequisite. This follows the normal coupled Premise
workflow rather than introducing a UVEK sector engine.

The user approved this exact support-list change on 18 November 2026.
The patch is applied. Installed-package, normal live public API, Brightway
export and 1,894 signed written-LCI checks now pass for the coupled sixteen-
sector REMIND SSP2-NPi 2050 workflow. See
`uvek-final-energy-public-qualification.json` for the exact scope and limits,
and `proposed-final-energy-public-admission.patch` for the approved change.
