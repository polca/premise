# First five battery-material correspondences, source 3.9 cut-off

These five mappings now have separate scientific reviews. They apply only to
the inspected ecoinvent **3.9 cut-off** inputs, with UVEK 2026 and the 3.10
biosphere as the Premise target. Each source and native product references one
positive kg. Signed caller quantities, formulas, supported uncertainty and
separately listed foreground emissions remain unchanged.

| Input | Result and main limitations |
| --- | --- |
| Hydrogen fluoride market, RER | One kg of complete native HF production, GLO, plus the original final rail and road delivery. Native formula and fluorspar reaction identify HF, but its CAS number conflicts with that identity. Native assay is not independently established; the kg-HF interpretation is an explicit screening assumption. Native acid, electricity and thermal inputs differ substantially. |
| Trichloroethylene market, GLO | Retain both original producer shares separately and all four delivery entries. Use complete native WEU TCE for each producer share. It is an older cumulative system inventory, with missing main-process infrastructure and land-use information and no assessed internal uncertainty. Global production routes, hydrochloric-acid allocation and emissions differ. |
| Ultrapure-water production, RER | Complete native ultrapure-water production, GLO, per kg. Both describe electrodeionization; equal detailed purity specifications are not established. Native water recovery, equipment, electricity and wastewater differ. No extra source water, energy, equipment or treatment is added. |
| Glass-fibre production, RER | Complete native European glass fibre, including sizing and auxiliaries, per kg. Both describe the same broad historical furnace family. Native limestone versus source lime, fuel-energy versus useful-heat boundaries, treatment and upstream inventories differ. No furnace, sizing or lime correction is invented. |
| Electronic-grade silicon production, DE | Complete allocated native electronic-grade silicon per kg. Both describe chlorosilane purification and deposition with silicon co-products. Allocation, acid/alkali concentration conventions, energy supply, upstream inventories and vintage differ. No source MG-silicon coefficient, generic purity label or coproduct quantity becomes a multiplier on the caller. |

The native HF record reports CAS **73602-61-6**, which a manufacturer identifies
as a triethylamine trihydrofluoride reagent, while HF is **7664-39-3**. Its name,
formula, process description and fluorspar feed support the HF interpretation;
its original metadata are preserved and the conflict is warned. It does not
justify a 37% solution factor or adding a triethylamine inventory.
[HF identity](https://pubchem.ncbi.nlm.nih.gov/compound/Hydrogen-fluoride),
[manufacturer reagent identity](https://www.tcichemicals.com/US/en/p/T2022).
These identity sources do not establish native acid assay or LCI equivalence.

The HF wrapper keeps its two final freight quantities. TCE keeps its two original
producer entries and four freight entries, including a complete native container
ship service. All freight remains in signed tkm, with its original supported
uncertainty. Native fleet, traction, fuel, geography and vintage are proxies.
Container transport already includes vessel and port infrastructure. Final
product delivery is separate from logistics already inside the native production
inventory. There is no renormalization or invented transport/loss multiplier.

These are documented **screening correspondences**, with unquantified score
and modelling-uncertainty differences. Available native inventories replace the
source production models, including their internal emissions and limitations;
this does not establish chemical assay, source-emission completeness or LCIA
equivalence. No caller exchange is omitted. Full licensed producer vectors and
original source comments remain private.

Only two market wrappers are added; the three production inputs link directly
to native products. The compiled mappings, frozen review and integrity manifest
change in Brightpath, with a maintainer replay hook. Premise gains five scoped
approvals. Its previous 1,279 approvals are unchanged. No runtime Python module
or ordinary notebook flow changes for this group.

See [verification](uvek-battery-first-materials-39-verification.json). These checks
qualify the five correspondences, not the battery sector or its eventual export.
