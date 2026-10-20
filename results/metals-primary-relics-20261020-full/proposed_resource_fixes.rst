Resource-attribution corrections — approved implementation scope
=====================================================================

This proposal was approved for implementation. Code, fresh scores and verification
are recorded in ../metals-primary-relics-20261020-implemented. The text below
retains the pre-implementation reasoning and estimated effects.
The baseline is the completed ecoinvent 3.12 cut-off / IMAGE SSP2-M 2020 full
metals update in this directory. Existing scores are kg extracted target element
per kg reference product. Expected values below are arithmetic estimates from
the inspected feeds and proposed attribution boundaries, not new LCIA results.

The proposals extend the existing declared resource-attribution convention.
They do not turn economic allocation into a complete physical recovery model.
Technosphere quantities, treatment assumptions and purification/reduction yields
are preserved. Consequential resource exchanges remain outside this correction.
RELICS characterization factors are unchanged.

A. Antimony: replace the fitted concentrate factor with documented content
------------------------------------------------------------------------

Observed: 0.497081 for ``antimony production, iron precipitation of stibnite``.
The current factor is 0.249438568 kg Sb/kg concentrate, fitted to the legacy
``antimony production`` input of 4.009 kg concentrate/kg metal. The source market
documentation instead describes this product as 70% stibnite, i.e. 70 wt% Sb2S3.
The new engineering route explicitly uses this assay and buys 1.992796 kg feed.

Proposed change: use 0.70 * 2 M(Sb)/(2 M(Sb) + 3 M(S)) = 0.50180748 kg Sb/kg
stibnite concentrate in the resource-content rule. Preserve all process feeds.

Expected effect: the new iron-precipitation route contributes about 1.000000 kg
primary Sb/kg output. IMPORTANT: legacy refining with 4.009 kg feed then
contributes about 2.011746 kg/kg, before auxiliary requirements. Its previously
near-one score was created by the fitted factor. This does not establish that
the legacy, lead-smelting-based proxy has a real 50% Sb recovery. That process
requires a separate inventory/yield review; its coefficients are not changed
by A. Approval of A includes this visible change to legacy results.

B. Copper co-product of zinc refining: distinguish reagents from primary feed
---------------------------------------------------------------------------

Observed: 0.013539 for ``primary zinc production from concentrate`` /
``copper, cathode`` / RoW. The only directly named copper-bearing process inputs
sum to 0.000245 kg/kg output, even before their compound content is considered.
Their upstream Cu presence currently prevents adding the primary co-product's
resource attribution. They cannot supply one kg copper output.

Proposed change: add an exact primary co-product boundary for this name/product,
guarded by a uniquely resolved positive zinc-concentrate feed and uniquely
resolved target-bearing inputs whose total mass is below one kg. Assign 1 kg
direct Copper resource under the existing attribution convention. Preserve
reagent amounts and all upstream reagent/resource burdens. Do not change the
already cleared metal resource flows on the zinc-concentrate carrier.

Expected effect: approximately 1.014 kg/kg including background requirements.
This is a declared co-product attribution boundary, not an inferred Cu assay
for zinc concentrate. Missing/ambiguous providers must preserve and diagnose.

C. Silver co-product of zinc refining: the corresponding reagent distinction
---------------------------------------------------------------------------

Observed: 0.0001754 for the same zinc-refining process / ``silver`` / RoW.
The silver process input is only 0.000167 kg/kg output.

Proposed change: the corresponding exact boundary and supplier/feed guards from
B, assigning 1 kg direct Silver resource and retaining the small silver feed
and its supply-chain burdens. Expected effect: approximately 1.0002 kg/kg.

D. Lithium: attribute primary extraction at carbonate production
---------------------------------------------------------------

Observed: 6.75e-11 for chloride electrolysis and 7.39e-11 for battery lithium
foil production. Brine attribution is cleared upstream; spodumene is represented
by a mineral resource, and the primary carbonate processes have no corresponding
elemental Lithium attribution. The final metal is supplied through LiCl and
Li2CO3; adding a resource independently at every downstream step would double
count.

Proposed change: establish an explicit primary boundary only for lithium
carbonate production from concentrated brine and from spodumene. Attribute
0.18785188 kg Lithium/kg Li2CO3, from stoichiometric composition. Keep the existing
brine carrier boundary; prevent duplicate elemental-Lithium attribution within
the eligible primary feeds. Exclude recycled carbonate, chloride conversion,
hydride/chemical co-products, electrolysis, markets and foil-finishing services
as new primary extraction boundaries. Preserve secondary shares and yields.

Expected feed-basis effects with the existing carbonate mix: approximately
1.094 kg/kg for electrolysis, and 1.136 for foil after its 1.038 kg/kg metal feed.
These include stated processing requirements and the present secondary share;
they are not targets to force to exactly one. Cyclic auxiliary chemistry needs
the same explicit-boundary planning/ambiguity guards as the existing zinc rule.

E. Arsenic: recover the missing sodium-arsenide co-product attribution
--------------------------------------------------------------------

Observed: 2.53e-7 for 7N arsenic. Its upstream route is sodium arsenide from
Imperial zinc smelting -> arsine -> high-purity arsenic -> 7N arsenic. The
sodium-arsenide supplier describes removal of arsenide from zinc; the own-element
attribution is absent, so essentially only background As is counted.

Proposed change: add a dataset-specific primary co-product rule for ``sodium
arsenide production from Imperial smelting furnace`` / ``sodium arsenide``.
For nominal Na3As, attribute 0.52068333 kg Arsenic/kg product. Resolve the declared
zinc feed uniquely and preserve its inventory; do not assign another virgin
resource at arsine production or at either purification stage.

Expected primary feed term for 7N arsenic: 1.529944 kg arsine/kg product *
1.94 kg Na3As/kg arsine * 0.52068333 = approximately 1.54544 kg As/kg product.
This remains above one because the inherited purification/arsine route has
losses. It is not appropriate to erase those losses to obtain a green cell.
The route remains an engineering/proxy chain, not an industrial qualification.

F. Silicon: an exact metallurgical-reduction attribution boundary
----------------------------------------------------------------

Observed: 0.001641–0.003052 for six metallurgical silicon producers. The pure
product recognizer does not accept ``metallurgical``, and silica/sand extraction
does not supply the elemental Silicon flow characterized by RELICS.

Proposed change: recognize the exact ``silicon, metallurgical grade`` product
and declare primary attribution only for the native metallurgical-reduction
process and its photovoltaic-supply-chain variant, when they consume silica
feed rather than previously manufactured silicon. Attribute 1 kg direct Silicon
under the existing nominal product-output convention. Preserve sand/silica
resource and technosphere amounts and keep later purification downstream.
Do not assign a silicon characterization factor to generic sand.

Expected effect: approximately 1.0016–1.0031 kg/kg with existing background
requirements. The nominal one-kg product convention is not an assay guarantee.

G. Rare earths: one coupled, carbonate-route oxide attribution correction
------------------------------------------------------------------------

The five new metal-reduction recipes buy separated oxides. Those separated
oxide outputs have no direct own-element resource; upstream ion-adsorption-clay
carbonate carries a basket of economically attributed rare-earth flows.
Their small results are not explained by the reduction recipes' element ledgers.

Proposed change: move rare-earth-element resource attribution from the two
``rare earth element mine operation and beneficiation, ion adsorption clays`` /
``rare earth carbonate concentrate`` suppliers to ALL single-element oxide
outputs of ``rare earth oxides production, from rare earth carbonate
concentrate``, including regional proxies. Use nominal stoichiometric oxide
content: CeO2, Pr6O11, Tb4O7, and the corresponding sesquioxides for the remaining
named products. These are declared formula conventions, matching the current
downstream reduction recipes; native labels alone do not establish every assay.

The transfer must be planned for the complete carbonate branch before any
resource is cleared, with verified unique providers and existing biosphere
flows. Clear only the transferred REE resources on its carbonate carriers;
preserve clay, gangue, non-REE resources, all process quantities and reduction
yields. The other 50% and 70% REO mining/separation branches, including their
mixed-oxide products, retain their existing attribution. They are not part of
this proposal and need no invented mixed-oxide composition.

This is one coupled fix: adding resources at the five metals while retaining
their attributed carbonate extraction would double count. Fixing only five
oxide outputs while clearing the shared carbonate basket would remove
extraction from other single-element oxide outputs.

Metal-specific arithmetic expectations (oxide feed times metal fraction):

* Dysprosium: current 0.252891; Dy2O3 factor 0.87132068;
  1.366517 kg oxide/kg metal -> approximately 1.190675 kg Dy/kg metal.
* Holmium: current 0.011340; Ho2O3 factor 0.87297598;
  1.192739 kg oxide/kg metal -> approximately 1.041233 kg Ho/kg metal.
* Neodymium: current 0.141979; Nd2O3 factor 0.85735599;
  1.241292 kg oxide/kg metal -> approximately 1.064229 kg Nd/kg metal.
* Praseodymium: current 0.120048; Pr6O11 factor 0.82770415;
  1.285760 kg oxide/kg metal -> approximately 1.064229 kg Pr/kg metal.
* Terbium: current 0.055340; Tb4O7 factor 0.85021554;
  1.224669 kg oxide/kg metal -> approximately 1.041233 kg Tb/kg metal.

These are the principal feed terms; auxiliary requirements can add to them.
For example, dysprosium deliberately models 85.7% conversion and 98%
fluorination, so a score around 1.19 is consistent with that recipe.

Remaining cases: recommended preservation and classification
------------------------------------------------------------

* 7N gallium, 1.428571: the documented 70% purification yield purchases
  1.428571 kg high-grade gallium/kg output. Preserve this result and the
  separately represented rejected metal. No extraction normalization proposed.
* Iron, 1.112925 and 1.284845: retain ore/pellet requirements; pig/sponge products
  are not guaranteed one kg pure Fe. Label as composition/recovery cases.
  Establish product assays and stage recoveries before proposing inventory edits.
* Nickel, 1.158146 and 1.190697: retain provisionally. The CN concentrate
  documentation says 6.78% Ni, despite the rounded ``7% Ni`` product name;
  its direct resource is 0.0806623 kg Ni/kg concentrate. That can include
  beneficiation losses. Replacing it by a guessed 0.07 or normalizing the
  refinery would erase unresolved losses. The generic nickel-rich-material
  market is also a contained-metal-equivalent service: 6.25 kg 16% concentrate
  supplies one kg of generic product. A separate assay/recovery audit is needed.
* Copper electrorefining, 0.904647 (outside 5%, inside 10%): retain provisionally.
  Its upstream copper-concentrate resource amounts remain unresolved fallbacks.
  Establish supplier-specific content/allocation/recovery bases before replacing
  them; no one-kg-per-kg concentrate assumption is proposed.

Validation after approval
-------------------------

Implement only approved IDs. Add regression cases for auxiliary reagents,
secondary routes, downstream duplicate prevention, missing/ambiguous suppliers,
iteration-order independence and repeated correction. Confirm that technosphere
amounts, yields and non-target biosphere exchanges are preserved. Confirm
consequential inventories remain unchanged. Rebuild a uniquely named database,
repeat all producer scores and independent numerical checks, and regenerate the
annotated heatmap with before/after comparisons and the changed attribution
boundaries explicitly identified.

Evidence
--------

* This directory's scores.csv, attribution_decisions.csv and outlier_contributions.csv.
* Read-only inspection of the exported Brightway dataset metadata, resource flows
  and target-bearing supply chains, including the source concentrate and market
  documentation and supplemental reduction/purification comments.
* Current premise/metals.py rules and docs/methodology/metals.rst attribution scope.
* Sodium arsenide formula: https://pubchem.ncbi.nlm.nih.gov/compound/Sodium-arsenide-_Na3As
* Stibnite formula: https://pubchem.ncbi.nlm.nih.gov/compound/16685273
* Praseodymium oxide formula: https://pubchem.ncbi.nlm.nih.gov/compound/159413

Approval may be given by ID A–G. A has the explicit legacy-Sb consequence above;
G must be approved as a complete coupled carbonate-branch transfer.
