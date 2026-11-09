# Mixed-fuel heat market in the UVEK electricity pilot

The encountered ecoinvent 3.9 cut-off RoW market has 188 supplier entries across
54 technology labels. Its positive reference is one MJ of district/industrial
heat. Preserve every market coefficient, order and marginal uncertainty
individually. Do not aggregate suppliers, omit heat shares, normalize the mix or
add a consumer district network. Negative caller heat credits remain negative;
the conversion factor is +1, with no waste-reference sign reversal.

The user authorized subsequent modelling decisions automatically within the
pilot. This review records screening decisions and their limitations; it does
not establish inventory or LCIA equivalence.

## Complete heat services

Use native useful-heat outputs for industrial hard coal, heavy fuel oil, wood
furnaces and coal, lignite, wood and diesel cogeneration. Preserve forest versus
industrial chips, hardwood versus softwood and nominal furnace capacity where
available. Native heat wrappers already contain efficiency and allocation; add
neither fuel inputs nor a second efficiency factor. Source 2014 variants use
historical native services with explicit emission/performance/vintage warnings.

Coal CHP uses available national suppliers or ENTSO-E. Other missing source
geographies use the individually documented native locations. Swiss 1 MWth wood
CHP proxies the source 6667 kW plants; the diesel heat proxy is 300 kWth rather
than source 200 kWe. Thermal/electrical power, cycle, efficiency, infrastructure,
allocation and geographic differences remain unquantified.

Several functional proxies have substantial differences that must stay visible:

- The available LPG heat process is a 50 kW condensing/modulating propane/butane
  boiler, replacing an industrial propane furnace above 100 kW.
- Coal-tar heat uses the complete heavy-fuel-oil furnace on which the source
  explicitly based its coal-tar proxy. Fuel production, composition, PAH/metals,
  combustion, allocation and disposal differ.
- Conventional/organic straw heat uses complete industrial-chip biomass heat at
  300 kW. Feedstock, cultivation/organic status, ash/alkali, emissions and
  efficiency differ; no mass, heating-value or ash-credit correction is invented.
- Pulp-mill recovered heat uses complete wood-chip CHP heat. This substitutes
  feedstock and production/allocation models for black-liquor/chemical-recovery
  and pulp co-product heat. It is not a verified native pulp-mill heat inventory.

These complete proxies replace source producer internals and uncertainty. Their
market shares and caller exchanges remain unchanged; their score effects are
unquantified. Matching heat units does not establish scientific equivalence.

## Allocated burden reconstructions

Seventy-five helpers retain a positive one-MJ heat reference and explicitly
document the basis of their native inventory demand:

- Forty-eight gas-plant heat outputs use the audited source allocated fuel MJ
  per heat MJ to weight native blast-furnace/coke-oven gas combustion. Native
  burners are per fuel MJ, not heat MJ. Native blast-furnace gas excludes fuel
  production, while source purchases gas; retain that upstream allocation and
  boundary difference. Native coke-oven fuel production stays included.
- The industrial coke furnace likewise retains its original coke MJ per heat
  MJ through a native coke burner. The native small-stove combustion model is a
  scale/emission/infrastructure proxy, not an equivalent industrial furnace.
- Refinery-gas heat retains audited gas mass by dividing source kg by the
  native burner's kg per fuel MJ. Its source heat/fuel reference ambiguity and
  native infrastructure difference remain warnings; no efficiency is inferred.
- Ten bagasse and fourteen oil CHP heat outputs use source heat-to-electricity
  allocated-inventory ratios. The same ratio holds for every individual source
  nonproduction coordinate, including repeated and signed supplier terms. Apply
  it to the complete native donor as an allocated burden proxy, not an
  electricity-to-heat energy conversion. Native bagasse includes economic
  agricultural/refinery/distillery allocation; it is not the source separate
  combustion inventory. Missing oil geographies use the actual historical UCTE
  fleet. Source/native co-product allocation, fuel sourcing, heat quality,
  technology, geography, vintage and uncertainty differ.
- The EAF heat output uses its independently verified source heat-to-steel
  inventory ratio to weight native complete low-alloyed EAF production.
  This represents an allocated burden, not a physical steel purchase or credit.
  Native steel/slag allocation, secondary feedstock, geography and recovery differ.

The proportionality check covers 2,152 individual source coordinates. It proves
the source ratios only; native donor allocation is not asserted equivalent.
No separate electricity, virgin-material, heat or avoided-fuel credit is added.
Internal source formulas/parameters are replaced by the specified native models
and frozen driver coefficients, not claimed preserved.

## Allocated-zero recovered heat

Twelve exact source heat providers have positive one-MJ production references
and no other exchanges. The actual native recovered-heat service also contains
production only and explicitly allocates 100% of burden to disposal and zero to
energy. Use this existing positive heat output as a scoped allocation proxy,
retaining all twelve original market entries. Waste/feedstock, facility and
geography differences remain explicit.

This does not imply zero physical combustion, a burden-free waste-treatment
service or a generic rule for empty datasets. No empty helper, new cut-off stock
correspondence or source omission is introduced. Six municipal recovered-heat
wrappers also use the native plant-gate recovered-heat output; their internal
regional routing and allocation are replaced with explicit warnings.

## Verification and support boundary

All 17 focused Brightpath tests and nine Premise tests pass. They cover signed
caller amounts, normal/lognormal uncertainty, formulas, non-mutation, cold/warm
caches, portable warnings, unsupported profiles and atomic authoring drift.
An independent comparison verifies all 188 original market coefficients,
uncertainty and order; all 75 driver helpers; source/native reference signs,
units and hashes; and the twelve allocated-zero boundaries.

The prior 3,518 recipes, 731 shared reviews and 413 Premise fingerprints remain
unchanged. The review adds 76 recipes, one shared review and one exact source
scope. Only the encountered 3.9 cut-off market qualifies. Standalone producers,
other versions, reverse routes and cross-system-model conversions remain outside
this review. The pinned native bundle's 2,991 upstream biosphere exclusions and
all unquantified proxy limitations remain in provenance.

The actual migrated graph has 77 foreground records and links to 26 native
endpoints. Its LCI is finite, exact foreground equations and independent
native/augmented solves pass, and native database scientific signatures are
unchanged. Numerical linking checks do not establish LCIA equivalence. Full
encrypted-IAM scenario transformation, certification and Brightway export are
still required to complete the pilot.

The complete generator reproduces the resource byte for byte. A built wheel
uses its own packaged facade and evidence to convert this market. Verified
wheel/build copies and superseded harmonization payloads are removed, retaining
current audits and lockfiles.

All 125 external suppliers in the nitrate source graph now have exact scopes.
The fourteen nitrate/storage source records were freshly checked against the
read-only reference database and match the original metadata/inventory hashes.
Their graph remains acyclic, with nineteen internal links and 280 original
nonproduction exchanges. The nitrate market itself still requires assembly;
external-supplier coverage does not complete that conversion or the broader
pilot's remaining 27 parent rules and 31 exact source/version cases.
