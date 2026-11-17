# Current truck waste mappings and shared update

The three remaining ecoinvent 3.12 waste-market refusals are resolved. All truck
inventories now prepare through the normal CLIC conversion path. The actual
shared truck update and exhaustive graph checks pass. Subsequent verification
also passes the normal installed fifteen-sector workflow and Brightway export.
Fifteen of nineteen sectors are now qualified. The standalone checks below
preceded that public verification; complete results are in
`uvek-trucks-public-export-verification.json`.

Each polyethylene, polyurethane and glass market retains all twelve original
disposal shares and its separate road input. Thirty-three uncontrolled-treatment
models retain the original signed emissions through the existing ecoinvent 3.10
biosphere mapping. The fifteen unsanitary-landfill models also retain diesel
machinery and excavation using complete native operations. Only the municipal
incineration share uses the corresponding complete native incinerator. The
polyurethane construction-waste mix is unsuitable because it adds deconstruction
and sorting and changes the disposal shares. No exchange is newly omitted and
no nonempty operation becomes a zero-burden handover.

Native machinery, incineration, geography, vintage, emissions controls, residues,
recovery and allocation remain screening proxies. The native glass incinerator's
zero-water name conflicts with its documented two-percent water composition;
we preserve the nominal kg without inventing a moisture adjustment. Original
source emissions use already reviewed canonical biosphere correspondences;
no biosphere definition or mapping file is changed by this work. Earlier 3.8 and
3.9 physical waste routes are unchanged. Exact parent permissions, recipe
checksums and decisions are in Brightpath's two `uvek_trucks_waste_312` reviews.

Seventy-two signed LCIs pass against thirty-six independently reconstructed
source/native demand graphs. The largest relative inventory difference is
1.043e-17, below the 1e-10 tolerance. These checks register no database or activity
and leave source/biosphere metadata and processed files unchanged. The normal
resource generator reproduces all three resources. Thirty-two Brightpath tests
and nine Premise waste/older-route checks pass.

The update exposed two operational gaps, corrected primarily in YAML:

- All 38 truck roles now have exact fuel product/unit filters, including the
  already reviewed generated hydrogen supplier. Equipment and wrong units are
  excluded. Compression electricity is not classified as hydrogen fuel.
- Twenty-one inspected complete native fleet-market names and nine reviewed
  converted source-market names enter the existing regional relinking map.
  Existing ecoinvent mappings remain unchanged. Explicit dated/EURO producer
  services and municipal collection stay outside this market substitution.
  Caller transport quantities and uncertainty are preserved.

One small Python guard is necessary: the old truck battery schedule is in kWh,
but native truck factories purchase batteries in kg and have different class
labels. The legacy routine now acts only on battery purchases explicitly in
kWh. Existing kWh sizing and its uncertainty limits still work. No battery
capacity or future range is inferred from kg.

The standalone update creates 260 regional services and 73 regional/World
markets. Independent checks verify their IAM energy factors, fleet shares,
fixed inputs, fuel-proportional emissions, 65 hydrogen-compression balances,
33 unchanged native mass-based battery factories and 4,867 redirected existing
consumer inputs. Compact storage may omit a false `negative` marker; the
comparison treats absent and false as the same uncertainty setting. Of the
260 service/region pairs, 244 have an IAM energy/service efficiency signal and
16 retain the standard unavailable-signal behavior. All 132 targeted energy,
battery and transport regressions and six market-alias regressions pass.

Weight-class fallbacks and replacement of historical EURO/duty detail with IAM
fleets are explicit limitations in `uvek-truck-market-alias-review.json`.
Converted helper names bind their complete recipe signatures and are tested
against Brightpath's current resource. A closure change requires updating that
alias review. Combined-sector physical checks, 218 installed-package tests and
the normal public export now pass. The written database contains 17,745 datasets
and passes 272 signed LCI solves; all technosphere links stay inside the export.
The combined run preserves 6,397 redirected transport quantities. Battery, final
energy, emissions and metals are the four unfinished sectors.
