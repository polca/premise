# Nitrate-salt supply required by the UVEK electricity pilot

Status: source-boundary inspection complete; the whole salt-market mapping
remains refused. Its purge-stream and industrial-KCl dependencies are now corrected
as described below.

This inspection concerns `bp-cfcab3636e7a220d`, the ecoinvent 3.9 cut-off global
market for nitrate salts for solar power applications. The destination remains
UVEK 2026 cut-off with ecoinvent 3.10 biosphere. The existing generic inorganic
chemicals target does not establish the required chemical or supply boundary.

The source market combines virgin and reused salt. Its virgin mixture uses
technical-grade sodium and potassium nitrates. The reused-salt route is a
positive allocated product supply, carrying nonempty inventories from four
thermal-storage construction processes: parabolic-trough and tower designs,
each with South African and rest-of-world suppliers. Their construction,
materials, operations and disposal inputs remain part of that allocated supply.
It cannot be treated as an empty cut-off stock or replaced with virgin salts
without a new modelling decision.

The exact supplier links form an **acyclic graph**. The four allocated storage
processes buy directly from **virgin nitrate-salt production**, rather than from
the final market containing reused salts. The current recipe schema accepts
this topology. Cyclic-recipe support is not required for these inspected links.
Product names alone do not identify these suppliers; the linked activity,
reference product, geography and unit distinguish them.

The earlier approved omission of the used-salt reuse credit from one CSP
decommissioning record applies only to that caller exchange. It does not
authorize removing the reused-salt supply branch or its allocated construction
burdens from this market. Likewise, the approved complete 110 MW CSP generator
does not replace the source's four historical storage suppliers automatically.

The inspected virgin production chain includes technical-grade refining,
industrial-grade potassium nitrate, and unrefined sodium nitrate from Chilean
caliche mining. The source distinguishes these grades and includes its own
refining quantities. Preserve those process references and quantities rather
than inventing purity, yield or concentration multipliers.

The native catalog has potassium-nitrate inventories allocated separately per
kg of nitrogen and per kg of potassium oxide. Their documentation requires
joint use. Neither reference is a kg of technical-grade potassium nitrate;
selecting one endpoint or using both at one kg each would misrepresent the
reference basis. A complete sodium-nitrate supplier has not been established
from the current catalog search. These findings support further source-based
reconstruction, rather than an unsupported generic chemical match.

The private inspection freshly read 14 complete source records, retaining
280 non-production exchanges and 19 links within that graph. The unchanged
migration schema accepts the source topology. Its internal supply solve is
finite and nonnegative, with equation residual below 1.18 × 10⁻¹⁶. All nine
direct elementary flows match the ecoinvent 3.10 catalog by publisher UUID,
unit and complete receiving compartment. This verifies the inspected source
structure; it is **not a linked UVEK inventory or a native LCI result**.

The inspected graph references 125 distinct external suppliers. Of those,
73 already have exact current CLIC approval fingerprints for their source
profile. All have raw mapping entries, but a raw entry does not establish a
scientifically reviewed target. The other suppliers cover 18 reference-product
labels across multiple geographies. Some may reuse standing reviews after
inspection, while others overlap existing pending material, heat or waste
decisions. They are not 52 automatically new scientific decisions. Coverage is
checked using the public scope auditor and original packaged rule bodies;
profile-selected rule projections are not approval fingerprints.

The Chilean sodium-nitrate-rich purge-stream market is now one of those
approved suppliers. Fresh source inspection found a positive, production-only
`Recycled Content cut-off` provider and a market containing only that handover.
The two corrected correspondences preserve this allocation through source-bound
helpers, with normal stock warnings, signed kg and uncertainty. They add no
potassium-nitrate nutrient, indium-residue, fuel or avoided-production burden.
This does not extend to the four nonempty reused-salt construction suppliers.

The exact 3.9 Chilean industrial-KCl site is now another approved supplier.
Its original industrial-grade reference is reconstructed from its four inputs
and five elementary exchanges, without a K2O nutrient or purity multiplier.
Two auxiliary helpers preserve the PVF film producers, sole ordinary waste
incineration fate, and every individual market freight entry. The complete
native PVF incinerator matches all 44 declared wet-composition/heating-value
coordinates. The film's nominal dispersion and pigment inputs match, while
historical geography, energy conventions, cumulative transport boundaries and
incomplete emissions remain screening limitations. Native Chilean consumption
at medium voltage retains its grid losses, infrastructure and SF6 inventory.
The source's negative waste input becomes positive treatment demand with
consistent uncertainty scaling; separate caller emissions remain unchanged.
The actual four-record migrated graph gives finite LCI on the unchanged native
background. This approval covers the site and its internal helper closure;
it does not independently admit the raw PVF, freight or operating heuristics,
other releases, the full nitrate market or a nutrient-fertilizer correspondence.

Eight direct operating services now have separate exact 3.9 cut-off approvals:
global and rest-of-world freight rail, global barge, unspecified global lorry,
global container ship, rest-of-world 16–32 t EURO3 lorry, Chilean medium-voltage
consumption electricity and global building-machine diesel combustion. Their
source hashes match existing whole-service evidence, and fresh native inventory
inspection verifies positive references and unchanged direct technosphere
vectors across the two UVEK releases. Original signed service quantities and
uncertainty are preserved at factor +1, with explicit fleet, geography, grid-mix,
emissions, allocation and vintage warnings. Complete native infrastructure and
operations are retained once. This adds no new transport, electricity, fuel or
recovery credit. It does not change the KCl parent review or any existing raw
migration rule. The previously approved 3.6 building-machine market remains
separately approved; no other source profile inherits these new 3.9 admissions.
All eight actual migrated services give finite LCI on unchanged native matrices,
and public cache and installed-wheel checks pass. The complete nitrate market
is still unresolved.

Four further exact 3.9 markets now have independent matching-function reviews:
the same historical 160 kWe CHP control cabinet, seamless steel-pipe drawing,
copper-wire drawing and Tovex blasting. Their markets add no freight, losses or
direct emissions to the positive unit supplier mix. Their native references and
functional boundaries are inspected, preserving the complete native function
and separate caller purchases without capacity, lifetime, extra main-metal or
explosive multipliers. Four finite native LCI checks, public cache checks and
installed-wheel checks pass. Native supply and exact foreground reference
balances agree under the documented mixed row-residual check, with original
unscaled numerical findings retained. The [material-boundary inspection](uvek-nitrate-material-boundaries.md)
records the delivery and water-accounting cases still requiring work. These
reviews change no raw mapping or recipe and do not approve the complete salt
market.

Nine further material markets now retain their original supply/delivery
boundaries through reviewed helpers. They preserve all 14 producer entries and
41 individual freight entries, including marginal uncertainty. The native
at-plant products have independently inspected matching reference classes;
complete internal production/logistics and separate original market delivery
are retained once. The explicit 3.9 thermal-MJ generator market is separately
admitted at factor +1; its corrected fuel basis differs from the earlier
ambiguous 3.6 source and must not inherit that normalization factor. Ten actual
native LCI and installed-wheel checks pass; all earlier approval fingerprints
remain unchanged. Recycled HDPE and foam glass without cullet are actual supply
variants in the remaining markets, not interchangeable geographic copies of
virgin production. Their shares remain unresolved rather than being silently
collapsed to the available generic native products.

The exact 3.9 Swiss waste-concrete market now independently reuses its existing
reviewed inert-landfill helper after fresh source and native closure inspection.
The [waste-boundary findings](uvek-nitrate-waste-boundaries.md) describe its
preserved collection and landfill boundary, together with the other waste
markets still requiring work. This adds no new migration rule or recipe.

Six exact 3.9 polyethylene waste markets now have source-preserving country
helpers: Peru, India, Brazil, Colombia, Cyprus and South Africa. Their original
treatment shares and collection freight remain separate, with uncertainty.
Fresh complete treatment witnesses match the already reviewed source helpers
for open burning, open dumps and unsanitary landfills at the original
infiltration classes. Brazil's small ordinary-incineration branch retains its
previously inspected complete native polyethylene incinerator for 0.4%-water
waste. A follow-up correction removes the generic disposal wrapper's additional
collection while retaining the original market freight. This does not admit a
Swiss FAE branch or substitute controlled incineration for uncontrolled disposal.
All earlier recipes, reviews and pilot approval fingerprints remain unchanged;
six new market recipes reuse five existing treatment recipes. Native LCI and
installed-wheel checks cover each complete returned graph. At that checkpoint,
the RoW market's additional hyperarid classes and the European group's Swiss
technology branch remained unresolved.

The exact 3.9 rest-of-world PE market now also preserves its complete source
mix. Its twelve treatment shares and four collection-freight entries remain
separate, with signed quantities and uncertainty. Two new source-based helpers
retain the hyperarid landfill and open-dump inventories; nine earlier treatment
recipes remain unchanged. The controlled portion selects the specific native
PE incinerator directly from the complete-composition authority. Freight uses
the previously inspected generic RER rail and heavy-lorry services, including
their geographic and technology warnings, rather than the raw coal-specific or
diesel-specific rail matches. The complete thirteen-record migrated graph
retains 1,145 elementary exchanges and gives finite native LCI. This resolves
the RoW hyperarid branch described at the earlier checkpoint. The European
group's Swiss FAE branch remains unresolved.

This dependency expansion sits behind one currently blocked parent rule.
Consequently, the headline count of 35 unresolved rules / 40 source-profile
cases is the current public refusal frontier, not a complete measure of the
remaining inspection or implementation effort. The pilot still requires the
same nine roots and 55 CLIC records; no plant, storage requirement, material
input or supply share has been removed to reduce that count.

Next work is to preserve the allocated reuse branch and source production
structure, bind the external suppliers to inspected native services, retain all
signed quantities and uncertainty, and verify the resulting complete target
graph. Further mismatches must remain explicit decisions or refusals. Full
public scenario transformation, certification, Brightway export and a finite
prospective LCI remain completion gates.

Detailed licensed coefficients and source metadata remain in the private
pilot work area: `nitrate_source_routes_full_evidence.private.json`,
`nitrate_raw_production_full_evidence.private.json` and
`nitrate_source_dag_preflight.json`.

The exact 3.9 RoW reinforcement-steel recycling service is independently
admitted as the complete native Swiss dismantling service. Fresh inspection
confirms the same single 0.626 MJ building-machine input per kg, and native
2026 documentation describes the same skid-steer dismantling boundary. The
source itself uses historical Swiss technology. Source reference −1 and native
reference +1 require a −1 caller factor. This introduces no remelting, scrap
credit, collection or density factor. Native internal uncertainty differs
slightly and remains an explicit warning; caller uncertainty and signed
formulas are preserved. The existing raw rule and all earlier approvals remain
unchanged. This resolves one external nitrate dependency; the full nitrate
market remains unapproved, with 57 of 125 suppliers scoped and 68 remaining.

Five exact 3.9 final-disposal markets are now composed independently: regional
concrete in Europe without Switzerland and RoW, and mineral-wool final disposal
in CH, Europe without Switzerland and RoW. Each treatment retains the source
1 kg generic landfill-operation service and 1.48×10⁻⁹ facility units per kg.
The two concrete markets retain all ten original rail/road entries, including
uncertainty and separate entries sharing a native endpoint. The mineral-wool
markets contain no collection or direct handling emissions; the broader native
demolition wrapper is therefore excluded.

The complete generic native operation and facility reuse the earlier reviewed
function/unit and geographic proxies, with their heat, materials, internal
uncertainty, allocation, vintage and upstream-exclusion limitations. Nine
operation and three facility land coordinates match the source quantities.
Neither the 450,000 m³ facility capacity nor the waste density or water content
is used to infer a multiplier. The source records' absence of direct leachate
is a documented source-data limitation. The five complete target graphs pass
native LCI without changing destination scientific contents. This brings exact
nitrate supplier coverage to 62 of 125, leaving 63. The full nitrate market and
prospective pilot remain incomplete.

The Swiss and RoW mineral-wool waste markets now preserve all source fates,
collection and operating inventories. Recycling retains three explicit dust
fractions; collection and sorting keep their own dust and the reviewed
final-disposal closure. The Swiss market has one recycling fate and two collection
entries; RoW has three fates and eight collection entries. The sorting service
retains its six same-territory delivered low-voltage supplies, 0.0222 m³
hydraulic-digger service and 10⁻¹⁰ sorting-plant units per kg. Three complete
source/canonical elementary definitions match by publisher UUID, kg, full
receiving compartment and emission role. All original source uncertainty and
signed quantities are retained without fate normalization or new recovery
credits.

The global machinery market keeps both source supplier weights. The complete
native legacy RER hydraulic-digger service matches the source nominal diesel,
lubricant and machine quantities; its “proj. 500” label does not imply a
quantity multiplier. The newer Swiss-average operator has different fuel
consumption and is not used. The whole sorting facility retains the same
200,000 t/year capacity and 50-year service basis, with warned internal heat,
material, disposal, uncertainty and vintage differences. Its three land
quantities match, but only two publisher identities match: source generic
“Transformation, from unspecified” and native generic “Transformation, from
unknown” are different UUIDs. That whole-facility inventory difference remains
an explicit warning; no independent land-flow equivalence is approved.

Coverage is now 64 of 125 nitrate suppliers, leaving 61. The European
mineral-wool branch still requires implementation of the separately approved
[electricity proxies](uvek-wool-european-electricity-proposal.md) for seven absent
country grids.
The remaining 32 countries have native low-voltage grids; no fallback has been
enabled by the Swiss/RoW review.

The user-approved European mineral-wool route is now implemented separately.
It retains all three source treatment shares, two collection entries, nine
direct dust exchanges across the treatment helpers, and the original sorting
quantities. Its electricity group keeps all 39 separate signed source entries
and uncertainty: 32 native country grids and seven scoped RER proxies (AL, MD,
MT, GI, XK, ME, BY), totaling 1.6162643621% of the sorting-electricity group.
No shares are aggregated or normalized. Each proxy remains named in portable
approval metadata and warnings; it grants no standalone country mapping.

The previously inspected machinery, facility and generic final-disposal
functions remain unchanged, including the warned generic land-origin UUID
difference. The complete eight-record graph retains its source reference
basis and gives finite native LCI on unchanged scientific database contents.
All three mineral-wool markets encountered in this nitrate branch are now
scoped. Exact nitrate coverage is 65 of 125 suppliers, leaving 60 across 19
product labels. The full nitrate supply, scenario certification and export
remain outstanding.

The exact 3.9 RoW inert-waste final-disposal market is now scoped through its
original generic landfill treatment and nine separate collection entries
(six rail, three road). Both the market-to-treatment exchange and the original
1 kg operation / 1.48×10⁻⁹ facility-unit coefficients retain their uncertainty
through nested helpers. All source collection quantities and uncertainty remain
separate, using the reviewed whole RER freight services. The source has no
direct biosphere exchange or waste-accounting indicator; none is added. The
source's explicit leachate inventory gap remains a warning.

The same raw rule already served source 3.12. Its selected sign decision,
default target and complete six-input scientific recipe remain unchanged.
Public-facade comparison before/after the extension finds only the converted
exchange's explanatory comment changed. The older approval binding is
explicitly reissued because the full raw rule now includes a 3.9 profile and
combined warning; this does not change the 3.12 mapping, indicator or omission
policy. Other 349 approval fingerprints remain unchanged. The exact nitrate
supplier coverage is now 66 of 125, leaving 59 across 18 product labels.
