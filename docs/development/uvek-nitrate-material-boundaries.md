# Material and utility boundaries within the nitrate supply chain

The inspection concerns only suppliers required by the same nine-root,
55-record UVEK electricity pilot. The complete nitrate-market conversion remains
unresolved. This is a boundary audit, not a general mapping-coverage claim.

The private readback contains 122 complete source/native records for 25 material
and service markets outside the already identified concrete-grade, paint-basis,
generator and scrap cases. Twenty markets have separate delivery, distribution
or elementary exchanges. Five have only a unit supply mix. Matching product
names therefore do not establish a complete market correspondence.

Four of those five supply-only markets are now independently reviewed for the
exact 3.9 cut-off profile: the control cabinet for the same historical 160 kWe
CHP unit, seamless steel-pipe drawing, copper-wire drawing and Tovex blasting.
The source providers and native processes have positive unit references.
The cabinet documentation identifies the same historical installation and
operating-life assumption; nominal major-material and assembly-electricity
quantities match, while background grades, energy and waste conventions differ.
The drawing processes exclude the main metal being worked. The pipe process's
wire-drawing approximation is already included internally. Blasting includes
one explosive supply and the detonation inventory, while retaining the original
limitations about estimated emissions, missing rock emissions and potentially
high contributions. These are complete native-function correspondences; they
do not separately approve their internal chemical, metal-grade or waste inputs
for unrelated callers. No raw mapping, coefficient, helper or route changes.

The fifth supply-only market is the European tap-water group. It delegates to
country markets, so its lack of direct distribution exchanges does not prove
that the water system lacks losses or infrastructure.

The initial fourteen other goods markets contain separate freight. These include
expanded perlite, foam glass, alloy, particleboard, polymers, refractory material, sand,
silicone, steel sheet and transformer supplies. Their existing at-plant targets
must not silently replace the source delivery boundary. Further source-based
compositions need to retain the individual original transport entries and
uncertainty, using independently inspected complete same-mode services. A
material match alone does not approve deleting delivery or changing its scale.

Nine of those markets now preserve their supply/delivery boundary through
reviewed source-bound compositions: expanded perlite, Fe-Ni-Cr alloy,
cement-bonded particleboard in two geographies, HIPS, basic packed refractory,
generic silicone, tin-plated chromium-steel sheet and the ICT transformer model.
The recipes retain all 14 original producer entries and 41 freight entries,
including uncertainty, without normalizing shares. Native kg, m3 and m2
references match the source quantity basis. The cumulative board inventory has
no explicit upstream links through which to propagate prospective energy
changes; that inherited source limitation remains. Internal production logistics
and original market delivery are distinct and retained once. The complete-product
screening limitations remain in warnings; specific performance and LCIA equality
are not established. The new review passes 114 focused Brightpath and 114
Premise checks, ten actual native LCI cases and ten installed-wheel conversions.

Five delivery markets still require further work. The HDPE market contains
four recycled-to-generic renaming suppliers alongside two virgin-production
suppliers. The foam-glass market combines production with recycled cullet and
production without cullet; equal density/conductivity does not establish equal
feedstock or energy. Neither can be collapsed to one virgin/native product.
The three sand markets include river extraction, with an extraction and
allocation boundary different from the generic native gravel/sand pit.

The 3.9 generator market is now separately reviewed at factor +1 per explicit
thermal MJ. Its fuel input supports that corrected reference basis, unlike the
older 3.6 ambiguous reference whose approved proxy normalizes fuel demand.
The existing complete-native-generator size/application proxy remains with its
limitations; neither a copied normalization nor a new efficiency, heat credit
or separate generator is introduced.

Seven water markets remain unapproved. Six national markets include distribution
losses, a network purchase and a direct water release. The source provider
quantity exceeds the delivered reference; these are physical exchanges rather
than rounding or negative-treatment conventions. The European group delegates
to its own country mix. Both source and native network references are in
kilometres: there is no established unit-versus-kilometre mismatch. Construction
specifications and the allocation of distribution duties still need inspection.

UVEK also contains country-labelled “water balance according to MoeK 2013”
utilities. Their documentation requires caller accounting for water embodied
in the product. They reuse historical Swiss treatment/infrastructure assumptions
with regional electricity and water balances. Country labels alone do not prove
source-equivalent treatment or accounting: the inspected Colombia-labelled
process uses a Peruvian electricity supplier, for example. The original generic
Swiss/European utility matches and these country-labelled alternatives remain
candidates, not automatically admitted replacements. Source loss/return flows,
network burden, delivery and embodied-water accounting must be resolved
explicitly before selecting a correspondence. No additional embodied-water
exchange or geographic water-balance factor is inferred for caller datasets.

A fresh internal-supply audit of the six national markets reads 31 complete
water records and retains 42 internal water links, including six explicit
market self-purchases. Treatment processes also buy water from their own
market. These are supply feedbacks, not independent delivered-water inputs.
All six recorded systems solve with finite, nonnegative supply; maximum
absolute equation residual is 2.23 × 10⁻¹⁶. The inspection retains 16 parameter
definitions; no exchange formula is present in these records.

For example, the Indian market purchases 0.428571428571429 kg from itself per
operation. Even without the smaller treatment feedback, its net supply
multiplier is 1/(1 − 0.428571428571429) = 1.75. With treatment feedback the
required market supply is 1.7500562587 operations per delivered kg. Brazil's
corresponding figures are 2.4230769231 and 2.4231880058. A self-purchase
coefficient must not be interpreted as a loss fraction or applied just once.
These are properties of the recorded source matrix, not corrected engineering
loss estimates or proposed destination factors.

Brightpath currently rejects cyclic recipe dependencies explicitly in
`brightpath/migrations/schema.py`. The earlier finding that the nitrate-salt
production graph is acyclic still holds, but does not establish that every
external supplier closure is acyclic. A source-preserving water reconstruction
must account for these feedbacks. Mean-only algebraic elimination would also
require an uncertainty/correlation review; silently flattening them or removing
self-links is not an admissible fix. This audit does not yet cover the full
European water-market group or approve any native water correspondence.
Private readback and numerical evidence: `nitrate_water_cycle_primary.private.json`
and `nitrate_water_internal_cycles.json`.

The earlier four-service checkpoint passed 74 focused Brightpath cases and 97
focused Premise cases, plus actual native LCI and installed-wheel checks. Native
scientific hashes remain unchanged. The private cabinet check initially used a
demand-normalized absolute residual unsuitable for its much larger internal
flows. Independent solutions agree; the report retains the unscaled residual
and pure componentwise ratio, and verifies mixed absolute/relative row balances
and exact foreground reference balance without changing coefficients. Pure
relative ratios are dominated by negligible numerical rows, as shown by the
separate diagnostic. This is numerical validation, not LCIA-equivalence proof.

Licensed coefficients and detailed metadata remain in the private pilot area:
`nitrate_material_boundary_full_evidence.private.json`,
`nitrate_material_boundary_preflight.json`,
`nitrate_water_network_full_evidence.private.json`,
`native_water_balance_alternatives.private.json`, and
`native_nitrate_matching_service_lci_report.json`.

## HDPE recyclate and foam-glass follow-up

Fresh read-only inspection covers 41 complete source/native records for the two
required supply markets, their explicit recyclate branches and native product
candidates. Four additional native recycling records cover both installed
releases of `xx Recycling PE` and `xx Recycling mixed plastics`.

The four HDPE recycled-to-generic entries are each positive
2.93285310007683e-14 kg per source kg. Each renaming provider buys a nonempty
recycled-HDPE market, which buys actual recycling production and delivery.
Their 0.001-kg declared production volumes do not make these entries zero or
burden-free. Native PE recycling handovers are explicitly empty and allocate
recycling burdens to the recycled product's production. Their documentation's
rough avoided-product/electricity suggestion does not establish a complete
attributed recyclate supplier. Mapping these source supplies to an empty
handover or virgin polymer would therefore misrepresent their functions.

The [HDPE trace-omission proposal](uvek-hdpe-trace-omission-proposal.md) requests
an explicit decision for only the four positive trace entries. Its private
five-input candidate retains both virgin shares and all three delivery entries
without normalization and produces finite native-background LCI. The omission's
score effect remains unquantified. No mapping, omission gate or approval has
been enabled while that decision is pending.

The foam-glass market contains 0.99009900990099 kg of cullet-based production
and 0.0099009900990099 kg of production without cullet, plus four delivery
entries. The two producers have positive kg references and the same declared
density and conductivity. Their actual feedstocks and melting inventory differ:

| Quantity per kg of foam glass | Cullet-based source | Source without cullet |
| --- | ---: | ---: |
| Sorted cullet, kg | 0.81385 | 0 |
| Lime, kg | 0 | 0.152596875 |
| Silica sand, kg | 0 | 0.4567540762 |
| Light soda ash, kg | 0.046154 | 0.2270387258 |
| Electricity, kWh | 1.4969 | 1.8034939759 |
| Direct fossil CO2, kg | 0.029385 | 0.1613851150 |

The native at-plant process matches the source's cullet-based electricity and
cullet quantities (the latter within one binary floating-point ULP). No native
cullet-free producer is established. The regional-storage wrappers add logistics
and electricity-certificate adjustments, so they do not supply a cullet-free
production inventory. Do not collapse or omit the second producer, infer a
density/conductivity multiplier, or claim feedstock equivalence from its copied
technology paragraph: the explicit description and actual exchanges identify
the no-cullet adjustment.

The source variant retains nine parameter definitions versus two in the original;
no exchange formula is present in the imported records. The independent delta
audit also finds changed uncertainty and pedigree fields outside the nominal
feedstock changes, including feldspar and potassium-nitrate inputs. Small
binary rounding differences in waste-wood quantities/locations are retained
in the report separately. A future reconstruction cannot assume that all
unchanged nominal ingredients have unchanged uncertainty, or discard the source
parameter declarations. No hybrid process or new foam-glass correspondence
is enabled by this inspection.

Private evidence: `nitrate_hdpe_foam_routes_primary.private.json`,
`native_pe_recycling_candidates.private.json` and `foam_glass_feedstock_delta.json`.
The delta check is reproduced by `check_foam_glass_feedstock_delta.py`.
