# UVEK trains through the shared Premise workflow

Thirteen of nineteen registered sectors now work for the inspected REMIND
SSP2-NPi 2050 scenario. Trains is the latest addition. Battery, cars, trucks,
final energy, emissions and metals remain unfinished. The full goal stays active.

The public `update(["trains"])` call passes exhaustive graph validation without
bypassing the scope gate. It imports 89 linked records from 34 foregrounds and
creates 39 regional train services plus thirteen regional/World markets. The
combined thirteen-sector checkpoint has 16,474 activities and 503,788 exchanges.
All 104 positive/negative train-service and market LCI calculations are finite.
No Brightway database was written; the latest physical export remains the
previous eleven-sector database. These results use the recorded November 10
archive snapshot. The product's live-resolution default is unchanged.

All three train technologies have actual IAM production. Checks trace each
regional service to its selected native or CLIC prototype, preserve its positive
tonne-kilometre reference, verify the relative efficiency change and fixed
ancillary inputs, and compare regional market shares with IAM data. The final
carbon check preserves scaled recorded exhaust totals and verifies the
fossil/biogenic split against final regional fuel suppliers. Other direct
biosphere flows follow the common efficiency convention. Conditional tank
compression follows hydrogen demand once, retaining the approved pressure caveats.

Most changes are mapping and review data. Native electric/diesel templates use
the existing regionalization, efficiency, market and consumer-relinking methods.
Three newly reviewed source-3.12 markets retain their current composition and
freight uncertainty. Ferrosilicon and mixed-plastic markets retain current shares
while explicitly reusing reviewed older manufacturing/treatment models; their
replaced internals and unquantified score effects are recorded in their reviews.
Twelve actual source-3.9 supply correspondences were checked independently.

Four existing Python modules needed bounded adaptations: bind hydrogen metadata
to exact YAML products; retain compression purpose after grid relinking; retain
original freight identities and apply the existing recorded-exhaust hook to the
mapped native rail names; and admit the verified sector in the public gate.
No alternative UVEK sector engine, NewDatabase argument or replacement sector
equation is introduced. The notebook and default biomass/electricity/heat call
remain unchanged. Detailed decisions and checks are in the companion JSON and
linked review documents.

The initial combined run found 95 exact duplicate freight rows after different
source countries were relinked to the same IAM market. Their origin is now
retained in comments, preserving each quantity and uncertainty. The validator
still rejects duplicate rows from the same original supplier. A separate native
label gap left diesel-rail exhaust at its source carbon split; using the actual
YAML-selected service names fixes it through the existing fuel module, retaining
recorded exhaust rather than imposing a new complete-oxidation total.

Native technology, allocation, geography, vintage and biosphere exclusions remain
screening limitations. No LCIA equivalence, full carbon-balance correction or new
hydrogen pressure proof is claimed. An optional exact repeated-build hash check
also differed in unrelated gas/car columns at sampled floating-point precision;
full columnwise equivalence is not claimed. Qualification rests on the explicit
train physical checks, finite inventories, graph certification and public call.
