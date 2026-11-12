# Steel prerequisite: small-scale heat other than natural gas

The inspected ecoinvent 3.9 cut-off RoW market contains 52 positive supplier
shares totalling one MJ, with deterministic market weights and no direct
biosphere exchanges. The reconstruction retains every share, without omission
or renormalization, in one mix helper. Eight additional helpers reconstruct
pure solar operation; the other branches use complete native heat suppliers.
Thirty-one distinct native suppliers support the resulting graph.

The dominant source branches remain anthracite and hard-coal briquette stoves.
Native lignite/coke stoves, oil boilers, wood heat, heat-pump and biogas CHP
suppliers cover the corresponding remaining classes. These are screening
correspondences, not identical plant or LCIA models. In particular, native
15 kW pellet heat replaces 9 kW; 50 kW replaces 25 kW pellet and 30/100 kW log
furnaces. The native baseline does not establish source 2014 emission-standard
performance. Geography, allocation, efficiency, capacity, climate, vintage,
internal uncertainty and native biosphere exclusions can differ. Score effects
remain unquantified. No fuel-mass, density, efficiency or CO2 factor is invented.

## Crop-stalk decision

The source 6 kW stalk-heater branch supplies 0.0006264488297394893 MJ per MJ
of market heat. Use complete native 6 kW mixed-log heat as an explicit feedstock
proxy for that branch. This substitutes wood/forestry supply for jute/kenaf
stalk supply; agricultural allocation, moisture, carbon and ash composition,
combustion emissions, land use and ash treatment differ. The small heat share
does not establish a small LCIA effect. This decision follows the standing
authorization for documented screening proxies and remains visible for review.

Despite generic waste wording, the source stalk market and all five supplying
activities have positive +1 kg references. The heater purchases stalk supply;
there is no negative-reference avoided-treatment credit to erase or invent.
The complete native heat process replaces the source producer inventory;
separate caller emissions and exchanges remain unchanged.

## Pure solar reconstruction

All eight source solar-operation profiles remain separate, with their original
positive market shares (total 0.000012383390806845222 MJ/MJ). Each retains the
original electricity rows, collector-system fraction and direct solar-resource
amount, including supported exchange uncertainty. Electricity uses complete
native low-voltage grids; Canadian provincial supply uses national CA, with
explicit geographic-proxy limitations. Available source country suppliers are
retained; GLO is used only when no corresponding native grid exists.

Native legacy systems match the source collector areas exactly: 12.3, 58.3,
4 and 10.5 m². Their complete collector-system inventories exclude auxiliary
heating. The actual collector core processes are nonempty, with 29–31
nonproduction exchanges each. No area/lifetime/yield scaling, gas backup,
additional storage or inferred delivery loss is added. This avoids the newer
native systems' different 12/81/5 m² geometry. The original solar-energy flow
identity and compartment resolve exactly in the ecoinvent 3.10 biosphere.
See `uvek-steel-small-heat-basis-verification.json`.

## Verification and remaining scope

Frozen review `export/uvek_steel_39_small_heat_review.json`, SHA256
`8f55c8165ad078cf09de31490bd183a0ac13d3df847623cc8da5be04d3c2a4db`.
Brightpath commit `1cc585dd671d04fff32be9f6d814e7fd23ebf1c4` is pushed and
installed from GitHub. Only the inspected 3.9 cut-off source profile is approved;
other releases, reverse and cross-system-model routes remain unapproved.
No runtime Python modules changed. Maintainer replay, generator hook and tests
support reproducibility and atomic parent/recipe drift refusal. Licensed source
vectors remain private; derived native recipes and identity/reference witnesses
are published.

Nine Brightpath checks pass (18.53 seconds), covering signed demand,
normal/lognormal uncertainty, formulas, immutable foreground, exact scope, all
nine helper inventories, solar geometry/resource retention, original shares,
stalk references and atomic drift refusal. The strict Premise fresh/cache/
approval-withdrawal test passes (16.83 seconds). Black and Ruff pass.
The full recursive mix passes two finite signed composite LCIs with exact sign
reversal. This includes all 31 weighted native suppliers and the additional
solar resource, not just the technosphere demands. Source database activity
count and modification metadata remain unchanged. See
`uvek-steel-small-heat-composite-lci-verification.json`.

Actual encrypted REMIND SSP2-NPi 2050 diagnostic steel preparation now has
**15 unapproved correspondences, down from 16**: thirteen waste/wastewater
cases, one other-fuel industrial heat mix and ferrosilicon. Only the closed
steel support guard was bypassed; strict migration validation stayed enabled.
No successful steel transformation/export is claimed. Private audit receipt:
`1660fad0c2a3f9955678f81555c449f278b003002ef75f123068582eb834045e.audit.json.gz`.

The regular environment's installed GitHub commit identity and all 795 approval
scopes validate. One superseded task wheel cache was removed (41,602,409 bytes);
the current wheel, installed environments, inventories, exports and unrelated
caches remain. Private receipt: `steel-small-heat-pip-cache-eviction.json`.
