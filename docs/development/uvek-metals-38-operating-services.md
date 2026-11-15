# Metals: five native operating services

The original ecoinvent **3.8 cut-off** source database and native UVEK 2025/2026
suppliers were inspected by exact identity. Five source services now use complete
native inventories. All source and native reference amounts are positive one.
No helper dataset, physical omission or production Python change is introduced.

| Service | Reviewed native correspondence |
| --- | --- |
| 1 MWe lean-burn gas CHP electricity | Same named 1 MWe lean-burn CHP, exergy allocation, per kWh |
| 1 MWe lean-burn gas CHP heat | Same named 1 MWe lean-burn CHP, exergy allocation, per MJ |
| RoW >32 t EURO3 lorry | Complete European 32–40 t historical freight fleet, per tonne-km |
| 18.5 kW diesel generating-set market | Complete native generating-set operation per MJ of burned-fuel service |
| High-load diesel machine operation | Complete native average building-machine operation, scaled to source diesel use per hour |

The CHP matches correct the former small-cogenerator and district-heat proxies.
Source and native describe the same technology and exergy allocation. Native
uses high-pressure gas and already includes a low-pressure leakage adjustment.
Geography, regulation, fuel supply, operation and vintage still differ. No extra
pressure service, grid loss, network, heat/electricity credit or efficiency
factor is added.

The lorry proxy narrows the former unrestricted fleet match to a matching heavy
mass class. Native historical fleet, upper mass, emission standards, loading,
empty trips and geography differ from the source >32 t EURO3 service. Quantities
remain tonne-km; there is no invented payload or vehicle-km factor.

The source diesel-generating-set market has one 1-MJ provider, whose fuel input
is **0.0234 kg diesel/MJ**. Native consumes **0.02336 kg/MJ** and embeds a 10 MW
set instead of 18.5 kW. Keep factor +1 on the burned-fuel service. This is not the
older source case consuming 0.0666667 kg/MJ; that case's 2.8538813 multiplier
must not be reused. No electrical-output or equal-infrastructure claim is made.
The already reviewed source-3.9 supplier and factor remain unchanged, with its
fuel basis independently confirmed and its full-rule fingerprint refreshed.

The machine-hour source consumes **37.6322039589003 kg diesel/hour**; complete
native average machine operation consumes **0.023364 kg/MJ**. Their ratio defines
the conversion to native MJ per source hour. This preserves the deterministic
diesel draw, not equivalent machine work. Machinery, power/load, filter mix,
sulfur, lubricant, infrastructure, geography and emissions differ. The unrelated
PEM-electrolyzer maintenance match is removed for this exact source profile.

All decisions use the user's standing auto-approval, retain signed caller
quantities and separate emissions, scale supported uncertainty consistently,
and disclose unquantified score/stochastic effects. Full licensed inventories
remain private. Frozen evidence:
`export/uvek_metals_38_operating_services_review.json` in Brightpath.

Twelve new Brightpath cases and the 84 prior nitrate-goods cases are verified
across the initial run and focused replay-fixture correction. The historical
authoring fixture restores only explicitly frozen later parents: five operating
rules and two pre-existing rail corrections. It still refuses changed or newer
parents; no authoring guard is weakened. Premise connector checks cover all five
services, signed normal/lognormal inputs, cache replay, exact approval refusal,
and the previous generator decision. Ten signed LCI solves pass using memory
only, with no new database, registered activity or processed file and unchanged
source/biosphere metadata. The full snapshot-freshness diagnostic retry leaves
**five chemical refusals in this first source-3.8 cohort**. This is not the count
of all remaining work. Metals remains unfinished and outside the public sector
gate; its native technology aliases and shared sector execution still need work.

The current batch also passes clean tracked generation of all three resources
byte for byte and 86 installed-package checks (31 Brightpath, 55 Premise) in the
private validation environment. The user environment is unchanged. See
`uvek-metals-mapping-batch-verification.json`.
