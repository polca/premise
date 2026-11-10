# Native three-layer heat-market planning

`plan_native_heat_markets` builds owned secondary-supply, buildings-end-use and
industrial-end-use mixes using the reviewed native and pinned supplemental
suppliers. It plans records without appending them or altering source inventories
or IAM arrays. Public heat remains gated pending consumer relinking, callback
integration, certification and export validation.

Final-energy volumes are multiplied by their supplier's nominal delivered-heat
factor **before** normalization into market shares. A supplier already supplies
one MJ of delivered heat; its LCI is not multiplied by that efficiency again.
Heat-output technologies, including exergy-allocated CHP heat and purchased heat,
retain factor one. Supplier reviews, limitations, source IAM volumes, converted
volumes, factors and existing IAM-role assumptions remain in market provenance.
The industrial resistance-heat mapping therefore retains its explicit assumption
about REMIND mechanical-work/low-temperature electricity.

Secondary markets are built first. The district-delivery planner then copies the
complete reviewed native network, replacing only its plant-heat supplier with
the regional secondary mix. Actual native coefficients (1.11 MJ plant heat and
one MJ network service per delivered MJ in this prototype), formulas, uncertainty,
other flows and infrastructure remain. Neither `1/(1-0.11)` nor another loss or
conversion factor is introduced. Purchased heat in both end-use layers points to
this delivered service, rather than bypassing the network.

World fallback markets follow Premise's existing regional aggregation principle:
regional markets are weighted by their actual delivered-heat volumes. The IAM
World row is not counted as another producing region. A World end-use demand
therefore preserves the regional end-use mixes and their regional purchased-heat
paths. World delivery uses the World secondary mix with the same complete native
network boundary. No source IAM arrays are rewritten. Positive World-only roles
without a regional volume basis are refused instead of being reported as a
successful unchanged update.

Nonfinite or negative IAM volumes, incompatible units/dimensions, wrong-layer
roles, missing reviewed suppliers, unreviewed residual/frozen compositions and
positive purchased heat without a regional secondary/delivery service are refused
before source mutation. Onsite-only and secondary-only IAM inputs do not invent
missing layers. Owned codes are deterministic; repeated planning and JSON-cache
replay create no duplicate markets or delivery copies, and conflicting identities
or codes are refused. Scenario shares are deterministic, not invented future
uncertainty distributions; complete supplier uncertainty remains unchanged.

The real encrypted REMIND SSP2-NPi 2050 development run uses input SHA256
`1720125d7890fc4dadda88cd3abc978b2421be764756b0a9624d6dac272f6aa4`, retained in a
private content-addressed archive. This differs from the encrypted input used by
the last public biomass/electricity baseline; no equality of those scenario
results is claimed. The private heat plan contains 39 markets (12 regional plus
one World fallback in each layer), 13 native delivery copies and one retained
boiler: 53 owned records. Together with 76 CLIC source records and 530 mapping
helpers, the 659 prepared/owned records link completely to the unchanged native
background and canonical ecoinvent 3.10 biosphere.

Fourteen signed demands cover a regional and World market in every layer plus
World district delivery. Their prepared supply and combined native/direct
inventories are finite; original native scientific signatures and the original
55 pins remain unchanged. These are private planning/LCI checks, not consumer
relinking, a public sector update, certification, export or LCIA equivalence.
Other IAM selections with unreviewed roles still need qualification. Geography,
heat grade, technology, allocation, vintage, native biosphere exclusions and
unquantified score effects remain explicit supplier limitations.
