# Shared metals update: freight and background context

The real UVEK metals diagnostic exposed three assumptions that mapping data alone
could not resolve. Its callback did not forward the background family/biosphere;
its freight selector required global ecoinvent market names; and its freight
exchange builder stamped every supplier as `GLO`. The validator also treated
`2026` as a biosphere version and raised the explicit UVEK release error.

The small adaptations pass the existing family and biosphere context through the
shared metals constructor, use the selected biosphere for resource lookup and
validation, and retain each selected freight supplier's actual location.
`NewDatabase` changes only by adding metals to its existing context-forwarding
list. Market construction, distances, mode shares, kg-to-tonne conversion,
allocation and material rules remain the shared implementation.

`data/metals/freight_providers.yaml` declares four exact native aliases: complete
long-haul aircraft freight (RER), ocean container freight (OCE), average
electric/diesel rail freight including shunting (RER), and diesel long-haul road
fleet freight covering all size classes (RER). All have positive one-tonne-km
references. Their complete inventories include their existing operation and
infrastructure; no extra vehicle, airport, port, road, track or fuel burden is
added. Warnings and scenario mapping provenance retain the geographic, fleet,
loading, technology, delivery-pattern and vintage limitations. Historical
ecoinvent provider selection is unchanged.

247 tests pass across the existing metals suite and new native-freight cases.
Coverage rejects similarly named components, incorrect units, unreviewed
geographies and invalid unit references. The existing distance/share arithmetic
is checked with native supplier locations; the original global-provider fixture
now declares its actual `GLO` location.

The actual REMIND SSP2-NPi2050 snapshot diagnostic completes the standard metals
update and exhaustive graph checks: 12,040 activities and zero validation errors,
without export or new Brightway databases. **This is not sector qualification**:
the explicit applicability/filter audit finds zero compiled metal-intensity
pairs because all 17 UVEK technology aliases are absent. There are 27 positive
rules with temporal support for 2050 across 12 technology roles, so this empty
result needs mapping work and cannot be accepted as legitimate inapplicability.

The 103 mining-market groups are a different case: their configured share data
only support 2020–2030, so the ordinary logic preserves their source markets in
2050. Neither extending those time series nor silently extrapolating them is
part of this change. The next work is explicit equipment aliases and compatible
material-intensity conversions, followed by an eligible-year mining-market
check. Public UVEK sector gates remain unchanged.
