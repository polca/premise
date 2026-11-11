# Frozen native historical heat

UVEK lacks the ecoinvent historical heat markets that Premise freezes when an IAM reports unspecified heat or no secondary supply detail. The shared frozen-market method now accepts a source identity from the existing source-family market mapping. Its original ecoinvent defaults, regional copying, reference products and exclusion from heat-market relinking remain unchanged. This small generic hook replaces neither the heat engine nor its skip behaviour.

## Native basis and delivery boundary

The installed Swiss exergy-allocated district-heat average contains eight plant-heat inputs totalling 1.10978 MJ per delivered MJ, plus a separately listed network service. The authored resource supplies one MJ **before delivery**: divide the eight producer contributions by their nominal total and leave the network service outside this reference. The standard delivery model is applied separately, once. No loss multiplier is inferred from prose or applied twice. The standard delivery prototype can differ from the historical Swiss network and its loss convention.

The portfolio contains natural gas, gas CHP, a heat pump, oil, wood CHP, wood heat, allocated nuclear heat and the native zero-allocated municipal-incinerator heat handover. Complete installed producer vectors are copied under fixed identities so subsequent heat-market relinking cannot turn the historical portfolio into the current IAM mix. Producer inputs, emissions, allocation, quantities and supported uncertainty remain. Native importer biosphere exclusions remain; this does not restore the original archive's excluded flows. The empty native waste-heat handover stays empty: no new incineration burden or recovery credit is invented. Normal electricity and fuel updates can still affect the upstream backgrounds.

This is a global application of a historical Swiss portfolio, not a known future regional fuel mix. Its application to building residual heat is also a district-portfolio proxy rather than an observed small-boiler composition. It includes gas and therefore does not reconstruct the ecoinvent non-gas residual boundary. Heat grade, allocation, end-use technology, geography, vintage, uncertainty and score differences remain explicit warnings. Existing IAM volumes are retained; only unspecified composition is proxied.

Nominal source weights are converted by a fixed basis factor, preserving their marginal uncertainty. No correlation or per-sample normalization of portfolio shares is inferred. The compiled resource contains eight fixed producer records and one generation mix. It is a shared heat prerequisite because missing-supply fallback is synthesized during the heat update, rather than necessarily appearing as an explicit IAM role. Exact source quantity witnesses are checked before use; a changed or absent supplier remains an error.

## Qualification

Controlled cases through normal `NewDatabase.update`, exhaustive validation and Brightway export:

| Case | Exported activities | Regional frozen suppliers | Frozen/market signed LCIs |
| --- | ---: | ---: | ---: |
| MESSAGE industrial solar with missing supply detail | 12,667 | 12 | 94 |
| MESSAGE building residual heat | 12,260 | 24 | 118 |
| IMAGE secondary residual heat | 12,375 | 26 | 124 |

The industrial solar case additionally passed 864 signed LCIs covering the complete CSP dependency graph. Its five-sevenths solar share retains the original final-energy conversion for coal heat: one EJ coal input becomes 0.8 EJ delivered heat, alongside two EJ of source-defined solar heat. The inherited electricity-normalized CSP screening limitation remains; thermal delivery performance is not established.

All eight fixed producer input vectors and effective uncertainty parameters match the installed native records. An omitted false `negative` flag is compared with its documented default false; no distribution parameter is discarded. Regional frozen inputs match the derived pre-delivery portfolio and contain neither an IAM heat-market input nor a network-service input. All links resolve internally and biosphere links use the requested ecoinvent 3.10 database. Every controlled exhaustive graph report has zero issues. The regression group passed 161 tests, with two solar filter tests separately passing. The ecoinvent 3.12 five-sector update still produces 31,205 activities; that regression did not export.

All individual heat role bindings now have controlled coverage. This does **not** finish whole-sector qualification. The first actual encrypted REMIND SSP2-NPi 2050 biomass/electricity/heat/cement run exposed electricity time-period market cycles and zero-amount clinker lognormal uncertainty during exhaustive validation. Those remain to investigate; no certificate or full real-scenario export is claimed. Combined fuel-origin behaviour and every remaining sector are also unfinished.

## Geography and attribution

Additional IMAGE lookups retain original supplier labels and quantities: CPA uses representative CHN, ERCOT and US-SPP use USA, and the documented Europe-without-Switzerland export alias LBFAC32 uses WEU. Native EEU coal storage describes generic worldwide values; CEU is a representative lookup, not a verified region-boundary identity. These geographic proxies do not reconstruct country shares.

Native basis: **Ökobilanz Datenbank der Schweizerischen Bundesverwaltung, BAFU:2026**, [BAFU database](https://www.bafu.admin.ch/de/oekobilanzierung). This authored screening derivative is not an official BAFU dataset. The basis conversion, excluded delivery boundary, fixed producer identities and global/residual application are documented above. BAFU-derived elements retain the [BAFU terms](https://www.bafu.admin.ch/dam/de/sd-web/-9ZObeMCZy8A/BAFU-20XY%20Terms%20of%20Use_DE.pdf), attribution and modification reference in resource metadata. No full proprietary ecoinvent inventory is published.
