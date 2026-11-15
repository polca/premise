# Shipping qualified through the shared Premise workflow

The combined REMIND SSP2-NPi 2050 run updates biomass, electricity, heat, cement, steel, mining, renewable, CDR, fuels and shipping using UVEK 2026 cut-off with the ecoinvent 3.10 biosphere. The public `NewDatabase.update` path runs with normal checkpoint persistence and enabled validation, then exports 16,191 activities to Brightway. All 11,947 native activity codes remain in the export.

The database is `BAFU:2026-REMIND-SSP2-NPi-2050-ten-shipping-20261115-155721` in project `bafu-2026-ecospold2-biosphere-310`. It contains 13 shipping markets, including EUR, NEU and World, and 13 regional datasets for each of marine oil, biodiesel and ammonia. Every non-World market’s written shares match all positive IAM service quantities. All three now use domestic-plus-international billion tonne-km/year. The previous ammonia alias used energy in EJ/year, and the other two omitted international service; YAML-only corrections resolve those incompatible scopes.

Fifty-two written functional units (13 markets plus 39 technologies) pass 104 signed LCIs: all results are finite and reverse with demand sign. The native fossil ship’s vessel, port, maintenance and waste quantities remain unchanged in all 13 regions. The separate read-only inspector creates no database and leaves source/biosphere metadata unchanged. The initial inspector used unsupported tuple keys; using Brightway integer activity IDs completed the calculation checks on the same export, without another database write.

## What changed

Most work is migration/selector data. Required Python changes pass existing family/biosphere settings through the shared vehicle callback, clear obsolete supplier IDs when the shared relinker redirects a demand, and add shipping to the verified public scope. No transformation equations, alternative UVEK transport engine, validation rule or default sector selection is introduced. Fuel stock bindings remain separate from vehicle technologies and preserve the existing published-role guard. The maintainer role compiler reads the new two-entry fuel support YAML through its existing support-filter mechanism.

The native historical container service and the published biodiesel/ammonia prototypes remain screening representations of regional fleets. Reviewed chemistry, moisture, furnace-size, source allocation, geographic and vintage differences are warnings, with unquantified score effects. This proves operational integration and finite LCIs for this context; it does not establish source/native LCIA equivalence or all IAM scenarios.

## Archive freshness and remaining scope

CLIC’s live metadata endpoint most recently returned HTTP 502. This run therefore uses the existing public option `clic_options={"freshness": "snapshot"}`, resolving the latest published archive (version 20261110T030235Z-613ea498808a, published 10 November) without pinning a specific archive. It does not verify changes published outside that archive. Default live freshness is unchanged; omit this option when live metadata is available.

Ten of nineteen sectors are verified. Metals, battery, cars, two-wheelers, trucks, buses, trains, final energy and emissions remain unfinished. The no-argument update still selects biomass, electricity and heat. Numerical receipts are in `uvek-shipping-public-qualification.json`; the source 3.12 and 3.8 review files document the mapping decisions.
