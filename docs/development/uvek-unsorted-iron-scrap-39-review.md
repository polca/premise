# Unsorted iron scrap market — 13 November 2026

The source market previously mapped to BAFU iron scrap that already includes collection, sorting and pressing. The source instead buys unsorted cut-off stock and adds only delivery. Adding the native process would introduce another processing stage.

The mapping now retains one kilogram of the existing production-only cut-off stock, 0.0112 tkm rail and 0.01932 tkm lorry per kilogram of scrap, with original quantities and supported uncertainty. Source inspection confirms the stock has no nonproduction exchanges. Its inventory hash exactly matches the already approved ecoinvent 3.9 cut-off stock witness. No new zero-burden supplier or direct stock approval is created. The dependency carries its exact existing profile approval.

Complete RER rail and generic lorry fleet are explicit geographic/fleet proxies for global delivery groups. Warnings record upstream energy, allocation, vintage, internal uncertainty and unquantified score differences. No sorting, pressing, extra collection, avoided-metal credit, yield correction, loss or source omission is added. The separate sorted-and-pressed scrap market has three nonempty producer routes and remains outside this decision.

Standing autonomous approval covers this exact ecoinvent 3.9 cut-off market. Other versions, system models and reverse routes gain no approval; all earlier approval operations and the stock mapping remain unchanged. No runtime Python module changed.

Verification: 17 new Brightpath checks and ten storage/schema checks passed across the initial and corrected runs. Five Premise checks passed, including fresh/cache, signs, uncertainty and refusal without the market approval. Initial test assertions wrongly expected Brightway input keys before ordinary identity-based linking; corrected the tests without changing runtime behavior. Two signed LCIs and one independent native-demand comparison passed entirely in memory, with no new Brightway database, registered activity or processed file. The refined inventory difference was below 5e-19.

A fresh battery preparation retry clears this market and reduces the inspected source-3.9 cohort from 169 to 168 unapproved rules. Swiss municipal-waste incineration is next. Eight of nineteen sectors remain qualified; battery preparation is unfinished.

Machine-readable evidence: `uvek-unsorted-iron-scrap-39-verification.json`. Packaged decision: Brightpath `export/uvek_unsorted_iron_scrap_39_review.json`. This joins the LCD and PVF changes in a six-commit coordinated batch.
