# Final ferrosilicon supply reconstruction review

The last three unapproved supplies in the ecoinvent 3.9 cut-off ferrosilicon producers are reconstructed using three on-demand helpers. This introduces no runtime Python change.

Coal tar retains all three producer shares and both original rail/road freight quantities. Each producer supplies native GLO coke-plant tar; the very small rare-earth-processing tar co-product is an explicit production-route/allocation proxy. Its reference product is coal tar, not rare-earth oxides. Petroleum coke retains all three producer shares and all four rail, barge, road and dry-bulk-sea freight quantities. Producer inventories use complete native RER refinery petroleum coke. Separate original rows and freight uncertainty remain intact, without renormalization.

Freight uses complete native RER rail, barge and fleet-average lorry services, and OCE transoceanic freight shipping. Fleet, traction, vessel, load, geography, technology, allocation and vintage differences are screening limitations. No additional freight, loss, combustion or recovery credit is introduced.

RoW low-pressure gas retains a 1 m³ helper reference with 36 MJ of complete native RoW low-pressure gas at consumer. The source production properties explicitly declare 36 MJ net heating value per m³. This is an energy-functional mapping, not identical volume-state, composition or carbon-mass proof. Existing source 3.12 selected rule and its RER helper remain unchanged; only its approval fingerprint was refreshed after checking the old fingerprint, identical selected rule and unchanged old recipe.

Caller signs, quantities, supported uncertainty, formulas and separately listed emissions are preserved. Product grade, moisture, carbon composition, allocation, internal uncertainty, biosphere exclusions and score effects can differ. Only inspected 3.9 cut-off profiles are newly approved.

Validation: 20 Brightpath tests, one Premise fresh/cache/withdrawal test, and six signed finite composite native LCIs covering all three new helpers passed. Source database modification metadata remained unchanged. The complete two producer inventories now migrate successfully into 21 records, including their converted dependencies; the probe reports zero unapproved correspondences. This proves prerequisite migration, not the missing global market mapping or full steel transformation/export. Both producer inventories retain their eight direct biosphere exchanges.

Review SHA256: 36f4920a95d3a9944f8c9f704da6505db8341272d1636972f7ad2b5550a7d932. Brightpath commit: 15f492021dcc9a2a0b20fd84018744a3788bd31f. Next: build and test the complete ferrosilicon market mapping, then resolve the industrial-heat preparation root and rerun steel.
