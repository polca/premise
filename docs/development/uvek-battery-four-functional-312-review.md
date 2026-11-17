# Four complete native screening proxies

Standing automatic approval and the mapping-only operationalization instruction
cover these explicit screening decisions. No Python module or test is changed.
No helper, manufacturing inventory, extra credit or omission is introduced.

| Required source | Complete native counterpart | Main limitation |
| --- | --- | --- |
| NMC811 oxide market | NMC622 positive active material | Changes Ni:Mn:Co from 8:1:1 to 6:2:2; source calcination energy, supply/delivery and allocation are replaced |
| Chinese NMC532 cell | NMC622 cell | Changes 5:3:2 to 6:2:2 and replaces complete cell components/manufacturing energy |
| European hazardous-waste incineration with energy recovery | Swiss hazardous-waste incineration, 25% water | Changes waste composition, moisture, energy recovery/allocation, emissions and ash treatment |
| Quebec direct wastewater treatment | Swiss capacity-class-1 treatment | Changes loading (native COD 0.1556 kg/m3), climate, removal, sludge digestion, utilities and water returns |

The NMC substitutions use +1 per kg powder or cell respectively. They do not
establish equal composition, energy demand, capacity, performance or impact
scores. Do not add source calcination energy or separate cell components to
the complete native models. No capacity multiplier is introduced.

Treatment references use factor -1: ecoinvent references negative waste input
while native BAFU references positive treatment output. Native signed internal
exchanges remain unchanged. No avoided-energy credit, COD multiplier, sewer
network, untreated-water fraction or water-balance correction is added.
All score differences and the documented BAFU biosphere exclusions remain
unquantified limitations. Exact source identity/profile approvals are retained.

Checks passed: 42 Brightpath and 10 Premise focused tests, 32 signed uncertainty
checks across both native releases, fresh/warm public harmonization, eight signed
finite in-memory LCIs, and byte equality of all three normal generator outputs.
Temporary generator outputs were removed. No Brightway database or activity
was written. Two historical standalone replay test failures remain disclosed
separately; these focused passes do not claim the entire test suite is green.

The saved real battery graph now has four unapproved identities, down from eight:
lithium titanate spinel, recycled HDPE, PVDF and vinylidene fluoride. Native
chemical names that denote another compound are not valid matches. Next work
should address these direct interfaces, preferring complete processes and
avoiding remote upstream reconstruction. The verified operational baseline
remains fifteen of nineteen sectors; battery is not yet qualified.
