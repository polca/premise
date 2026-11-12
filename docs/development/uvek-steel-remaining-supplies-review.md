# Steel: fifteen remaining material and supply correspondences

Fifteen independently inspected ecoinvent 3.9 cut-off identities now have exact
Premise approvals for complete native UVEK 2026 supplies. Fourteen existing
Brightpath rules are unchanged. The hard-coal rule gains only a verified +1 kg
reference decision for 3.9; its complete selected 3.8 resource remains identical.
No runtime Python modules or new helpers are introduced.

| Group | Cases | Reference and principal limitations |
| --- | ---: | --- |
| Liquid oxygen, argon and nitrogen | 3 | kg of liquid product; complete separation/purification inventories replace market mix, allocation and delivery |
| Pig iron and iron sinter | 2 | kg of complete product; native production replaces source BAT inventory and by-product handling |
| Quicklime pieces and milled quicklime | 3 | kg in the stated product form; native Swiss kiln/milling supply replaces geography and producer mix |
| Hard coal | 2 | kg of complete supply; native European storage/transport models replace source regions and quality details |
| Rail services | 3 | tonne-km of complete service; Indian/South African markets use RER fleet, US-labelled diesel model is European-based |
| Heavy industrial machinery | 1 | kg of machinery infrastructure, not a whole machine; same rock-crusher-based 25-year reference |
| Global high-voltage electricity | 1 | kWh of complete HV grid supply; native portfolio and grid conventions replace source market-group weights |

All source and native production references are +1 in the same unit; factor +1
preserves caller quantities, signs, uncertainty and formulas. Complete native
inventories retain their own upstream quantities and direct emissions. Separate
caller freight and emissions are not removed or doubled. Screening limitations
cover allocation, geography, technology, vintage, internal uncertainty, native
biosphere exclusions and unquantified score differences.

Important boundaries: liquid gases receive no density or second-liquefaction
factor. Pig iron retains native 0.985 economic allocation and 94% slag reuse/6%
landfill assumptions, without an extra recovery credit. Quicklime pieces and
milled quicklime remain distinct; no additional calcination, milling or CO2
multiplier is added. Coal stays on a mass basis: the source's 27.91 MJ/kg does
not establish identical native fuel quality. The native grid vector is retained
literally; mixed wording in its documentation does not justify assuming zero
losses or adding an inferred loss multiplier. Machinery documentation recommends
more specific data if this input materially affects results.

Frozen review: `export/uvek_steel_39_remaining_supplies_review.json`, SHA256
`bc3f80a505bef6a960aef939194151a52be1ebe4508114abdb3faf27e5423cbe`.
Brightpath commit `425199bfa05aa0556d836958863a216d25105632` is pushed.
The reference extension preserves the old coal rule's source, target and factor.
Its existing 3.8 approval fingerprint is refreshed because fingerprints bind the
whole shared rule, with a recorded exact historical-selection comparison.
No other source-version or cross-system-model approval is added. Licensed
vectors remain private; only identity/basis facts and hashes are published.

Two strict Premise supply/product tests pass together (22.09 seconds), covering
all fifteen new cases, exact approvals and target catalogs, source immutability,
negative quantities, uncertainty/formulas, fresh/cache results, warnings and
approval-withdrawal refusal. Three Brightpath coal checks pass (12.86 seconds):
complete historical 3.8 selection equality, replay idempotence and atomic parent
drift refusal. Black and Ruff pass. Thirty signed native LCIs cover the fifteen
mapping rows (thirteen distinct suppliers), with finite inventories, exact sign
reversal and unchanged source database. See
`uvek-steel-remaining-supplies-native-lci-verification.json`.

Mill scale remains unapproved: its older helper combines a misidentified rail
leg with lead-smelter slag disposal, whereas the inspected source has a specific
mill-scale residual-landfill inventory with 68 biosphere exchanges. Correct
freight alone would not establish a valid complete disposal model. This batch
does not silently approve that helper or claim a successful steel export.

## Real preparation and installed package

The actual encrypted REMIND SSP2-NPi 2050 diagnostic preparation now reports
**18 pending correspondences, down from 33**. All fifteen cases in this batch
cleared. Remaining cases comprise thirteen waste/residue/wastewater services,
four fuel/heat boundaries and ferrosilicon reconstruction. See
`uvek-steel-current-preparation-frontier.json` for the exact current cohort.
Only the still-closed steel support guard was bypassed; strict migration
validation stayed enabled. Later preparation cohorts and the transformation
itself remain unverified; no successful steel update/export is claimed.
The regular environment installs the recorded Brightpath commit from GitHub.
Private audit receipt:
`6ef3686bd60c7e7b73c1693a15c8fec0fbd12555790f683a220062c38cab699d.audit.json.gz`.
