# Mining: ecoinvent 3.8 liquid nitrogen

The remaining-sector probe first stopped mining inventory preparation at the
European liquid-nitrogen market. The source supplies +1 kg of liquid nitrogen,
with an allocated cryogenic-air-separation producer and road/rail delivery.
The native UVEK process also supplies +1 kg of liquid nitrogen from cryogenic
air separation, with its own allocated production inventory.

Authorize the existing complete native process per signed source kg, for the
exact ecoinvent 3.8 cut-off profile only. This is a screening market-to-plant
proxy. Internal source delivery is replaced by the native plant boundary;
separately listed caller freight remains untouched. Electricity, resources,
infrastructure, allocation, geography and vintage differ. No extra liquefaction,
density scaling, allocation correction or recovery credit is introduced. Score
effects are unquantified. The installed source's biosphere exclusions remain a
limitation. Standing autonomous approval applies.

The inspected 3.8 market and producer vectors differ from 3.9. Existing 3.9
approval therefore does not establish inventory equivalence; this adds a new
independent exact-profile qualification. Signed caller quantities, formulas and
supported uncertainty are preserved.

Brightpath commit `d3e5ea6ecefd98a566d52607248b9c9c13d9eea7` packages the review
and eight passing migration tests. The built wheel contains the same review
bytes. Premise adds one fingerprinted approval, records limitations in fresh and
cached conversions, and pins that Git revision. No runtime Python changes or
new helper datasets are required. Mining remains publicly gated until the
remaining inventories, shared update and export are verified.
