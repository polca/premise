# Untreated wood and polyethylene waste markets

The truck workflow needs two ecoinvent 3.8 waste-treatment markets that have no
complete native BAFU counterparts. The new mappings retain each market's twelve
treatment shares and seven collection-transport entries, without renormalizing
or merging them. Signs, freight uncertainty and the wood market's constant
transport formula are retained.

The eleven uncontrolled-treatment routes in each market reuse complete,
previously reviewed models for the same waste, technology and infiltration
class. The adopted wood models come from ecoinvent 3.12; polyethylene models
come from 3.9. **These are explicit inventory-version screening proxies**, not
reconstructions of the original 3.8 treatment inventories. Warnings record the
changed operating inputs, emissions, time assumptions, allocation, geography and
uncertainty. The resulting score differences have not been quantified.

Municipal incineration uses the previously reviewed complete native service.
Collection uses the existing native lorry and rail services, retaining US diesel
rail and documenting the European proxy for other countries. The wood model's
existing nonphysical waste-accounting indicator is retained; its conditional
omission warning remains applicable if a selected biosphere lacks it. It is
present in the tested ecoinvent 3.10 biosphere.

No paid processor is replaced by an empty recycling handover. No new source
exchange omission, avoided-material credit or energy credit is introduced. All
previously approved source profiles and existing recipe bodies remain unchanged.
There are two new market helpers and no production Python changes.

Checks cover both signs, normal/lognormal uncertainty, both UVEK releases,
transport statistics, treatment quantities, old permissions, cache replay and
permission withdrawal. Four signed LCIs and two independent assembled-treatment
comparisons pass in memory, with no new Brightway database or registered
activity. The normal generator reproduces all three resources exactly.

The real workflow retry reduces its first source-3.8 truck cohort from four
unapproved rules to two: recycled HDPE and PET with recycled supply. This does
not yet qualify the full truck sector. Details are in
`uvek-trucks-two-waste-markets-38-verification.json` and Brightpath's
`data/export/uvek_trucks_two_waste_markets_38_review.json`.
