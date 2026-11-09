# Polystyrene waste markets: inspected boundaries and pending FAE proxy

This is an inspection checkpoint, not an enabled mapping. It covers the eight
remaining ecoinvent 3.9 cut-off polystyrene markets in the UVEK 2026 technosphere
/ ecoinvent 3.10 biosphere electricity pilot: PE, RoW, CO, IN, CY, BR, ZA and the
European RER market group. No source market, country or treatment share is
removed to simplify the pilot.

Fresh read-only inspection retrieved 181 complete source/native inventories,
including the European country's delegated waste markets and their direct
operating/freight suppliers. The source waste graph has 195 inspected edges.
All 263 distinct source elementary definitions used by these PS inventories
have installed canonical 3.10 counterparts with the same publisher UUID, unit,
complete compartment and flow role. No publisher identity is missing in this
inspection. Label differences still require explicit provenance when recipes
are authored; identity coverage alone does not approve a mapping.

Regional markets contain open burning, open dumps and unsanitary landfills
with dry, moist, wet, very-wet and hyperarid infiltration classes. The European
group also delegates to country mixes containing sanitary landfill and Swiss
fly-ash extraction. Retain every source fate, elementary exchange, operating
input, collection entry and uncertainty; no fate normalization or replacement
of an uncontrolled disposal class by Swiss incineration is justified.

The complete native PS incinerator is the specific 0.2%-water process, not the
broader building-waste wrapper or expanded-PS process. Both ordinary 3.9 RoW
incineration and Swiss FAE document the same 44 wet-composition coordinates as
the native 2026 process and the previously inspected 3.6 PS source. All three
have the same inspected direct fossil-CO2 amount per kg. This does not establish
full inventory or LCIA equivalence. Reserved pure-polymer carbon properties in
the ordinary source differ from its documented wet waste model; do not infer a
carbon, density or moisture multiplier from that inconsistency.

## Pending modelling decision: Swiss fly-ash-extraction portion

The European group purchases 0.016887685835756984 kg of the Swiss waste market
per kg. That Swiss market assigns 0.98999922959431 kg to FAE per kg, giving
0.016718795967030155 kg of FAE treatment per kg of European group. The Swiss
market's remaining open-dump/open-burning fractions must also remain unchanged.

Proposed screening proxy: use the complete native 0.2%-water PS municipal
incinerator per original signed kg only for this FAE portion. The waste type,
wet composition and kg reference basis match. Native UVEK does not represent
the source's fly-ash extraction and zinc-bearing hydroxide-sludge recovery;
source documentation reports 0.000515 kg dry sludge per kg of FAE waste.
Warnings must retain ash/residue handling, recovery, wastewater/emission,
energy/allocation, geography, vintage and unquantified-score differences. Add
no hypothetical recovery credit, second incinerator or moisture scaling.

This needs a separate user decision: earlier PET/cable FAE approvals do not
establish this PS correspondence. Approval would apply only inside this exact
3.9 cut-off European PS market closure. It would not approve standalone FAE
services, expanded PS, flame-retarded insulation, other releases, reverse or
cross-system-model routes. Other country fates and collection must be preserved.

Private primary evidence: `nitrate_ps_full_evidence.private.json` and
`nitrate_ps_biosphere_definitions.private.json`. Reproducible inspection reports:
`nitrate_ps_market_preflight.json`, `nitrate_ps_composition_preflight.json` and
`nitrate_ps_documented_composition_preflight.json`. The production-property
comparison and documented wet-composition comparison are deliberately distinct.
No new migration rule, helper or approval has been enabled at this checkpoint.

## Regional helper preparation

A private derived-target draft now covers the seven regional markets outside
the European group. Fresh read-only checks bind 37 required source/native
inventories to the earlier inspection. Eleven shared source-treatment helpers
retain 1,081 direct elementary exchanges, including signed/zero quantities and
uncertainty. Their 212 distinct elementary definitions match the installed
canonical 3.10 UUID, unit, complete compartment and flow role. Ten original
operating inputs match the previously inspected complete diesel-combustion and
skid-steer services. The seven market drafts retain all 30 original treatment
terms and ten collection-freight entries, without aggregation or normalization.

Ordinary incineration uses the specific reviewed native PS process. No FAE
branch is included. These are preparation artifacts, not an enabled converter:
packaged review/scopes, deterministic replay, regression tests, actual native
LCI and installed-package verification are still required before admission.
Private artifacts: `nitrate_ps_regional_helper_draft.json` and
`nitrate_ps_regional_draft_primary.private.json`.
