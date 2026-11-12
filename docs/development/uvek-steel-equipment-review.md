# Steel: six equipment prerequisites

Six ecoinvent 3.9 cut-off infrastructure correspondences have independently
matching facility bases in native UVEK 2026:

| Facility | Annual capacity (tonnes) | Lifetime (years) |
| --- | ---: | ---: |
| Blast oxygen furnace converter | 1,500,000 | 50 |
| Electric arc furnace converter | 500,000 | 50 |
| Aluminium electrolysis facility | 130,000 | 50 |
| Aluminium hydroxide factory | 1,220,000 | 50 |
| Anode factory | 80,000 | 50 |
| Blast furnace | 1,500,000 | 50 |

Both source and native production references are +1 facility unit. The reviewed
native suppliers are complete, nonempty infrastructure inventories. Mapping
factor +1 preserves the original caller's infrastructure fraction; no capacity,
lifetime or operating-energy multiplier is introduced. Shared-site descriptions
and the electrolysis/casting 50/50 land allocation were checked. The mapping
does not add casting infrastructure or a second shared-site burden.

The frozen Brightpath evidence is
`export/uvek_steel_39_equipment_review.json`, SHA256
`f9b76ab6a03ee71c5633d5775a38605565e3e7d2e63496bd511a1156e236ce71`.
It records identities, reference quantities, source/producer/native inventory
hashes and limitations, without publishing licensed inventory vectors.
Existing Brightpath rules remain unchanged; Premise adds only six exact
3.9 cut-off approval scopes. Previous approval operations remain unchanged.
No runtime Python module changes or new helper datasets are required.

Complete native regional infrastructure replaces the source geography and
background. Materials, disposal, allocation, vintage, internal uncertainty and
native biosphere exclusions can differ. Score differences are unquantified.
No reverse, other-release or cross-system-model approval is implied.

Verification: the focused strict harmonization test passes (16.87 seconds).
It checks all six scoped fingerprints, target catalog validation, unchanged
negative quantities, uncertainty and formulas, source non-mutation, fresh and
cached results, reported limitations, and refusal after approval withdrawal.
Black and Ruff pass. The regular conda environment installs Brightpath from
GitHub commit `00f6c902ae5a5c262507979f0df45ceabd29ac5a`; its installed
commit identity and all 764 approval scopes validate. This is preparation evidence, not successful steel
transformation, exported database or signed LCI qualification. A real encrypted
REMIND steel preparation rerun is being checked separately.
