# Active UVEK operationalization goal

## Original objective (retained)

Resolve all remaining mapping work for the local Premise UVEK electricity pilot autonomously, starting from 22 of 42 originally deferred CLIC exchanges already restored and 20 exchanges still deferred across 14 supplier identities (16 exact source-profile cases).

Scope and constraints:
- Work in /Users/romain/GitHub/premise (premise-CLIC) and /Users/romain/GitHub/brightpath (main), using UVEK 2026 cut-off technosphere with ecoinvent 3.10 biosphere. Preserve the existing public user experience and ecoinvent behavior.
- Preserve the selected nine CLIC roots and 55 source records, foreground ownership, signed physical quantities, formulas, supported uncertainty, provenance and complete links.
- Cover remaining waste-treatment markets/routes (7 exchanges), gas-delivery boundaries (3), solution reference bases (6), and specialty-material counterparts (4). Inspect actual reference inventories and evidence rather than trusting previous heuristic candidates.
- Do not introduce cut-off/consequential conversions, unsupported unit or concentration factors, wrong-material substitutions, invented credits, or silent omissions.
- Continue the current in-progress inert-waste and mineral-oil market reconstruction. Preserve original treatment shares and freight; retain exact audits for verified empty allocated clinker handovers. Do not substitute a landfill for a kiln handover or an empty recycling inventory for a nonempty source.

Autonomy and decision records:
- Use the user's standing auto-approval of forthcoming modelling decisions and existing authorization to omit unsupported exchanges with warnings; do not interrupt for routine scientific or implementation choices.
- Prefer verified counterparts, justified complete functional proxies and faithful reconstructions. Where evidence truly cannot establish a defensible counterpart, make an explicit final unsupported-exchange omission decision under that authorization, recording the scientific limitation and unquantified effect. Do not simply relabel a postponed investigation as resolved.
- Maintain a reviewable decision log in the repository with each original deferral's identity, source pin/profile, signed amount and reference basis, evidence/hashes, alternatives rejected, selected mapping/factor or justified omission, uncertainties/limitations, validation and commit references. Record unresolved evidence honestly.
- Keep original omission decisions and restoration history auditable. Make targeted local commits as verified batches finish. Do not push without user instruction. Preserve unrelated user files and clean only verified unused task caches/build artifacts.

Verification and completion:
- Use exact-profile approvals, resource/recipe fingerprint and integrity checks, focused regression tests for changed signs, quantities, uncertainty and refusals; replay generated resources and verify final packaged resources.
- Re-run the encrypted REMIND SSP2-NPi 2050 public API electricity scenario, certification, Brightway export and finite LCI. Verify readback of restored inputs, complete retained links, warning/audit/provenance retention and unchanged original background databases.
- Completion requires a final logged disposition for every one of the 20 starting deferred exchanges, no merely postponed mapping cases, passing required checks and a reproducible working export. Explicit final omissions and accepted proxy limitations must remain visible; do not claim scientific or LCIA equivalence or impact completeness where not established.
- Report restored versus final-omitted counts, remaining scientific limitations, validation results, commits and the decision-log location.

## Scope extension approved by the user

Operationalize all 18 remaining standard Premise sector updates for UVEK 2026 cut-off technosphere with the ecoinvent 3.10 biosphere, in addition to completing the electricity-pilot mapping objective above. This is part of the same active goal, not a replacement or a separate optional task.

The actual public update registry in premise/database/new_database.py contains these 18 updates besides electricity and the generic external-scenario hook:
biomass, cement, steel, fuels, renewable, metals, mining, heat, cdr, battery, cars, two_wheelers, trucks, ships, buses, trains, final energy, emissions.
The external hook remains compatible where invoked; it is not counted as a nineteenth standard sector.

### Implementation and scientific requirements

- Finish the electricity mapping work and its packaged-resource verification first, preserving its nine selected roots and 55 source records as a regression baseline. Those counts constrain the original electricity recipe, not the expanded sector graph; add only the documented, necessary inventories/roots for each other sector.
- Audit each sector's actual native supplier coverage, IAM dependencies, supplementary inventories, units/reference quantities, production and treatment signs, utilities, infrastructure, relinking, emissions and export contracts. Track concrete blockers and dispositions per sector before claiming it supported.
- Add family-specific uvek_aliases alongside existing ecoinvent_aliases in the appropriate YAML mappings where applicable. Use verified native counterparts, scoped functional proxies or evidence-based reconstructions; do not derive UVEK versions or identities by pretending they are ecoinvent releases.
- Implement the necessary family-aware transformations, inventory preparation, data/role selection, scenario caching, validation/certification, reporting and exports. Shared code must preserve existing ecoinvent behavior.
- Preserve the existing public user experience: the normal NewDatabase constructor, update selection, combined/default update workflow and export paths remain the route. Do not require a separate user-facing conversion workflow, manual helper installation or sector-specific repair steps.
- Keep strict refusal gates for sectors until their real implementation and verification pass. Removing an electricity-only gate, silently skipping unmatched technologies, returning unchanged inventories or substituting undocumented defaults does not operationalize a sector.
- Handle valid IAM-specific availability explicitly through established behavior; show actual scientifically intended changes for supported inputs. Document legitimate non-applicability and missing required inputs instead of claiming a no-op as success.
- Keep cut-off/consequential conversion restrictions, native/reference database immutability, signed quantities, supported uncertainty, correct solution/unit bases, foreground ownership, complete links, provenance and explicit scientific limitations. No silent fallback from CLIC to workbooks.
- Continue autonomous modelling decisions under standing auto-approval. Record each new proxy, reconstruction, omission and boundary change with evidence and limitations for later review. Final unsupported source-exchange omissions may be used where evidence warrants them, with visible warnings; entire promised sector updates cannot simply be relabelled unsupported to satisfy the goal.
- Make targeted local commits as verified batches finish, preserve unrelated files and clean only verified obsolete task caches/builds. Do not push without instruction.

### Verification and completion

- Maintain a sector tracker covering all 18 named updates: requirements, native mapping coverage, blockers, decisions, implementation commits, tests and integration/export evidence.
- Test sector-specific physical invariants and failure/refusal paths, relevant shared regressions and unchanged ecoinvent behavior. Verify cold/warm cache equivalence, quantity/sign/uncertainty handling and warning/provenance retention where affected.
- Run real public-API sector updates against the installed UVEK2026/biosphere3.10 background with valid existing IAM inputs. Check expected transformed quantities, relinking and inventory integrity; require certification and reproducible Brightway exports with finite LCI for representative demands.
- Verify a combined run in dependency order, including the normal default update path, so individually working sectors do not mask integration conflicts. Use additional valid IAM scenarios only when needed to exercise capabilities absent from the electricity pilot's REMIND SSP2-NPi2050 data; do not invent IAM data to claim integration coverage.
- Verify packaged resources in isolated wheel installations and update user/developer documentation and generated references when support or selection changes.
- Goal completion requires the original electricity mapping objective plus all 18 sector updates operational through the existing UX, with no postponed implementation or mapping blockers, passing required checks, reproducible exports and a requirement-by-requirement completion audit. Explicit scientific limitations must remain visible. Do not mark the goal complete after the electricity milestone alone.

No new deadline or token budget has been imposed. The previous electricity-only ETA does not apply to this expanded scope. Estimate the additional work after the sector blocker audit.

