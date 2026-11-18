# Emissions: combined-workflow integration check

Status: unfinished; the public emissions update remains unavailable for UVEK. No runtime code was changed for this investigation.

The existing combustion reviews describe source-country callers. The standard heat and electricity updates regionalise those callers and replace many original inputs with handovers to IAM-region datasets. Consequently, replaying the original exact caller/input reviews after those updates cannot succeed unchanged. This is an integration boundary problem, not a request to build more upstream inventories.

| Review | Reviewed pairs | Original input still present | Original input replaced |
| --- | ---: | ---: | ---: |
| uvek2026_gas_review.json | 1032 | 252 | 780 |
| uvek2026_heating_combustion_review.json | 1707 | 1667 | 40 |
| uvek2026_ordinary_combustion_review.json | 837 | 812 | 25 |

Example: the original Dutch “Heat, natural gas, at heat plant” now supplies one MJ through the EUR dataset. The EUR dataset has the actual burner inputs. The existing review still expects the original Dutch caller to consume its Dutch burner directly. It is therefore checking an input that the normal regionalisation has moved.

Next: bind emissions changes to the actual emitting suppliers used by regionalised callers, using their IAM operating region. Preserve fuel demand, technology shares, production references and all non-emission inputs. Check direct process/transport emissions against the shared Premise updater as a separate coverage requirement. Do not qualify a combustion-only subset as the complete sector.

Evidence: read-only endpoint audit of the 19,735-activity combined physical graph used for the wind/solar material-kernel checks. This graph predates the final installed eighteen-sector rerun; these counts are diagnostic, not public-workflow qualification. There were no missing caller or supplier identities: the missing items are the old direct exchanges. No Brightway database was written.

## Follow-up on the exact installed eighteen-sector result

The completed combined graph has 19,735 activities. Matching the already-reviewed caller/supplier **process classes** (name, reference product and unit), while allowing their regional locations, covers 4,000 input positions across the same 18 combustion sectors. There are no conflicting sector assignments for a reviewed pair of classes. Of these positions, 2,587 have different caller and supplier location labels; an operational adapter must distinguish the emitter's operating region from its supplier prototype's geography.

Another 532 positions use reviewed burner names but different caller classes. These are diagnostic results, not 532 new upstream modelling tasks. Of them, 307 use a burner with one existing sector assignment across all reviewed uses. The remaining 225 use four burner names shared between sectors: gas turbine (113), small coal-coke stove (3), non-modulating light-oil boiler (13), and average light-oil boiler (96). Ninety gas-turbine positions are country-to-IAM-region handovers with no direct biosphere exchanges. Their actual emitting inventory is at the regional endpoint; duplicating or adjusting the empty handover would not update emissions.

A separate mapping-only issue is fixed: `GAINS_ei_pollutants.yaml` now includes the exact canonical label `NMVOC, non-methane volatile organic compounds` under the existing VOC category. That label occurs at 5,792 positions in this graph. Its ecoinvent 3.10 biosphere identity, kilogram reference and air compartment were verified. Four shared-updater cases preserve signed quantities and lognormal/normal uncertainty and remain idempotent; all 63 existing emissions tests pass. This does not imply that every one of those positions is selected by the sector mappings, or qualify the unfinished emissions callback.

No additional Python source changes or Brightway writes were made for this follow-up. The remaining integration work is to bind the actual regional emitter to the existing shared factor/update logic and extend the emitter mappings, rather than classify every upstream exchange separately.
