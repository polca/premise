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
