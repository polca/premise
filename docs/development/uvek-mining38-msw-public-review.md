# Mining: municipal-waste model checkpoint, 13 November 2026

Brightpath commit `f30958d58e21acd07f0bacccbcdcd0f9f28f8008` records independently recalculated public treatment
models for the remaining first-cohort municipal-waste refusal
`bp-168324086066bce9`. The calculations are now verified; the complete disposal
mapping and mining workflow are still unfinished.

The models retain open burning and four climate classes each for unsanitary
landfills and open dumps. All 1,008 direct biosphere quantities (emissions and resources), basic and total
uncertainties, and lognormal locations match the privately inspected ecoinvent
3.8 cut-off cases. The resulting identities, receiving compartments and units
were checked against the installed ecoinvent 3.10 biosphere. Existing verified
nickel/potassium publisher synonyms cover two changed UUIDs. No new biosphere
proxy or mass conversion was introduced.

The quantities were calculated from [Doka's public 2017 tools](https://www.doka.ch/WasteTools2017.zip),
not extracted from licensed inventories for redistribution. Calibration used the
licensed source privately afterward. The original public reports/workbooks and
licensed inventories are not committed. The correct public legacy-MSW mixture
is retained; the similarly named incineration-only mixture was rejected because
its composition differs. Open burning retains urban air, industrial soil and
95% iron/copper recovery. Groundwater and long-term groundwater remain separate.

Seven focused Brightpath tests pass, Ruff and Black pass, and a built wheel
contains the exact reviewed JSON. The adjacent verification receipt gives the
resource hash and per-case counts. See Brightpath's
[calculation notes](https://github.com/romainsacchi/brightpath/blob/f30958d58e21acd07f0bacccbcdcd0f9f28f8008/docs/uvek-msw-public-calculation.md)
for settings, provenance and limits.

This checkpoint changes calculation resources, tests and documentation only.
It does not change Premise's Python modules, sector algorithms, installed
Brightpath dependency pin, mapping approvals or supported-sector gate. The
public tools also report operating inputs and recovered-metal outputs; these
remain required parts of the complete disposal recipe, rather than implicit
omissions or avoided-primary-production credits.

Next: assemble the full disposal portfolio with reviewed native engineered
processes, operating inputs, existing cut-off stock handovers and original
collection freight. Preserve all route weights and uncertainty; convert the
negative waste convention exactly once at the caller. Then run migration,
signed LCI and the real mining workflow through validation and export. Later
inventory groups and the other remaining sectors still need qualification.
