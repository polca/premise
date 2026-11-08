# Small diesel generator: fuel-reference proxy

Status: pending explicit approval. No correction or approval scope is enabled.
The exact ecoinvent 3.6 cut-off case is `bp-07968f74bde91ad7`,
`diesel, burned in diesel-electric generating set, 18.5kW`.

The actual source inventory consumes **0.0666667 kg diesel per labelled MJ**.
Its copied 10 MW parameter describes 0.0234 kg/MJ, so the labels and parameter
comment do not establish an unambiguous fuel/electrical-output convention.
The existing native service consumes **0.02336 kg diesel per MJ of fuel**.
A factor of +1 would therefore preserve only about 35% of the source's actual
diesel demand.

The proposed screening proxy preserves the audited fuel demand:

`0.0666666666666667 / 0.02336 = 2.853881278538814`

Use that many native MJ per source labelled MJ, retaining the complete native
service and preserving signs, foreground uncertainty, formulas and separate
caller exchanges. This choice is based on consumed fuel; it does not establish
an electrical-output reference or introduce an assumed electrical efficiency.
Lubricating-oil demand is independently close: approximately 0.00019088 kg in
the source and 0.00019121 kg in the scaled native service. That numerical
agreement does not establish complete inventory or LCIA equivalence.

The native process uses **10 MW generator infrastructure**, whereas the source
represents an **18.5 kW reefer generator**. Its documentation describes the
exploration-site application and operating data from Norway/US. Infrastructure,
size, lifetime/load, operating emissions, lubrication, allocation, geography
and vintage remain explicit approximations. No separate capacity ratio,
additional generator, heat coproduct credit or reconstructed emissions would
be added. Source-site transport absent from the native definition is not
reconstructed. Score effects remain unquantified.

The normalized native demand has a finite baseline LCI with unchanged source
scientific hashes. The strict public harmonizer continues to refuse this case
until the modelling choice is approved. Approval would cover only the inspected
3.6 cut-off source profile; no inverse or cross-system-model route is implied.

Private evidence: `energy_reference_boundary_evidence.json`,
`native_diesel_generator_candidates.json`,
`generator_fuel_proxy_candidate_review.json` and
`native_generator_fuel_proxy_candidate_lci_report.json` in the pilot work area.
