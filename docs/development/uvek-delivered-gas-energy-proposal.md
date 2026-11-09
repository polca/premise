# Swiss high-pressure gas on its documented fuel-energy basis

Status: pending explicit scientific approval. The exact source profile remains
refused by the public pilot; no mapping or scope has been installed.

The proposal covers `bp-6cada1fa986bcf8b`, the Swiss high-pressure natural-gas
market in ecoinvent 3.9 cut-off, for UVEK 2026 cut-off with ecoinvent 3.10
biosphere. Its current match is gas production in the United Arab Emirates,
which does not represent delivery to a Swiss high-pressure consumer.

The source positive reference is one cubic metre. Both its reference-product
properties and product documentation explicitly give **36 MJ net fuel energy
per source m³**, with a separate gross value of 40 MJ. The source documents
standard cubic metres at 15°C and 1013 mbar. Do not substitute the gross value
or infer equivalence with a native normal-cubic-metre reference.

UVEK has an existing complete Swiss `Natural gas, high pressure, at consumer`
process whose reference is **1 MJ of gas fuel**, with a documented net-energy
basis. It includes upstream gas production and imports, compression energy,
high-pressure network infrastructure, leakage and its own distribution losses.
This is a fuel-supply process; the caller's end-use combustion remains separate.

The proposed direct correspondence is **+36 MJ of that native delivered gas
per signed source m³**. No new gas-production or treatment dataset is needed.
The private numerical candidate uses a temporary one-reference bridge only to
check the proposed quantity basis against the installed background.

This is a fuel-energy screening correspondence. It preserves the source's
declared net fuel energy while retaining the complete native supply chain.
It does not establish identical physical gas volumes, gas mass, composition or
carbon content. The source and native import shares, dates, leakage,
compression, infrastructure, emissions and allocation also differ. Warnings
must retain those limitations and unquantified effects on scores.

Keep the native supply inventory once. Add no temperature/pressure correction,
second gas supply, distribution burden, end-use combustion or avoided-emission
credit. Preserve separately listed caller stack emissions, other inputs,
foreground ownership and formulas; scale the caller's signed amount and
supported uncertainty consistently by the approved factor.

The private candidate links to the installed database and has a finite,
nonempty LCI. Native demand is exactly 36 MJ for one source reference.
After one residual-correction step, independent native and extended-system
solutions agree within 2.08 × 10⁻¹⁶ in relative supply norm; the equation
residual is below 1.44 × 10⁻¹⁴. Native scientific-content hashes match the
unchanged pilot baseline. This does not prove impact-score equivalence,
a public migration, prospective scenario certification or export.

Only the enumerated ecoinvent 3.9 cut-off Swiss profile would qualify.
The older 3.6 European gas market has no verified net-calorific property in
the inspected source and must not inherit the factor 36. Direct gas-volume
correspondences, other source releases, inverse routes and cross-system-model
conversions require their own evidence. The existing user experience and
warning/provenance path would remain.

Private evidence in the pilot work area:
`gas_energy_delivery_full_evidence.private.json`,
`gas_energy_proxy_candidate_review.json` and
`native_gas_energy_proxy_candidate_lci_report.json`.
