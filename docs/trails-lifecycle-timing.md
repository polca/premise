# End of life timing in Trails exports

End-of-life timing is relative to the event represented by the calling dataset.
The optional `timing_reference` column in the temporal CSV distinguishes:

* `new_asset`: retirement after the caller's fixed service life, rounded to the
  nearest year. There is no automatic additional year.
* `existing_fleet`: retirement after the remaining life of assets supplying
  current service. Explicit `fleet_age_offsets` and `fleet_age_weights` are JSON
  lists of production offsets (zero or negative) and their weights. The optional
  `fleet_lifetime` explicitly pairs a service life with those weights, taking
  precedence over an unrelated legacy stock classification. For a fixed
  lifetime L and age a, the remaining life is L-a. The age profile must refer to
  the asset being retired, rather than unrelated infrastructure in the process.
* `current_output`: treatment of waste generated with current production stays
  in the caller year.
* `lifecycle_event`: treatment, recycling and market substeps stay in the event
  year. They do not start another asset lifetime.

The supplied table makes these reference events explicit. Stock-asset datasets
are treated as new production when traversed. Their incoming vintage profiles
can place them in several historical years, after which their fixed lifetime
already produces several retirement years. Do not apply the vintage profile a
second time to the retirement exchange. Ordinary output and market rows do not
receive a new full lifetime merely because they call a treatment supplier.

For identified operating services whose inventories allocate asset disposal,
`temporal/retirement_links.yaml` connects reviewed retirement suppliers to the
construction suppliers for the same asset. The caller must consume exactly one
matching construction exchange with a supported age profile. Retirement then
uses that construction profile's annual weights and the corresponding asset's
service life: a construction offset of -a gives a retirement offset of L-a.
This applies to explicit empirical profiles and to explicitly assumed legacy
profiles. It does not upgrade assumed evidence to empirical evidence. The
construction profile itself, inventory amounts and maintenance are unchanged.

An explicit caller fleet-age profile takes precedence over this derivation.
When an asset link or usable construction profile is missing, ambiguous or
incompatible with the fixed life, the exporter retains the declared fallback
and records the reason. The supplied fallback assumes a stationary stock with
fixed life, constant annual entry and uniform annual ages from 1 through
floor(L). It is not an empirical fleet history or IAM-specific cohort estimate.
Assets older than their assumed service life are not silently clipped or
discarded to force a match. Their remaining life needs additional evidence.
The fallback-generation rules remain in `premise/lifecycle_reference_table.py`.
For example, lifetime 20 and offsets `[-5, -15]` with weights `[0.5, 0.5]` give
retirement offsets `[5, 15]` with weights `[0.5, 0.5]`. Ages exceeding the fixed
lifetime, invalid weights and conflicting reference events are rejected.

The exporter writes `trails_temp/end_of_life_timing.csv` with the actual caller,
supplier, reference event and applied profile. It also reports the retirement
basis, matching asset, construction weights and evidence, and any fallback
reason. Unresolved assignments appear in
the existing faulty-exchange report. The inventories and their exchange amounts
are not modified by these timing rules. Maintenance parameterisation is unchanged.

These are deterministic timing assumptions. The correction does not reconstruct
historical commissioning, stock growth, utilisation or a distribution of service
lives within a cohort. The supplied vintage profiles remain proxies, not
scenario-specific cohort estimates. A full cohort model and its data validation
are outside this correction.
