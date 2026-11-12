# End of life timing in Trails exports

End-of-life timing is relative to the event represented by the calling dataset.
The optional `timing_reference` column in the temporal CSV distinguishes:

* `new_asset`: retirement after the caller's fixed service life, rounded to the
  nearest year. There is no automatic additional year.
* `existing_fleet`: retirement after the remaining life of assets supplying
  current service. Explicit `fleet_age_offsets` and `fleet_age_weights` are JSON
  lists of production offsets (zero or negative) and their weights. For a fixed
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

The table does not supply empirical fleet histories for direct existing-fleet
retirement calls. The `existing_fleet` option therefore requires explicit input
weights and is tested separately. It is not inferred from the caller lifetime.
For example, lifetime 20 and offsets `[-5, -15]` with weights `[0.5, 0.5]` give
retirement offsets `[5, 15]` with weights `[0.5, 0.5]`. Ages exceeding the fixed
lifetime, invalid weights and conflicting reference events are rejected.

The exporter writes `trails_temp/end_of_life_timing.csv` with the actual caller,
supplier, reference event and applied profile. Unresolved assignments appear in
the existing faulty-exchange report. The inventories and their exchange amounts
are not modified by these timing rules. Maintenance parameterisation is unchanged.

These are deterministic timing assumptions. The correction does not reconstruct
historical commissioning, stock growth, utilisation or a distribution of service
lives within a cohort. The supplied vintage profiles remain proxies, not
scenario-specific cohort estimates. A full cohort model and its data validation
are outside this correction.
