# Approved battery compatibility change

Battery inventory mapping already prepares 951 records. A prior private preview
completed the existing battery update, including 23 mass-learning targets, with
zero graph errors. It exposed two code assumptions that mapping data cannot
correct: the callback defaults to ecoinvent rather than receiving UVEK context,
and battery mass validation assumes that every waste service uses ecoinvent's
negative reference convention. The converted UVEK services have positive
references. Treating their positive waste input as manufactured battery mass
would give the wrong balance.

The approved four-file change forwards source family and biosphere version,
reads the actual reference sign of generated used-battery suppliers, excludes
those services from the manufactured-pack mass sum, and admits battery through
the public support list. It retains the existing mass-balance equations,
tolerances, capacity shares, density rules and fallback for ordinary ecoinvent
waste services. It adds no supplier inventory or sector engine. The new sign
helper is limited to generated used-Li-ion-battery services.

This updates the earlier unapplied proposal to preserve the now-approved final-
energy context and support list. All 92 existing battery tests pass in a private
interpreter preview; no repository Python file was changed by that preview.
The user approved this exact four-file patch on 18 November 2026. Its bytes
are applied exactly. All 92 battery tests pass against the applied source. Two
public-boundary tests still expect battery refusal; their small test-only
refresh is proposed separately. The installed normal live seventeen-sector run, exhaustive graph validation,
Brightway export and 1,958 signed written-LCI checks now pass. Battery is
qualified for the inspected coupled REMIND SSP2-NPi 2050 scenario; see
`uvek-battery-public-qualification.json` for the exact scope and limits.

Review the exact `proposed-battery-current-context-compatibility.patch` and
`uvek-battery-current-context-proposal-verification.json`. The previous proposal
is retained as historical evidence and has not been applied.
