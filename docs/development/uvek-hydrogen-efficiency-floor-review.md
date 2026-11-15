# Hydrogen efficiency: apply the existing energy minimum

Tracing the actual regional PEM production dataset isolated a shared calculation error. The efficiency callback calculated an IAM reduction below the configured minimum, reported the capped value of45.3kWh, but applied the original uncapped factor. Electricity therefore became45.1233kWh during that callback. Supplier relinking did not change it further.

The correction recomputes the scaling factor from the existing capped target and original energy use. It adds three lines to the shared hydrogen callback. The energy equation, minimum parameter, units, uncertainties and regionalization remain the existing Premise approach. No UVEK-specific engine, migration rule or helper inventory is added.

A regression reproduced the error with the ordinary lowercase ecoinvent electricity name. It checks both a reduction that hits the minimum and one that stays above it. All233focused and broader fuel/carbon/vehicle-grade regressions pass, together with Black,Ruff and the documentation-reference check.

The real REMINDSSP2-NPi2050fuel workflow now has actual electricity equal to its logged target in all twelve non-World native PEM clones. EUR uses45.3kWh,with a one-kg hydrogen reference. All22positive IAM fuel roles still have matched suppliers;the168regional fuel markets remain present. Exhaustive graph validation reports no errors.

Six signed LCIs on the complete13122activity transformed graph were calculated using an in-memory matrix:the EUR andWorld hydrogen markets and the native regional EUR PEM producer. All calculations are finite and negative functional units reverse their positive inventories consistently. The verifier uses the same root-versus-unspecified biosphere-compartment convention as export while retaining exact substance names and units. No Brightway database or activity was registered. Its temporary matrix input is private and removed after verification.

These results verify this correction. They do not establish source/native score equivalence or complete public fuel-sector qualification. Remaining consumer/production boundaries, emission origins and a current public workflow/export still require qualification. Eight sectors remain qualified.
