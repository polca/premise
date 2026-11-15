# Native vehicle-motor filters and existing scope guards

Three native motor inventories now match the EV role through exact name/product
pairs and a kilogram unit: the scooter motor, average electric-vehicle motor,
and passenger-car motor. Their documentation explicitly references kg of motor;
the cited example device masses and rated powers are not reference quantities.
Three conversion entries therefore use factor one with `kg metal/kg motor`.
Whole devices, complete vehicles, industrial motors and crossed product labels
are excluded. Existing aliases and all 65 previous conversions are unchanged.

This is filter and unit support, not new permission to override uncertain
material data. The existing rules already preserve 21 EV observations because
whole-vehicle material totals cannot be treated as motor-component intensities.
Those guards remain unchanged. The new filters make source-preserving decisions
visible for these native motors; they do not require new rare-earth supplier
mappings for guarded rules or authorize a second permanent-magnet burden.

256 tests pass across the native motor, electrolyzer and PEM supplier cases plus
the existing metals suite. The real REMIND SSP2-NPi2050 snapshot diagnostic has
97 compiled/executed decisions, including 57 explicit component-scope
preservations. The matched PEM catalyst group still applies; other unavailable
intensity, observation-year, design and provider cases remain reported skips.
The update and exhaustive graph checks pass with 12,040 activities and zero
errors, without export or new Brightway databases.

Four of the seventeen technology roles now have native aliases, covering six
native equipment records. Thirteen roles remain unmapped. Metals stays
unqualified: equipment coverage, ceramic/titanium supply boundaries, applicable
mining-market checks, combined updates and export still require work. This
mapping change modifies no runtime Python module.
