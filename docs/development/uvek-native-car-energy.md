# Native vehicle energy labels and validation

The native car review reproduced four naming gaps. Capitalized passenger-car
names were skipped by shared class and validation readers. Native gasoline and
gas supply names were not recognized by the existing fuel-energy floor. Native
electricity and hydrogen products were not recognized consistently as purchased
vehicle energy. Optional source-year labels displaced the old positional
powertrain parser used by pollutant validation.

The adaptation reads exact product/unit correspondences from
`premise/data/transport/vehicle_energy_aliases.yaml`, applies case-insensitive
vehicle-prefix comparisons, and reads the powertrain immediately after that
prefix. It keeps the shared energy floor, calorific values, efficiency changes,
emissions accounting and validation rules. No supplier is renamed or converted
by the label reader. A native CNG label uses the existing nominal 46.5 MJ/kg
screening policy; that is not a newly measured BAFU heating value. An unsupported
exact emission grade remains a warning, with no neighboring-grade fallback.

Four regression fixtures failed before the validation adaptation: a native
combustion service below the energy floor was skipped; a valid native electricity
purchase was missed; a native year-labelled service escaped exact emission-grade
checking; and a native car escaped the final fuel-carbon check. All now pass.
Synthetic cases also check uncertainty scaling, fixed ancillary inputs, repeated
floor application and rejection of equipment or wrong units.

The relevant car-energy, energy-classification, emission, fuel-carbon, hydrogen
and rail groups pass 226 tests. This is focused regression evidence, not a claim
that the entire test suite or the real car scenario has passed. Car scope stays
gated until its import and shared sector run are verified.

These checks also exposed a regression in the preceding rail callback: an empty
rail-only keyword was passed to car and truck accounting callbacks. That keyword
is now supplied only for trains, preserving the original car/truck protocol and
the reviewed rail behavior.
