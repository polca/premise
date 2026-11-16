# Include mapped native rail services in recorded-exhaust accounting

The combined quantity check found a real native-label filter gap. The common
fuel-carbon routine recognizes the legacy “transport, freight train,” prefix.
Native BAFU services use “Transport, freight, rail,”. Their IAM fuel quantities
were adjusted, but the final fossil/biogenic exhaust split could retain the
source country's blend instead of the final linked regional fuel blend.

The common train callback now supplies its actual YAML-selected service names
and tonne-kilometre unit to the fuel module's existing explicit
`recorded_exhaust_filters` hook. This includes only the mapped complete train
services. It does not add a new name heuristic, alter the fuel module, infer a
new exhaust total or change the energy-efficiency calculation. Ecoinvent's
already recognized train names keep the same recorded-total convention.

Seven rail tests pass. The new callback regression checks that a native service
with 0.2 kg recorded exhaust and an equal fossil/biogenic fuel blend emits
0.1 kg in each carbon-origin category. It preserves 0.2 kg rather than replacing
it with the larger complete-oxidation potential. An unmapped native rail service
remains unchanged. Production references, source selection, parallel freight
origins and real duplicate detection are covered by the other rail cases.

The combined thirteen-sector graph and 104 signed train LCI checks had passed
before this final selector correction. Its full combined run is being repeated;
trains remains publicly gated until physical, carbon-origin, inventory and public
workflow checks pass. This document does not claim a new Brightway export.
