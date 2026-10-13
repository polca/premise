Constant-range battery accounting
---------------------------------

Refreshed car and truck inventories carry a ``vehicle battery sizing`` metadata
contract for BEV and plug-in-hybrid energy batteries. Premise uses the final
traction-electricity purchase per service, after scenario efficiency scaling,
relative to that contract's 2020 purchase. If this ratio is ``f``, then:

* nominal capacity = 2020 nominal capacity × ``f``;
* pack mass = 2020 pack mass × ``f`` × 2020 cell density / scenario cell density;
* curb and driving mass = their respective 2020 values + the pack-mass change.

This is an accounting approximation. The IAM energy target represents the
combined improvement, including any mass benefit. No additional energy saving
is calculated from the lighter vehicle. Charging efficiency, depth of discharge,
cell mass fraction, lifetime, annual distance, load and non-battery hardware
retain the original 2020 design. Density follows Premise's existing projection
for the original chemistry, independently of the upstream MIX capacity market.

For PHEVs the ratio uses grid electricity and preserves electric-mode range
at unchanged electric-driving share. It does not use aggregate fuel-plus-electric
energy to size the battery. At constant range and lifetime usage, battery
throughput and nominal capacity change together, so the existing replacement
count remains applicable. The kWh market purchases nominal capacity times one
plus replacements; its end-of-life boundary remains unchanged. HEV and FCEV
power buffers are excluded. Combustion/hydrogen tank resizing is separate from
this battery rule.

Road construction follows driving mass, passenger-car maintenance follows curb
mass, and truck assembly follows curb mass, as in the source inventory model.
Wear treatment uses a few precomputed mass powers retaining the original cycle,
PHEV mode weights and recuperation fractions. These powers are verified against
the native model during generation. Distance-based road maintenance, truck
maintenance/end-of-life recipes based on rated gross mass, HBEFA emission factors
and refrigerant losses are not assigned a battery-mass relationship.

Changed regional designs receive their own vehicle-production activities with
the same names and reference products at the service's IAM location. This
location identifies the regional design, not a new manufacturing origin:
existing component supplier locations are retained. The original 2020 RER
production templates remain available. Unregionalized templates and inventories
without this metadata retain their previous behavior; missing IAM signals are
not replaced by an invented engineering trend.

Runtime requires cached metadata/density lookups, arithmetic and small inventory
copies. It does not import carculator, simulate driving cycles, optimize
engineering parameters or iterate a sizing model. Invalid capacities and truck
mass-limit violations raise visible errors. This approximation preserves range
and inventory accounting; it does not establish detailed power or thermal
feasibility of a newly engineered battery pack.
