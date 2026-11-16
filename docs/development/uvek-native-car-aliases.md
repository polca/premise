# Native passenger-car mapping review

The inspected REMIND scenario has thirty positive passenger-car roles. Exact
aliases now select complete native services for twenty-one roles, backed by
thirty-three positive one-vehicle-kilometre reference definitions in UVEK 2026.
Nine remaining Mini/Van roles retain their CLIC path; an absent class is not
replaced with an unrelated car or discarded from IAM production.

The representative prototypes are ordinary-grid NMC-622 battery cars and the
native gasoline, compressed-gas, plug-in gasoline and SMR-hydrogen fuel-cell
services labelled 2021. Some documentation states a 2020 technology basis.
CH/RER variants retain their source inventories. Complete vehicle manufacture,
maintenance, wear, road and end-of-life burdens enter once per source kilometre.
No occupancy, vehicle lifetime or reference multiplier is introduced.

Compact is an explicit proxy for the IAM Subcompact category. Native Large SUV
represents the broad actual REMIND Large Car and SUV variable behind the legacy
internal medium-SUV label. Micro represents only the Mini BEV role. Each proxy
retains class, geography, technology, allocation and unquantified-score warnings;
none establishes physical equivalence between the classes.

These native vehicles contain installed kg-based batteries and have no declared
capacity-market sizing contract. The existing shared legacy-inventory policy
therefore preserves their native packs. It does not fabricate a kWh-capacity
purchase or constant-range contract from rounded source specifications. Marked
CLIC vehicles retain their existing battery policy. Plug-in hybrids retain their
source utility factor under the common relative IAM energy change. The complete
native 700-bar hydrogen supply is not given an extra terminal compressor.

Fuel filters use exact name/unit pairs, including both fuel and electricity for
plug-in hybrids. Negative fixtures exclude wrong units, components and changed
source years. Existing IAM and ecoinvent aliases are preserved.

The companion `uvek-native-car-alias-review.json` contains identities, source
hashes and role-specific limitations, not licensed exchange inventories.
This is a mapping milestone only. The first actual nine-root CLIC import finds
47 unapproved correspondences in its first source-version cohort. Additional
cohorts and the real car update remain unfinished. Cars is not admitted through
the public UVEK gate on the strength of this static mapping review.

The shared transport registry must also identify the car alias file. The actual
combined output initially had no native source-assumption comments, despite
those limitations being present in the YAML. Adding
`car.aliases_file: transport_passenger_cars.yaml` in `vehicles_map.yaml`
activates the existing shared annotator. A focused test verifies warnings,
retained original comments, unchanged exchanges, idempotency and the unchanged
ecoinvent path. All five native-car mapping tests pass. This is a YAML-only
runtime correction; the export verification checks the resulting comments.
