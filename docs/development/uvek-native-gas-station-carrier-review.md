# Native gas-station carrier identities

The remaining-sector REMIND SSP2-NPi 2050 probe stopped in fuel-carbon
annotation: the station's native medium-voltage electricity purchase was absent
from the explicit ancillary-input list. The failure was reproduced with strict
validation enabled.

Read-only inspection of `BAFU:2026-reimported`, project
`bafu-2026-ecospold2-biosphere-310`, confirmed the station purchases native
`Electricity, medium voltage, at grid` in kWh, native high-pressure consumer gas
in MJ, and the existing gas-station infrastructure in units. The transformed
NEU station uses the same electricity name with the canonical reference product
`electricity, medium voltage`. Both exact electricity name/product pairs are
therefore declared, alongside the exact native MJ gas pair.

This extends only `uvek_gas_property_carriers` in `fuels.yaml`. It does not
introduce a supplier, alter an exchange quantity, change a fuel reference, or
change runtime Python. Electricity remains an ancillary compression input and
contributes no delivered-gas carbon. The existing nominal 46.5 MJ/kg convention,
composition normalization and documented screening limitations remain unchanged.
Standing authorization for autonomous mapping corrections applies.

The regression covers native and canonical electricity reference products with
both native-MJ and canonical-m3 upstream gas. It checks the unchanged complete
station and forwarding exchanges, including uncertain compression electricity,
and the resulting fossil/non-fossil carbon proportions. The focused gas-carrier,
fuel-energy, carbon-balance and vehicle-carbon suite passes 226 tests.

This correction alone does not qualify the entire fuels update or export.

The corrected real encrypted REMIND SSP2-NPi 2050 diagnostic completed the
shared fuels update and exhaustive graph validation with 13,122 activities.
The source database modification marker was unchanged. No export was performed
and the unsupported-sector gate was bypassed only in the diagnostic process.
Local unrelated GAINS edits were present; clean-commit reproduction, target
coverage, combined-sector and export/LCI qualification are still required.
See `uvek-native-gas-station-carrier-verification.json`.
