# Vehicle carbon and heat-supplier investigation, 2026-09-11

Both failures from the four-scenario profiling exercise were reproduced using
ecoinvent 3.12 cutoff, the legacy backend, encrypted IMAGE inputs, import
uncertainty retained, and source uncertainty removed. The existing local
electricity mapping edits were retained. Validation was enabled throughout;
these diagnostic runs did not export a Brightway database or SimaPro files.

## Vehicles: repeated petrol carbon adjustment

IMAGE SSP2-L 2050 reproduces the same **27 `LEGACY.CO2_EMISSIONS_INCORRECT`
errors** with only `ndb.update(["fuels", "cars"])`. The errors are the regional
copies of the Medium EURO-6 gasoline car, including Brazil.

`SyntheticFuelsMixin.generate_synthetic_fuel_activities` creates three petrol
market variants. Its carbon adjustment was inside that loop, but every call
targeted both low-sulfur and unleaded petrol consumers. In the observed build,
the first call did not find the new suppliers; the next two both updated the
same consumers. The second effective update appended biogenic CO2 again and
clipped the remaining fossil CO2 to zero, increasing total exhaust carbon.
The imbalance is already present before vehicle efficiency scaling, which
preserves the erroneous ratio. It is not caused by the car energy floor.

For the Brazilian Medium EURO-6 gasoline car:

| Quantity, kg/vehicle-km | Before fix | After fix |
|---|---:|---:|
| Fuel-specific CO2 expectation | 0.171468622 | 0.171468622 |
| Fossil CO2 | 0 | 0.066547706 |
| Biogenic CO2 | 0.209883334 (two identical flows) | 0.104941667 |
| Total exhaust CO2 | 0.209883334 | 0.171489373 |

The fix moves the adjustment outside the petrol market loop, matching the
existing diesel structure. All market variants are available before consumers
are processed once. The regression test exercises real carbon reclassification
with a 60% biofuel share and checks total carbon, the split, the supplier
location, and the absence of duplicate biogenic flows.

## Heat: negligible end-use demand with zero secondary supply

IMAGE SSP2-M 2020 reproduces the missing Brazilian supplier with only
`ndb.update(["heat"])`. All Brazilian secondary-heat supply technologies are
zero, so no regional secondary market is created. The buildings layer nevertheless
contains **3.295029874e-14 EJ/year**, approximately **33 kJ/year**, of district heat.
The market builder treats every positive quantity as requiring a supplier.
Its selection order is the IAM region, contained ecoinvent locations, then RoW;
the generated World market is not a fallback in this path.

There are 11 such regions: BRA, EAF, INDO, NAF, OCE, RCAM, RSAF, RSAM, RSAS,
SEAS, and WAF. Together their unsupported buildings demand is
**6.179703434e-7 EJ/year**. After conversion to delivered heat, the largest
affected regional share is **7.947974891e-7**, below **0.8 parts per million**.
The Brazilian value is extremely small; the larger African values are negligible
residual demand rather than ordinary floating-point roundoff.

The fix excludes an unsupported purchased-heat quantity only when it is below
the existing heat closure tolerance:

```text
max(1e-8, 1e-5 * regional total)
```

This is applied to the market volumes after heat conversion (after marginal
mix calculation for consequential builds). The cutoff volumes are in EJ/year;
consequential market volumes are shares. Supported quantities remain unchanged.
The market builder normalizes the remaining suppliers. The original IAM arrays
are preserved, and each excluded quantity, regional total, and tolerance is
recorded under `heat diagnostics` → layer → `negligible unserved purchased heat`.
Material unsupported demand still raises an explicit error. No World supplier
is substituted and no regional supply is invented.

Tests cover the recorded Brazil/East Africa magnitudes, interpolation, unchanged
supported quantities, immutable input arrays, diagnostic records, material
missing supply, and IAM/contained-location/RoW supplier selection.

## Verification and reproduction

Both original scenarios now complete **full `ndb.update()` calls**:

| Scenario | Original failure | Full update after fixes |
|---|---|---|
| IMAGE SSP2-L 2050 | 27 vehicle CO2 errors | Passed; 54 combustion-car checks, zero CO2 failures |
| IMAGE SSP2-M 2020 | Missing BRA district-heat supplier | Passed; 11 negligible heat exclusions, zero car CO2 failures |

The fuel, car-energy, heat, heat-data, validation, validation-framework,
NewDatabase, and transformation selections pass **173 tests**. Formatting and
`git diff --check` pass. Full scenario verification covers the two cutoff builds
above; other IAM pathways and consequential builds were not run in this
investigation.

Use the diagnostic script in an environment containing the existing source
project. Supply `PREMISE_KEY` or `IAM_FILES_KEY` through the environment:

```sh
python benchmarks/diagnose_vehicle_heat.py \
  --output-dir export/vehicle-heat-investigation/minimal-new
python benchmarks/diagnose_vehicle_heat.py --full-update \
  --output-dir export/vehicle-heat-investigation/full-new
```

The script records original validation exceptions and continues to the second
case. Inspect each case's `status` in `evidence.json`; the script's process exit
code is not a build-success assertion. It never skips validation inside a build.
Raw before/after evidence is in the ignored directories
`export/vehicle-heat-investigation/minimal-1/` and
`export/vehicle-heat-investigation/fixed-full/`. These files contain inventory
data and must remain local.
