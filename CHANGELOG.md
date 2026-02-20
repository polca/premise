# Changelog

All notable changes to this project are documented in this file.

## [2.3.7] - Unreleased

Changes in this section summarize work since tag `v.2.3.6` (including current local branch updates).

### Added
- Added MESSAGE scenarios.
- Region-to-feedstock mapping files for biofuels:
  - `premise/iam_variables_mapping/iam_region_to_biodiesel_feedstock.yaml`
  - `premise/iam_variables_mapping/iam_region_to_bioethanol_feedstock.yaml`
- Metals post-allocation correction files for newer ecoinvent versions:
  - `premise/data/metals/post-allocation_correction/corrections_311.yaml`
  - `premise/data/metals/post-allocation_correction/corrections_312.yaml`

### Changed
- Expanded fuel regionalization, including higher-level fuel activities.
- Expanded biomass supply-chain regionalization.
- Updated IAM variable mappings across sectors (final energy, fuels, heat, transport, electricity, steel, CDR).
- Updated GCAM topology and related mapping assets.
- Updated Brightway export/import integration (`premise/brightway2.py`, `premise/brightway25.py`).
- Updated inventory assets:
  - `premise/data/additional_inventories/lci-biofuels.xlsx`
  - `premise/data/additional_inventories/lci-final-energy.xlsx`
  - `premise/data/additional_inventories/lci-pass_cars.xlsx`
- Updated SimaPro category and import classification mappings.

### Fixed
- Multiple MESSAGE mapping fixes (fuels, final energy, industrial heat).
- Duplicate regionalization / duplicate dataset creation issue.
- Improved missing-file error messages.
- Unit normalization for IAM inputs provided in `PJ/yr` (conversion to `EJ/yr`).
- Updated NOx and PM2.5 emission factors for diesel passenger cars.
- Reintroduced `SSP2-Base` as an accepted scenario.

### Data cleanup
- Removed obsolete fuel config files:
  - `premise/data/fuels/fuel_efficiency_parameters.yml`
  - `premise/data/fuels/fuel_markets.yml`
  - `premise/data/fuels/hydrogen_supply_losses.yml`
  - `premise/data/fuels/supply_chain_scenarios.yml`
- Renamed metals correction file:
  - `premise/data/metals/post-allocation_correction/corrections.yaml`
  - to `premise/data/metals/post-allocation_correction/corrections_310.yaml`

### Documentation
- Updated `README.md`, multiple docs pages, and example notebooks.

## [2.3.6] - 2026-01-29

### Changed
- Maintenance/formatting release.

### Fixed
- Fixed issue preventing regionalization of CDR and fuel supply chains.

### Other
- Temporarily removed `SSP2-Base` from `SUPPORTED_PATHWAYS`.

## [2.3.5] - 2026-01-29

### Fixed
- Validation robustness improvements (including missing-key handling).
- Compatibility fixes for `wurst` and pandas.
- UTF-8 decoding fix for migration JSON files.
- Security and dependency vulnerability fixes.

### Documentation
- Updated docs, examples, and notebooks.

## [2.3.4] - 2026-01-06

### Changed
- Added ecoinvent `3.12` compatibility and updated default ecoinvent version.
- Updated REMIND scenarios to `3.5.2`.
- Improved migration system using `ecoinvent_migrate`.
- Added headers to A and B matrix CSV exports for datapackage/raw matrix export paths.
- Updated PV inventories and diesel/biodiesel/methanol ship SOx/PM factors.
- Improved mapping generation robustness when candidate datasets are missing.
- Extended country-specific PV electricity inventories from 33 to 171 countries.

### Fixed
- Fixes in `external.py` key access.
- Fixed biomass-market linking to synthetic gas / methanol chains.
- Prevented negative period-weighted electricity mix shares from being used.
- Preserved existing dataset comments when appending new comments.
- Fixed an error in PV farm transformer inventories (copper amount overestimated by a factor of 10).
- Fixed unspecified production volume issue in country-specific PV electricity inventories.

## [2.3.3] - 2025-12-02

### Added
- Added geo-coverage information in dataset comments.
- Added/extended classifications in export paths.

### Changed
- Updated REMIND fuel variables and notebooks.

### Fixed
- Fixed missing world electricity market.
- Fixed biosphere-name validation in `PathwaysDataPackage`.
- Fixed electricity validation and lower-casing of dataset keys.
- Fixed openpyxl `pd.NA` handling by passing `None`.

## [2.3.2] - 2025-11-16

### Added
- Added classifications to datapackages.
- Added README accessibility tests and alt text improvements.
- Added additional module documentation and typing/documentation improvements.

### Changed
- Significant MESSAGE mapping expansion/updates for heat, fuels, final energy, biomass, steel, cement, carbon removal, and PV.
- Improved report generation and SimaPro export behavior.
- Updated migration map and docs/tests.

### Fixed
- Fixed tests and type-related edge cases.
- Added bypass for metals validation in consequential mode where marginal/average shares diverge.
- Connected biomass market to biogas and ethanol production.
- Renamed biomass market to `market for lignocellulosic biomass, used as fuel`.

## [2.3.1] - 2025-10-22

### Added
- Added GCAM variables, tests, and documentation.
- Added accessibility scanning in CI and improved datapackage variable export serialization.

### Changed
- Updated Python requirement metadata.
- Updated notebooks and docs.

### Fixed
- Fixed low-temperature DAC end-of-life dataset issue.
- Cement mapping mask updates (`average`) and minor test/doc fixes.

## [2.3.0] - 2025-09-30

### Added
- Updated/additional IAM scenario support: REMIND v3.5, IMAGE v3.4, REMIND-EU, GCAM.
- Major sector updates:
  - Transport (shipping, rail, road)
  - Shipping inventories for marine oil, ammonia, methanol, and hydrogen powertrains
  - Rail inventories for additional rail technologies
  - Road inventories for ICEV/BEV/FCEV road technologies
  - Carbon dioxide removal mixes
  - Metals intensity updates
  - Mining waste pathway updates
  - Heat mixes (residential/industrial)
  - Battery mix and energy-density evolution
- Additional technology representation for primary steel routes.
- Regionalization of biomass-producing forestry activities.
- Added `PathwaysDataPackage` export workflow.

### Changed
- Broadened scenario and sector coverage for prospective database transformation.
