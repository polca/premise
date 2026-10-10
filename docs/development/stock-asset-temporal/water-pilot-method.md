# Quebec potable-water network: pipes, storage and conditional disposal

Updated 10 October 2026. This is an opt-in, bounded 2022–2030 reconstruction.
It supplies observed stock quantities and explicit timing assumptions, not an
empirically calibrated water-demand forecast. Public defaults are unchanged.

## Observations and their physical meaning

Use the pinned full CSV for [Statistics Canada table 34-10-0289-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410028901),
source S14 in the acquisition manifest. Select Quebec, all public organisations,
2022, and the Number measure. Select each asset category separately:

| Asset | Stock total | Unknown construction date | Unknown share | Physical unit |
|---|---:|---:|---:|---|
| Total linear potable-water assets | 45,114 | 2,328 | 5.16% | km |
| Water storage assets | 974 | 249 | 25.56% | count |

The CSV's generic Number label does not establish the physical unit. The
[survey instrument, questions 13 and 15](https://www.statcan.gc.ca/en/statistical-programs/instrument/5173_Q12_V3#s2)
specifies kilometres for linear assets and counts for storage assets. Preserve
source rows, vectors, quality flags and unknown quantities. The pre-1940 storage
bin has quality flag C; annual disaggregation does not improve its precision.
Totals here sum mutually exclusive construction bins and unknown dates; they
are not checked against an independent census total. Do not add pipe subgroups
to their aggregate. The 2020/2022 ownership coverage changed, so their difference
is not treated as observed additions or retirements.

Storage counts include asset types, sizes and materials that the table does not
resolve. They are an explicit geographic stock proxy for the generic concrete
tank portion of the network inventory. Counts do not measure storage volume.
Pipe kilometres do not measure delivered water by cohort. Equal service per
kilometre and per storage asset are the primary timing assumptions, separately
normalised within each asset group; pipe km and tank counts are never added.

## Annualising the surviving stock

Keep the 2022 observed quantities as surviving stock. Do not multiply them by
unconditional survival. The primary annual reconstruction distributes each
finite bin uniformly over its constituent years: 2020–2022, 2010–2019,
2000–2009, 1970–1999 and 1940–1969. The pre-1940 bin is provisionally uniform
over 1850–1939. This lower bound is an assumption, not evidence of the oldest
Quebec pipe. Unknown-date stock follows the known distribution in the primary
case, a missing-at-random assumption. Every bin and unknown allocation is audited
back to its original quantity.

Test all stock at the oldest/newest year of each bin, tail starts at 1800/1900,
and all unknown stock at the primary oldest year/2022. These are scenario bounds,
not confidence intervals. They explicitly expose the large storage-date gap.
Construction completion is used as the production-date proxy; manufacture and
construction lead times are not observed.

The primary 2022 mean ages are 37.1176 years for pipes and 28.0124 years for
storage. Finite-bin endpoint cases give 25.36–48.88 and 18.65–37.38 years,
respectively. Assigning unknown dates to 1850 moves the storage mean to 64.82
years. These differences must accompany any interpretation of the pilot.

## Future stocks, service weights and disposal

No suitable native IAM water-stock trajectory was established. The primary
future scenario holds each asset group's closing stock at its observed total.
Each year, conditional survival removes part of existing cohorts and inferred
new construction closes the target. Report opening stock, natural retirement,
additions, closing stock and a checked balance residual. A 1% annual stock-growth
case tests replacement-only closure. These are maintenance scenarios, not claims
about Quebec's planned expansion. No material-specific stock breakdown is fitted.

The provisional survival law is Weibull with a mean of 70 years, borrowing the
source inventory's design life, and an assumed shape of 3. This is a declared
engineering prior, not an observed age density or an estimated failure curve.
Means of 50/100 and shapes of 2/4 are tested. Observed old survivors remain in
the initial stock even when unlikely under that prior. Future survival is
conditional on their attained age.

Primary service weights are stock shares within each group. An age-dependent
service sensitivity uses a relative factor `exp(-age/50)`. No absolute water
volume is inferred from these proxy service totals. Annual service represents
the end-year portfolio; within-year installation/failure exposure is absent.

Disposal dates condition on membership in the stock providing the requested
service. Their probabilities lie strictly after that service year. During the
explicit projection, use its cohort declines; afterwards continue the same
survival law without extra forced exits. Fold only a remaining probability below
`1e-12` into the final date and report that mass. Physical retirement and the
source inventory's disposal event are assumed coincident. Buried pipes left in
place and demolition delays are not measured. The pilot preserves the supplied
disposal quantities and does not claim to estimate actual removed material.

## Real inventory boundary and common amortisation

| Role | ecoinvent 3.12 cut-off activity code | Unit |
|---|---|---|
| Quebec tap-water market | `f89f17b92a4c207c708fae02a3ccd890` | kg |
| Water-network market | `ee37b146a20a800628e5dbe95bf809ed` | km |
| RoW network construction | `31b5b1b80a306de7cb628978f285fa9b` | km |

The source capital coefficient is `5.09836534440235e-10 km` per activity. The
market also consumes `0.191 kg` of its own output to represent distribution
losses. Preserve this own-product input in the scoped service copy: net reference
production remains `0.809 kg`. Capital demand per delivered kg is therefore
the original coefficient divided by `0.809`; this adjustment already belongs
to the original inventory, not to a new lifetime allocation.

Copy the service, market and constructor for the selected pilot. Retain all
other users of the source activities. Explicit reviewed paths lift these ports
to separate deterministic adapters at the service caller:

- Eight geographic concrete inputs and one reinforcing-steel input are tank
  construction, using storage vintage weights.
- Two geographic reinforced-concrete waste ports are tank disposal, using
  conditional storage retirement. Do not add reinforcement disposal twice.
- The other 22 negative construction inputs are pipe/network disposal, each
  retaining its original category, sign and geography.

The remaining construction materials and excavation receive the pipe/network
date. Generic metals/materials are new component inputs, not independently aged
stocks. All 33 lifted coefficients equal their original capital-path products;
none is recalculated from the new survival assumptions. Internal adapters and
market links have explicit zero shifts, including dates outside matrix anchors.
Water production and losses remain on the service side. The source's waste
classification for cast iron and its omission of some metals at end of life are
retained, not silently revised in a timing study.

The helper name `split_embedded_lifecycle` describes the coefficient-separation
mechanism. It makes no scientific role inference: the nine lifted construction
ports have `existing_asset_service` timing, while the 24 disposal ports have
`lifecycle_service` timing. All use preserved common amortisation. This is a
deterministic equivalence claim; Monte Carlo correlations are not preserved.

## Reproduction and release evidence

Run in the implementation premise environment:

```sh
python dev/stock_vintage/generate_water_cohorts.py \
  --input /path/to/canada-infrastructure.zip --output /tmp/water-cohorts.json
python dev/stock_vintage/export_water_pilot.py \
  --inventory /path/to/ecoinvent-3.12-cutoff-local.json.gz \
  --cohorts /tmp/water-cohorts.json --output-dir /tmp/water-pilot
```

The source extract is generated by TRAILS' `extract_local_inventory.py` and
remains restricted. Use a fresh export directory. The actual exporter writes
legacy/corrected inventories at 2022/2025/2030, with constant background
technology, model label `observed` and pathway `quebec-water-replacement`.
This prevents the maintenance scenario being mislabelled as a REMIND result.

TRAILS' `check_water_pilot.py` checks the scoped full-matrix operator identity,
signed amounts, distribution losses, annual component/retirement dates, zero
shifts, routed calendars and independently accumulated biosphere totals. It
supports interpolation, warm caches, both solvers, signed functional units and
legacy diagnostics. Actual completed checks are recorded in the implementation
status; this method document alone does not establish a passed runtime gate.

The cohort generator provides 13 sensitivity cases, with nine annual service and
retirement records for each of two asset groups. Public observations retain
Statistics Canada attribution and its Open Licence. Source ecoinvent inventories,
exported matrices and derived inventory time series remain local.
