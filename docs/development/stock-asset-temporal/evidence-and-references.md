# Evidence, reproducibility and references

Audit date: 9 October 2026. This file distinguishes observations from proposed
modelling changes. Private correspondence and attachments are not included.

## Revisions inspected

| Repository | Revision | Scope |
|---|---|---|
| premise, `trals` | `82f5c5d063473fd3984e818d11a54962989b1130` | Packaged table, review files, lifetime workbooks, assignment and export |
| TRAILS | `b7d1bb78f0388ba3916270e6654353e01624807e` | Temporal parser, distribution evaluator, routing lookup and interpolation |
| LCA wiki | `7e394b32c0f0e64c6a0a42812200374de8c06f90` | Service-life/utilisation and attributional modelling context |

The premise revision was checked against the remote `trals` reference on the
audit date. TRAILS observations describe the inspected local source revision;
they are not a claim about every installed release.

## Findings

### F1. Packaged stock rows and negative lognormal support

The [packaged CSV][csv] has 987 `stock_asset` rows: 317 type 2 and 670 type 5.
All 317 lognormal stock rows have nonpositive upper support: 315 end before
zero and two end at zero. These are row counts, not unique/matched suppliers.

The [consumer's lognormal evaluator][td] masks for positive offsets. If none
exist, it returns uniform unnormalised weights. The normalisation then produces
a uniform distribution over the supplied integer support. Negative-lag
lognormals are therefore not evaluated as the mirrored distributions intended
by the [review dashboard][dashboard].

### F2. Passenger-car reproduction

There are 204 stock rows with `passenger car` in the activity name. The main
154-row parameter set has lifetime 14.74 years, type 2, `loc` approximately
`-12.28333333`, `scale=1.5`, bounds approximately `[-15.28333333, -1]`.
Some values differ only in printed precision. The other 50 rows use lifetime
12, `loc=-10`, `scale=1.5`, bounds `[-13, -1]`.

The premise loader/assignment and TRAILS parser/evaluator were exercised with
these rows in a small synthetic service scenario. The main set yields equal
weights at offsets -15 through -1, with mean age 8 years. The other set yields
equal weights at -13 through -1, with mean age 7 years.

For the main parameters, the dashboard's intended mirrored lognormal has:

| Interpretation | Mean age |
|---|---:|
| Untruncated median 12.28333333, GSD 1.5 | 13.335695 years |
| Continuous dashboard density truncated to [1, 15.28333333] and renormalised | 10.461662 years |
| Same density sampled and normalised at integer ages 1–15 | 10.542537 years |
| Actual inspected TRAILS runtime | 8 years |

ACEA's cited 12.3-year stock mean is not reproduced by these interpretations.
The [review script][review] instructs rescaling the grouped age/lifetime ratio,
yielding `14.74 * 10 / 12 = 12.28333333`. This is not a fit to a measured age
histogram or a constrained fit of the post-truncation arithmetic mean.

### F3. Triangular evaluation differs too

The consumer implements a tent proportional to
`max(abs(offsets - loc)) - abs(offsets - loc)`. With an asymmetric support,
this is not the standard triangular PDF with zero density at both outer
endpoints. The review plotting scripts implement the conventional asymmetric
triangle instead. Integer sampling also differs from integrating annual bins.

Examples using the grouped workbook parameters:

| Group | Current runtime mean age | Intended continuous triangle mean age |
|---|---:|---:|
| Buildings | 44.2737 | 42.0000 |
| Wind farms/turbines | 13.9286 | 13.3333 |
| Charging infrastructure | 6.1176 | 7.0000 |

These examples establish a semantic discrepancy, not empirically correct
replacement parameters. Standard distribution conventions are documented by
[SciPy][scipy-triangle].

### F4. Roles and annual profiles are not represented fully

The current [premise loader and assignment][premise-trails] keys stock parameters
by supplier name/reference product and applies them to matching technosphere
exchanges. It does not resolve the proposed caller/service roles or generate
regional/year-specific stock reconstructions. The [exporter][export] copies the
temporal fields; it does not repair their statistical semantics.

The [TRAILS temporal lookup][trails-main] selects the nearer template when
explicit offsets or weights differ between anchors. Interpolation of inventory
matrices alone is therefore not evidence of annual cohort/profile evolution.

Current maintenance and end-of-life assignment uses caller lifetime, including
an end-of-life pulse at lifetime plus one. Its meaning must be reviewed together
with the caller's reference event and stock age. Replacing vintage weights
alone does not validate those lifecycle dates.

### F5. Fixed and stochastic lifetimes must be distinguished

Equal lifetime service and equal current utilisation make stock and normalised
production shares equal. Different current ages alone do not change that result.
See the worked examples in [strategy](strategy.md).

A common stochastic survival law permits different realised lifetimes. Older
survivors are selected from its longer-lived portion, so individual-lifetime
service allocation can produce different manufacturing weights. This is not
equivalent to assigning unrelated lifetime assumptions to old and young cohorts.

For an illustrative discrete stock calculation with normal lifetime mean 14.74
years and standard deviation 3.685 years, steady additions give mean stock age
about 7.568 years at the comparison year. Individual-lifetime allocation with
constant annual use gives a production-weighted age about 7.120 years. These
are model examples, not observed fleet statistics or validated CIRCOMOD curves.

### F6. Source metadata need review

The [CIRCOMOD vehicle workbook][circomod], `Data` row 129, gives global private-car
lifetime 14.74 years and labels the distribution Weibull, with additional
parameter text that needs verification. The source record does not justify
assuming a normal lifetime distribution without a separate modelling choice.
Other rows in the same workbook cite national ACEA stock-age statistics.

The group workbook calls the vehicle parameter a median/GSD pair, while some
CSV notes still refer to a log-space value and sigma 0.55. Those notes cannot
be used as a current unambiguous parameter contract.

## Minimal runtime reproduction

Run from the inspected TRAILS checkout in its compatible Python environment.
This verifies the consumer interpretation, not a full exported database or LCA.

```python
from trails.datapackage import _parse_temporal_exchange_row
from trails.temporal_distributions import TemporalDistribution

row = {
    "temporal_distribution": 2,
    "temporal_loc": -12.28333333,
    "temporal_scale": 1.5,
    "temporal_min": -15.28333333,
    "temporal_max": -1,
}
exchange = _parse_temporal_exchange_row(row)
pulses = list(TemporalDistribution(exchange).iter_offsets_and_weights())
print(pulses)
print("mean age:", -sum(offset * weight for offset, weight in pulses))
# Inspected revision: 15 equal weights, mean age 8.0.
```

Run this row-count check from the audited premise checkout:

```python
import csv
from collections import Counter
from pathlib import Path

path = Path("premise/data/trails/temporal_distributions.csv")
with path.open(encoding="utf-8-sig", newline="") as stream:
    stock = [r for r in csv.DictReader(stream)
             if r["temporal_tag"] == "stock_asset"]
print(len(stock))
print(Counter(r["age distribution type"] for r in stock))
# 987; Counter({'5': 670, '2': 317})
```

## Source register and limits

| Source | Supports | Does not establish |
|---|---|---|
| [ACEA average fleet age][acea] | EU stock mean ages in the cited 9 September 2024 page | Full vintage histogram, technical lifetime or future fleet evolution |
| [CIRCOMOD/IEDC workbook][circomod] | Traceable input rows, including global private-car 14.74-year value | That all rows measure lifetime, or that its parameter text is ready for direct execution |
| [Held et al. 2021][held] | Empirical turnover modelling and importance of used-car trade | A universal global lifetime or a direct replacement age profile for every vehicle technology |
| [ODYM stock-model documentation][odym] | Cohort survival, stock balance and stock-/inflow-driven formulations | A prescribed LCA service-allocation convention |
| [SciPy triangular documentation][scipy-triangle] | Standard triangular shape conventions | Empirical suitability of a triangular stock-age model |
| [Global Energy Monitor tracker][gem] | Available power-unit capacity, start-year and retirement information | Complete coverage of all installations, especially every distributed asset |
| [LCA wiki transport-service guidance][wiki] | Lifetime utilisation and functional-service context; source IDs `ilcd-2010`, `lca-wiki-editorial` | Universal temporal defaults or an independently verified population-allocation model |

External methodological/data sources above were consulted during the investigation;
the workbook and repository sources were read from the stated revisions. Data
availability is a starting point for review, not permission to redistribute
licensed records. Use source-specific access and redistribution conditions.

No full ecoinvent database rebuild, complete 987-row exchange coverage audit,
or manuscript/result reassessment was completed in this investigation. These
remain implementation-plan tasks. The new schemas and numerical tolerances are
proposals requiring implementation and validation.

[csv]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/premise/data/trails/temporal_distributions.csv
[premise-trails]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/premise/trails.py
[export]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/premise/export.py
[dashboard]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/dev/trails/2_stock_asset_dashboard.py
[review]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/dev/trails/1_stock_asset_review_with_codex.py
[circomod]: https://github.com/polca/premise/blob/82f5c5d063473fd3984e818d11a54962989b1130/dev/trails/lt_data/3_LT_Vehicles_CIRCOMOD.xlsx
[td]: https://github.com/romainsacchi/trails/blob/b7d1bb78f0388ba3916270e6654353e01624807e/trails/temporal_distributions.py
[trails-main]: https://github.com/romainsacchi/trails/blob/b7d1bb78f0388ba3916270e6654353e01624807e/trails/trails.py
[acea]: https://www.acea.auto/figure/average-age-of-eu-vehicle-fleet-by-country/
[held]: https://doi.org/10.1186/s12544-020-00464-0
[odym]: https://odym.readthedocs.io/en/latest/autoapi/odym/dynamic_stock_model/
[scipy-triangle]: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.triang.html
[gem]: https://globalenergymonitor.org/projects/global-integrated-power-tracker
[wiki]: https://github.com/sentier-dev/lca-wiki/blob/7e394b32c0f0e64c6a0a42812200374de8c06f90/core/use-cases/assess-a-transport-service-in-industrials.md
