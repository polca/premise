# Modelling strategy

Status: proposed. The equations define the target model, not current behaviour.
See [evidence](evidence-and-references.md) for observed behaviour and
[implementation](implementation-plan.md) for the migration sequence.

## 1. Define the temporal reference and exchange role

The reference year `t` is the year the calling activity supplies its reference
product or service. Installation year `c`, manufacturing year, retirement year
`e`, inventory scenario year, and impact year are distinct quantities.

An existing asset of installation cohort `c` has age `a = t - c`. Its
manufacturing offset is approximately `c - t`, with a separate manufacturing or
construction lead-time distribution where evidence supports one. Installation
must not be called manufacturing without declaring that approximation.

| Role | Meaning | Typical timing |
|---|---|---|
| `existing_asset_service` | An operating asset contributes to a service | Cohort-specific manufacturing history |
| `new_asset_component` | A new component/material enters manufacture or construction | Relative to the new asset's construction schedule |
| `replacement_component` | A component enters an identified replacement event | Relative to that event |
| `operating_input` | Energy, fuel, chemicals, or other operating consumption | At the service year unless another process-specific delay is evidenced |
| `lifecycle_service` | Maintenance, repair, or disposal | From the asset/cohort event model |
| `unresolved` | Available metadata does not identify the role | Report explicitly; block promotion to corrected defaults |

Supplier labels help classification but do not determine these roles alone.
Match the caller, supplier, products, geography, and event context. Market
wrappers must not apply the same vintage shift a second time. A new building
containing a new pump must not inherit the in-use pump fleet's age distribution.

## 2. Distinguish four distributions

1. **Lifetime distribution:** probability of an asset's total service life.
2. **Stock-age distribution:** age composition of assets surviving at `t`.
3. **Service distribution:** contributions of those assets to the functional
   service at `t`, accounting for capacity and utilisation.
4. **Attributed production distribution:** manufacturing assigned to that
   service under a stated allocation convention.

An observed mean age constrains the second distribution in its reported units
(asset count, capacity, area, etc.). It does not define all four distributions.
Means, medians, modes, design lives, and observed retirement ages must have
separate fields and source descriptions.

## 3. Reconstruct the stock

For a homogeneous technology and geography, let `I(c)` be gross additions in
cohort `c`, and `S_c(a) = P(T_c > a)` its survival function. In a closed system:

```text
N(c, t) = I(c) * S_c(t - c)
p_stock(c | t) = N(c, t) / sum_c N(c, t)
```

This is the standard inflow/cohort relationship used in [ODYM][odym]. If an
observed cohort stock is available at a reference date, initialise from it and
evolve it forward. Do not invent historical inflows merely to obtain an exact
fit to a single mean age.

Transfers of used assets need entry/exit flows retaining original manufacture
and installation history. Exit from a country's fleet is not necessarily
physical retirement. Geography may change during an asset's life.

Future scenarios require gross additions or a stock-driven calculation of
additions and retirements. Net capacity changes are not gross additions, and
annual generation is not installed capacity. Where output is converted to
capacity, retain the capacity-factor assumptions and uncertainty.

### Evidence hierarchy

1. Observed cohorts and utilisation for the relevant technology, region, year,
   and weighting basis.
2. Gross additions plus a supported survival model, checked against observed
   stock and any available age statistics.
3. A lifetime distribution and explicitly assumed stationary additions.
4. Mean lifetime alone: a fixed-lifetime stationary baseline, with alternative
   survival shapes assessed as structural uncertainty.
5. Mean age alone: a constraint on an explicitly selected prior, with the shape
   left uncertain. Do not describe a mean-only fit as recovered vintage data.

Conflicting observations require a source/scope review. Do not force a global
lifetime assumption and a regional mean age to agree by moving `loc`.

### Stationary fallback

For constant additions, identical cohort lifetime distributions, and no
transfers, the continuous stock-age density is:

```text
p_stock(a) = S(a) / E[T]
```

With a fixed lifetime `L`, this is uniform on `[0, L)`. Its continuous mean age
is `L / 2`; annual representations have a convention-dependent discretisation
error. With variable lifetimes, mean age is `E[T^2] / (2 * E[T])`, assuming the
moments exist. It is therefore incorrect to infer lifetime as twice observed
mean age without these restrictive assumptions.

The fallback is a modelling prior, not an empirical claim. Do not apply it to
rapidly growing or declining stocks when usable installation history exists.

## 4. Select the allocation model before adding complexity

The baseline may use identical fixed lifetime and utilisation within a
homogeneous asset group. With identical asset units and lifetime service, stock
shares, service shares and normalised production shares are equal. Current age
alone does not require reweighting. The main correction can therefore proceed
without adopting heterogeneous individual-lifetime allocation for every asset.

This equality must be checked, not assumed across heterogeneous technologies,
capacities or duty cycles. Similar lifetime service gives approximately similar
weights; record the resulting sensitivity rather than claiming exact equality.

A common lifetime **probability distribution** for all entering cohorts is a
different assumption from a common **fixed lifetime** for all assets. Older
surviving cohorts exclude assets that retired early. Under individual-lifetime
service allocation, this selection can change production weights even when all
cohorts started with the same probability law.

If survival is variable but a common lifetime-service denominator is deliberately
retained from the database, label the model `common_amortisation`. This is an
average-cohort allocation approximation, not proof that realised lifetimes are
identical. Keep the survival and allocation assumptions separate.

### Worked examples

Consider two cars contributing 10,000 km each in the use year. Their ages are
2 and 12 years. For a functional unit of 10,000 vehicle-kilometres, each supplies
half the requested service.

| Assumption | Asset production at offset -2 | At offset -12 | Total cars per functional unit | Normalised timing |
|---|---:|---:|---:|---|
| Both last 15 years at 10,000 km/year | 1/30 | 1/30 | 1/15 | 50% / 50% |
| First lasts 10 years, second 20; same annual use | 0.05 | 0.025 | 0.075 | 2/3 / 1/3 |

The first case needs no production reweighting relative to stock. The second
case illustrates a lifetime-service difference, not a causal effect of age
itself. Both examples assume the manufactured asset unit is comparable.

### Optional heterogeneous lifetime-service allocation

Select and record the allocation convention before generating production
weights. Retaining a database's existing average-cohort amortisation is a valid
declared approximation; it must not be presented as individual-lifetime service
allocation.

For the latter approach, define homogeneous populations `g`, each with a
manufacturing/installation history and retirement year. Let:

- `N_g(t)` be the number of that population alive at `t`;
- `u_g(t)` be service per asset during the reference interval;
- `U_g` be total service delivered by one asset over its full life;
- `F(t) = sum_g N_g(t) * u_g(t)` be total service in the interval.

For `F(t) > 0` and `U_g > 0`, attributed asset production per unit service is:

```text
q_g(t) = N_g(t) * u_g(t) / (U_g * F(t))
q_total(t) = sum_g q_g(t)
w_g(t) = q_g(t) / q_total(t)
```

`q_total` has units of asset production per unit service; `w_g` is dimensionless.
Sum `q_g` over manufacture-year bins before exporting a marginal manufacturing
profile. If asset units represent capacity or mass rather than counts, convert
all quantities consistently before applying these equations. For heterogeneous
asset sizes, use separate populations or explicit conversion factors.

Use allocation shares other than service fractions only when the study requires
them, and record the alternative formula. Lifetime-average electricity output,
traffic, or occupancy must not be confused with output in the use year. The
[transport-service guidance][wiki-transport] discusses lifetime utilisation;
the population formula above is the proposed model, not an ISO requirement.

### Inventory amounts and temporal amount source

For the first timing correction, distribute the existing signed exchange amount
`q_existing` as `q_existing * w_g`. This preserves its total and isolates timing
effects. It does not establish the correctness of `q_existing` under the new
allocation convention.

For full adoption, compare `q_total` with the existing amortised coefficient,
explain the difference, and update it only with a consistent inventory model.
Never divide an already amortised coefficient by lifetime a second time.

For these normalised timing profiles, `temporal_amount_source="port"` is the
proposed default: distribute the caller-year coefficient. Upstream production
inventories are still selected at their pulse years. `matrix` mode rereads the
exchange coefficient at pulse years and is a different model; it needs its own
declared use case and amount-balance checks. Do not silently change either
mode for existing packages.

## 5. Keep lifecycle identity

A population surviving at `t` cannot have retired before that service. Its
remaining life is conditional on survival:

```text
P(R > r | T > a) = S(a + r) / S(a), for S(a) > 0
```

For cohorts with changing hazards, use their cohort-specific survival model.
Construction lead times, refurbishments, replacements, maintenance, and disposal
must refer to the same population and allocation convention.

Independent manufacturing and disposal marginals are insufficient when the
consumer later combines them as if independent. A fleet-age shift followed by
an unconditional mean-lifetime disposal pulse can produce impossible histories.

For a shared fixed lifetime, retirement follows directly from installation plus
that lifetime; a retirement-year probability axis is unnecessary. Check the
annual convention and any separate construction lead time.

For the heterogeneous extension, the preferred prototype is a cohort/lifetime-
resolved service wrapper, using sparse virtual activities or equivalent routing
state. Preserve cohort identity until lifecycle contributions have been formed.
Do not attach new disposal
flows alongside disposal already embedded in an inventory without reconciling
the existing exchanges. Prototype the representation on small fixtures before
choosing the production architecture; memory scaling is a release criterion.

Operating inputs remain tied to the requested service. A full-lifecycle study
of a newly built asset is a different reference activity and uses its own
construction and use schedule, not the existing stock's vintage kernel.

## 6. Annual representation and horizon

The generated contract uses integer calendar years. Source data may use finer
resolution, but binning must be explicit. Proposed convention: integrate a
continuous event density into nearest-year bins `[k - 0.5, k + 0.5)`, with ties
assigned consistently; preserve already annual cohorts at their supplied years.
Clip age-domain bins to the physical support before integrating. Do not round
each supplied fractional offset independently and silently lose or duplicate
mass. Report the difference between source and binned moments.

Allow the current-year cohort when physically applicable. A mean lifetime is
not a support maximum unless a fixed-lifetime model is explicitly chosen.
Select any finite tail cutoff from a documented omitted
mass tolerance, report that mass, and test impact sensitivity before renormalising.
For heavy-tailed lifetime-service models, a small probability tail can still
matter; probability tolerance alone is not an impact guarantee.

Keep physical event year separate from the inventory scenario year selected
for background data. Extending a physical lifetime beyond the inventory horizon
does not license deleting the event or moving it into the last available year.
Record endpoint inventory reuse or another extrapolation assumption explicitly.

## 7. Uncertainty and interpretation

Distinguish variability within a fleet from uncertainty in source parameters.
Sample uncertain lifetime, utilisation, installation history, and transfers
jointly when correlated. A geographic transfer assumption, a stationary prior,
and an allocation convention are alternative model cases, not automatically
independent probability distributions.

Reconstruct and validate each sampled stock/profile. Do not sample annual
weights independently. Rank review priorities by both scientific uncertainty
and potential contribution to study results; low confidence alone does not
measure impact significance.

[odym]: https://odym.readthedocs.io/en/latest/autoapi/odym/dynamic_stock_model/
[wiki-transport]: https://github.com/sentier-dev/lca-wiki/blob/7e394b32c0f0e64c6a0a42812200374de8c06f90/core/use-cases/assess-a-transport-service-in-industrials.md
