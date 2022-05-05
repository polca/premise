import warnings

import numpy as np
import pandas as pd

from premise.framework.logics import contains_any_from_list, equals
from premise.utils import c, create_hash, create_hash_for_database, e, s

from .framework.tags import TagLibrary

tags = TagLibrary().load()


def create_exchange_from_ref(index, overrides=None, prod_equals_con=False):
    # create production dataset

    if overrides is None:
        raise KeyError("exchanges have to define producer and consumer information!")

    new_exc = pd.Series(index=index, dtype=object)

    _mandatory_fields = set(
        [
            e.prod_name,
            e.prod_prod,
            e.prod_loc,
            e.cons_name,
            e.cons_prod,
            e.cons_loc,
        ]
    )

    _provides_prod_info = (
        e.prod_name in overrides
        and e.prod_loc in overrides
        and e.prod_prod in overrides
    )
    _provides_cons_info = (
        e.cons_name in overrides
        and e.cons_loc in overrides
        and e.cons_prod in overrides
    )

    if prod_equals_con and not (_provides_cons_info or _provides_prod_info):
        raise KeyError("exchanges need to provide consumer and producer information!")
    elif not (_provides_cons_info or _provides_prod_info):
        raise KeyError("exchanges need to provide consumer and producer information!")

    if prod_equals_con and _provides_prod_info:
        overrides[e.cons_name] = overrides[e.prod_name]
        overrides[e.cons_prod] = overrides[e.prod_prod]
        overrides[e.cons_loc] = overrides[e.prod_loc]

    if prod_equals_con and _provides_prod_info:
        overrides[e.prod_name] = overrides[e.cons_name]
        overrides[e.prod_prod] = overrides[e.cons_prod]
        overrides[e.prod_loc] = overrides[e.cons_loc]

    if prod_equals_con:
        overrides[e.prod_key] = overrides[e.cons_key] = create_hash(
            overrides[e.prod_name], overrides[e.prod_prod], overrides[e.prod_loc]
        )
        overrides[(s.exchange, c.type)] = "production"
    else:
        if e.prod_key not in overrides:
            overrides[e.prod_key] = create_hash(
                overrides[e.prod_name], overrides[e.prod_prod], overrides[e.prod_loc]
            )
        if e.cons_key not in overrides:
            overrides[e.cons_key] = create_hash(
                overrides[e.cons_name], overrides[e.cons_prod], overrides[e.cons_loc]
            )


    overrides[e.exc_key] = create_hash(
        overrides[e.prod_name],
        overrides[e.prod_prod],
        overrides[e.prod_loc],
        overrides[e.cons_name],
        overrides[e.cons_prod],
        overrides[e.cons_loc],
    )

    assert _mandatory_fields.issubset(
        set(overrides.keys())
    ), f"Mandatory fields are missing: {_mandatory_fields}"

    new_exc.update(overrides)

    return new_exc


def apply_transformation_losses(market_exc, transfer_loss, scenarios):
    # add transformation losses (apply to low, medium and high voltage)
    # transformation losses are ratios

    tloss_exc = market_exc.copy()
    tloss_exc[(s.exchange, c.type)] = "technosphere"

    # tloss_exc[[(col[0], c.amount) for col in tloss_exc.index if col[1] == c.amount]] = 0

    cols = []
    vals = []
    scenario_cols = [col[0] for col in tloss_exc.index
                     if col[0] not in [s.exchange, s.tag]
                     ]
    for i in scenario_cols:
        cols.extend(
            [
                (i, c.cons_prod_vol),
                (i, c.amount),
                (i, c.efficiency),
                (i, c.comment),
            ]
        )
        vals.extend([np.nan, np.nan, np.nan, ""])


    tloss_exc[cols] = vals

    tloss_exc[[(col[0], c.amount)
               for col in tloss_exc.index
               if col[1] == c.amount]] = transfer_loss * np.array([
        1 if col[0] in scenarios else 0 for col in tloss_exc.index
        if col[1] == c.amount
    ])

    return tloss_exc


def calculate_energy_mix(
    iam_data,
    region,
    scenarios,
    period,
    years,
    calculate_solar_share=True,
    year_interpolation_range=(2010, 2101),
    voltage="high"
):

    electricity_mix = iam_data.electricity_markets.sel(
        region=region, scenario=scenarios
    ).interp(
        year=range(*year_interpolation_range),
        kwargs={"fill_value": "extrapolate"},
    )

    for i, iyear in enumerate(years):

        _filter = (electricity_mix.year > (iyear + period)) + (
            electricity_mix.year < iyear
        )
        electricity_mix.loc[{"year": _filter, "scenario": scenarios[i]}] = np.nan

    electricity_mix = electricity_mix.mean(dim="year")

    if voltage== "high":
        # exclude the technologies which contain residential solar power (for high voltage markets)
        _nonsolarfilter = [
            tech
            for tech in electricity_mix.coords["variables"].values
            if "residential" not in tech.lower()
        ]

        return electricity_mix.sel(variables=_nonsolarfilter)

    else:
        _solarfilter = [
            tech
            for tech in electricity_mix.coords["variables"].values
            if "residential" in tech.lower()
        ]
        return electricity_mix.sel(variables=_solarfilter)


def reduce_database(region, electricity_mix, database, location_translator=None):
    if location_translator is None:
        location_translator = {}


    techs = [(s.tag, i.item(0)) for i in electricity_mix.coords["variables"]]

    sel = (
        database[techs].sum(axis=1).astype(bool)
        & equals((s.exchange, c.type), "production")(database)
        & equals((s.exchange, c.unit), "kilowatt hour")(database)
    )

    reduced_dataset = database[sel]
    eco_locs = location_translator[region]

    all_locs = [eco_locs, [region], ["RoW"], ["GLO"], ["RER"], ["CH"]]
    locs = eco_locs
    _filter_loc = contains_any_from_list((s.exchange, c.prod_loc), locs)(
        reduced_dataset
    )
    counter = 1

    while not reduced_dataset.loc[_filter_loc, techs].sum().all():
        sums = reduced_dataset.loc[_filter_loc, techs].sum()
        techs_not_found = (sums[sums == 0]).index

        _filter_loc = _filter_loc | (
            contains_any_from_list((s.exchange, c.prod_loc), all_locs[counter])(
                reduced_dataset
            )
            & database[techs_not_found].sum(axis=1).astype(bool)
        )
        counter += 1

    reduced_dataset = reduced_dataset.loc[_filter_loc]

    return reduced_dataset


def create_new_energy_exchanges(
    electricity_mix,
    reduced_dataset,
    cons_name,
    cons_prod,
    cons_loc,
    voltage
):
    extensions = pd.DataFrame(
        columns=reduced_dataset.columns, index=range(len(reduced_dataset))
    )

    techs = [(s.tag, i.item(0)) for i in electricity_mix.coords["variables"]]

    columns_to_transfer = [
        (s.exchange, c.prod_name),
        (s.exchange, c.prod_prod),
        (s.exchange, c.prod_loc),
        (s.exchange, c.prod_key),
        (s.exchange, c.unit),
        (s.ecoinvent, c.amount),
        (s.ecoinvent, c.efficiency),
        (s.ecoinvent, c.cons_prod_vol),
    ] + techs

    extensions[columns_to_transfer] = reduced_dataset[columns_to_transfer].values

    extensions[
        [
            (s.exchange, c.cons_name),
            (s.exchange, c.cons_prod),
            (s.exchange, c.cons_loc)]
    ] = (
        cons_name,
        cons_prod,
        cons_loc,
    )

    extensions[(s.exchange, c.type)] = "technosphere"
    extensions[(s.ecoinvent, c.amount)] = 0

    extensions[(s.exchange, c.cons_key)] = create_hash_for_database(
        extensions[
            [
                (s.exchange, c.cons_name),
                (s.exchange, c.cons_prod),
                (s.exchange, c.cons_loc),
            ]
        ]
    )
    extensions[(s.exchange, c.exc_key)] = create_hash_for_database(
        extensions[
            [
                (s.exchange, c.prod_name),
                (s.exchange, c.prod_prod),
                (s.exchange, c.prod_loc),
                (s.exchange, c.cons_name),
                (s.exchange, c.cons_prod),
                (s.exchange, c.cons_loc),
            ]
        ]
    )

    # ensure all production volumes are non-zero
    _filter_prod_vol = equals((s.ecoinvent, c.cons_prod_vol), 0)(reduced_dataset)
    reduced_dataset.loc[_filter_prod_vol, (s.ecoinvent, c.cons_prod_vol)] = 1

    techs = (extensions[techs] > 0).apply(lambda x: x[x].index[-1][-1], axis=1)
    techs = pd.DataFrame(techs, columns=[("tech", "tech")])
    techs.index = reduced_dataset.index

    weighting = lambda x: x / x.sum()  # needs to be declared here as it is referring
    # to the solar_amount, which changes every call of the function

    normalized_prod_vol = (
        pd.concat([reduced_dataset, techs], axis=1)
        .groupby(("tech", "tech"))[[(s.ecoinvent, c.cons_prod_vol)]]
        .apply(weighting)
    )

    cols = [i for i in extensions.columns if i[1] == c.amount and i[0] != s.ecoinvent]

    extensions[cols] = (
        normalized_prod_vol.values.T
        * electricity_mix.sel(variables=techs[("tech", "tech")].values)
    ).values.T

    share_sum = extensions[cols].sum(axis=0)
    share_sum[share_sum == 0] = 1

    if voltage == "high":
        extensions[cols] /= share_sum

    cols = []
    vals = []
    for i in extensions.columns.get_level_values(0):
        if "::" in str(i):
            cols.extend([(i, c.cons_prod_vol), (i, c.efficiency), (i, c.comment)])
            vals.extend([np.nan, np.nan, ""])

    extensions[cols] = vals
    return extensions
