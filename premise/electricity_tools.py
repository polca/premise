import warnings

import numpy as np
import pandas as pd

from premise.utils import c, create_hash, create_hash_for_database, e, s


def create_exchange_from_ref(index, overrides=None, prod_equals_con=False):
    # create production dataset

    if overrides is None:
        raise KeyError("exchanges have to define producer and consumer information!")

    new_exc = pd.Series(index=index)

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
    elif not (_provides_cons_info and _provides_prod_info):
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
        overrides[e.prod_key] = overrides[e.con_key] = create_hash(
            overrides[e.prod_name], overrides[e.prod_prod], overrides[e.prod_loc]
        )
    else:
        overrides[e.prod_key] = create_hash(
            overrides[e.prod_name], overrides[e.prod_prod], overrides[e.prod_loc]
        )
        overrides[e.con_key] = create_hash(
            overrides[e.con_name], overrides[e.con_prod], overrides[e.con_loc]
        )

    overrides[e.exc_key] = create_hash(
        overrides[e.prod_name],
        overrides[e.prod_prod],
        overrides[e.prod_loc],
        overrides[e.con_name],
        overrides[e.con_prod],
        overrides[e.con_loc],
    )

    assert _mandatory_fields.issubset(
        set(overrides.keys())
    ), f"Mandatory fields are missing: {_mandatory_fields}"

    new_exc.update(overrides)

    return new_exc


def apply_transformation_losses(market_exc, transfer_loss):
    # add transformation losses (apply to low, medium and high voltage)
    # transformation losses are ratios

    tloss_exc = market_exc.copy()
    tloss_exc[(s.exchange, c.type)] = "technosphere"

    cols = []
    vals = []
    for i in tloss_exc.index.get_level_values(0):
        if "::" in str(i) or s.ecoinvent == i:
            cols.extend(
                [
                    (i, c.cons_prod_vol),
                    (i, c.amount),
                    (i, c.efficiency),
                    (i, c.comment),
                ]
            )
            vals.extend([np.nan, transfer_loss, np.nan, ""])

    tloss_exc[cols] = vals

    return tloss_exc


def calculate_energy_mix(
    iam_data,
    region,
    scenarios,
    period,
    years,
    calculate_solar_share=True,
    year_interpolation_range=(2010, 2100),
):

    electricity_mix = iam_data.electricity_markets.sel(
        region=region, scenario=scenarios
    ).interp(
        year=range(*year_interpolation_range),
        kwargs={"fill_value": "extrapolate"},
    )

    for iyear in years:
        _filter = (electricity_mix.year > (iyear + period)) + (
            electricity_mix.year < iyear
        )
        electricity_mix[{"year": _filter}] = np.nan

    electricity_mix = electricity_mix.mean(dim="year")

    print(
        "Test - sum of market shares over techs:",
        electricity_mix.sum(dim="variables"),
    )

    if not calculate_solar_share:
        # returns an empty pd.DataFrame to provide a stable interface as in the else case it would return the solar_share in this place
        return electricity_mix, pd.DataFrame()

    _solarfilter = [
        tech
        for tech in electricity_mix.coords["variables"].values
        if "residential" in tech.lower()
    ]
    solar_amount = electricity_mix.sel(variables=_solarfilter).sum(dim="variables")

    # reshape and convert solar_amount xarray to pd.DataFrame with correct column structure for broadcasting to scenarios
    solar_amount = solar_amount.to_dataframe().drop("region", axis=1)["value"]
    idx = pd.MultiIndex.from_product(
        (tuple(solar_amount.coords["scenario"].values), [c.amount])
    )
    solar_amount.columns = idx

    print("solar_amount:", solar_amount)
    # TODO double-check scientific correctness - solar_amount seems to be always zero in all scenarios

    # exclude the technologies which contain residential solar power (for high voltage markets)
    _nonsolarfilter = [
        tech
        for tech in electricity_mix.coords["variables"].values
        if "residential" not in tech.lower()
    ]

    return electricity_mix.sel(variables=_nonsolarfilter), solar_amount


def reduce_database(region, electricity_mix, database, location_translator=None):
    if location_translator is None:
        location_translator = {}

    techs = [(s.tag, i.item(0)) for i in electricity_mix.coords["variables"]]
    sel = database[techs].sum(axis=1).astype(bool)
    reduced_dataset = database[sel]

    if region in location_translator:
        eco_locs = location_translator[region]
        sel = reduced_dataset[(s.exchange, c.prod_loc)].isin(eco_locs)
        reduced_dataset = reduced_dataset[sel]
    else:
        warnings.warn(f"no matching ecoinvent location for region {region}")

    return reduced_dataset


def create_new_energy_exchanges(
    electricity_mix,
    reduced_dataset,
    solar_share,
    cons_name,
    cons_prod,
    cons_loc,
):
    extensions = pd.DataFrame(
        columns=reduced_dataset.columns, index=range(len(reduced_dataset))
    )

    columns_to_transfer = [
        (s.exchange, c.prod_name),
        (s.exchange, c.prod_prod),
        (s.exchange, c.prod_loc),
        (s.exchange, c.unit),
        (s.exchange, c.type),
        (s.ecoinvent, c.amount),
        (s.ecoinvent, c.efficiency),
        (s.ecoinvent, c.cons_prod_vol),
    ]

    extensions[columns_to_transfer] = reduced_dataset[columns_to_transfer].values

    extensions[
        [(s.exchange, c.cons_name), (s.exchange, c.cons_prod), (s.exchange, c.cons_loc)]
    ] = (
        cons_name,
        cons_prod,
        cons_loc,
    )

    extensions[[(s.exchange, c.prod_key)]] = create_hash_for_database(
        extensions[
            [
                (s.exchange, c.prod_name),
                (s.exchange, c.prod_prod),
                (s.exchange, c.prod_loc),
            ]
        ]
    )
    extensions[[(s.exchange, c.cons_key)]] = create_hash_for_database(
        extensions[
            [
                (s.exchange, c.cons_name),
                (s.exchange, c.cons_prod),
                (s.exchange, c.cons_loc),
            ]
        ]
    )
    extensions[[(s.exchange, c.exc_key)]] = create_hash_for_database(
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

    weighting = (
        lambda x: x / x.sum() / (1 - solar_share)
    )  # needs to be declared here as it is refering to the solar_amount, which changes every call of the function

    normalized_prod_vol = (
        reduced_dataset.groupby([(s.exchange, c.cons_loc)])[
            [(s.ecoinvent, c.cons_prod_vol)]
        ]
        .apply(weighting)
        .drop((s.ecoinvent, c.cons_prod_vol), axis=1)
    )

    cols = [i for i in extensions.columns if "::" in str(i[0]) and i[1] == c.amount]
    extensions[cols] = normalized_prod_vol

    cols = []
    vals = []
    for i in extensions.columns.get_level_values(0):
        if "::" in str(i):
            cols.extend([(i, c.cons_prod_vol), (i, c.efficiency), (i, c.comment)])
            vals.extend([np.nan, np.nan, ""])

    extensions[cols] = vals
    return extensions
