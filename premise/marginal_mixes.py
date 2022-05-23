import csv

import numpy as np
import pandas as pd
import xarray as xr
import yaml

from . import DATA_DIR

IAM_LIFETIMES = DATA_DIR / "consequential" / "lifetimes.yaml"


def get_lifetime(list_tech):
    """
    Fetch lifetime values for different technologies from a .csv file.
    :param list_tech: technology labels to find lifetime values for.
    :type list_tech: list
    :return: a numpy array with technology lifetime values
    :rtype: np.array
    """
    with open(IAM_LIFETIMES, "r") as stream:
        dict_ = yaml.safe_load(stream)

    arr = np.zeros_like(list_tech)

    for i, tech in enumerate(list_tech):
        lifetime = dict_[tech]
        arr[i] = lifetime

    return arr.astype(float)


def baseline_method(data, year, time_horizon):
    """
    Used for consequential modeling only.
    Returns marginal market mixes
    according to the Baseline method.
    :param data: IAM data
    :return: marginal market mixes
    """

    shape = list(data.shape)
    shape[-1] = 1

    market_shares = xr.DataArray(
        np.zeros(tuple(shape)),
        dims=["region", "variables", "year"],
        coords={
            "region": data.coords["region"],
            "variables": data.variables,
            "year": [year],
        },
    )

    for region in data.coords["region"].values:

        current_shares = data.sel(region=region, year=year) / data.sel(
            region=region, year=year
        ).sum(dim="variables")

        # we first need to calculate the average capital replacement rate of the market
        # which is here defined as the inverse of the production-weighted average lifetime
        lifetime = get_lifetime(current_shares.variables.values)

        avg_lifetime = np.sum(current_shares.values * lifetime)

        avg_cap_repl_rate = -1 / avg_lifetime

        volume_change = (
            data.sel(region=region)
            .sum(dim="variables")
            .interp(year=year + time_horizon)
            / data.sel(region=region).sum(dim="variables").interp(year=year)
        ) - 1

        # first, we set CHP suppliers to zero
        # as electricity production is not a determining product for CHPs
        tech_to_ignore = ["CHP", "biomethane"]
        data.loc[
            dict(
                variables=[
                    v
                    for v in data.variables.values
                    if any(x in v for x in tech_to_ignore)
                ],
                region=region,
            )
        ] = 0

        # second, we fetch the ratio between production
        # in `self.year` and `self.year` + `time_horizon`
        # for each technology
        market_shares.loc[dict(region=region)] = (
            data.sel(region=region).interp(year=year + time_horizon).values
            / data.sel(region=region).interp(year=year).values
        )[:, None] - 1

        market_shares.loc[dict(region=region)] = market_shares.loc[
            dict(region=region)
        ].round(3)

        # we remove NaNs and np.inf
        market_shares.loc[dict(region=region)].values[
            market_shares.loc[dict(region=region)].values == np.inf
        ] = 0
        market_shares.loc[dict(region=region)] = market_shares.loc[
            dict(region=region)
        ].fillna(0)

        # we fetch the technologies' lifetimes
        lifetime = get_lifetime(market_shares.variables.values)
        # get the capital replacement rate
        # which is here defined as -1 / lifetime
        cap_repl_rate = -1 / lifetime

        # subtract the capital replacement (which is negative) rate
        # to the changes market share
        market_shares.loc[dict(region=region, year=year)] += cap_repl_rate

        # market decreasing faster than the average capital renewal rate
        # in this case, the idea is that oldest/non-competitive technologies
        # are likely to supply by increasing their lifetime
        # as the market does not justify additional capacity installation
        if volume_change < avg_cap_repl_rate:

            # we remove suppliers with a positive growth
            market_shares.loc[dict(region=region)].values[
                market_shares.loc[dict(region=region)].values > 0
            ] = 0
            # we reverse the sign of negative growth suppliers
            market_shares.loc[dict(region=region)] *= -1
            market_shares.loc[dict(region=region)] /= market_shares.loc[
                dict(region=region)
            ].sum(dim="variables")

            # multiply by volumes at T0
            market_shares.loc[dict(region=region)] *= data.sel(region=region, year=year)
            market_shares.loc[dict(region=region)] /= market_shares.loc[
                dict(region=region)
            ].sum(dim="variables")

        # increasing market or
        # market decreasing slowlier than the
        # capital renewal rate
        else:

            # we remove suppliers with a negative growth
            market_shares.loc[dict(region=region)].values[
                market_shares.loc[dict(region=region)].values < 0
            ] = 0
            market_shares.loc[dict(region=region)] /= market_shares.loc[
                dict(region=region)
            ].sum(dim="variables")

            # multiply by volumes at T0
            market_shares.loc[dict(region=region)] *= data.sel(region=region, year=year)
            market_shares.loc[dict(region=region)] /= market_shares.loc[
                dict(region=region)
            ].sum(dim="variables")

    return market_shares
