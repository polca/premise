"""
Calculate the marginal mix of a market in different ways.
"""

from typing import Tuple
from functools import lru_cache
import numpy as np
import xarray as xr
import yaml

from premise import DATA_DIR

# I've put the numbers I used for the paper in
# the lead times file, but there were a lot less
# technologies there, so the current file has a
# lot of placeholder values at the moment.
# TODO: I will update this later to get more accurate values for the lead times.

IAM_LEADTIMES = DATA_DIR / "consequential" / "leadtimes.yaml"
IAM_LIFETIMES = DATA_DIR / "consequential" / "lifetimes.yaml"

@lru_cache
def get_lifetime(list_tech: Tuple) -> np.ndarray:
    """
    Fetch lifetime values for different technologies from a .yaml file.
    :param list_tech: technology labels to find lifetime values for.
    :type list_tech: list
    :return: a numpy array with technology lifetime values
    :rtype: DataArray
    """
    with open(IAM_LIFETIMES, "r", encoding="utf-8") as stream:
        dict_ = yaml.safe_load(stream)

    dict_ = {k: v for k, v in dict_.items() if k in list_tech}

    return np.array(list(dict_.values()), dtype=float)

@lru_cache
def get_leadtime(list_tech: Tuple) -> np.ndarray:
    """
    Fetch leadtime values for different technologies from a .yaml file.
    :param list_tech: technology labels to find leadtime values for.
    :type list_tech: list
    :return: a numpy array with technology leadtime values
    :rtype: np.array
    """
    with open(IAM_LEADTIMES, "r", encoding="utf-8") as stream:
        dict_ = yaml.safe_load(stream)

    dict_ = {k: v for k, v in dict_.items() if k in list(list_tech)}

    return np.array(list(dict_.values()), dtype=float)

def fetch_avg_leadtime(leadtime: np.ndarray, shares: np.ndarray) -> int:
    """
    Calculate the average lead time of a market.
    """
    return (shares * leadtime).sum().astype(int).values.item(0)

def fetch_avg_capital_replacement_rate(avg_lifetime: int, data: xr.DataArray) -> float:
    """
    Calculate the average capital replacement rate of a market.
    """
    return (
            -1
            / avg_lifetime
            * data.sum(dim="variables").values
        ).item(0) or 0.0

def fetch_avg_lifetime(lifetime: np.ndarray, shares: np.ndarray) -> int:
    """
    Calculate the average lifetime of a market.
    """
    return (shares * lifetime).sum().astype(int).values.item(0) or 30


def fetch_volume_change(data: xr.DataArray, start_year: int, end_year: int) -> float:
    """
    Calculate the volume change of a market.
    """
    return (
            (
                data.sel(year=end_year).sum(dim="variables")
                - data.sel(year=start_year).sum(dim="variables")
            )
            / (end_year - start_year)
        ).values

def remove_constrained_sulppliers(data: xr.DataArray) -> xr.DataArray:
    """
    Remove the shares of suppliers that are constrained from the market.
    """

    # we set CHP suppliers to zero
    # as electricity production is not a
    # determining product for CHPs
    tech_to_ignore = ["CHP", "biomethane"]
    data.loc[
        dict(
            variables=[
                v
                for v in data.variables.values
                if any(x in v for x in tech_to_ignore)
            ],
        )
    ] = 0

    return data

def consequential_method(data: xr.DataArray, year: int, args: dict) -> xr.DataArray:

    """
    Used for consequential modeling only.
    Returns marginal market mixes
    according to the chosen method.

    If range_time and duration are None, then the lead time is taken as
    the time interval (just as with ecoinvent v.3.4).
    foresight: 0 = myopic, 1 = perfect foresight
    lead time: 0 = market average lead time is taken for all technologies,
    lead time: 1 = individual lead time for each technology.
    capital_repl_rate: 0 = horizontal baseline is used,
    capital_repl_rate: 1 = capital replacement rate is used as baseline.
    measurement: 0 = slope, 1 = linear regression,
    measurement: 2 = area under the curve, 3 = weighted slope,
    measurement: 4 = time interval is split in individual years and measured
    weighted_slope_start and end: is needed for measurement method 3,
    the number indicates where the short slope starts
    and ends and is given as the fraction of the total time interval.

    :param data: IAM data
    :param year: year to calculate the mix for
    :param args: arguments for the method

    :return: marginal market mixes
    """

    range_time = args.get("range time", 0)
    duration = args.get("duration", 0)
    foresight = args.get("foresight", 0)
    lead_time = args.get("lead time", 0)
    capital_repl_rate = args.get("capital replacement rate", 0.0)
    measurement = args.get("measurement", 0.0)
    weighted_slope_start = args.get("weighted slope start", 0.75)
    weighted_slope_end = args.get("weighted slope end", 1.0)

    market_shares = xr.zeros_like(
        data.interp(year=[year]),
    )

    # as the time interval can be different for each technology
    # if the individual lead time is used,
    # I use DataArrays to store the values
    # (= start and end of the time interval)

    start = xr.zeros_like(market_shares)
    start.name = "start"
    end = xr.zeros_like(market_shares)
    end.name = "end"
    start_end = xr.merge([start, end])

    # Since there can be different start and end values,
    # I interpolated the entire data of the IAM instead
    # of doing it each time over
    minimum = min(data.year.values)
    maximum = max(data.year.values)
    years_to_interp_for = list(range(minimum, maximum + 1))
    data_full = data.interp(year=years_to_interp_for)

    techs = tuple(data_full.variables.values.tolist())
    leadtime = get_leadtime(techs)

    for region in data.coords["region"].values:

        # I don't yet know the exact start year
        # of the time interval, so as an approximation
        # I use for current_shares the start year
        # of the change
        shares = data_full.sel(region=region, year=year) / data_full.sel(
            region=region, year=year
        ).sum(dim="variables")

        time_parameters = {
            (False, False, False, False): {"start": year, "end": year + fetch_avg_leadtime(leadtime, shares)},
            (False, False, True, True): {"start": year - fetch_avg_leadtime(leadtime, shares), "end": year},
            (False, False, False, True): {"start": year, "end": year + leadtime},
            (True, False, False, True): {
                "start": year + fetch_avg_leadtime(leadtime, shares) - range_time,
                "end": year + fetch_avg_leadtime(leadtime, shares) + range_time,
            },
            (True, False, True, False): {
                "start": year - range_time,
                "end": year + range_time,
            },
            (True, False, False, True): {
                "start": year + leadtime - range_time,
                "end": year + leadtime + range_time,
            },
            (True, False, True, True): {
                "start": year - range_time,
                "end": year + range_time,
            },
            (True, True, False, False): {
                "start": year + fetch_avg_leadtime(leadtime, shares),
                "end": year + fetch_avg_leadtime(leadtime, shares) + duration,
            },
            (True, True, True, False): {"start": year, "end": year + duration},
            (True, True, False, True): {
                "start": year + leadtime,
                "end": year + leadtime + duration,
            },
            (True, True, True, True): {"start": year, "end": year + duration},
        }

        avg_start = time_parameters[
            bool(range_time), bool(duration), bool(foresight), bool(lead_time)
        ]["start"]
        avg_end = time_parameters[
            bool(range_time), bool(duration), bool(foresight), bool(lead_time)
        ]["end"]

        # Now that we do know the start year of the time interval,
        # we can use this to "more accurately" calculate the current shares
        shares = data_full.sel(region=region, year=avg_start) / data_full.sel(
            region=region, year=avg_start
        ).sum(dim="variables")


        # we first need to calculate the average capital replacement rate of the market
        # which is here defined as the inverse of the production-weighted average lifetime
        lifetime = get_lifetime(techs)

        # again was put in to deal with Nan values in data
        avg_lifetime = fetch_avg_lifetime(lifetime, shares)

        # again was put in to deal with Nan values in data
        avg_cap_repl_rate = fetch_avg_capital_replacement_rate(avg_lifetime, data_full.sel(region=region, year=avg_start))

        volume_change = fetch_volume_change(data_full.sel(region=region), avg_start, avg_end)

        data_full = remove_constrained_sulppliers(data_full)


        # second, we measure production growth
        # within the determined time interval
        # for each technology
        # using the selected measuring method and baseline
        if capital_repl_rate == 0 and measurement == 0:

            market_shares.loc[dict(region=region)] = (
                data_full.sel(
                    region=region,
                    year=avg_end,
                ).values
                - data_full.sel(
                    region=region,
                    year=avg_start,
                ).values
            ) / (avg_end - avg_start)

        if capital_repl_rate == 0 and measurement == 1:
            for supplier in data.coords["variables"]:
                a = data_full.sel(region=region, variables=supplier).where(
                    data_full.sel(region=region, variables=supplier).year >= avg_start
                )
                b = a.where(a.year <= avg_end)
                c = b.polyfit(dim="year", deg=1)
                market_shares.loc[
                    dict(region=region, variables=supplier)
                ] = c.polyfit_coefficients[0].values

        if capital_repl_rate == 0 and measurement == 2:
            for supplier in data.coords["variables"]:
                a = data_full.sel(region=region, variables=supplier).where(
                    data_full.sel(region=region, variables=supplier).year >= avg_start
                )
                b = a.where(a.year <= avg_end)
                c = b.sum(dim="year").values
                n = avg_end - avg_start
                total_area = 0.5 * (
                    2 * c
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=avg_end,
                    ).values
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=avg_start,
                    ).values
                )
                baseline_area = (
                    data_full.sel(
                        region=region,
                        variables=supplier,
                        year=avg_start,
                    ).values
                    * n
                )
                market_shares.loc[dict(region=region, variables=supplier)] = (
                    total_area - baseline_area
                )

        if capital_repl_rate == 0 and measurement == 3:
            for supplier in data.coords["variables"]:
                slope = (
                    data_full.sel(
                        region=region,
                        variables=supplier,
                        year=avg_end,
                    ).values
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=avg_start,
                    ).values
                ) / (avg_end - avg_start)

                short_slope_start = (
                    avg_start + (avg_end - avg_start) * weighted_slope_start
                ).round()
                short_slope_end = (
                    avg_start + (avg_end - avg_start) * weighted_slope_end
                ).round()
                short_slope = (
                    data_full.sel(
                        region=region, variables=supplier, year=short_slope_end
                    ).values
                    - data_full.sel(
                        region=region, variables=supplier, year=short_slope_start
                    ).values
                ) / (short_slope_end - short_slope_start)
                if slope == 0:
                    x = 0
                else:
                    x = short_slope / slope
                if x > 500:
                    y = 1
                elif x < -500:
                    y = -1
                else:
                    y = 2 * (np.exp(-1 + x) / (1 + np.exp(-1 + x)) - 0.5)
                market_shares.loc[dict(region=region, variables=supplier)] = (
                    slope + slope * y
                )

        if capital_repl_rate == 0 and measurement == 4:
            n = avg_end - avg_start
            split_years = list(
                range(
                    int(start.sel(region=region).isel(variables=[0]).values),
                    int(end.sel(region=region).isel(variables=[0]).values),
                )
            )
            for y in split_years:
                market_shares_split = xr.DataArray(
                    np.zeros(tuple(shape)),
                    dims=["region", "variables", "year"],
                    coords={
                        "region": data.coords["region"],
                        "variables": data.variables,
                        "year": [year],
                    },
                )
                for supplier in data.coords["variables"]:
                    market_shares_split.loc[
                        dict(region=region, variables=supplier)
                    ] = (
                        data_full.sel(
                            region=region, variables=supplier, year=y + 1
                        ).values
                        - data_full.sel(
                            region=region, variables=supplier, year=y
                        ).values
                    )

                # we remove NaNs and np.inf
                market_shares_split.loc[dict(region=region)].values[
                    market_shares_split.loc[dict(region=region)].values == np.inf
                ] = 0
                market_shares_split.loc[dict(region=region)] = market_shares_split.loc[
                    dict(region=region)
                ].fillna(0)

                if (
                    capital_repl_rate == 0
                    and volume_change < 0
                    or capital_repl_rate == 1
                    and volume_change < avg_cap_repl_rate
                ):
                    # we remove suppliers with a positive growth
                    market_shares.loc[dict(region=region)].values[
                        market_shares.loc[dict(region=region)].values > 0
                    ] = 0
                    # we reverse the sign of negative growth suppliers
                    market_shares.loc[dict(region=region)] *= -1
                    market_shares.loc[dict(region=region)] /= market_shares.loc[
                        dict(region=region)
                    ].sum(dim="variables")
                else:
                    # we remove suppliers with a negative growth
                    market_shares_split.loc[dict(region=region)].values[
                        market_shares_split.loc[dict(region=region)].values < 0
                    ] = 0
                    market_shares_split.loc[
                        dict(region=region)
                    ] /= market_shares_split.loc[dict(region=region)].sum(
                        dim="variables"
                    )
                market_shares.loc[dict(region=region)] += market_shares_split.loc[
                    dict(region=region)
                ]
            market_shares.loc[dict(region=region)] /= n

        if capital_repl_rate == 1 and measurement == 0:
            for supplier in data.coords["variables"]:
                market_shares.loc[dict(region=region, variables=supplier)] = (
                    data_full.sel(
                        region=region,
                        variables=supplier,
                        year=end.sel(region=region, variables=supplier),
                    ).values
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                ) / (
                    end.sel(region=region, variables=supplier)
                    - start.sel(region=region, variables=supplier)
                )

                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = (
                    -1
                    / lifetime.sel(variables=supplier).values
                    * data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[
                    dict(region=region, variables=supplier)
                ] -= cap_repl_rate

        if capital_repl_rate == 1 and measurement == 1:
            for supplier in data.coords["variables"]:
                a = data_full.sel(region=region, variables=supplier).where(
                    data_full.sel(region=region, variables=supplier).year
                    >= start.sel(region=region, variables=supplier).values
                )
                b = a.where(
                    a.year <= end.sel(region=region, variables=supplier).values
                )
                c = b.polyfit(dim="year", deg=1)
                market_shares.loc[
                    dict(region=region, variables=supplier)
                ] = c.polyfit_coefficients[0].values

                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = (
                    -1
                    / lifetime.sel(variables=supplier).values
                    * data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[
                    dict(region=region, variables=supplier)
                ] -= cap_repl_rate

        if capital_repl_rate == 1 and measurement == 2:
            for supplier in data.coords["variables"]:
                a = data_full.sel(region=region, variables=supplier).where(
                    data_full.sel(region=region, variables=supplier).year
                    >= start.sel(region=region, variables=supplier).values
                )
                b = a.where(
                    a.year <= end.sel(region=region, variables=supplier).values
                )
                c = b.sum(dim="year").values
                n = (
                    end.sel(region=region, variables=supplier).values
                    - start.sel(region=region, variables=supplier).values
                )
                total_area = 0.5 * (
                    2 * c
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=end.sel(region=region, variables=supplier),
                    ).values
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                )
                baseline_area = (
                    data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                    * n
                )
                market_shares.loc[dict(region=region, variables=supplier)] = (
                    total_area - baseline_area
                )

                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = (
                    -1
                    / lifetime.sel(variables=supplier).values
                    * data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                    * (
                        (
                            end.sel(region=region, variables=supplier)
                            - start.sel(region=region, variables=supplier)
                        )
                        ^ 2
                    )
                    * 0.5
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[
                    dict(region=region, variables=supplier)
                ] -= cap_repl_rate

        if capital_repl_rate == 1 and measurement == 3:
            for supplier in data.coords["variables"]:
                slope = (
                    data_full.sel(
                        region=region,
                        variables=supplier,
                        year=end.sel(region=region, variables=supplier),
                    ).values
                    - data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                ) / (
                    end.sel(region=region, variables=supplier).values
                    - start.sel(region=region, variables=supplier).values
                )

                short_slope_start = (
                    start.sel(region=region, variables=supplier).values
                    + (
                        end.sel(region=region, variables=supplier).values
                        - start.sel(region=region, variables=supplier).values
                    )
                    * weighted_slope_start
                ).round()
                short_slope_end = (
                    start.sel(region=region, variables=supplier).values
                    + (
                        end.sel(region=region, variables=supplier).values
                        - start.sel(region=region, variables=supplier).values
                    )
                    * weighted_slope_end
                ).round()
                short_slope = (
                    data_full.sel(
                        region=region, variables=supplier, year=short_slope_end
                    ).values
                    - data_full.sel(
                        region=region, variables=supplier, year=short_slope_start
                    ).values
                ) / (short_slope_end - short_slope_start)

                cap_repl_rate = (
                    -1
                    / lifetime.sel(variables=supplier).values
                    * data_full.sel(
                        region=region,
                        variables=supplier,
                        year=start.sel(region=region, variables=supplier),
                    ).values
                )
                slope -= cap_repl_rate
                short_slope -= cap_repl_rate

                if slope == 0:
                    x = 0
                else:
                    x = short_slope / slope
                if x > 500:
                    y = 1
                elif x < -500:
                    y = -1
                else:
                    y = 2 * (np.exp(-1 + x) / (1 + np.exp(-1 + x)) - 0.5)
                market_shares.loc[dict(region=region, variables=supplier)] = (
                    slope + slope * y
                )

        if capital_repl_rate == 1 and measurement == 4:
            n = (
                end.sel(region=region).isel(variables=[0]).values
                - start.sel(region=region).isel(variables=[0]).values
            )
            split_years = list(
                range(
                    int(start.sel(region=region).isel(variables=[0]).values),
                    int(end.sel(region=region).isel(variables=[0]).values),
                )
            )
            for y in split_years:
                market_shares_split = xr.DataArray(
                    np.zeros(tuple(shape)),
                    dims=["region", "variables", "year"],
                    coords={
                        "region": data.coords["region"],
                        "variables": data.variables,
                        "year": [year],
                    },
                )
                for supplier in data.coords["variables"]:
                    market_shares_split.loc[
                        dict(region=region, variables=supplier)
                    ] = (
                        data_full.sel(
                            region=region, variables=supplier, year=y + 1
                        ).values
                        - data_full.sel(
                            region=region, variables=supplier, year=y
                        ).values
                    )
                    cap_repl_rate = (
                        -1
                        / lifetime.sel(variables=supplier).values
                        * data_full.sel(
                            region=region,
                            variables=supplier,
                            year=start.sel(region=region, variables=supplier),
                        ).values
                    )

                    max_cap_repl_rate = (
                        data_full.sel(
                            region=region,
                            variables=supplier,
                            year=start.sel(region=region, variables=supplier),
                        ).values
                        / n
                    )

                    if cap_repl_rate > max_cap_repl_rate:
                        cap_repl_rate = max_cap_repl_rate

                    market_shares_split.loc[
                        dict(region=region, variables=supplier)
                    ] -= cap_repl_rate

                # we remove NaNs and np.inf
                market_shares_split.loc[dict(region=region)].values[
                    market_shares_split.loc[dict(region=region)].values == np.inf
                ] = 0
                market_shares_split.loc[dict(region=region)] = market_shares_split.loc[
                    dict(region=region)
                ].fillna(0)

                if (
                    capital_repl_rate == 0
                    and volume_change < 0
                    or capital_repl_rate == 1
                    and volume_change < avg_cap_repl_rate
                ):
                    # we remove suppliers with a positive growth
                    market_shares.loc[dict(region=region)].values[
                        market_shares.loc[dict(region=region)].values > 0
                    ] = 0
                    # we reverse the sign of negative growth suppliers
                    market_shares.loc[dict(region=region)] *= -1
                    market_shares.loc[dict(region=region)] /= market_shares.loc[
                        dict(region=region)
                    ].sum(dim="variables")
                else:
                    # we remove suppliers with a negative growth
                    market_shares_split.loc[dict(region=region)].values[
                        market_shares_split.loc[dict(region=region)].values < 0
                    ] = 0
                    market_shares_split.loc[
                        dict(region=region)
                    ] /= market_shares_split.loc[dict(region=region)].sum(
                        dim="variables"
                    )
                market_shares.loc[dict(region=region)] += market_shares_split.loc[
                    dict(region=region)
                ]
            market_shares.loc[dict(region=region)] /= n

        # market_shares.loc[dict(region=region)] = market_shares.loc[dict(region=region)][:, None]
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

        # market decreasing faster than the average capital renewal rate
        # in this case, the idea is that oldest/non-competitive technologies
        # are likely to supply by increasing their lifetime
        # as the market does not justify additional capacity installation
        if (
            capital_repl_rate == 0
            and volume_change < 0
            or capital_repl_rate == 1
            and volume_change < avg_cap_repl_rate
        ):
            # we remove suppliers with a positive growth
            market_shares.loc[dict(region=region)].values[
                market_shares.loc[dict(region=region)].values > 0
            ] = 0
            # we reverse the sign of negative growth suppliers
            market_shares.loc[dict(region=region)] *= -1
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
    return market_shares