"""
Calculate the marginal mix of a market in different ways.
Based on the method described in:
https://chemrxiv.org/engage/chemrxiv/article-details/63ee10cdfcfb27a31fe227df

"""

from functools import lru_cache
from typing import Tuple

import numpy as np
import xarray as xr
import yaml
from numpy import ndarray
from prettytable import PrettyTable

from .filesystem_constants import DATA_DIR

IAM_LEADTIMES = DATA_DIR / "consequential" / "leadtimes.yaml"
IAM_LIFETIMES = DATA_DIR / "consequential" / "lifetimes.yaml"
CONSTRAINED_SUPPLIERS = DATA_DIR / "consequential" / "constrained_suppliers.yaml"


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

    val = []
    for tech in list_tech:
        if tech in dict_.keys():
            val.append(dict_[tech])
        else:
            print(f"WARNING: {tech} not found in lifetimes.yaml")

    return np.array(val, dtype=float)


@lru_cache
def get_leadtime(list_tech: Tuple) -> np.ndarray:
    """
    Fetch lead-time values for different technologies from a .yaml file.
    :param list_tech: technology labels to find lead-time values for.
    :type list_tech: list
    :return: a numpy array with technology lead-time values
    :rtype: np.array
    """
    with open(IAM_LEADTIMES, "r", encoding="utf-8") as stream:
        dict_ = yaml.safe_load(stream)

    dict_ = {k: dict_[k] for k in list(list_tech)}

    val = []
    for tech in list_tech:
        if tech in dict_.keys():
            val.append(dict_[tech])
        else:
            print(f"WARNING: {tech} not found in leadtimes.yaml")

    return np.array(val, dtype=float)


def get_list_contrained_suppliers():
    """
    Get a list of constrained suppliers from the leadtimes.yaml file.
    :return: a list of constrained suppliers
    :rtype: list
    """
    with open(CONSTRAINED_SUPPLIERS, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def fetch_avg_leadtime(leadtime: np.ndarray, shares: [np.ndarray, xr.DataArray]) -> int:
    """
    Calculate the average lead-time of a market.
    """

    return (shares * leadtime).sum().astype(int).values.item(0)


def fetch_avg_capital_replacement_rate(avg_lifetime: int, data: xr.DataArray) -> float:
    """
    Calculate the average capital replacement rate of a market.
    """
    return (-1 / avg_lifetime) or 0.0


def fetch_capital_replacement_rates(
    lifetime: np.ndarray, data: xr.DataArray
) -> np.ndarray:
    """
    Calculate the average capital replacement rate of a market.
    """
    return (-1 / lifetime * data).values


def fetch_avg_lifetime(lifetime: np.ndarray, shares: [np.ndarray, xr.DataArray]) -> int:
    """
    Calculate the average lifetime of a market.
    """
    return (shares * lifetime).sum().astype(int).values.item(0) or 30


def fetch_volume_change(data: xr.DataArray, start_year: int, end_year: int) -> ndarray:
    """
    Calculate the volume change of a market.
    """

    return (
        (
            data.interp(year=end_year).sum(dim="variables")
            - data.interp(year=start_year).sum(dim="variables")
        )
        / (end_year - start_year)
    ).values


def consequential_method(
    data: xr.DataArray, year: int, args: dict, sector: str
) -> xr.DataArray:
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
    :param sector: sector to calculate the mix for

    :return: marginal market mixes
    """

    args = args or {}

    range_time: int = args.get("range time", 2)
    duration: int = args.get("duration", False)
    foresight: bool = args.get("foresight", False)
    lead_time: int = args.get("lead time", False)
    capital_repl_rate: bool = args.get("capital replacement rate", True)
    measurement: int = args.get("measurement", 0)
    weighted_slope_start: float = args.get("weighted slope start", 0.75)
    weighted_slope_end: float = args.get("weighted slope end", 1.0)

    market_shares = xr.zeros_like(
        data.interp(year=[year]),
    )

    # Since there can be different start and end values,
    # we interpolate the entire data of the IAM instead
    # of doing it each time over
    minimum = min(data.year.values)
    maximum = max(data.year.values)
    years_to_interp_for = list(range(minimum, maximum + 1))

    data_full = xr.DataArray(
        np.nan,
        dims=["region", "variables", "year"],
        coords={
            "region": data.region,
            "year": years_to_interp_for,
            "variables": data.variables,
        },
    )
    data_full.loc[{"year": data.year}] = data
    # interpolation is done using cubic spline interpolation
    data_full = data_full.interpolate_na(dim="year", method="akima")

    techs = tuple(data_full.variables.values.tolist())
    constrained_suppliers = get_list_contrained_suppliers()
    leadtime = get_leadtime(techs)
    lifetime = get_lifetime(techs)

    # set constrained suppliers to zero
    data_full.loc[
        dict(variables=[tech for tech in techs if tech in constrained_suppliers])
    ] = 0

    # create a list to store variables values
    # for each region
    # to print a pretty table at the end
    summary = []

    for region in data.coords["region"].values:
        # we don't yet know the exact start year
        # of the time interval, so as an approximation
        # we use for current_shares the start year
        # of the change
        shares = data_full.sel(region=region, year=year) / data_full.sel(
            region=region, year=year
        ).sum(dim="variables")

        # are all shares to zero?
        if shares.isnull().all():
            continue

        time_parameters = {
            (False, False, False, False): {
                "start": year,
                "end": year + fetch_avg_leadtime(leadtime, shares),
                "start_avg": year,
                "end_avg": year + fetch_avg_lifetime(lifetime=lifetime, shares=shares),
            },
            (False, False, True, False): {
                "start": year - fetch_avg_leadtime(leadtime, shares),
                "end": year,
                "start_avg": year - fetch_avg_leadtime(leadtime, shares),
                "end_avg": year,
            },
            (False, False, False, True): {
                "start": year,
                "end": year + fetch_avg_leadtime(leadtime, shares),
                "start_avg": year,
                "end_avg": year + fetch_avg_lifetime(lifetime=lifetime, shares=shares),
            },
            (False, False, True, True): {
                "start": year - fetch_avg_leadtime(leadtime, shares),
                "end": year,
                "start_avg": year
                - fetch_avg_lifetime(lifetime=lifetime, shares=shares),
                "end_avg": year,
            },
            (True, False, False, False): {
                "start": year + fetch_avg_leadtime(leadtime, shares) - range_time,
                "end": year + fetch_avg_leadtime(leadtime, shares) + range_time,
                "start_avg": year + fetch_avg_leadtime(leadtime, shares) - range_time,
                "end_avg": year + fetch_avg_leadtime(leadtime, shares) + range_time,
            },
            (True, False, True, False): {
                "start": year - range_time,
                "end": year + range_time,
                "start_avg": year - range_time,
                "end_avg": year + range_time,
            },
            (True, False, False, True): {
                "start": year + fetch_avg_leadtime(leadtime, shares) - range_time,
                "end": year + fetch_avg_leadtime(leadtime, shares) + range_time,
                "start_avg": year + fetch_avg_leadtime(leadtime, shares) - range_time,
                "end_avg": year + fetch_avg_leadtime(leadtime, shares) + range_time,
            },
            (True, False, True, True): {
                "start": year - range_time,
                "end": year + range_time,
                "start_avg": year - range_time,
                "end_avg": year + range_time,
            },
            (False, True, False, False): {
                "start": year + fetch_avg_leadtime(leadtime, shares),
                "end": year + fetch_avg_leadtime(leadtime, shares) + duration,
                "start_avg": year + fetch_avg_leadtime(leadtime, shares),
                "end_avg": year + fetch_avg_leadtime(leadtime, shares) + duration,
            },
            (False, True, True, False): {
                "start": year,
                "end": year + duration,
                "start_avg": year,
                "end_avg": year + duration,
            },
            (False, True, False, True): {
                "start": year + fetch_avg_leadtime(leadtime, shares),
                "end": year + fetch_avg_leadtime(leadtime, shares) + duration,
                "start_avg": year + fetch_avg_leadtime(leadtime, shares),
                "end_avg": year + fetch_avg_leadtime(leadtime, shares) + duration,
            },
            (False, True, True, True): {
                "start": year,
                "end": year + duration,
                "start_avg": year,
                "end_avg": year + duration,
            },
        }

        try:
            params = time_parameters[
                (bool(range_time), bool(duration), foresight, lead_time)
            ]
            start = params["start"]
            end = params["end"]

            avg_start = params["start_avg"]
            avg_end = params["end_avg"]

        except KeyError:
            print(
                f"The combination of range_time, duration, foresight, and lead_time {range_time, duration, foresight, lead_time} "
                "is not possible. Please check your input. Specifically, if `range_time` is non-null, `duration` must be null, "
                "and vice versa."
            )
            continue

        # Now that we do know the start year of the time interval,
        # we can use this to "more accurately" calculate the current shares

        if avg_start not in data_full.coords["year"].values:
            # pick nearest value
            avg_start = data_full.coords["year"].values[
                np.abs(data_full.coords["year"].values - avg_start).argmin()
            ]
        if avg_end not in data_full.coords["year"].values:
            # pick nearest value
            avg_end = data_full.coords["year"].values[
                np.abs(data_full.coords["year"].values - avg_end).argmin()
            ]

        if start not in data_full.coords["year"].values:
            # pick nearest value
            start = data_full.coords["year"].values[
                np.abs(data_full.coords["year"].values - start).argmin()
            ]

        if end not in data_full.coords["year"].values:
            # pick nearest value
            end = data_full.coords["year"].values[
                np.abs(data_full.coords["year"].values - end).argmin()
            ]

        shares = data_full.sel(region=region, year=avg_start) / data_full.sel(
            region=region, year=avg_start
        ).sum(dim="variables")

        # we first need to calculate the average capital replacement rate of the market
        # which is here defined as the inverse of the production-weighted average lifetime

        # again was put in to deal with Nan values in data
        avg_lifetime = fetch_avg_lifetime(lifetime, shares)

        # again was put in to deal with Nan values in data
        avg_cap_repl_rate = fetch_avg_capital_replacement_rate(
            avg_lifetime, data_full.sel(region=region, year=avg_start)
        )

        volume_change = fetch_volume_change(
            data_full.sel(region=region), avg_start, avg_end
        )

        # second, we measure production growth
        # within the determined time interval
        # for each technology
        # using the selected measuring method and baseline
        if measurement == 0:
            # if the capital replacement rate is not used,
            if isinstance(start, np.ndarray):
                data_start = (
                    data_full.sel(
                        region=region,
                        year=start,
                    )
                    * np.identity(start.shape[0])
                ).sum(dim="variables")
            else:
                data_start = data_full.sel(
                    region=region,
                    year=start,
                )

            if isinstance(end, np.ndarray):
                data_end = (
                    data_full.sel(
                        region=region,
                        year=end,
                    )
                    * np.identity(end.shape[0])
                ).sum(dim="variables")
            else:
                data_end = data_full.sel(
                    region=region,
                    year=end,
                )

            market_shares.loc[{"region": region}] = (
                (data_end.values - data_start.values) / (end - start)
            )[:, None]

            if capital_repl_rate:
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = fetch_capital_replacement_rates(
                    lifetime, data_full.sel(region=region, year=avg_start)
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[{"region": region}] -= cap_repl_rate[:, None]

        if measurement == 1:
            if isinstance(end, np.ndarray):
                new_end = np.zeros_like(data_full.sel(region=region))
                new_end[:, :] = end[:, None]
                end = new_end

            if isinstance(start, np.ndarray):
                new_start = np.zeros_like(data_full.sel(region=region))
                new_start[:, :] = start[:, None]
                start = new_start

            mask_end = data_full.sel(region=region).year.values[None, :] <= end
            mask_start = data_full.sel(region=region).year.values[None, :] >= start
            mask = mask_end & mask_start

            maskxr = xr.zeros_like(data_full.sel(region=region))
            maskxr += mask

            masked_data = data_full.sel(region=region).where(maskxr, drop=True)

            coeff = masked_data.polyfit(dim="year", deg=1)

            market_shares.loc[{"region": region}] = coeff.polyfit_coefficients[
                0
            ].values[:, None]

            if capital_repl_rate:
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = fetch_capital_replacement_rates(
                    lifetime, data_full.sel(region=region, year=avg_start)
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[{"region": region}] -= cap_repl_rate[:, None]

        if measurement == 2:
            if isinstance(end, np.ndarray):
                data_end = (
                    data_full.sel(
                        region=region,
                        year=end,
                    )
                    * np.identity(end.shape[0])
                ).sum(dim="variables")

                new_end = np.zeros_like(data_full.sel(region=region))
                new_end[:, :] = end[:, None]
                end = new_end
            else:
                data_end = data_full.sel(
                    region=region,
                    year=end,
                )

            if isinstance(start, np.ndarray):
                data_start = (
                    data_full.sel(
                        region=region,
                        year=start,
                    )
                    * np.identity(start.shape[0])
                ).sum(dim="variables")

                new_start = np.zeros_like(data_full.sel(region=region))
                new_start[:, :] = start[:, None]
                start = new_start
            else:
                data_start = data_full.sel(
                    region=region,
                    year=start,
                )

            mask_end = data_full.sel(region=region).year.values[None, :] <= end
            mask_start = data_full.sel(region=region).year.values[None, :] >= start
            mask = mask_end & mask_start

            maskxr = xr.zeros_like(data_full.sel(region=region))
            maskxr += mask

            masked_data = data_full.sel(region=region).where(maskxr, drop=True)

            coeff = masked_data.sum(dim="year").values

            if isinstance(end, np.ndarray):
                end = np.mean(end, 1)

            if isinstance(start, np.ndarray):
                start = np.mean(start, 1)

            n = end - start

            total_area = 0.5 * (2 * coeff - data_end.values - data_start.values)

            if isinstance(n, np.ndarray):
                if n.shape != data_start.shape:
                    n = n.mean(axis=1)

            baseline_area = data_start * n

            market_shares.loc[{"region": region}] = (
                (total_area - baseline_area) / n
            ).values[:, None]

            if capital_repl_rate:
                # this bit differs from above
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = (
                    fetch_capital_replacement_rates(
                        lifetime, data_full.sel(region=region, year=avg_start)
                    )
                    * ((avg_end - avg_start) ^ 2)
                    * 0.5
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[{"region": region}] -= cap_repl_rate[:, None]

        if measurement == 3:
            if isinstance(end, np.ndarray):
                data_end = (
                    data_full.sel(
                        region=region,
                        year=end,
                    )
                    * np.identity(end.shape[0])
                ).sum(dim="variables")

            else:
                data_end = data_full.sel(
                    region=region,
                    year=end,
                )

            if isinstance(start, np.ndarray):
                data_start = (
                    data_full.sel(
                        region=region,
                        year=start,
                    )
                    * np.identity(start.shape[0])
                ).sum(dim="variables")

            else:
                data_start = data_full.sel(
                    region=region,
                    year=start,
                )

            slope = (data_end.values - data_start.values) / (end - start)

            short_slope_start = start + (end - start) * weighted_slope_start
            short_slope_end = start + (end - start) * weighted_slope_end

            if isinstance(short_slope_start, np.ndarray):
                data_short_slope_start = (
                    data_full.sel(
                        region=region,
                        year=short_slope_start,
                    )
                    * np.identity(short_slope_start.shape[0])
                ).sum(dim="variables")

            else:
                data_short_slope_start = data_full.sel(
                    region=region,
                    year=short_slope_start,
                )

            if isinstance(short_slope_end, np.ndarray):
                data_short_slope_end = (
                    data_full.sel(
                        region=region,
                        year=short_slope_end,
                    )
                    * np.identity(short_slope_end.shape[0])
                ).sum(dim="variables")

            else:
                data_short_slope_end = data_full.sel(
                    region=region,
                    year=short_slope_end,
                )

            short_slope = (
                data_short_slope_end.values - data_short_slope_start.values
            ) / (short_slope_end - short_slope_start)

            if short_slope.shape != slope.shape:
                short_slope = np.repeat(short_slope, slope.shape[0])

            if capital_repl_rate:
                cap_repl_rate = fetch_capital_replacement_rates(
                    lifetime, data_full.sel(region=region, year=avg_start)
                )
                slope -= cap_repl_rate
                short_slope -= cap_repl_rate

            x = np.divide(
                short_slope,
                slope,
                out=np.zeros(short_slope.shape, dtype=float),
                where=slope != 0,
            )

            split_year = np.where(x < 0, -1, 1)
            split_year = np.where(
                (x > -500) & (x < 500),
                2 * (np.exp(-1 + x) / (1 + np.exp(-1 + x)) - 0.5),
                split_year,
            )

            market_shares.loc[{"region": region}] = (slope + slope * split_year)[
                :, None
            ]

        if measurement == 4:
            n = avg_end - avg_start

            if isinstance(n, int):
                n = np.array([n])

            # use average start and end years
            split_years = range(avg_start, avg_end)
            for split_year in split_years:
                market_shares_split = xr.zeros_like(market_shares)
                market_shares_split.loc[{"region": region}] = (
                    data_full.sel(region=region, year=split_year + 1)
                    - data_full.sel(region=region, year=split_year)
                ).values[:, None]

                if capital_repl_rate:
                    cap_repl_rate = fetch_capital_replacement_rates(
                        lifetime, data_full.sel(region=region, year=avg_start)
                    )
                    # In cases where a technology is fully phased out somewhere during the time interval we do not want to add capital replacement rate
                    mask = data_full.sel(region=region, year=split_year) != 0
                    cap_repl_rate = cap_repl_rate * mask.values
                    market_shares_split.loc[{"region": region}] -= cap_repl_rate[
                        :, None
                    ]

                if (not capital_repl_rate and volume_change < 0) or (
                    capital_repl_rate and volume_change < avg_cap_repl_rate
                ):
                    # we remove suppliers with a positive growth
                    market_shares_split.loc[{"region": region}].values[
                        market_shares_split.loc[{"region": region}].values > 0
                    ] = 0
                    market_shares_split.loc[
                        {"region": region}
                    ] /= market_shares_split.loc[{"region": region}].sum(
                        dim="variables"
                    )
                    # we reverse the sign so that the suppliers are still seen as negative in the next step
                    market_shares_split.loc[{"region": region}] *= -1

                else:
                    # we remove suppliers with a negative growth
                    market_shares_split.loc[{"region": region}].values[
                        market_shares_split.loc[{"region": region}].values < 0
                    ] = 0
                    market_shares_split.loc[
                        {"region": region}
                    ] /= market_shares_split.loc[{"region": region}].sum(
                        dim="variables"
                    )

                market_shares.loc[{"region": region}] += market_shares_split.loc[
                    {"region": region}
                ]

            market_shares.loc[{"region": region}] /= n[:, None]

        if measurement == 5:
            # if the capital replacement rate is not used,

            if isinstance(start, np.ndarray):
                data_start = (
                    data_full.sel(
                        region=region,
                        year=start,
                    )
                    * np.identity(start.shape[0])
                ).sum(dim="variables")
            else:
                data_start = data_full.sel(
                    region=region,
                    year=start,
                )

            if isinstance(end, np.ndarray):
                data_end = (
                    data_full.sel(
                        region=region,
                        year=end,
                    )
                    * np.identity(end.shape[0])
                ).sum(dim="variables")
            else:
                data_end = data_full.sel(
                    region=region,
                    year=end,
                )

            market_shares.loc[{"region": region}] = (
                (data_end.values - data_start.values) / (end - start)
            )[:, None]

            if capital_repl_rate:
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = fetch_capital_replacement_rates(
                    lifetime, data_full.sel(region=region, year=avg_start)
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[{"region": region}] -= cap_repl_rate[:, None]

            if (not capital_repl_rate and volume_change < 0) or (
                capital_repl_rate and volume_change < avg_cap_repl_rate
            ):
                # we remove suppliers with a positive growth
                market_shares.loc[{"region": region}].values[
                    market_shares.loc[{"region": region}].values >= 0
                ] = 0
                # we keep suppliers with a negative growth
                # we use negative 1 so that in the next step they are still seen as negative
                market_shares.loc[{"region": region}].values[
                    market_shares.loc[{"region": region}].values < 0
                ] = -1
                # and use their production volume as their indicator
                market_shares.loc[{"region": region}] *= data_start.values[:, None]
            # increasing market or
            # market decreasing slower than the
            # capital renewal rate
            else:
                # we remove suppliers with a negative growth
                market_shares.loc[{"region": region}].values[
                    market_shares.loc[{"region": region}].values <= 0
                ] = 0
                # we keep suppliers with a positive growth
                market_shares.loc[{"region": region}].values[
                    market_shares.loc[{"region": region}].values > 0
                ] = 1
                # and use their production volume as their indicator
                market_shares.loc[{"region": region}] *= data_start.values[:, None]

        market_shares.loc[{"region": region}] = market_shares.loc[
            {"region": region}
        ].round(3)

        # we remove NaNs and np.inf
        market_shares.loc[{"region": region}].values[
            market_shares.loc[{"region": region}].values == np.inf
        ] = 0
        market_shares.loc[{"region": region}] = market_shares.loc[
            {"region": region}
        ].fillna(0)

        summary.append(
            (
                region,
                measurement,
                foresight,
                duration,
                avg_start,
                avg_end,
                np.round(avg_cap_repl_rate, 2),
                np.round(volume_change, 2),
            )
        )

        # market decreasing faster than the average capital renewal rate
        # in this case, the idea is that oldest/non-competitive technologies
        # are likely to supply by increasing their lifetime
        # as the market does not justify additional capacity installation
        if (not capital_repl_rate and volume_change < 0) or (
            capital_repl_rate and volume_change < avg_cap_repl_rate
        ):
            # we remove suppliers with a positive growth
            market_shares.loc[{"region": region}].values[
                market_shares.loc[{"region": region}].values > 0
            ] = 0
            # we reverse the sign of negative growth suppliers
            market_shares.loc[{"region": region}] *= -1
            market_shares.loc[{"region": region}] /= market_shares.loc[
                {"region": region}
            ].sum(dim="variables")

        # increasing market or
        # market decreasing slowlier than the
        # capital renewal rate
        else:
            # we remove suppliers with a negative growth
            market_shares.loc[{"region": region}].values[
                market_shares.loc[{"region": region}].values < 0
            ] = 0
            market_shares.loc[{"region": region}] /= market_shares.loc[
                {"region": region}
            ].sum(dim="variables")

        if market_shares.sel(region=region).sum(dim="variables").values == 0:
            # in such case, we use the average shares, minus the constrained suppliers

            print(f"WARNING: All market shares for {region} are zero for {sector}. ")
            print("Using average shares for unconstrained suppliers.")

            market_shares.loc[{"region": region}] = shares

    # print a summary of the results
    print()
    print(f"Summary of the {sector} marginal market mixes:")
    table = PrettyTable(
        [
            "Region",
            "Method",
            "Foresight",
            "Duration",
            "Start",
            "End",
            "Cap repl.",
            "Vol ch.",
        ]
    )
    for row in summary:
        table.add_row(row)

    table._max_width = {
        "Region": 10,
        "Method": 10,
        "Foresight": 10,
        "Duration": 10,
        "Start": 10,
        "End": 10,
        "Cap repl.": 10,
        "Vol ch.": 10,
    }
    print(table)

    return market_shares
