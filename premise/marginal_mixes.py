"""
Calculate the marginal mix of a market in different ways.
Based on the method described in:
https://chemrxiv.org/engage/chemrxiv/article-details/63ee10cdfcfb27a31fe227df

"""

import warnings
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


def get_list_contrained_suppliers() -> list[str]:
    """
    Get the exact supplier names excluded from consequential marginal mixes.
    :return: a list of constrained suppliers
    :rtype: list
    """
    with open(CONSTRAINED_SUPPLIERS, "r", encoding="utf-8") as stream:
        suppliers = yaml.safe_load(stream)

    if not isinstance(suppliers, list) or not all(
        isinstance(supplier, str) for supplier in suppliers
    ):
        raise TypeError(
            "constrained_suppliers.yaml must contain a YAML list of supplier names."
        )

    return suppliers


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


def _as_nonnegative_integer(name: str, value) -> int:
    """Return a consequential time argument as a non-negative integer."""

    if isinstance(value, (bool, np.bool_)):
        return int(value)
    if not isinstance(value, (int, np.integer)):
        raise TypeError(f"`{name}` must be an integer number of years.")
    if value < 0:
        raise ValueError(f"`{name}` cannot be negative.")
    return int(value)


def _as_boolean(name: str, value) -> bool:
    """Validate a consequential Boolean argument."""

    if not isinstance(value, (bool, np.bool_)):
        raise TypeError(f"`{name}` must be True or False.")
    return bool(value)


def _validate_consequential_args(args: dict) -> tuple:
    """Validate and normalize consequential modelling arguments."""

    range_time = _as_nonnegative_integer("range time", args.get("range time", 2))
    duration = _as_nonnegative_integer("duration", args.get("duration", 0))
    foresight = _as_boolean("foresight", args.get("foresight", False))
    individual_lead_times = _as_boolean("lead time", args.get("lead time", False))
    capital_repl_rate = _as_boolean(
        "capital replacement rate", args.get("capital replacement rate", True)
    )
    measurement = _as_nonnegative_integer("measurement", args.get("measurement", 0))
    weighted_slope_start = args.get("weighted slope start", 0.75)
    weighted_slope_end = args.get("weighted slope end", 1.0)

    if range_time and duration:
        raise ValueError(
            "`range time` and `duration` cannot both be non-zero. Use `range time` "
            "for a short demand change or `duration` for a long demand change."
        )
    if duration in (1, 2):
        raise ValueError(
            "`duration` must be 0 or at least 3 years. Use `range time` for a "
            "change lasting less than 3 years."
        )
    if measurement not in range(6):
        raise ValueError("`measurement` must be an integer from 0 to 5.")
    if measurement == 4 and individual_lead_times:
        raise ValueError(
            "Measurement method 4 requires a common market interval and cannot "
            "be combined with technology-specific lead times. Set `lead time` "
            "to False."
        )
    if not isinstance(weighted_slope_start, (int, float, np.number)) or not isinstance(
        weighted_slope_end, (int, float, np.number)
    ):
        raise TypeError("Weighted-slope bounds must be numeric fractions.")

    weighted_slope_start = float(weighted_slope_start)
    weighted_slope_end = float(weighted_slope_end)
    if not 0 <= weighted_slope_start < weighted_slope_end <= 1:
        raise ValueError("Weighted-slope bounds must satisfy 0 <= start < end <= 1.")

    return (
        range_time,
        duration,
        foresight,
        individual_lead_times,
        capital_repl_rate,
        measurement,
        weighted_slope_start,
        weighted_slope_end,
    )


def _get_time_parameters(
    year: int,
    range_time: int,
    duration: int,
    foresight: bool,
    individual_lead_times: bool,
    lead_times: np.ndarray,
    shares: xr.DataArray,
) -> dict:
    """Return supplier-specific and market-average observation intervals."""

    avg_lead_time = fetch_avg_leadtime(lead_times, shares)

    if range_time:
        average_centre = year if foresight else year + avg_lead_time
        if individual_lead_times and not foresight:
            centre = year + lead_times
        else:
            centre = average_centre
        start = centre - range_time
        end = centre + range_time
        avg_start = average_centre - range_time
        avg_end = average_centre + range_time
    elif duration:
        average_start = year if foresight else year + avg_lead_time
        if individual_lead_times and not foresight:
            start = year + lead_times
            end = start + duration
        else:
            start = average_start
            end = average_start + duration
        avg_start = average_start
        avg_end = average_start + duration
    else:
        # Legacy ecoinvent-style fallback: use lead time itself as the interval.
        if foresight:
            start = year - lead_times if individual_lead_times else year - avg_lead_time
            end = np.full_like(lead_times, year) if individual_lead_times else year
            avg_start = year - avg_lead_time
            avg_end = year
        else:
            start = np.full_like(lead_times, year) if individual_lead_times else year
            end = year + lead_times if individual_lead_times else year + avg_lead_time
            avg_start = year
            avg_end = year + avg_lead_time

    return {
        "start": start,
        "end": end,
        "start_avg": avg_start,
        "end_avg": avg_end,
        "average lead time": avg_lead_time,
    }


def _nearest_available_year(
    value, available_years: np.ndarray, parameter_name: str = "year"
):
    """Map one or more requested years to the nearest available IAM year."""

    available_years = np.asarray(available_years)
    requested = np.asarray(value)
    indices = np.abs(requested[..., None] - available_years).argmin(axis=-1)
    nearest = available_years[indices]
    if not np.array_equal(requested, nearest):
        warnings.warn(
            f"Requested {parameter_name} {requested.tolist()} is outside the "
            f"available IAM years and was mapped to {np.asarray(nearest).tolist()}.",
            RuntimeWarning,
            stacklevel=2,
        )
    return nearest.item() if requested.ndim == 0 else nearest


def _select_technology_values(data: xr.DataArray, years) -> np.ndarray:
    """Select or interpolate one value per technology for scalar or array years."""

    requested = np.asarray(years, dtype=float)
    available_years = np.asarray(data.coords["year"].values, dtype=float)
    values = np.asarray(data.values, dtype=float)

    if requested.ndim == 0:
        return np.asarray(
            [np.interp(float(requested), available_years, row) for row in values]
        )
    if requested.shape != (values.shape[0],):
        raise ValueError("Expected one observation year per technology.")
    return np.asarray(
        [
            np.interp(requested_year, available_years, row)
            for requested_year, row in zip(requested, values)
        ]
    )


def consequential_method(
    data: xr.DataArray, year: int, args: dict, sector: str
) -> xr.DataArray:
    """
    Used for consequential modeling only.
    Returns marginal market mixes
    according to the chosen method.

    If range time and duration are both zero, lead time itself is used as the
    interval, as in the ecoinvent v3.4 electricity method.

    ``foresight=False`` selects myopic behaviour and ``True`` selects perfect
    foresight. ``lead time=False`` uses a production-weighted market-average
    lead time; ``True`` uses an individual interval for each technology.
    Measurement method 4 requires a common interval and therefore only supports
    the market-average lead-time mode.

    :param data: IAM data
    :param year: year to calculate the mix for
    :param args: arguments for the method
    :param sector: sector to calculate the mix for

    :return: marginal market mixes
    """

    args = args or {}

    (
        range_time,
        duration,
        foresight,
        individual_lead_times,
        capital_repl_rate,
        measurement,
        weighted_slope_start,
        weighted_slope_end,
    ) = _validate_consequential_args(args)

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

        params = _get_time_parameters(
            year=year,
            range_time=range_time,
            duration=duration,
            foresight=foresight,
            individual_lead_times=individual_lead_times,
            lead_times=leadtime,
            shares=shares,
        )
        start = params["start"]
        end = params["end"]
        avg_start = params["start_avg"]
        avg_end = params["end_avg"]

        # Now that we do know the start year of the time interval,
        # we can use this to "more accurately" calculate the current shares

        available_years = data_full.coords["year"].values
        avg_start = _nearest_available_year(
            avg_start, available_years, "average start year"
        )
        avg_end = _nearest_available_year(avg_end, available_years, "average end year")
        start = _nearest_available_year(start, available_years, "start year")
        end = _nearest_available_year(end, available_years, "end year")

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
            region_data = data_full.sel(region=region)
            data_start = _select_technology_values(region_data, start)
            data_end = _select_technology_values(region_data, end)

            market_shares.loc[{"region": region}] = (
                (data_end - data_start) / (end - start)
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
            region_data = data_full.sel(region=region)
            number_technologies = region_data.sizes["variables"]
            start_by_technology = (
                start
                if isinstance(start, np.ndarray)
                else np.full(number_technologies, start)
            )
            end_by_technology = (
                end
                if isinstance(end, np.ndarray)
                else np.full(number_technologies, end)
            )
            data_start = _select_technology_values(region_data, start_by_technology)
            data_end = _select_technology_values(region_data, end_by_technology)

            mask_end = region_data.year.values[None, :] <= end_by_technology[:, None]
            mask_start = (
                region_data.year.values[None, :] >= start_by_technology[:, None]
            )
            mask = mask_end & mask_start
            coeff = np.where(mask, region_data.values, 0).sum(axis=1)
            n = end_by_technology - start_by_technology

            total_area = 0.5 * (2 * coeff - data_end - data_start)

            baseline_area = data_start * n

            market_shares.loc[{"region": region}] = ((total_area - baseline_area) / n)[
                :, None
            ]

            if capital_repl_rate:
                # this bit differs from above
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = (
                    fetch_capital_replacement_rates(
                        lifetime, data_full.sel(region=region, year=avg_start)
                    )
                    * (n**2)
                    * 0.5
                )

                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[{"region": region}] -= cap_repl_rate[:, None]

        if measurement == 3:
            region_data = data_full.sel(region=region)
            data_start = _select_technology_values(region_data, start)
            data_end = _select_technology_values(region_data, end)

            slope = (data_end - data_start) / (end - start)

            short_slope_start = start + (end - start) * weighted_slope_start
            short_slope_end = start + (end - start) * weighted_slope_end
            data_short_slope_start = _select_technology_values(
                region_data, short_slope_start
            )
            data_short_slope_end = _select_technology_values(
                region_data, short_slope_end
            )

            short_slope = (data_short_slope_end - data_short_slope_start) / (
                short_slope_end - short_slope_start
            )

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
            region_data = data_full.sel(region=region)
            data_start = _select_technology_values(region_data, start)
            data_end = _select_technology_values(region_data, end)

            market_shares.loc[{"region": region}] = (
                (data_end - data_start) / (end - start)
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
                market_shares.loc[{"region": region}] *= data_start[:, None]
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
                market_shares.loc[{"region": region}] *= data_start[:, None]

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
                "individual" if individual_lead_times else "average",
                params["average lead time"],
                range_time,
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
            "Lead time",
            "L avg",
            "Range",
            "Duration",
            "Avg start",
            "Avg end",
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
        "Lead time": 10,
        "L avg": 10,
        "Range": 10,
        "Duration": 10,
        "Avg start": 10,
        "Avg end": 10,
        "Cap repl.": 10,
        "Vol ch.": 10,
    }
    print(table)

    return market_shares
