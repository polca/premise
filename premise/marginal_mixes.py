import csv

import numpy as np
import pandas as pd
import xarray as xr
import yaml
import math

from . import DATA_DIR

IAM_LIFETIMES = DATA_DIR / "consequential" / "lifetimes.yaml"
#I've put the numbers I used for the paper in the leadtimes file, but there were a lot less technologies there, so the current file has a lot of placeholder values at the moment. I will update this later to get more accurate values
IAM_LIFETIMES = DATA_DIR / "consequential" / "leadtimes.yaml"

def get_lifetime(list_tech):
    """
    Fetch lifetime values for different technologies from a .yaml file.
    :param list_tech: technology labels to find lifetime values for.
    :type list_tech: list
    :return: a numpy array with technology lifetime values
    :rtype: DataArray
    """
    with open(IAM_LIFETIMES, "r") as stream:
        dict_ = yaml.safe_load(stream)

    arr = np.zeros_like(list_tech)

    for i, tech in enumerate(list_tech):
        lifetime = dict_[tech]
        arr[i] = lifetime
    xrarr = xr.DataArray(
        data=arr,
        dims=["variables"],
        coords={
            "variables": list_tech,
        },
    )
    return xrarr

def get_leadtime(list_tech):
    """
    Fetch leadtime values for different technologies from a .yaml file.
    :param list_tech: technology labels to find leadtime values for.
    :type list_tech: list
    :return: a numpy array with technology leadtime values
    :rtype: np.array
    """
    with open("leadtimes.yaml", "r") as stream:
        dict_ = yaml.safe_load(stream)

    arr = np.zeros_like(list_tech)

    for i, tech in enumerate(list_tech):
        leadtime = dict_[tech]
        arr[i] = leadtime

    return arr.astype(float)

def consequential_method(data, year, Range = None, duration = None, foresight = 0, lead_time = 0, CRR = 0, measurement = 0, weighted_slope_start = 0.75, weighted_slope_end = 1):
    """
    Used for consequential modeling only.
    Returns marginal market mixes
    according to the chosen method.
    :param data: IAM data
    :return: marginal market mixes
    """

    """
    If Range and duration are None, then the lead time is taken as the time interval (just as with ecoinvent v.3.4)
    foresight: 0 = myopic, 1 = perfect foresight 
    lead time: 0 = market average lead time is taken for all technologies, 1 = individual lead time for each technology
    CRR: 0 = horizontal baseline is used, 1 = capital replacement rate is used as baseline
    measurement: 0 = slope, 1 = linear regression, 2 = area under the curve, 3 = weighted slope, 4 = time interval is split in individual years and measured
    weighted_slope_start and end: is needed for measurement method 3, the number notes where the short slope starts and ends and is given as the fraction of the total time interval
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

    #as the time interval can be different for each technology if the individual lead time is used, I use DataArrays to store the values (= start and end)
    start = xr.DataArray(
        np.zeros(tuple(shape)),
        dims=["region", "variables", "year"],
        coords={
            "region": data.coords["region"],
            "variables": data.variables,
            "year": [year],
        },
    )
    
    end = xr.DataArray(
        np.zeros(tuple(shape)),
        dims=["region", "variables", "year"],
        coords={
            "region": data.coords["region"],
            "variables": data.variables,
            "year": [year],
        },
    )

    #Since there can be so many different start and end values, I interpolated the entire data of the IAM instead of doing it each time over
    minimum = min(data.year.values)
    maximum = max(data.year.values)  
    Years = list(range(minimum,maximum + 1))
    data_full = data.interp(year=Years) 

    for region in data.coords["region"].values:

        #I don't yet know the exact start year of the time interval, so as an approximation I use for current_shares the start year of the change
        current_shares1 = data_full.sel(region=region, year=year) / data_full.sel(
            region=region, year=year
        ).sum(dim="variables")
        leadtime = get_leadtime(current_shares1.variables.values)
        avg_leadtime = np.sum(current_shares1.values * leadtime).round()

        #I put this in because my datafile had no values for a region, if this is not a problem with the data that is gonna be used, this can be removed
        if math.isnan(avg_leadtime) is True:
            avg_leadtime = 5

        #we first need to define the time interval
        #this depends on: year, Range, duration, foresight and lead_time
        if Range is None and duration is None and foresight == 0 and lead_time == 0:
            start.loc[dict(region=region, year=year)] += year
            end.loc[dict(region=region, year=year)] += year + avg_leadtime
            avg_start = year
            avg_end = year + avg_leadtime
            
        if Range is None and duration is None and foresight == 1 and lead_time == 1:   
            start.loc[dict(region=region, year=year)] += year - avg_leadtime
            end.loc[dict(region=region, year=year)] += year
            avg_start = year- avg_leadtime
            avg_end = year
            
        if Range is None and duration is None and foresight == 0 and lead_time == 1:   
            start.loc[dict(region=region, year=year)] += year
            end.loc[dict(region=region, year=year)] += year + leadtime
            avg_start = year
            avg_end = year + avg_leadtime
            
        if Range is None and duration is None and foresight == 1 and lead_time == 1:   
            start.loc[dict(region=region, year=year)] += year - leadtime
            end.loc[dict(region=region, year=year)] += year
            avg_start = year - avg_leadtime
            avg_end = year
            
        if Range is not None and duration is None and foresight == 0 and lead_time == 0:
            start.loc[dict(region=region, year=year)] += year + avg_leadtime - Range
            end.loc[dict(region=region, year=year)] += year + avg_leadtime + Range
            avg_start = year + avg_leadtime - Range
            avg_end = year + avg_leadtime + Range
            
        if Range is not None and duration is None and foresight == 1 and lead_time == 0:
            start.loc[dict(region=region, year=year)] += year - Range
            end.loc[dict(region=region, year=year)] += year + Range
            avg_start = year - Range
            avg_end = year + Range
            
        if Range is not None and duration is None and foresight == 0 and lead_time == 1:
            start.loc[dict(region=region, year=year)] += year + leadtime - Range
            end.loc[dict(region=region, year=year)] += year + leadtime + Range
            avg_start = year + avg_leadtime - Range
            avg_end = year + avg_leadtime +  Range
            
        #this is the same as when lead_time = 0 since lead time is not important now
        if Range is not None and duration is None and foresight == 1 and lead_time == 1:
            start.loc[dict(region=region, year=year)] += year - Range
            end.loc[dict(region=region, year=year)] += year + Range
            avg_start = year - Range
            avg_end = year + Range
            
        #duration gets preference over range if both are given
        if  duration is not None and foresight == 0 and lead_time == 0:
            start.loc[dict(region=region, year=year)] += year + avg_leadtime
            end.loc[dict(region=region, year=year)] += year + avg_leadtime + duration
            avg_start = year + avg_leadtime   
            avg_end = year + avg_leadtime + duration
            
        if duration is not None and foresight == 1 and lead_time == 0:
            start.loc[dict(region=region, year=year)] += year
            end.loc[dict(region=region, year=year)] += year + duration
            avg_start = year
            avg_end = year + duration
            
        if duration is not None and foresight == 0 and lead_time == 1:
            start.loc[dict(region=region, year=year)] += year + leadtime
            end.loc[dict(region=region, year=year)] += year + leadtime + duration
            avg_start = year + avg_leadtime
            avg_end = year + avg_leadtime + duration
            
        #this is the same as when lead_time = 0 since lead time is not important now
        if duration is not None and foresight == 1 and lead_time == 1:
            start.loc[dict(region=region, year=year)] += year
            end.loc[dict(region=region, year=year)] += year + duration
            avg_start = year
            avg_end = year + duration

        #Now that we do know the start year of the time interval, we can use this to "more accurately" calculate the current shares
        current_shares2 = data_full.sel(region=region, year = avg_start) / data_full.sel(
            region=region, year=avg_start
            ).sum(dim="variables")    

        # we first need to calculate the average capital replacement rate of the market
        # which is here defined as the inverse of the production-weighted average lifetime
        lifetime = get_lifetime(current_shares2.variables.values)

        avg_lifetime = float(np.sum(current_shares2.values * lifetime).round())
        #again was put in to deal with Nan values in data
        if avg_lifetime == 0:
            avg_lifetime = float(30)

        avg_cap_repl_rate = -1 / avg_lifetime * data_full.sel(region=region, year = avg_start).sum(dim="variables").values
        #again was put in to deal with Nan values in data
        if math.isnan(avg_cap_repl_rate) is True:
            avg_leadtime = float(0)

        volume_change = ((
            data_full.sel(region=region, year = avg_end)
            .sum(dim="variables")
            - data.sel(region=region, year = avg_start).sum(dim="variables")
            )/(avg_end-avg_start)
        ).values

        # first, we set CHP suppliers to zero
        # as electricity production is not a determining product for CHPs
        tech_to_ignore = ["CHP", "biomethane"]
        data.loc[
            dict(
                variables=[
                    v
                    for v in data_full.variables.values
                    if any(x in v for x in tech_to_ignore)
                ],
                region=region,
            )
        ] = 0

        # second, we measure production growth
        # within the determined time interval
        # for each technology
        #using the selected measuring method and baseline
        if CRR == 0 and measurement == 0:
            #the data is further split for each variable
            #this was the best way I found to get the start and end years for each technology
            for variables in data.coords["variables"]:
                market_shares.loc[dict(region=region, variables = variables)] = ((
                    data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values
                    - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                    )/(end.sel(region=region, variables = variables) - start.sel(region=region, variables = variables))
                )
                
        if CRR == 0 and measurement == 1:
            for variables in data.coords["variables"]:
                a = data_full.sel(region = region, variables = variables).where(data_full.sel(region = region, variables = variables).year >= start.sel(region=region, variables = variables).values)
                b = a.where(a.year <= end.sel(region=region, variables = variables).values)
                c = b.polyfit(dim = "year", deg = 1)
                market_shares.loc[dict(region=region, variables = variables)] = c.polyfit_coefficients[0].values
                
        if CRR == 0 and measurement == 2:
            for variables in data.coords["variables"]:
                a = data_full.sel(region = region, variables = variables).where(data_full.sel(region = region, variables = variables).year >= start.sel(region=region, variables = variables).values)
                b = a.where(a.year <= end.sel(region=region, variables = variables).values)
                c = b.sum(dim = "year").values
                n = end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values
                total_area = 0.5*(2*c-data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values)
                baseline_area = data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values * n
                market_shares.loc[dict(region=region, variables = variables)] = total_area - baseline_area
                
        if CRR == 0 and measurement == 3:
            for variables in data.coords["variables"]:
                slope = ((
                    data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values
                    - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                    )/(end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)
                )

                short_slope_start = (start.sel(region=region, variables = variables).values + (end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)  * weighted_slope_start).round()
                short_slope_end = (start.sel(region=region, variables = variables).values + (end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)  * weighted_slope_end).round()
                short_slope = ((data_full.sel(region=region, variables = variables, year = short_slope_end).values
                    - data_full.sel(region=region, variables = variables, year = short_slope_start).values
                    )/(short_slope_end-short_slope_start))
                if slope == 0:
                    x = 0
                else:
                    x = short_slope/slope
                if x>500:
                    y = 1
                elif x < -500:
                    y = -1
                else:
                    y = 2*(np.exp(-1+x)/(1+np.exp(-1+x))-0.5)
                market_shares.loc[dict(region=region, variables = variables)] = slope + slope*y  
                
        if CRR == 0 and measurement == 4:
            n = end.sel(region=region).isel(variables = [0]).values - start.sel(region=region).isel(variables = [0]).values
            split_years = list(range(int(start.sel(region=region).isel(variables = [0]).values), int(end.sel(region=region).isel(variables = [0]).values)))
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
                for variables in data.coords["variables"]:
                    market_shares_split.loc[dict(region=region, variables = variables)] = (
                        data_full.sel(region=region, variables = variables, year= y+1).values
                        - data_full.sel(region=region, variables = variables, year= y).values
                    )

                # we remove NaNs and np.inf
                market_shares_split.loc[dict(region=region)].values[
                    market_shares_split.loc[dict(region=region)].values == np.inf
                ] = 0
                market_shares_split.loc[dict(region=region)] = market_shares_split.loc[
                    dict(region=region)
                ].fillna(0)
                
                if CRR == 0 and volume_change < 0 or CRR == 1 and volume_change < avg_cap_repl_rate:
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
                    market_shares_split.loc[dict(region=region)] /= market_shares_split.loc[
                        dict(region=region)
                    ].sum(dim="variables")
                market_shares.loc[dict(region=region)] += market_shares_split.loc[dict(region=region)]
            market_shares.loc[dict(region=region)] /= n      

        if CRR == 1 and measurement == 0:   
            for variables in data.coords["variables"]:
                market_shares.loc[dict(region=region, variables = variables)] = ((
                    data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values
                    - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                    )/(end.sel(region=region, variables = variables) - start.sel(region=region, variables = variables))
                )
                
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = -1 / lifetime.sel(variables = variables).values * data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
    
                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[dict(region=region, variables = variables)] -= cap_repl_rate
                
                
        if CRR == 1 and measurement == 1:
            for variables in data.coords["variables"]:
                a = data_full.sel(region = region, variables = variables).where(data_full.sel(region = region, variables = variables).year >= start.sel(region=region, variables = variables).values)
                b = a.where(a.year <= end.sel(region=region, variables = variables).values)
                c = b.polyfit(dim = "year", deg = 1)
                market_shares.loc[dict(region=region, variables = variables)] = c.polyfit_coefficients[0].values
                
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = -1 / lifetime.sel(variables = variables).values * data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
    
                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[dict(region=region, variables = variables)] -= cap_repl_rate
                
        if CRR == 1 and measurement == 2:
            for variables in data.coords["variables"]:
                a = data_full.sel(region = region, variables = variables).where(data_full.sel(region = region, variables = variables).year >= start.sel(region=region, variables = variables).values)
                b = a.where(a.year <= end.sel(region=region, variables = variables).values)
                c = b.sum(dim = "year").values
                n = end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values
                total_area = 0.5*(2*c-data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values)
                baseline_area = data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values * n
                market_shares.loc[dict(region=region, variables = variables)] = total_area - baseline_area
                
                # get the capital replacement rate
                # which is here defined as -1 / lifetime
                cap_repl_rate = -1 / lifetime.sel(variables = variables).values * data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values * ((end.sel(region=region, variables = variables) - start.sel(region=region, variables = variables))^2) *0.5
    
                # subtract the capital replacement (which is negative) rate
                # to the changes market share
                market_shares.loc[dict(region=region, variables = variables)] -= cap_repl_rate
                          
        if CRR == 1 and measurement == 3:
            for variables in data.coords["variables"]:
                slope = ((
                    data_full.sel(region=region, variables = variables, year=end.sel(region=region, variables= variables)).values
                    - data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                    )/(end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)
                )

                short_slope_start = (start.sel(region=region, variables = variables).values + (end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)  * weighted_slope_start).round()
                short_slope_end = (start.sel(region=region, variables = variables).values + (end.sel(region=region, variables = variables).values - start.sel(region=region, variables = variables).values)  * weighted_slope_end).round()
                short_slope = ((data_full.sel(region=region, variables = variables, year = short_slope_end).values
                    - data_full.sel(region=region, variables = variables, year = short_slope_start).values
                    )/(short_slope_end-short_slope_start))
                
                cap_repl_rate = -1 / lifetime.sel(variables = variables).values * data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                slope -= cap_repl_rate
                short_slope -= cap_repl_rate
                
                if slope == 0:
                    x = 0
                else:
                    x = short_slope/slope
                if x>500:
                    y = 1
                elif x < -500:
                    y = -1
                else:
                    y = 2*(np.exp(-1+x)/(1+np.exp(-1+x))-0.5)
                market_shares.loc[dict(region=region, variables = variables)] = slope + slope*y           
                
        if CRR == 1 and measurement == 4:
            n = end.sel(region=region).isel(variables = [0]).values - start.sel(region=region).isel(variables = [0]).values
            split_years = list(range(int(start.sel(region=region).isel(variables = [0]).values), int(end.sel(region=region).isel(variables = [0]).values)))
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
                for variables in data.coords["variables"]:
                    market_shares_split.loc[dict(region=region, variables = variables)] = (
                        data_full.sel(region=region, variables = variables, year= y+1).values
                        - data_full.sel(region=region, variables = variables, year= y).values
                    )
                    cap_repl_rate = -1 / lifetime.sel(variables = variables).values * data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values
                    
                    max_cap_repl_rate = data_full.sel(region=region, variables = variables, year=start.sel(region=region, variables = variables)).values/n
                    
                    if cap_repl_rate > max_cap_repl_rate:
                        cap_repl_rate = max_cap_repl_rate
                    
                    market_shares_split.loc[dict(region=region, variables = variables)] -= cap_repl_rate

                # we remove NaNs and np.inf
                market_shares_split.loc[dict(region=region)].values[
                    market_shares_split.loc[dict(region=region)].values == np.inf
                ] = 0
                market_shares_split.loc[dict(region=region)] = market_shares_split.loc[
                    dict(region=region)
                ].fillna(0)
                
                if CRR == 0 and volume_change < 0 or CRR == 1 and volume_change < avg_cap_repl_rate:
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
                    market_shares_split.loc[dict(region=region)] /= market_shares_split.loc[
                        dict(region=region)
                    ].sum(dim="variables")
                market_shares.loc[dict(region=region)] += market_shares_split.loc[dict(region=region)]
            market_shares.loc[dict(region=region)] /= n                
                
        #market_shares.loc[dict(region=region)] = market_shares.loc[dict(region=region)][:, None]
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
        if CRR == 0 and volume_change < 0 or CRR == 1 and volume_change < avg_cap_repl_rate:
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