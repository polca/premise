#### generate iamc template components for 
# Carbon Capture|Storage|Direct Air Capture|hightemp DAC NG [Mt CO2/yr]
# Carbon Capture|Storage|Direct Air Capture|hightemp DAC elec [Mt CO2/yr]
# Carbon Capture|Storage|Direct Air Capture|lowtemp DAC heatpump [Mt CO2/yr]
# Energy|Consumption|Direct Air Capture|hightemp DAC NG|Heat [EJ/yr]
# Energy|Consumption|Direct Air Capture|hightemp DAC NG|Electricity [EJ/yr]
# Energy|Consumption|Direct Air Capture|hightemp DAC elec|Electricity [EJ/yr]
# Energy|Consumption|Direct Air Capture|lowtemp DAC heatpump|Electricity [EJ/yr]

import pandas as pd
import numpy as np
from pathlib import Path
# import yaml
def run_scenario(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    ### co2 sequestration data from GCAM

    # load data
    co2seq = pd.read_csv(DATA_DIR / 'CO2 sequestration by tech.csv')
    co2seq = co2seq[co2seq['subsector']=='dac']
    co2seq = co2seq[['Units','scenario', 'region','technology', 'Year', 'value']]

    # add world region by aggragating all data
    df = co2seq.copy()
    df = df.groupby(['Units','scenario','technology','Year'])['value'].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units','scenario', 'region', 'technology','Year', 'value']]
    # print(df)
    co2seq = pd.concat([co2seq,df])

    # generate iamc template
    # scenario_name=scenario
    year = ['2005','2010','2015','2020','2025','2030','2035','2040','2045','2050','2055','2060','2065','2070','2075','2080','2085','2090','2095','2100']
    # region = ['USA']
    region = co2seq['region'].unique().tolist()
    tech = co2seq['technology'].unique().tolist()
    co2seq['Year'] = co2seq['Year'].astype('str')
    model = []
    scenario = []
    regions = []
    variable = []
    unit = []
    value = []
    years = []

    for reg in region:
        for t in tech: 
            for y in year:
                 model.append("GCAM")
                 scenario.append(scenario_name)
                 regions.append(reg)
                 variable.append("Carbon Capture|Storage|Direct Air Capture|" + t)
                 unit.append("Mt CO2/yr")
                 df = co2seq[(co2seq['technology']==t)&(co2seq['Year'] == str(y)) & (co2seq['region'] == reg)].reset_index()
                 years.append(y)
                 if len(df) == 1:
                    value.append(df['value'][0])
                 elif len(df) == 0:
                    # print(reg,y)
                    # print('no technologies found')
                    value.append("")
                 # else:
                    # print(reg)
                    # print('multiple values. Issues check')

    iamc = pd.DataFrame()
    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
    iamc2 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    # iamc = iamc2.copy()
    # iamc2.to_excel('../iamc_template_gcam_cement_world.xlsx', index = False)


    ### heat and electricity use of DACS from GCAM

    # load data
    energy = pd.read_csv(DATA_DIR / 'industry energy by tech and fuel.csv')
    energy = energy[energy['subsector']=='dac']
    energy = energy[['Units','scenario','region','technology','input','Year','value']]

    # add world region by aggragating all data
    df = energy.copy()
    df = df.groupby(['Units','scenario','technology','input','Year'])['value'].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units','scenario','region','technology','input','Year','value']]
    energy = pd.concat([energy,df])

    # generate iamc template
    # scenario_name=scenario
    year = ['2005','2010','2015','2020','2025','2030','2035','2040','2045','2050','2055','2060','2065','2070','2075','2080','2085','2090','2095','2100']
    # region = ['USA']
    region = energy['region'].unique().tolist()
    tech = energy['technology'].unique().tolist()
    input_oldname = ['electricity','process heat dac']
    input_newname = ['Electricity','Heat']
    energy['Year'] = energy['Year'].astype('str')
    model = []
    scenario = []
    regions = []
    variable = []
    unit = []
    value = []
    years = []

    for reg in region:
        for t in tech: 
            for i in range(len(input_newname)):
                for y in year:
                     model.append("GCAM")
                     scenario.append(scenario_name)
                     regions.append(reg)
                     variable.append("Energy|Consumption|Direct Air Capture|" + t + "|" + input_newname[i])
                     unit.append("EJ/yr")
                     df = energy[(energy['technology']==t) & (energy['input'] == input_oldname[i]) & (energy['Year'] == str(y)) & (energy['region'] == reg)].reset_index()
                     years.append(y)
                     if len(df) == 1:
                        value.append(df['value'][0])
                     elif len(df) == 0:
                        # print(reg,y)
                        # print('no technologies found')
                        value.append("")
                     # else:
                        # print(reg)
                        # print('multiple values. Issues check')

    iamc= pd.DataFrame()
    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
    iamc3 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    #iamc2.to_excel('iamc_template_gcam.xlsx', index = False)

    iamc = pd.concat([iamc2,iamc3])
    # iamc.to_excel('../iamc_template_gcam_cement_world.xlsx', index = False)


    iamc.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_dac_world.xlsx', index = False)

# run_scenario("SSP2 RCP26")
run_scenario("SSP2 Base")

