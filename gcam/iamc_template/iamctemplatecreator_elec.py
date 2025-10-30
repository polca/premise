
import pandas as pd
import yaml
from pathlib import Path

def run_elec(scenario_name):

    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    bridge_file = pd.read_excel("gcamtechnologies_elec.xlsx", index_col = 'tech_var')
    # scenario_name="SSP2 RCP26"
    # scenario_name=scenario
    iamc = pd.DataFrame()

    year = ['2005','2010','2015','2020','2025','2030','2035','2040','2045','2050','2055','2060','2065','2070','2075','2080','2085','2090','2095','2100']
    # bridge_file = pd.read_excel("./gcamtechnologies.xlsx", index_col = 'tech_var')



    elec_generation = pd.read_csv(DATA_DIR / 'elec gen by gen technology.csv')
    elec_generation['generation'] = elec_generation['value']
    elec_input = pd.read_csv(DATA_DIR / 'elec energy input by generation technology.csv')
    elec_input['energyinput'] = elec_input['value']


    elec_generation = elec_generation[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'output', 'Year',
            'generation']]


    rooftoppvcorrection=elec_generation[elec_generation['technology'] == 'rooftop_pv']
    rooftoppvcorrection['output'] = 'electricity'
    elec_generation = pd.concat([elec_generation,rooftoppvcorrection])

    elec_generation = elec_generation[elec_generation['output'] == 'electricity']
    elec_generation_grouped = elec_generation.groupby(['Units', 'scenario', 'region', 'technology','Year'])['generation'].agg('sum').reset_index()
    elec_total = elec_generation_grouped 


    elec_input_cl = elec_input[~elec_input['input'].str.contains('elec_')]

    technologies_list_for_editing=['solar','wind']
    df1 = pd.DataFrame()
    for tech in technologies_list_for_editing:
         df = elec_input_cl[elec_input_cl['subsector'] == tech] 
         df['subsector'] = df['technology']
         df1 = pd.concat([df1,df])

    elec_input_cl1 = elec_input_cl[~(elec_input_cl['subsector'] == 'solar') & ~(elec_input_cl['subsector'] == 'wind')]
    elec_input_cl2 = pd.concat([elec_input_cl1,df1])
    elec_input_cl2 = elec_input_cl2[['Units', 'scenario', 'region', 'sector', 'subsector','input', 'Year', 'value', 'energyinput']]

    elec_input_grouped = elec_input_cl2.groupby(['Units', 'scenario', 'region', 'subsector','Year'])['energyinput'].agg('sum').reset_index()
    elec_input_grouped['technology'] = elec_input_grouped['subsector']

    elec_total = elec_generation_grouped.merge(elec_input_grouped, on = ['Units', 'scenario', 'region', 'technology','Year'])

    # add world region by aggragating all data
    df = elec_total.copy()
    df = df.groupby(['Units', 'scenario', 'subsector', 'technology','Year'])[['generation','energyinput']].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units', 'scenario', 'region', 'subsector', 'technology','Year','generation','energyinput']]
    # print(df)
    elec_total = pd.concat([elec_total,df])

    # calculate efficiency
    elec_total['efficiency'] = elec_total['generation']/elec_total['energyinput']

    ### print secondary energy
    # add world region by aggragating all data
    df = elec_generation_grouped.copy()
    df = df.groupby(['Units', 'scenario', 'technology','Year'])['generation'].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units', 'scenario', 'region', 'technology','Year','generation']]
    # print(df)
    elec_generation_grouped = pd.concat([elec_generation_grouped,df])

    # region = ['USA']
    region = elec_generation_grouped['region'].unique().tolist()
    tech = bridge_file['gcam_tech']
    elec_generation_grouped ['Year'] = elec_generation_grouped['Year'].astype('str')
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
                 variable.append("Secondary Energy|Electricity|" + t)
                 unit.append("EJ")
                 df = elec_generation_grouped[(elec_generation_grouped['technology'] == t) & (elec_generation_grouped ['Year'] == str(y)) & (elec_generation_grouped ['region'] == reg)].reset_index()
                 years.append(y)
                 if len(df) == 1:
                    value.append(df['generation'][0])
                 elif len(df) == 0:
                    # print(reg,t,y)
                    # print('no technologies found')
                    value.append("")
                 else:
                    print(reg,t)
                    print('multiple efficiences. Issues check')


    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
    iamc2 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    print("Secondary Energy finished. Number of row: ",iamc2.shape[0])
    # iamc2.to_excel('iamc_template_gcam.xlsx', index = False)



    ### Print efficiency
    # region = ['USA']
    region = elec_total['region'].unique().tolist()
    tech = bridge_file['gcam_tech']
    elec_total['Year'] = elec_total['Year'].astype('str')
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
                 variable.append("Efficiency|Electricity|" + t)
                 unit.append("")
                 df = elec_total[(elec_total['technology'] == t) & (elec_total['Year'] == str(y)) & (elec_total['region'] == reg)].reset_index()
                 years.append(y)
                 if len(df) == 1:
                    value.append(df['efficiency'][0])
                 elif len(df) == 0:
                    # print(reg,t,y)
                    # print('no technologies found')
                    value.append("")
                 else:
                    print(reg,t)
                    print('multiple efficiences. Issues check')

    iamc = pd.DataFrame()
    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
    iamc3 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    #iamc2.to_excel('iamc_template_gcam.xlsx', index = False)

    print("Efficiency finished. Number of row: ",iamc3.shape[0])
    iamc = pd.concat([iamc2,iamc3])
    # iamc.to_excel('iamc_template_gcam.xlsx', index = False)


    ### Print non CO2 emission
    elec_emissions = pd.read_csv(DATA_DIR /'electricity nonCO2 emissions by technology.csv')
    elec_emissions = elec_emissions[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'GHG', 'Year','value']]
    elec_emissions = elec_emissions[elec_emissions['GHG'] != 'CO2']
    elec_emissions_grouped = elec_emissions.groupby(['Units', 'scenario', 'region', 'technology','Year','GHG'])['value'].agg('sum').reset_index()
    # add world region by aggragating all data
    df = elec_emissions_grouped.copy()
    df = df.groupby(['Units', 'scenario', 'technology','Year','GHG'])['value'].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units', 'scenario', 'region', 'technology','Year','GHG','value']]
    # print(df)
    elec_total= pd.concat([elec_emissions_grouped,df])

    emissions = list(pd.unique(elec_total['GHG']))
    # region = ['USA']
    region = elec_total['region'].unique().tolist()
    tech = bridge_file['gcam_tech']
    elec_total['Year'] = elec_total['Year'].astype('str')
    model = []
    scenario = []
    regions = []
    variable = []
    unit = []
    value = []
    years = []
    iamc= pd.DataFrame()
    for em in emissions:
        
        for reg in region:
        
            for t in tech:
        
                for y in year:
        
                     model.append("GCAM")
                     scenario.append(scenario_name)
                     regions.append(reg)
                     variable.append("Emissions|" + em+"|"+t+"|Electricity Production")
                     unit.append("Tg")
                     df = elec_total[(elec_total['technology'] == t) & (elec_total['Year'] == str(y)) & (elec_total['region'] == reg) & (elec_total['GHG'] == em)].reset_index()
                     years.append(y)
                     if len(df) == 1:
                        value.append(df['value'][0])
                     elif len(df) == 0:
                        # print(reg,t,y)
                        # print('no technologies found')
                        value.append("")
                     else:
                        print(reg,t)
                        print('multiple values. Issues check')
        
        
    iamc = pd.DataFrame()
    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
        
        
        
    iamc4 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    #iamc2.to_excel('iamc_template_gcam.xlsx', index = False)

    print("non CO2 emission finished. Number of row: ",iamc4.shape[0])
    iamc = pd.concat([iamc2,iamc3,iamc4])
    # iamc.to_excel('iamc_template_gcam.xlsx', index = False)

    ### print CO2 emission
    elec_emissions = pd.read_csv(DATA_DIR /'electricity nonCO2 emissions by technology.csv')
    elec_emissions = elec_emissions[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'GHG', 'Year','value']]
    elec_emissions = elec_emissions[elec_emissions['GHG'] == 'CO2']
    elec_emissions['technology'] = elec_emissions['subsector']
    elec_emissions_grouped = elec_emissions.groupby(['Units', 'scenario', 'region', 'technology','Year','GHG'])['value'].agg('sum').reset_index()
    # add world region by aggragating all data
    df = elec_emissions_grouped.copy()
    df = df.groupby(['Units', 'scenario', 'technology','Year','GHG'])['value'].agg('sum').reset_index()
    df['region'] = 'World'
    df = df[['Units', 'scenario', 'region', 'technology','Year','GHG','value']]
    # print(df)
    elec_total= pd.concat([elec_emissions_grouped,df])

    emissions = list(pd.unique(elec_total['GHG']))
    # region = ['USA']
    region = elec_total['region'].unique().tolist()
    tech = bridge_file['gcam_tech']
    elec_total['Year'] = elec_total['Year'].astype('str')
    model = []
    scenario = []
    regions = []
    variable = []
    unit = []
    value = []
    years = []
    iamc= pd.DataFrame()
    for em in emissions:
        
        for reg in region:
        
            for t in tech:
        
                for y in year:
        
                     model.append("GCAM")
                     scenario.append(scenario_name)
                     regions.append(reg)
                     variable.append("Emissions|" + em+"|"+t+"|Electricity Production")
                     unit.append("MTC")
                     df = elec_total[(elec_total['technology'] == t) & (elec_total['Year'] == str(y)) & (elec_total['region'] == reg) & (elec_total['GHG'] == em)].reset_index()
                     years.append(y)
                     if len(df) == 1:
                        value.append(df['value'][0])
                     elif len(df) == 0:
                        # print(reg,t,y)
                        # print('no technologies found')
                        value.append("")
                     else:
                        print(reg,t)
                        print('multiple values. Issues check')
        
        
    iamc = pd.DataFrame()
    iamc['Region'] = regions
    iamc['Model'] = model
    iamc['Variable'] = variable
    iamc['Unit'] = unit
    iamc['Years'] = years
    iamc['Value'] = value
    iamc['Scenario'] = scenario
        
        
        
    iamc5 = iamc.pivot(values = 'Value', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Years').reset_index()
    #iamc2.to_excel('iamc_template_gcam.xlsx', index = False)

    print("CO2 emission finished. Number of row: ",iamc5.shape[0])
    iamc = pd.concat([iamc2,iamc3,iamc4,iamc5])
    iamc.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_elec_world.xlsx', index = False)
