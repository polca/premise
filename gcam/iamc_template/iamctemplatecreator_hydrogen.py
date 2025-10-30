
import pandas as pd
import numpy as np
# import yaml





def run_hydrogen(scenario):
    ### hydrogen production from GCAM

    # load hydrgen production file
    hydrogen_generation = pd.read_csv('../GCAM_queryresults_'+scenario+'/hydrogen production by tech.csv')
    hydrogen_generation['generation'] = hydrogen_generation['value']

    # process hydrogen generation data
    hydrogen_generation = hydrogen_generation[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'Year','generation']]
    matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')
    hydrogen_generation2 = hydrogen_generation.merge(matching_bridge, on = 'technology')

    hydrogen_generation2['Scenario'] = scenario

    hydrogen_generation2['Region'] = hydrogen_generation2['region']
    hydrogen_generation2['Model'] = "GCAM"
    hydrogen_generation2['Unit'] = "EJ"
    hydrogen_generation2['Variable'] = 'Secondary Energy|Production|' + hydrogen_generation2['fuels renamed']

    #group by technology name
    hydrogen_generation3 = hydrogen_generation2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['generation'].agg('sum')
    hydrogen_generation3 = hydrogen_generation3.reset_index()

    # Add world region by aggregating all generation data
    hydrogen_generation4 = hydrogen_generation3.groupby(['Scenario','Model','Variable','Unit','Year'])['generation'].agg('sum').reset_index()
    hydrogen_generation4['Region'] = 'World'
    hydrogen_generation4 = hydrogen_generation4[['Scenario','Region','Model','Variable','Unit','Year','generation']]
    # print(df)
    hydrogen_generation3 = pd.concat([hydrogen_generation3,hydrogen_generation4])

    iamc = hydrogen_generation3[['Scenario', 'Region','Model','Variable','Unit','Year','generation']]
    iamc3 = iamc.pivot(values = 'generation', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()
    # iamc3.to_excel('./iamc_template/base/iamc_template_gcam_hydrogen_world.xlsx', index = False)


    # load hydrogen feedstock and energy input file
    hydrogen_efficiency = pd.read_csv('../GCAM_queryresults_'+scenario+'/hydrogen inputs by tech (feedstocks and energy).csv')
    hydrogen_efficiency = hydrogen_efficiency.loc[hydrogen_efficiency['Units']=='EJ'] # exclude water
    hydrogen_efficiency = hydrogen_efficiency.groupby(['scenario', 'region', 'technology', 'Year'])['value'].agg('sum')
    hydrogen_efficiency = hydrogen_efficiency.reset_index()

    # merge generation and input and calculate efficiency
    hydrogen_efficiency4 = hydrogen_efficiency.merge(hydrogen_generation, on = ['scenario', 'region', 'technology', 'Year'])
    hydrogen_efficiency4['efficiency'] = hydrogen_efficiency4['generation']/hydrogen_efficiency4['value']

    matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')
    hydrogen_efficiency2 = hydrogen_efficiency4.merge(matching_bridge, on = 'technology')
    hydrogen_efficiency2['Scenario'] = scenario
    hydrogen_efficiency2['Region'] = hydrogen_efficiency2['region']
    hydrogen_efficiency2['Model'] = "GCAM"
    hydrogen_efficiency2['Unit'] = ""
    hydrogen_efficiency2['Variable'] = 'Efficiency|' + hydrogen_efficiency2['fuels renamed']


    hydrogen_efficiency3 = hydrogen_efficiency2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['efficiency'].agg('mean')
    hydrogen_efficiency3 = hydrogen_efficiency3.reset_index()

    # Add world region by aggregating all input data
    hydrogen_efficiency4 = hydrogen_efficiency3.groupby(['Scenario','Model','Variable','Unit','Year'])['efficiency'].agg('mean').reset_index()
    hydrogen_efficiency4['Region'] = 'World'
    hydrogen_efficiency4 = hydrogen_efficiency4[['Scenario','Region','Model','Variable','Unit','Year','efficiency']]
    hydrogen_efficiency3 = pd.concat([hydrogen_efficiency3,hydrogen_efficiency4])
    # print(hydrogen_efficiency.columns)

    iamc = hydrogen_efficiency3[['Scenario', 'Region','Model','Variable','Unit','Year','efficiency']]
    iamc4 = iamc.pivot(values = 'efficiency', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()

    iamc5 = pd.concat([iamc3,iamc4])

    iamc5.to_excel('./iamc_template/'+scenario+'/iamc_template_gcam_hydrogen_world.xlsx', index = False)

# run("SSP2 RCP26")
# run('SSP2 Base')
