
import pandas as pd
import numpy as np
# import yaml

### refined liquids production from GCAM

def run_fuel(scenario):
        # load data file
        refined_liquids_generation = pd.read_csv('../GCAM_queryresults_'+scenario+'/refined liquids production by tech.csv')
        refined_liquids_generation['generation'] = refined_liquids_generation['value']


        ### process refined liquids generation data
        refined_liquids_generation = refined_liquids_generation[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'output', 'Year','generation']]

        matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')

        refined_liquids_generation2 = refined_liquids_generation.merge(matching_bridge, on = 'technology')
        refined_liquids_generation2['Scenario'] = scenario

        refined_liquids_generation2['Region'] = refined_liquids_generation2['region']
        refined_liquids_generation2['Model'] = "GCAM"
        refined_liquids_generation2['Unit'] = "EJ"
        refined_liquids_generation2['Variable'] = 'Secondary Energy|Production|' + refined_liquids_generation2['fuels renamed']

        refined_liquids_generation3 = refined_liquids_generation2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['generation'].agg('sum')
        refined_liquids_generation3 = refined_liquids_generation3.reset_index()

        # Add world region by aggregating all generation data
        refined_liquids_generation4 = refined_liquids_generation3.groupby(['Scenario','Model','Variable','Unit','Year'])['generation'].agg('sum').reset_index()
        refined_liquids_generation4['Region'] = 'World'
        refined_liquids_generation4 = refined_liquids_generation4[['Scenario','Region','Model','Variable','Unit','Year','generation']]
        # print(df)
        refined_liquids_generation3 = pd.concat([refined_liquids_generation3,refined_liquids_generation4])

        iamc = refined_liquids_generation3[['Scenario', 'Region','Model','Variable','Unit','Year','generation']]
        iamc3 = iamc.pivot(values = 'generation', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()
        # iamc3.to_excel('iamc_template_gcam_fuel.xlsx', index = False)


        # load data file
        refined_liquids_efficiency = pd.read_csv('../GCAM_queryresults_'+scenario+'/refinery inputs by tech (energy and feedstocks).csv')
        refined_liquids_efficiency = refined_liquids_efficiency.groupby(['scenario', 'region', 'technology', 'Year'])['value'].agg('sum')
        refined_liquids_efficiency = refined_liquids_efficiency.reset_index()


        refined_liquids_efficiency4 = refined_liquids_efficiency.merge(refined_liquids_generation, on = ['scenario', 'region', 'technology', 'Year'])
        refined_liquids_efficiency4['efficiency'] = refined_liquids_efficiency4['generation']/refined_liquids_efficiency4['value']

        matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')
        refined_liquids_efficiency2 = refined_liquids_efficiency4.merge(matching_bridge, on = 'technology')
        refined_liquids_efficiency2['Scenario'] = scenario

        refined_liquids_efficiency2['Region'] = refined_liquids_efficiency2['region']
        refined_liquids_efficiency2['Model'] = "GCAM"
        refined_liquids_efficiency2['Unit'] = ""
        refined_liquids_efficiency2['Variable'] = 'Efficiency|' + refined_liquids_efficiency2['fuels renamed']

        refined_liquids_efficiency3 = refined_liquids_efficiency2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['efficiency'].agg('mean')
        refined_liquids_efficiency3 = refined_liquids_efficiency3.reset_index()

        # Add world region by aggregating all input data
        refined_liquids_efficiency4 = refined_liquids_efficiency3.groupby(['Scenario','Model','Variable','Unit','Year'])['efficiency'].agg('mean').reset_index()
        refined_liquids_efficiency4['Region'] = 'World'
        refined_liquids_efficiency4 = refined_liquids_efficiency4[['Scenario','Region','Model','Variable','Unit','Year','efficiency']]
        refined_liquids_efficiency3 = pd.concat([refined_liquids_efficiency3,refined_liquids_efficiency4])


        iamc = refined_liquids_efficiency3[['Scenario', 'Region','Model','Variable','Unit','Year','efficiency']]
        iamc4 = iamc.pivot(values = 'efficiency', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()

        iamc5 = pd.concat([iamc3,iamc4])

        iamc5.to_excel('./iamc_template/'+scenario+'/iamc_template_gcam_fuel_world.xlsx', index = False)


# run('SSP2 Base')
# run('SSP2 RCP26')