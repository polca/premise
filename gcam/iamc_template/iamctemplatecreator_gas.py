
import pandas as pd
import numpy as np
# import yaml

### gas production from GCAM




def run_gas(scenario):
		# load hydrgen production file
		gas_generation = pd.read_csv('../GCAM_queryresults_'+scenario+'/gas production by tech.csv')
		# load gas feedstock and energy input file
		gas_efficiency = pd.read_csv('../GCAM_queryresults_'+scenario+'/gas inputs by tech (energy and feedstocks).csv')

		gas_generation['generation'] = gas_generation['value']

		# process gas generation data
		gas_generation = gas_generation[['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'Year','generation']]
		matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')
		gas_generation2 = gas_generation.merge(matching_bridge, on = 'technology')
		gas_generation2['Scenario'] = scenario
		gas_generation2['Region'] = gas_generation2['region']
		gas_generation2['Model'] = "GCAM"
		gas_generation2['Unit'] = "EJ"
		gas_generation2['Variable'] = 'Secondary Energy|Production|' + gas_generation2['fuels renamed']

		#group by technology name
		gas_generation3 = gas_generation2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['generation'].agg('sum')
		gas_generation3 = gas_generation3.reset_index()

		# Add world region by aggregating all generation data
		gas_generation4 = gas_generation3.groupby(['Scenario','Model','Variable','Unit','Year'])['generation'].agg('sum').reset_index()

		gas_generation4['Region'] = 'World'
		gas_generation4 = gas_generation4[['Scenario','Region','Model','Variable','Unit','Year','generation']]
		# print(df)
		gas_generation3 = pd.concat([gas_generation3,gas_generation4])

		iamc = gas_generation3[['Scenario', 'Region','Model','Variable','Unit','Year','generation']]
		iamc3 = iamc.pivot(values = 'generation', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()
		# iamc3.to_excel('./iamc_template/base/iamc_template_gcam_gas_world.xlsx', index = False)



		#gas_efficiency = gas_efficiency.loc[gas_efficiency['Units']=='EJ'] # exclude water
		gas_efficiency = gas_efficiency.groupby(['scenario', 'region', 'technology', 'Year'])['value'].agg('sum')
		gas_efficiency = gas_efficiency.reset_index()

		# merge generation and input and calculate efficiency
		gas_efficiency4 = gas_efficiency.merge(gas_generation, on = ['scenario', 'region', 'technology', 'Year'])
		gas_efficiency4['efficiency'] = gas_efficiency4['generation']/gas_efficiency4['value']

		matching_bridge = pd.read_csv('gcam_technology_to_fuel_name_bridge.csv')
		gas_efficiency2 = gas_efficiency4.merge(matching_bridge, on = 'technology')
		gas_efficiency2['Scenario'] = scenario
		gas_efficiency2['Region'] = gas_efficiency2['region']
		gas_efficiency2['Model'] = "GCAM"
		gas_efficiency2['Unit'] = ""
		gas_efficiency2['Variable'] = 'Efficiency|' + gas_efficiency2['fuels renamed']


		gas_efficiency3 = gas_efficiency2.groupby(['Scenario','Region','Model','Variable','Unit','Year'])['efficiency'].agg('mean')
		gas_efficiency3 = gas_efficiency3.reset_index()

		# Add world region by aggregating all input data
		gas_efficiency4 = gas_efficiency3.groupby(['Scenario','Model','Variable','Unit','Year'])['efficiency'].agg('mean').reset_index()
		gas_efficiency4['Region'] = 'World'
		gas_efficiency4 = gas_efficiency4[['Scenario','Region','Model','Variable','Unit','Year','efficiency']]
		gas_efficiency3 = pd.concat([gas_efficiency3,gas_efficiency4])
		# print(gas_efficiency.columns)

		iamc = gas_efficiency3[['Scenario', 'Region','Model','Variable','Unit','Year','efficiency']]
		iamc4 = iamc.pivot(values = 'efficiency', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()

		iamc5 = pd.concat([iamc3,iamc4])

		iamc5.to_excel('./iamc_template/'+scenario+'/iamc_template_gcam_gas_world.xlsx', index = False)

# run('SSP2 Base')
# run('SSP2 RCP26')
