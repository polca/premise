import pandas as pd
from pathlib import Path
import os

def run_biofuel(scenario_name):

	DATA_DIR = Path(os.path.join('..', 'queryresults', scenario_name))

	biofuel = pd.read_csv(DATA_DIR / "purpose-grown biomass production.csv")

	temp_df = biofuel.copy()
	temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'Year'])['value'].agg('sum').reset_index()
	biofuel_output = temp_df.copy()
	# add world region by aggregating all data
	temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'Year'])['value'].agg('sum').reset_index()
	temp_df['region'] = 'World'
    # concatenate dfs
	biofuel_output = pd.concat([biofuel_output, temp_df], axis=0)

	residue = pd.read_csv(DATA_DIR / "Residue biomass production.csv")

	temp_df = residue.copy()
	temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'Year'])['value'].agg('sum').reset_index()
	residue_output = temp_df.copy()
	# add world region by aggregating all data
	temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'Year'])['value'].agg('sum').reset_index()
	temp_df['region'] = 'World'
	# concatenate dfs
	residue_output = pd.concat([residue_output, temp_df], axis=0)

	# now we need to format these dfs into IAMC format
	# first, rename existing columns to columns in IAMC format
	biofuel_output = biofuel_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
	residue_output = residue_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

	# replace Scenario with scenario_name
	biofuel_output['Scenario'] = scenario_name
	residue_output['Scenario'] = scenario_name

	# add GCAM as Model
	biofuel_output['Model'] = 'GCAM'
	residue_output['Model'] = 'GCAM'

	# replace Unit column with expected values (EJ/yr, million tkm/yr)
	biofuel_output['Unit'] = 'EJ/yr'
	residue_output['Unit'] = 'EJ/yr'

	# define Variable
	biofuel_output['Variable'] = 'Production|Energy|Biomass|Energy Crops'
	residue_output['Variable'] = 'Production|Energy|Biomass|Residues'

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
	biofuel_output = biofuel_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
	residue_output = residue_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

	# pivot dfs, creating columns for each year

	biofuel_output_pivot = pd.pivot_table(biofuel_output,
										values=['value'],
										index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
										columns=['Year'],
										aggfunc='sum').reset_index()
	residue_output_pivot = pd.pivot_table(residue_output,
										values=['value'],
										index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
										columns=['Year'],
										aggfunc='sum').reset_index()
	out_df = pd.concat([biofuel_output_pivot, residue_output_pivot]).reset_index(drop=True)

	# tidy up dataframe (fix multiple index column names in year columns)
	out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

	# write to file
	out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_biofuel.xlsx'), index=False)
