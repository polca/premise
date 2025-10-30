
import pandas as pd
import numpy as np
# import yaml


def run_cropLUC(scenario):
    ### land use / crop [Ha/GJ] from GCAM

    # load bridge file only to extract ag used for biomass in premise
    ag_bridge = pd.read_excel('gcam_land_use_name_bridge.xlsx')

    ## ag production

    # load ag production file
    ag_production = pd.read_csv('../GCAM_queryresults_'+scenario+'/ag production by tech.csv')
    ag_production = ag_production.merge(ag_bridge, how='left', left_on='technology', right_on = 'GCAM_technology')
    
    ag_production_ej = ag_production[ag_production['Units'] == 'EJ']
    ag_production_mt = ag_production[ag_production['Units'] == 'Mt']

    ag_production_ej['ag_prod_GJ'] = ag_production_ej['value']*(10**9) # unit conversion from EJ to GJ
    ag_production_mt['ag_prod_GJ'] = ag_production_mt['value']*(30) # Gj / metric tonne

    ag_production = pd.concat([ag_production_ej,ag_production_mt])
    ag_production_US = ag_production[ag_production['region'] == "USA"]
    ag_production_US.to_csv('ag_production_usa.csv',index=False)

    # group by Premise name
    ag_production = ag_production.groupby(['region','Year','Premise'])['ag_prod_GJ'].agg('sum')
    ag_production = ag_production.reset_index()

    # Add world region by aggregating all production data
    ag_production2 = ag_production.groupby(['Year','Premise'])['ag_prod_GJ'].agg('sum').reset_index()
    ag_production2['region'] = 'World'
    ag_production2 = ag_production2[['region','Year','Premise','ag_prod_GJ']]
    # print(df)
    ag_production = pd.concat([ag_production,ag_production2])

    ## land use

    # load land allocation file
    land_use = pd.read_csv('../GCAM_queryresults_'+scenario+'/detailed land allocation.csv')
    land_use = land_use.merge(ag_bridge, how='left', left_on='LandLeaf', right_on = 'GCAM_technology')
    land_use['land_use_ha'] = land_use['value']*1000*100 # unit conversion from 1000km2 to ha

    # group by Premise name
    land_use = land_use.groupby(['region','Year','Premise'])['land_use_ha'].agg('sum')
    land_use = land_use.reset_index()

    # Add world region by aggregating all land use data
    land_use2 = land_use.groupby(['Year','Premise'])['land_use_ha'].agg('sum').reset_index()
    land_use2['region'] = 'World'
    land_use2 = land_use2[['region','Year','Premise','land_use_ha']]
    # print(df)
    land_use = pd.concat([land_use,land_use2])

    ## calculate land use / crop [Ha/GJ] and export
    land_use = land_use.merge(ag_production, how='inner', on=['region','Year','Premise'])
    land_use['land_use_ha_per_GJ'] = land_use['land_use_ha']/land_use['ag_prod_GJ']

    land_use['Scenario'] = scenario
    land_use['Region'] = land_use['region']
    land_use['Model'] = "GCAM"
    land_use['Unit'] = 'Ha/GJ' # if cause error in Premise, change it to Ha/GJ-Prim
    land_use['Variable'] = 'Land Use|Average|Biomass|' + land_use['Premise']

    iamc = land_use[['Scenario', 'Region','Model','Variable','Unit','Year','land_use_ha_per_GJ']]
    iamc2 = iamc.pivot(values = 'land_use_ha_per_GJ', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()
    # iamc2.to_excel('./iamc_template/'+scenario+'/iamc_template_gcam_cropLUC_test.xlsx', index = False)

    ### CO2 emission from land use change / crop [kg CO2/GJ] from GCAM

    # LUC emission file
    LUC_emission = pd.read_csv('../GCAM_queryresults_'+scenario+'/detailed LUC emissions.csv')
    LUC_emission_US = LUC_emission[LUC_emission['region'] == "USA"]


    LUC_emission = LUC_emission.merge(ag_bridge, how='left', left_on='LandLeaf', right_on = 'GCAM_technology')
    LUC_emission['LUC_emission_kgCO2'] = LUC_emission['value']*(1000) # unit conversion from Metric ton C to kg CO2
    LUC_emission['LUC_emission_kgCO2'] = -1 * LUC_emission['LUC_emission_kgCO2'] # The values provided are land fluxes or negative emission fluxes. So the signs need to be changed
    
    LUC_emission_US = LUC_emission[LUC_emission['region'] == "USA"]
    LUC_emission_US.to_csv('LUC_emissions-USA_ungrouped.csv',index=False)



    # group by Premise name
    LUC_emission = LUC_emission.groupby(['region','Year','Premise'])['LUC_emission_kgCO2'].agg('sum').reset_index()
    LUC_emission_US = LUC_emission[LUC_emission['region'] == "USA"]
    LUC_emission_US.to_csv('LUC_emissions-USA.csv',index=False)

    # Add world region by aggregating all land use data
    LUC_emission2 = LUC_emission.groupby(['Year','Premise'])['LUC_emission_kgCO2'].agg('sum').reset_index()
    LUC_emission2['region'] = 'World'
    LUC_emission2 = LUC_emission2[['region','Year','Premise','LUC_emission_kgCO2']]
    # print(df)
    LUC_emission = pd.concat([LUC_emission,LUC_emission2])

    ## calculate LUC_emission / crop [kg CO2/GJ] and export
    LUC_emission = LUC_emission.merge(ag_production, how='inner', on=['region','Year','Premise'])
    LUC_emission['LUC_emission_kgCO2_per_GJ'] = LUC_emission['LUC_emission_kgCO2']/LUC_emission['ag_prod_GJ']

    LUC_emission['Scenario'] = scenario
    LUC_emission['Region'] = LUC_emission['region']
    LUC_emission['Model'] = "GCAM"
    LUC_emission['Unit'] = 'kg CO2/GJ' # if cause error in Premise, change it to kg CO2/GJ-Prim
    LUC_emission['Variable'] = 'Emission Factor|CO2|Land Use Change|Average|Biomass|' + LUC_emission['Premise']

    iamc = LUC_emission[['Scenario', 'Region','Model','Variable','Unit','Year','LUC_emission_kgCO2_per_GJ']]
    iamc3 = iamc.pivot(values = 'LUC_emission_kgCO2_per_GJ', index = ['Scenario','Region', 'Model', 'Variable','Unit'], columns = 'Year').reset_index()
    iamc3 = pd.concat([iamc2,iamc3])

    iamc3.to_excel('./iamc_template/'+scenario+'/iamc_template_gcam_cropLUC_world.xlsx', index = False)



run_cropLUC("SSP2 RCP26")
run_cropLUC("SSP2 Base")

# if __name__ == '__main__':
#     run_cropLUC()