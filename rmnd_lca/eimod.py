import pandas as pd
import numpy as np

import pyprind
from constructive_geometries import Geomatcher
import wurst
from wurst import searching as ws
from wurst.ecoinvent.electricity_markets import \
    empty_low_voltage_markets, empty_high_voltage_markets, empty_medium_voltage_markets
import brightway2 as bw
from bw2data import Database
from wurst.ecoinvent import filters
import os.path
from helpers import activitymaps

DEFAULT_DATA_DIR = "../data"
DEFAULT_EMI_FILE = os.path.join(DEFAULT_DATA_DIR, "GAINS emission factors.csv")
DEFAULT_GAINS_MAPPING = os.path.join(DEFAULT_DATA_DIR, "GAINStoREMINDtechmap.csv")

## Functions to clean up Wurst import and additional technologies
def fix_unset_technosphere_and_production_exchange_locations(db, matching_fields=('name', 'unit')):
    for ds in db:
        for exc in ds['exchanges']:
            if exc['type'] == 'production' and exc.get('location') is None:
                exc['location'] = ds['location']
            elif exc['type'] == 'technosphere' and exc.get('location') is None:
                locs = find_location_given_lookup_dict(
                    db, {k: exc.get(k) for k in matching_fields})
                if len(locs) == 1:
                    exc['location'] = locs[0]
                else:
                    print("No unique location found for exchange:\n{}\nFound: {}".format(
                        pprint.pformat(exc), locs))





def remove_nones(db):
    exists = lambda x: {k: v for k, v in x.items() if v is not None}
    for ds in db:
        ds['exchanges'] = [exists(exc) for exc in ds['exchanges']]


def set_global_location_for_additional_datasets(db):
    """ This function is needed because the wurst function
    relink_technosphere exchanges needs global datasets if if can't find a regional one.
    """
    non_ecoinvent_datasets = [
        x['name'] for x in db  if x['database'] not in ['ecoinvent', 'ecoinvent_unchanged']]
    ecoinvent_datasets = [
        x['name'] for x in db  if x['database'] not in ['ecoinvent', 'ecoinvent_unchanged']]

    for ds in [x for x in db if x['database'] in ['Carma CCS', 'CSP']]:
        print('Dataset: ', ds['name'], ds['location'], ' Changing to Global')
        ds['location'] = 'GLO'
        for exc in [x for x in ds['exchanges'] if x['type'] != 'biosphere']:
            if exc['name'] in non_ecoinvent_datasets:
                if (exc['name'] in ecoinvent_datasets and
                    exc['location'] != 'GLO'):
                    print('Ecoinvent exchange: ', exc['name'], exc['location'])
                else:
                    print('Exchange: ', exc['name'], exc['location'], 'Changing to Global')
                    exc['location'] = 'GLO'


# # Region and location mapping

#these locations aren't found correctly by the constructive geometries library - we correct them here:
fix_names= {#'CSG' : 'CN-CSG',
            #'SGCC': 'CN-SGCC',

             #'RFC' : 'US-RFC',
             #'SERC' : 'US-SERC',
             #'TRE': 'US-TRE',
             #'ASCC': 'US-ASCC',
             #'HICC': 'US-HICC',
             #'FRCC': 'US-FRCC',
             #'SPP' : 'US-SPP',
             #'MRO, US only' : 'US-MRO',
             #'NPCC, US only': 'US-NPCC',
             #'WECC, US only': 'US-WECC',

             'IAI Area, Africa':'IAI Area 1, Africa',
             'IAI Area, South America':'IAI Area 3, South America',
             'IAI Area, Asia, without China and GCC':'IAI Area 4&5, without China',
             'IAI Area, North America, without Quebec':'IAI Area 2, without Quebec',
             'IAI Area, Gulf Cooperation Council':'IAI Area 8, Gulf'
            }


fix_names_back = {v: k for k,v in fix_names.items()}


def rename_locations(db, name_dict=fix_names):
    for ds in db:
        # If the location name of the dataset is found in the dictionary
        if ds['location'] in name_dict:
            # Change to the new location name
            ds['location'] = name_dict[ds['location']]

        for exc in ws.technosphere(ds):
            # If the location name of the exchange is found in the dictionary
            if exc['location'] in name_dict:
                # Change to the new location name
                exc['location'] = name_dict[exc['location']]


def get_remind_geomatcher(mapping="../data/regionmappingH12.csv"):
    """Return geomatcher object which includes REMIND regions."""

    regionmapping = pd.read_csv(mapping, sep=";")
    iso_2_rmnd = regionmapping.set_index("CountryCode").to_dict()["RegionCode"]
    rmnd_2_iso = regionmapping.groupby("RegionCode")["CountryCode"].apply(list).to_dict()

    geomatcher = Geomatcher()

    not_found = ["CCK", "CXR", 'GGY', 'JEY', 'BLM', 'MAF']
    rmnd_2_iso_fix = {
        rmnd: [iso for iso in rmnd_2_iso[rmnd] if iso not in not_found] for rmnd in rmnd_2_iso
    }

    geomatcher.add_definitions(rmnd_2_iso_fix, "REMIND")
    return geomatcher


def init(mapping="../data/regionmappingH12.csv"):
    """Module initialization."""
    global geomatcher
    geomatcher = get_remind_geomatcher(mapping)


def ecoinvent_to_remind_locations(loc, fixnames=True):
    """Find REMIND locations for a valid Geomatcher location."""
    if loc == 'RoW':
        loc = 'GLO'

    if fixnames:
        if loc in fix_names.keys():
            loc = fix_names[loc]

    if loc == 'IAI Area, Russia & RER w/o EU27 & EFTA':
        loc = 'RU'

    try:
        remind_loc = [r[1] for r in geomatcher.intersects(loc) if r[0] == 'REMIND']
    except KeyError as e:
        print("Can't find location {} using the geomatcher.".format(loc))
        remind_loc = ""

    ei_35_new_locs = {'XK': ['EUR']}

    if not remind_loc:
        if loc in ei_35_new_locs:
            remind_loc =  ei_35_new_locs[loc]
        else:
            print('No location found for: ' + loc)
            return None
    return remind_loc


# # Import Remind data

def get_remind_data(scenario_name):
    """Reads the REMIND csv result file and returns a dataframe
    containing all the information.
    """
    from glob import glob
    file_name = os.path.join("../data/Remind output files", scenario_name + "_*.mif")
    files = glob(file_name)
    if len(files) != 1:
        raise FileExistsError("No or more then one file found for {}.".format(file_name))
    df = pd.read_csv(
        files[0], sep=';',
        index_col=['Region', 'Variable', 'Unit']
    ).drop(columns=['Model', 'Scenario', 'Unnamed: 24'])
    df.columns = df.columns.astype(int)

    return df


# # Documenting changes to ecoinvent

def get_exchange_amounts(ds, technosphere_filters=None, biosphere_filters=None):
    result={}
    for exc in ws.technosphere(ds, *(technosphere_filters or [])):
        result[(exc['name'], exc['location'])]=exc['amount']
    for exc in ws.biosphere(ds, *(biosphere_filters or [])):
        result[(exc['name'], exc['categories'])]=exc['amount']
    return result


# # Modify electricity markets

# ## Import remind electricity markets

remind_electricity_market_labels = {

 'Biomass CHP': 'SE|Electricity|Biomass|CHP|w/o CCS',
 'Biomass IGCC CCS': 'SE|Electricity|Biomass|IGCCC|w/ CCS',
 'Biomass IGCC': 'SE|Electricity|Biomass|IGCC|w/o CCS',

 'Coal PC': 'SE|Electricity|Coal|PC|w/o CCS',
 'Coal IGCC': 'SE|Electricity|Coal|IGCC|w/o CCS',
 'Coal PC CCS': 'SE|Electricity|Coal|PCC|w/ CCS',
 'Coal IGCC CCS': 'SE|Electricity|Coal|IGCCC|w/ CCS',
 'Coal CHP':'SE|Electricity|Coal|CHP|w/o CCS',

'Gas OC': 'SE|Electricity|Gas|GT',
'Gas CC':'SE|Electricity|Gas|CC|w/o CCS',
'Gas CHP': 'SE|Electricity|Gas|CHP|w/o CCS',
'Gas CCS':  'SE|Electricity|Gas|w/ CCS',

 'Geothermal': 'SE|Electricity|Geothermal',

 'Hydro': 'SE|Electricity|Hydro',

 'Hydrogen': 'SE|Electricity|Hydrogen',

 'Nuclear': 'SE|Electricity|Nuclear',

 'Oil': 'SE|Electricity|Oil|w/o CCS',

 'Solar CSP': 'SE|Electricity|Solar|CSP',
 'Solar PV': 'SE|Electricity|Solar|PV',

 'Wind': 'SE|Electricity|Wind',
}

rename_remind_electricity_market_labels = {v:k for k, v in remind_electricity_market_labels.items()}


def get_remind_markets(remind_data, year, drop_hydrogen=True):
    if year < 2005 or year >2150:
        print('year not valid, must be between 2005 and 2150')
        return

    elif year in remind_data.columns:
        result =  remind_data.unstack(level=0)[year].loc[list(remind_electricity_market_labels.values())].reset_index(level=1, drop=True).rename(index = rename_remind_electricity_market_labels).divide(
            remind_data.unstack(level=0)[year].loc[list(remind_electricity_market_labels.values())].sum(axis=0)).drop('World', axis=1)

    else:
        temp = remind_data.unstack(level=0).loc[list(remind_electricity_market_labels.values())].reset_index(level=1, drop=True).rename(index = rename_remind_electricity_market_labels).stack(level=1).T
        new = pd.DataFrame(index = temp.columns,columns = [year],  data = np.nan).T

        result =  pd.concat([temp, new]).sort_index().interpolate(method = 'values').loc[year].unstack(level=1)

    if drop_hydrogen == False:
        return result
    else:
        print('Excluding hydrogen from electricity markets.\nHydrogen had a maximum share of '+ str(round(result.loc['Hydrogen'].max() * 100, 2)) + ' %')
        return result.drop('Hydrogen', axis = 0).divide(result.drop('Hydrogen', axis = 0).sum())

# ## Functions for modifying ecoinvent electricity markets

electricity_market_filter_high_voltage= [ws.contains('name', 'market for electricity, high voltage'),
                                ws.doesnt_contain_any('name', ['aluminium industry','internal use in coal mining'])]

electricity_market_filter_medium_voltage= [ws.contains('name', 'market for electricity, medium voltage'),
                                ws.doesnt_contain_any('name', ['aluminium industry','electricity, from municipal waste incineration'])]

electricity_market_filter_low_voltage= [ws.contains('name', 'market for electricity, low voltage')]


def delete_electricity_inputs_from_market(ds):
    #This function reads through an electricity market dataset and deletes all electricity inputs that are not own consumption.
    ds['exchanges'] = [exc for exc in ws.get_many(ds['exchanges'], *[ws.either(*[ws.exclude(ws.contains('unit', 'kilowatt hour')),
                                                                           ws.contains('name', 'market for electricity, high voltage'),
                                                                           ws.contains('name', 'market for electricity, medium voltage'),
                                                                           ws.contains('name', 'market for electricity, low voltage'),
                                                                           ws.contains('name', 'electricity voltage transformation')])])]


def find_average_mix(df):
    #This function considers that there might be several remind regions that match the ecoinvent region. This function returns the average mix across all regions.
    #note that this function doesn't do a weighted average based on electricity production, but rather treats all regions equally.
    return df.mean(axis=1).divide(df.mean(axis=1).sum())


def find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(tech, location, db):
    #first try ecoinvent location code:
    try: return [x for x in ws.get_many(db, *[ws.either(*[ws.equals('name', name) for name in activitymaps.powerplants[tech]]),
                                            ws.equals('location', location), ws.equals('unit', 'kilowatt hour')])]
    #otherwise try remind location code (for new datasets)
    except:
        try: return [x for x in ws.get_many(db, *[ws.either(*[ws.equals('name', name) for name in activitymaps.powerplants[tech]]),
                                            ws.equals('location', ecoinvent_to_remind_locations(location)), ws.equals('unit', 'kilowatt hour')])]
        except: return []


def find_other_ecoinvent_regions_in_remind_region(loc):
    if loc== 'RoW':
        loc='GLO'

    if loc in fix_names:
        loc = fix_names[loc]

    remind_regions = [r for r in geomatcher.intersects(loc) if r[0]=='REMIND']

    temp = []
    for remind_region in remind_regions:
        temp.extend([r for r in geomatcher.contained(remind_region)])

    result = []
    for temp in temp:
        if type(temp) ==tuple:
            result.append(temp[1])
        else: result.append(temp)
    return set(result)


def find_ecoinvent_electricity_datasets_in_remind_location(tech, location, db):
    try: return [x for x in ws.get_many(db, *[ws.either(*[ws.equals('name', name) for name in activitymaps.powerplants[tech]]),
                                            ws.either(*[ws.equals('location', loc) for loc in find_other_ecoinvent_regions_in_remind_region(location)]),
                                        ws.equals('unit', 'kilowatt hour')
                          ])]
    except: return []


def find_ecoinvent_electricity_datasets_in_all_locations(tech, db):
       return [x for x in ws.get_many(db, *[ws.either(*[ws.equals('name', name) for name in activitymaps.powerplants[tech]]),ws.equals('unit', 'kilowatt hour')])]


def add_new_datasets_to_electricity_market(ds, db, remind_electricity_market_df, year):
    #This function adds new electricity datasets to a market based on remind results. We pass not only a dataset to modify, but also a pandas dataframe containing the new electricity mix information, and the db from which we should find the datasets
    # find out which remind regions correspond to our dataset:

    remind_locations= ecoinvent_to_remind_locations(ds['location'])

    # here we find the mix of technologies in the new market and how much they contribute:
    mix =  find_average_mix(remind_electricity_market_df[remind_locations]) #could be several remind locations - we just take the average


    # here we find the datasets that will make up the mix for each technology
    datasets={}
    for i in mix.index:
        if mix[i] !=0:

            #print('Next Technology: ',i)

            # We have imports defined for Switzerland. Let's do those first:
            if i == 'Imports':
                datasets[i] = [x for x in ws.get_many(db, *[ws.equals('name', 'market group for electricity, high voltage'), ws.equals('location', 'ENTSO-E')])]
            else:
                # First try to find a dataset that is from that location (or remind region for new datasets):
                datasets[i] = find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(i, ds['location'], db)
                #print('First round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])

                #If this doesn't work, we try to take a dataset from another ecoinvent region within the same remind region
                if len(datasets[i]) == 0:
                    datasets[i] = find_ecoinvent_electricity_datasets_in_remind_location(i, ds['location'], db)
                    #print('Second round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])

                # If even this doesn't work, try taking a global datasets
                if len(datasets[i]) == 0:
                    datasets[i] = find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(i, 'GLO', db)
                    #print('Third round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])

                #if no global dataset available, we just take the average of all datasets we have:
                if len(datasets[i]) ==0:
                    datasets[i] = find_ecoinvent_electricity_datasets_in_all_locations(i, db)
                    #print('Fourth round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])

            #If we still can't find a dataset, we just take the global market group
            if len(datasets[i]) ==0:
                print('No match found for location: ', ds['location'], ' Technology: ', i,'. Taking global market group for electricity')
                datasets[i] = [x for x in ws.get_many(db, *[ws.equals('name', 'market group for electricity, high voltage'), ws.equals('location', 'GLO')])]


    # Now we add the new exchanges:
    for i in mix.index:
        if mix[i] !=0:
            total_amount = mix[i]
            amount= total_amount / len(datasets[i])
            for dataset in datasets[i]:
                ds['exchanges'].append({
                'amount': amount,
                'unit': dataset['unit'],
                'input': (dataset['database'], dataset['code']),
                'type': 'technosphere',
                'name': dataset['name'],
                'location': dataset['location']
                    })

    #confirm that exchanges sum to 1!
    sum = np.sum([exc['amount'] for exc in ws.technosphere(ds, *[ws.equals('unit', 'kilowatt hour'), ws.doesnt_contain_any('name', ['market for electricity, high voltage'])])])
    if round(sum,4) != 1.00:  print(ds['location'], " New exchanges don't add to one! something is wrong!", sum )
    return


def update_electricity_markets(db, year, remind_data):

    #import the remind market mix from the remind result files:
    remind_electricity_market_df = get_remind_markets(remind_data, year, drop_hydrogen=True)

    #Remove all electricity producers from markets:
    db = empty_low_voltage_markets(db)
    db = empty_medium_voltage_markets(db)
    db = empty_high_voltage_markets(db) # This function isn't working as expected - it needs to delete imports as well.

    changes={}
    #update high voltage markets:
    for ds in ws.get_many(db, *electricity_market_filter_high_voltage):
        changes[ds['code']]={}
        changes[ds['code']].update( {('meta data', x) : ds[x] for x in ['name','location']})
        changes[ds['code']].update( {('original exchanges', k) :v for k,v in get_exchange_amounts(ds).items()})
        delete_electricity_inputs_from_market(ds) # This function will delete the markets. Once Wurst is updated this can be deleted.
        add_new_datasets_to_electricity_market(ds, db, remind_electricity_market_df, year)
        changes[ds['code']].update( {('updated exchanges', k) :v for k,v in get_exchange_amounts(ds).items()})
    return changes


# # Modify Fossil Electricity Generation Technologies

# ## get remind technology efficiencies

def get_remind_fossil_electricity_efficiency(remind_data, year, technology):


    fossil_electricity_efficiency_name_dict = {
     'Biomass IGCC CCS':'Tech|Electricity|Biomass|IGCCC|w/ CCS|Efficiency',
     'Biomass CHP':     'Tech|Electricity|Biomass|CHP|w/o CCS|Efficiency',
     'Biomass IGCC':    'Tech|Electricity|Biomass|IGCC|w/o CCS|Efficiency',

    'Coal IGCC':'Tech|Electricity|Coal|IGCC|w/o CCS|Efficiency',
     'Coal IGCC CCS':'Tech|Electricity|Coal|IGCCC|w/ CCS|Efficiency',
     'Coal PC':'Tech|Electricity|Coal|PC|w/o CCS|Efficiency',
     'Coal PC CCS':'Tech|Electricity|Coal|PCC|w/ CCS|Efficiency',
     'Coal CHP': 'Tech|Electricity|Coal|CHP|w/o CCS|Efficiency',

    'Gas OC':'Tech|Electricity|Gas|GT|Efficiency',
     'Gas CC':'Tech|Electricity|Gas|CC|w/o CCS|Efficiency',
     'Gas CHP': 'Tech|Electricity|Gas|CHP|w/o CCS|Efficiency',
     'Gas CCS':'Tech|Electricity|Gas|CCC|w/ CCS|Efficiency',

    'Oil':'Tech|Electricity|Oil|DOT|Efficiency',
    }

    if year < 2005 or year > 2150:
        print('year not valid, must be between 2005 and 2150')
        return
    if technology not in fossil_electricity_efficiency_name_dict:
        print("Technology name not recognized: {}".format(technology))
        return

    if fossil_electricity_efficiency_name_dict[technology] not in remind_data.index.levels[1]:
        print('Technology efficiency not in REMIND output file: {}'.format(technology))
        return



    elif year in remind_data.columns:
        result =  remind_data.unstack(level=0).loc[fossil_electricity_efficiency_name_dict[technology]].stack(level = 0).reset_index(level = 0, drop=True).loc[year]
    else:
        temp = remind_data.unstack(level=0).loc[fossil_electricity_efficiency_name_dict[technology]].stack(level = 0).reset_index(level = 0, drop=True)
        new = pd.DataFrame(index = temp.columns,columns = [year],  data = np.nan).T
        result =  pd.concat([temp, new]).sort_index().interpolate(method = 'values').loc[year]

    if 0 in result.values:
        print('Warning: technology has regions with zero efficiency: {}'.format(technology))
        print(result)

    return result


# ## get ecoinvent efficiencies

def find_ecoinvent_coal_efficiency(ds):
    # Nearly all coal power plant datasets have the efficiency as a parameter.
    # If this isn't available, we back calculate it using the amount of coal used and
    # an average energy content of coal.
    try:
        return ds['parameters']['efficiency']
    except KeyError:
        pass

    #print('Efficiency parameter not found - calculating generic coal efficiency factor', ds['name'], ds['location'])

    fuel_sources = ws.technosphere(ds,
                                ws.either(ws.contains('name', 'hard coal'), ws.contains('name', 'lignite')),
                                ws.doesnt_contain_any('name', ('ash','SOx')),
                                ws.equals('unit', 'kilogram'))
    energy_in = 0
    for exc in fuel_sources:
        if 'hard coal' in exc['name']:
            energy_density = 20.1 / 3.6 #kWh/kg
        elif 'lignite' in exc['name']:
            energy_density = 9.9 / 3.6 # kWh/kg
        else:
            raise ValueError("Shouldn't happen because of filters!!!")
        energy_in += (exc['amount'] * energy_density)
    ds['parameters']['efficiency'] = ws.reference_product(ds)['amount'] / energy_in
    #print(ds['parameters']['efficiency'])
    return ws.reference_product(ds)['amount'] / energy_in


def find_ecoinvent_gas_efficiency(ds):

    #Nearly all gas power plant datasets have the efficiency as a parameter.
    #If this isn't available, we back calculate it using the amount of gas used and an average energy content of gas.
    try:
        return ds['parameters']['efficiency']
    except KeyError:
        pass

    #print('Efficiency parameter not found - calculating generic gas efficiency factor', ds['name'], ds['location'])

    fuel_sources = ws.technosphere(ds,
                                ws.either(ws.contains('name', 'natural gas, low pressure'), ws.contains('name', 'natural gas, high pressure')),
                                ws.equals('unit', 'cubic meter'))
    energy_in = 0
    for exc in fuel_sources:
        #(based on energy density of natural gas input for global dataset 'electricity production, natural gas, conventional power plant')
        if 'natural gas, high pressure' in exc['name']:
            energy_density= 39 / 3.6 # kWh/m3

        #(based on average energy density of high pressure gas, scaled by the mass difference listed between high pressure and low pressure gas in the dataset:
        #natural gas pressure reduction from high to low pressure, RoW)
        elif 'natural gas, low pressure' in exc['name']: energy_density= 39 * 0.84 / 3.6 #kWh/m3
        else:
            raise ValueError("Shouldn't happen because of filters!!!")
        energy_in += (exc['amount'] * energy_density)
    ds['parameters']['efficiency'] = ws.reference_product(ds)['amount'] / energy_in
    #print(ds['parameters']['efficiency'])
    return ws.reference_product(ds)['amount'] / energy_in


def find_ecoinvent_oil_efficiency(ds):

    #Nearly all oil power plant datasets have the efficiency as a parameter. If this isn't available, we use global average values to calculate it.
    try: return ds['parameters']['efficiency_oil_country']
    except KeyError:
        pass
    #print('Efficiency parameter not found - calculating generic oil efficiency factor', ds['name'], ds['location'])
    fuel_sources=[x for x in ws.technosphere(ds, *[ws.contains('name', 'heavy fuel oil'),
                                    ws.equals('unit', 'kilogram')]
                                    )]
    energy_in=0
    for exc in fuel_sources:
        #(based on energy density of heavy oil input and efficiency parameter for dataset 'electricity production, oil, RoW')
        energy_density= 38.5 / 3.6 # kWh/m3
        energy_in += (exc['amount'] * energy_density)
    ds['parameters']['efficiency'] = ws.reference_product(ds)['amount'] / energy_in
    #print(ds['parameters']['efficiency'])
    return ws.reference_product(ds)['amount'] /energy_in


def find_ecoinvent_biomass_efficiency(ds):
    #Nearly all power plant datasets have the efficiency as a parameter. If this isn't available, we excl.
    try: return ds['parameters']['efficiency_electrical']
    except: pass

    if ds['name'] == 'heat and power co-generation, biogas, gas engine, label-certified':
        ds['parameters'] = {'efficiency_electrical': 0.32}
        return ds['parameters']['efficiency_electrical']#in general comments for dataset

    elif ds['name'] == 'wood pellets, burned in stirling heat and power co-generation unit, 3kW electrical, future':
        ds['parameters'] = {'efficiency_electrical': 0.23}
        return ds['parameters']['efficiency_electrical'] #in comments for dataset

    print(ds['name'], ds['location'],' Efficiency not found!')
    return 0


def update_ecoinvent_efficiency_parameter(ds, scaling_factor):
    parameters = ds['parameters']
    possibles = ['efficiency', 'efficiency_oil_country', 'efficiency_electrical']

    for key in possibles:
        try:
            parameters[key] /= scaling_factor
            return
        except KeyError:
            pass


# ## Find efficiency scaling factors:

def find_coal_efficiency_scaling_factor(ds, year, remind_efficiency, agg_func=np.average):
    #input a coal electricity dataset and year. We look up the efficiency for this region and year from the remind model and return the scaling factor by which to multiply all efficiency dependent exchanges.
    #If the ecoinvent region corresponds to multiple remind regions we simply average them.
    ecoinvent_eff = find_ecoinvent_coal_efficiency(ds)
    remind_locations= ecoinvent_to_remind_locations(ds['location'])
    remind_eff = agg_func(remind_efficiency [remind_locations].values)/100 # we take an average of all applicable remind locations
    return ecoinvent_eff / remind_eff

def find_gas_efficiency_scaling_factor(ds, year, remind_efficiency, agg_func=np.average):
    #input a gas electricity dataset and year. We look up the efficiency for this region and year from the remind model and return the scaling factor by which to multiply all efficiency dependent exchanges.
    #If the ecoinvent region corresponds to multiple remind regions we simply average them.
    ecoinvent_eff = find_ecoinvent_gas_efficiency(ds)
    remind_locations= ecoinvent_to_remind_locations(ds['location'])
    remind_eff = agg_func(remind_efficiency [remind_locations].values)/100 # we take an average of all applicable remind locations
    return ecoinvent_eff / remind_eff

def find_oil_efficiency_scaling_factor(ds, year, remind_efficiency, agg_func=np.average):
    #input a oil electricity dataset and year. We look up the efficiency for this region and year from the remind model and return the scaling factor by which to multiply all efficiency dependent exchanges.
    #If the ecoinvent region corresponds to multiple remind regions we simply average them.
    ecoinvent_eff = find_ecoinvent_oil_efficiency(ds)
    remind_locations= ecoinvent_to_remind_locations(ds['location'])
    remind_eff = agg_func(remind_efficiency [remind_locations].values)/100 # we take an average of all applicable remind locations
    return ecoinvent_eff / remind_eff

def find_biomass_efficiency_scaling_factor(ds, year, remind_efficiency, agg_func=np.average):
    #input an electricity dataset and year. We look up the efficiency for this region and year from the remind model and return the scaling factor by which to multiply all efficiency dependent exchanges.
    #If the ecoinvent region corresponds to multiple remind regions we simply average them.
    ecoinvent_eff = find_ecoinvent_biomass_efficiency(ds)
    remind_locations= ecoinvent_to_remind_locations(ds['location'])
    remind_eff = agg_func(remind_efficiency [remind_locations].values)/100 # we take an average of all applicable remind locations
    return ecoinvent_eff / remind_eff


# ## Get remind emissions

def get_emission_factors(
        scenario="SSP2",
        fname=DEFAULT_EMI_FILE,
        mappingfile=DEFAULT_GAINS_MAPPING):

    gains_emi = pd.read_csv(fname, skiprows=4,
                            names=["year", "region", "GAINS", "pollutant", "scenario", "factor"])
    gains_emi["unit"] = "Mt/TWa"
    gains_emi = gains_emi[gains_emi.scenario == scenario]

    sector_mapping = pd.read_csv(mappingfile).drop(["noef", "elasticity"], axis=1)

    return gains_emi.join(sector_mapping.set_index("GAINS"), on="GAINS").dropna().drop(['scenario', 'REMIND'], axis=1).set_index(['year', 'region','GAINS', 'pollutant'])['factor'].unstack(level = [0,1,3]) /8760 #kg / kWh


# unfortunately, we currently don't have much resolution in these technologies, but it's better than nothing:
emissions_lookup_dict = {
    'Biomass IGCC CCS':'Power_Gen_Bio_Trad',
    'Biomass IGCC': 'Power_Gen_Bio_Trad',
    'Biomass CHP':'Power_Gen_Bio_Trad',
    'Coal IGCC':'Power_Gen_Coal' ,
    'Coal IGCC CCS':'Power_Gen_Coal' ,
    'Coal PC':    'Power_Gen_Coal'     ,
    'Coal CHP': 'Power_Gen_Coal' ,
    'Coal PC CCS':'Power_Gen_Coal' ,
    'Gas CCS':'Power_Gen_NatGas',
    'Gas OC':  'Power_Gen_NatGas'   ,
    'Gas CC':  'Power_Gen_NatGas',
    'Gas CHP':'Power_Gen_NatGas',
    'Oil': 'Power_Gen_LLF'}


def get_remind_emissions(remind_emissions_factors, year, region, tech):
    if year in remind_emissions_factors.columns.levels[0]:
        result = remind_emissions_factors.loc[emissions_lookup_dict[tech]].unstack(level=[1,2])[region].loc[year]
    else:
        temp = remind_emissions_factors.loc[emissions_lookup_dict[tech]].unstack(level=[1,2])[region]
        new = pd.DataFrame(index = temp.columns,columns = [year],  data = np.nan).T

        result =  pd.concat([temp, new]).sort_index().interpolate(method = 'values').loc[year]

    if type(region) == list:
        result = result.unstack(level=0)

    return result


# ## Modify ecoinvent fossil electricity generation technologies

remind_air_pollutants = { # for now we don't have any emissions data from remind, so we scale everything by efficiency.
    'Sulfur dioxide': 'SO2',
    'Carbon monoxide, fossil': 'CO',
    'Nitrogen oxides': 'NOx',
    'Ammonia': 'NH3',
    'NMVOC, non-methane volatile organic compounds, unspecified origin': 'VOC',
    #'BC',
    #'OC',
}

#define filter functions that decide which ecoinvent processes to modify
no_al = [ws.exclude(ws.contains('name', 'aluminium industry'))]
no_ccs = [ws.exclude(ws.contains('name', 'carbon capture and storage'))]
no_markets = [ws.exclude(ws.contains('name', 'market'))]
no_imports = [ws.exclude(ws.contains('name', 'import'))]
generic_excludes = no_al + no_ccs + no_markets


#there are some problems with the Wurst filter functions - we create a quick fix here:
gas_open_cycle_electricity = [
        ws.equals('name', 'electricity production, natural gas, conventional power plant')]


biomass_chp_electricity = [
        ws.either(ws.contains('name', ' wood'), ws.contains('name', 'bio')),
        ws.equals('unit', 'kilowatt hour'),
        ws.contains('name', 'heat and power co-generation')]

def get_remind_mapping():

    return {
        'Coal PC': {
            'eff_func': find_coal_efficiency_scaling_factor,
            'technology filters': filters.coal_electricity + generic_excludes,
            'technosphere excludes': [], # which technosphere exchanges to not change at all
        },
        'Coal CHP': {
            'eff_func': find_coal_efficiency_scaling_factor,
            'technology filters': filters.coal_chp_electricity + generic_excludes,
            'technosphere excludes': [],  # which technosphere exchanges to not change at all
        },
        'Gas OC': {
            'eff_func': find_gas_efficiency_scaling_factor,
            'technology filters': gas_open_cycle_electricity + generic_excludes + no_imports,
            'technosphere excludes': [],  # which technosphere exchanges to not change at all
        },
        'Gas CC': {
            'eff_func': find_gas_efficiency_scaling_factor,
            'technology filters': filters.gas_combined_cycle_electricity + generic_excludes + no_imports,
            'technosphere excludes': [],  # which technosphere exchanges to not change at all
        },
        'Gas CHP': {
            'eff_func': find_gas_efficiency_scaling_factor,
            'technology filters': filters.gas_chp_electricity + generic_excludes + no_imports,
            'technosphere excludes': [],    # which technosphere exchanges to not change at all
        },
        'Oil': {
            'eff_func': find_oil_efficiency_scaling_factor,
            'technology filters': (filters.oil_open_cycle_electricity
                                   + generic_excludes
                                   + [ws.exclude(ws.contains('name', 'nuclear'))]),
            'technosphere excludes': [],# which technosphere exchanges to not change at all
        },

        #'Biomass ST': {
        #    'eff_func': find_biomass_efficiency_scaling_factor,
        #    'technology filters': biomass_electricity + generic_excludes,
        #    'technosphere excludes': [],# which technosphere exchanges to not change at all
        #},
        'Biomass CHP': {
            'eff_func': find_biomass_efficiency_scaling_factor,
            'technology filters': biomass_chp_electricity + generic_excludes,
            'technosphere excludes': [],# which technosphere exchanges to not change at all
        },
        #    'Biomass CC': {
        #        'eff_func': find_biomass_efficiency_scaling_factor,
        #        'technology filters': biomass_combined_cycle_electricity + generic_excludes,
        #        'technosphere excludes': [],# which technosphere exchanges to not change at all
        #    },
    }

remind_mapping = get_remind_mapping()

def update_electricity_datasets_with_remind_data(
        db, remind_data, year,
        agg_func=np.average,
        update_efficiency = True,
        update_emissions = True,
        emi_fname=DEFAULT_EMI_FILE):
    """
    This function modifies each ecoinvent coal, gas,
    oil and biomass dataset using data from the remind model.
    """
    print("Don't forget that we aren't modifying PM emissions!")

    changes ={}

    for remind_technology in remind_mapping:
        print('Changing ', remind_technology)
        md = remind_mapping[remind_technology]
        remind_efficiency = get_remind_fossil_electricity_efficiency(remind_data, year, remind_technology)
        remind_emissions_factors = get_emission_factors(fname=emi_fname)

        for ds in ws.get_many(db, *md['technology filters']):
            changes[ds['code']]={}
            changes[ds['code']].update( {('meta data', x) : ds[x] for x in ['name','location']})
            changes[ds['code']].update( {('meta data', 'remind technology') : remind_technology})
            changes[ds['code']].update( {('original exchanges', k) :v for k,v in get_exchange_amounts(ds).items()})
            if update_efficiency == True:
                # Modify using remind efficiency values:
                scaling_factor = md['eff_func'](ds, year, remind_efficiency, agg_func)
                update_ecoinvent_efficiency_parameter(ds, scaling_factor)
                wurst.change_exchanges_by_constant_factor(ds, scaling_factor, md['technosphere excludes'],
                                                [ws.doesnt_contain_any('name', remind_air_pollutants)])

            # we use this bit of code to explicitly rewrite the value for certain emissions.
            if update_emissions == True:
                # Modify using remind specific emissions data
                remind_locations = ecoinvent_to_remind_locations(ds['location'])
                remind_emissions = get_remind_emissions(remind_emissions_factors, year, remind_locations, remind_technology)
                for exc in ws.biosphere(ds, ws.either(*[ws.contains('name', x) for x in remind_air_pollutants])):

                    flow = remind_air_pollutants[exc['name']]
                    amount =  agg_func(remind_emissions.loc[flow].values)

                    #if new amount isn't a number:
                    if np.isnan(amount):
                        print('Not a number! Setting exchange to zero' + ds['name'], exc['name'], ds['location'])
                        wurst.rescale_exchange(exc, 0)

                    #if old amound was zero:
                    elif exc['amount'] ==0:
                        exc['amount'] = 1
                        wurst.rescale_exchange(exc, amount / exc['amount'], remove_uncertainty = True)

                    else:
                        wurst.rescale_exchange(exc, amount / exc['amount'])

            changes[ds['code']].update( {('updated exchanges', k) :v for k,v in get_exchange_amounts(ds).items()})

        #check if exchange amounts are real numbers:
            for k,v in get_exchange_amounts(ds).items():
                if np.isnan(v): print(ds, k)
    return changes


# # Modifying Carma datasets

carma_electricity_ds_name_dict = {
 'Electricity, at BIGCC power plant 450MW, no CCS/2025': 'Biomass IGCC',

 'Electricity, at BIGCC power plant 450MW, pre, pipeline 200km, storage 1000m/2025': 'Biomass IGCC CCS',
 'Electricity, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m/2025': 'Biomass IGCC CCS',
 'Electricity, from CC plant, 100% SNG, truck 25km, post, pipeline 200km, storage 1000m/2025': 'Biomass IGCC CCS',

 'Electricity, at power plant/hard coal, IGCC, no CCS/2025': 'Coal IGCC',
 'Electricity, at power plant/lignite, IGCC, no CCS/2025': 'Coal IGCC',
 'Electricity, at power plant/hard coal, pre, pipeline 200km, storage 1000m/2025': 'Coal IGCC CCS',
 'Electricity, at power plant/lignite, pre, pipeline 200km, storage 1000m/2025': 'Coal IGCC CCS',

 'Electricity, at power plant/hard coal, post, pipeline 200km, storage 1000m/2025': 'Coal PC CCS',
 'Electricity, at power plant/lignite, post, pipeline 200km, storage 1000m/2025': 'Coal PC CCS',

 'Electricity, at power plant/natural gas, post, pipeline 200km, storage 1000m/2025': 'Gas CCS',
 'Electricity, at power plant/natural gas, pre, pipeline 200km, storage 1000m/2025': 'Gas CCS'}

carma_biomass_ccs_dataset_names =[
 '100% SNG, burned in CC plant, truck 25km, post, pipeline 200km, storage 1000m/2025',
 '100% SNG, burned in CC plant, truck 25km, post, pipeline 400km, storage 3000m/2025',
 'Wood chips, burned in power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m/2025',
 'Syngas, from biomass gasification, pre, pipeline 200km, storage 1000m/2025',
 'Wood chips, burned in power plant 20 MW, truck 25km, post, pipeline 400km, storage 3000m/2025',
 'Hydrogen, from steam reforming of biomassgas, at reforming plant, pre, pipeline 200km, storage 1000m/2025',
 'Syngas, from biomass gasification, pre, pipeline 400km, storage 3000m/2025']



def add_negative_CO2_flows_for_biomass_CCS(db):
    """All CO2 capture and storage in the Carma datasets is assumed to be 90% efficient.
    Thus, we can simply find out what the new CO2 emission is and then we know how much gets stored in the ground.
    It's very important that we ONLY do this for biomass CCS plants, as only they will have negative emissions!
    """

    carbon_to_ccs = [x for x in Database("biosphere3") if 'CO2 to geological storage, non-fossil' in x['name']][0]


    for ds in ws.get_many(db, *[ws.either(*[ws.equals('name', dataset_name) for dataset_name in carma_biomass_ccs_dataset_names])]):
        for exc in ws.biosphere(ds):
            if 'Carbon dioxide, non-fossil' == exc['name']:
                new_exc = exc.copy()
                break
        if 'Carbon dioxide, non-fossil' not in exc['name']:
            print('no CO2 exchange found in dataset: {}'.format(ds['name']))
            print([(exc['name'], exc['amount']) for exc in ds['exchanges'] if exc['type'] == 'biosphere'])
            return

        new_exc['input'] = (carbon_to_ccs['database'], carbon_to_ccs['code'])
        new_exc['name'] = carbon_to_ccs['name']
        new_exc['categories'] = carbon_to_ccs['categories']
        wurst.rescale_exchange(new_exc, (0.9 / 0.1), remove_uncertainty = True)
        ds['exchanges'].append(new_exc)
    return


def modify_all_carma_electricity_datasets(
        db,
        remind_data,
        year,
        update_efficiency=True,
        update_emissions=True,
        emi_fname=DEFAULT_EMI_FILE):
    remind_emissions_factors = get_emission_factors(fname=emi_fname)
    changes ={}

    for name, remind_technology in carma_electricity_ds_name_dict.items():
        remind_efficiency = get_remind_fossil_electricity_efficiency(remind_data, year, remind_technology) / 100 # Convert from percent.


        for ds in ws.get_many(db, ws.equals('name', name)):
            changes[ds['code']]={}
            changes[ds['code']].update( {('meta data', x) : ds[x] for x in ['name','location']})
            changes[ds['code']].update( {('meta data', 'remind technology') : remind_technology})
            changes[ds['code']].update( {('original exchanges', k) :v for k,v in get_exchange_amounts(ds).items()}
                                      )
            if update_efficiency:
                if 'Electricity, at BIGCC power plant 450MW' in ds['name']:
                    modify_carma_BIGCC_efficiency(ds, remind_efficiency)
                else:
                    modify_standard_carma_dataset_efficiency(ds, remind_efficiency)
            if update_emissions:
                modify_carma_dataset_emissions(db, ds, remind_emissions_factors, year, remind_technology)

        changes[ds['code']].update( {('updated exchanges', k) :v for k,v in get_exchange_amounts(ds).items()})

    #The efficiency defined by image also includes the electricity consumed in the carbon capture process, so we have to set this exchange amount to zero:
    if update_efficiency:
        for ds in ws.get_many(db, ws.contains('name', 'CO2 capture')):
            for exc in ws.technosphere(ds, *[ws.contains('name', 'Electricity'), ws.equals('unit', 'kilowatt hour')]):
                exc['amount'] = 0

    return changes


def modify_carma_dataset_emissions(db, ds, remind_emissions_factors, year, remind_technology):
    """ The dataset passed to this function doesn't have the biosphere flows directly.
    Rather, it has an exchange (with unit MJ) that contains
    the biosphere flows per unit fuel input.
    """

    biosphere_mapping={'SO2':'Sulfur dioxide',
                       'CO': 'Carbon monoxide, fossil',
                       'NOx': 'Nitrogen oxides',
                       }

    remind_locations = ecoinvent_to_remind_locations(ds['location'])
    remind_emissions = get_remind_emissions(remind_emissions_factors, year, remind_locations, remind_technology)

    exc_dataset_names = [x['name'] for x in ws.technosphere(ds, ws.equals('unit', 'megajoule'))]

    for exc_dataset in ws.get_many(db, *[ws.either(*[ws.equals('name', exc_dataset_name) for exc_dataset_name in exc_dataset_names])]):

        if len(list(ws.biosphere(exc_dataset)))==0:
            modify_carma_dataset_emissions(db, exc_dataset, remind_emissions_factors, year, remind_technology)
            continue

        #Modify using IMAGE emissions data
        for key, value in biosphere_mapping.items():
            for exc in ws.biosphere(exc_dataset, ws.contains('name', value)):
                exc['amount'] = np.average(remind_emissions.loc[key][remind_locations])
                if np.isnan(exc['amount']):
                    print('Not a number! Setting exchange to zero' + ds['name'], exc['name'], ds['location'])
                    exc['amount']=0
    return


def modify_carma_BIGCC_efficiency(ds, remind_efficiency):
    remind_locations = ecoinvent_to_remind_locations(ds['location'])
    remind_efficiency = np.average(remind_efficiency[remind_locations])

    old_efficiency = 3.6 / ws.get_one(
        ws.technosphere(ds),
        *[ws.contains('name', 'Hydrogen, from steam reforming')])['amount']

    for exc in ws.technosphere(ds):
        exc['amount'] = exc['amount']*old_efficiency/remind_efficiency
        return


def modify_standard_carma_dataset_efficiency(ds, remind_efficiency):
    if 'Electricity, at BIGCC power plant 450MW' in ds['name']:
        print("This function can't modify dataset {}. " +
              "It's got a different format.".format(ds['name']))
        return

    remind_locations = ecoinvent_to_remind_locations(ds['location'])
    remind_efficiency = np.average(remind_efficiency[remind_locations])

    # All other carma electricity datasets have a single exchange
    # that is the combustion of a fuel in MJ.
    # We can just scale this exchange and efficiency related changes will be done

    for exc in ws.technosphere(ds):
        exc['amount'] = 3.6/remind_efficiency

    return


def modify_electricity_generation_datasets(
        database_dict,
        emi_fname=DEFAULT_EMI_FILE,
        write_changeset=False):

    """Modify all ecoinvent processes that are mapped to a REMIND technology
    for all scenarios specified in the database_dict.
    """

    for key in pyprind.prog_bar(database_dict.keys()):

        db = wurst.extract_brightway2_databases(['Carma CCS', 'ecoinvent_3.5'])
        wurst.default_global_location(db)
        fix_unset_technosphere_and_production_exchange_locations(db)

        remove_nones(db)
        rename_locations(db, fix_names)
        add_negative_CO2_flows_for_biomass_CCS(db)

        year = database_dict[key]['year']
        scenario = database_dict[key]['scenario']
        remind_data = get_remind_data(scenario)
        print(key)

        # Electricity generation datasets:
        print('Changing ecoinvent electricity generation datasets')
        technology_changes = update_electricity_datasets_with_remind_data(
            db, remind_data, year,
            agg_func=np.average,
            update_efficiency=True,
            update_emissions=True,
            emi_fname=emi_fname)

        # Electricity markets:
        print('Changing electricity Markets')
        market_changes = update_electricity_markets(db, year, remind_data)

        # Electricity generation datasets from project Carma
        print('Changing Carma electricity datasets')
        modify_all_carma_electricity_datasets(
            db, remind_data, year,
            update_efficiency=True,
            update_emissions=True,
            emi_fname=emi_fname)

        print('Saving changes to excel')
        tech_df = pd.DataFrame.from_dict(technology_changes)
        tech_df.index = pd.MultiIndex.from_tuples(tech_df.index)
        tech_df = tech_df.T
        tech_df = tech_df.set_index(
            [('meta data', 'remind technology'),
             ('meta data', 'name'),
             ('meta data', 'location')],
            drop=True).sort_index()
        market_df = pd.DataFrame.from_dict(market_changes).T
        market_df = market_df.set_index(
            [('meta data', 'name'), ('meta data', 'location')],
            drop=True).sort_index().T

        if write_changeset:
            writer = pd.ExcelWriter('electricity changes ' + str(year) + ' ' + scenario + '.xlsx')
            market_df.to_excel(writer, sheet_name='markets')
            for tech in tech_df.index.levels[0]:
                tech_df \
                    .loc[tech] \
                    .dropna(how='all', axis=1) \
                    .swaplevel(i=0, j=1, axis=1) \
                    .T \
                    .to_excel(writer, sheet_name=tech)
            writer.save()
            del writer
        del tech_df
        del market_df

        rename_locations(db, fix_names_back)
        if key in bw.databases:
            del bw.databases[key]

        wurst.write_brightway2_database(db, key)

    print("Add ecoinvent version with all technolgies.")
    key = 'ecoinvent_added_technologies'

    if key not in bw.databases:

        db = wurst.extract_brightway2_databases(['Carma CCS', 'ecoinvent_3.5'])
        wurst.default_global_location(db)
        fix_unset_technosphere_and_production_exchange_locations(db)

        remove_nones(db)
        rename_locations(db, fix_names)
        add_negative_CO2_flows_for_biomass_CCS(db)

        rename_locations(db, fix_names_back)
        if key in bw.databases:
            del bw.databases[key]

        wurst.write_brightway2_database(db, key)

    else:
        print('Database already exists')
