"""Useful definitions for lca operations."""
import pyprind
import pandas as pd
import numpy as np
import brightway2 as bw
# import wurst
from wurst import searching as ws
from pprint import pprint

lcia_methods = {
    'CC':('IPCC 2013', 'climate change', 'GWP 100a'),
    'TA':('ReCiPe Midpoint (H)', 'terrestrial acidification', 'TAP100'),
    'POF':('ReCiPe Midpoint (H)','photochemical oxidant formation','POFP'),
    'PMF':('ReCiPe Midpoint (H)', 'particulate matter formation', 'PMFP'),
    'MD':('ReCiPe Midpoint (H)', 'metal depletion', 'MDP'),
    'HT':('ReCiPe Midpoint (H)', 'human toxicity', 'HTPinf'),
    'MET':('ReCiPe Midpoint (H)', 'marine ecotoxicity', 'METPinf'),
    'ME':('ReCiPe Midpoint (H)', 'marine eutrophication', 'MEP'),
    'FD':('ReCiPe Midpoint (H)', 'fossil depletion', 'FDP'),
    'IR':('ReCiPe Midpoint (H)', 'ionising radiation', 'IRP_HE'),
    'OD':('ReCiPe Midpoint (H)', 'ozone depletion', 'ODPinf'),
    'FET':('ReCiPe Midpoint (H)', 'freshwater ecotoxicity', 'FETPinf'),
    'TET':('ReCiPe Midpoint (H)', 'terrestrial ecotoxicity', 'TETPinf'),
    'ALO':('ReCiPe Midpoint (H)', 'agricultural land occupation', 'ALOP'),
    'NLT':('ReCiPe Midpoint (H)', 'natural land transformation', 'NLTP'),
    'ULO':('ReCiPe Midpoint (H)', 'urban land occupation', 'ULOP'),
    'WD':('ReCiPe Midpoint (H)', 'water depletion', 'WDP'),
    'FE':('ReCiPe Midpoint (H)', 'freshwater eutrophication', 'FEP'),
    'R_HH' : ('ReCiPe Endpoint (H,A)', 'human health', 'total'),
    'R_EQ' : ('ReCiPe Endpoint (H,A)', 'ecosystem quality', 'total'),
    'R_R' : ('ReCiPe Endpoint (H,A)', 'resources', 'total'),
    'R_Total' : ('ReCiPe Endpoint (H,A)', 'total', 'total'),

    'CEDB': ('cumulative energy demand','biomass','renewable energy resources, biomass'),
      'CEDF': ('cumulative energy demand','fossil','non-renewable energy resources, fossil'),
      'CEDG': ('cumulative energy demand','geothermal','renewable energy resources, geothermal, converted'),
      'CEDN': ('cumulative energy demand','nuclear','non-renewable energy resources, nuclear'),
      'CEDFr': ('cumulative energy demand','primary forest','non-renewable energy resources, primary forest'),
      'CEDS': ('cumulative energy demand','solar','renewable energy resources, solar, converted'),
      'CEDH': ('cumulative energy demand','water','renewable energy resources, potential (in barrage water), converted'),
      'CEDW': ('cumulative energy demand','wind','renewable energy resources, kinetic (in wind), converted')

    }

lcia_methods_short={
    'CC':('IPCC 2013', 'climate change', 'GWP 100a', 'CO2 storage'),
    'HT':('ReCiPe Midpoint (H)', 'human toxicity', 'HTPinf'),
    'POF':('ReCiPe Midpoint (H)','photochemical oxidant formation','POFP'),
    'PMF':('ReCiPe Midpoint (H)', 'particulate matter formation', 'PMFP'),
    'MD':('ReCiPe Midpoint (H)', 'metal depletion', 'MDP'),
    'CEDB': ('cumulative energy demand','biomass','renewable energy resources, biomass'),
      'CEDF': ('cumulative energy demand','fossil','non-renewable energy resources, fossil'),
      'CEDG': ('cumulative energy demand','geothermal','renewable energy resources, geothermal, converted'),
      'CEDN': ('cumulative energy demand','nuclear','non-renewable energy resources, nuclear'),
      'CEDFr': ('cumulative energy demand','primary forest','non-renewable energy resources, primary forest'),
      'CEDS': ('cumulative energy demand','solar','renewable energy resources, solar, converted'),
      'CEDH': ('cumulative energy demand','water','renewable energy resources, potential (in barrage water), converted'),
      'CEDW': ('cumulative energy demand','wind','renewable energy resources, kinetic (in wind), converted')
    }

titles={'CC': 'Climate Change',
        'TA': 'Terrestrial Acidification',
        'POF':'Photochemical Oxidant Formation',
        'PMF':'Particulate Matter Formation',
        'MD': 'Mineral Depletion',
        'HT':'Human Toxicity',
        'CED':'Cumulative Energy Demand',

        'MET':'Marine Ecotoxicity',
        'ME':'Marine Eutrophication',
        'FD':'Fossil Depletion',
        'IR': 'Ionising Radiation',
        'OD':'Ozone Depletion',
        'FET':'Freshwater Ecotoxicity',
        'TET':'Terrestrial Ecotoxicity',
        'ALO': 'Agricultural Land Occupation',
        'NLT': 'Natural Land Transformation',
        'ULO': 'Urban Land Occupation',
        'WD': 'Water Depletion',
        'FE': 'Freshwater Eutrophication',
        'R_HH':'ReCiPe Endpoint Human Health',
        'R_EQ':'ReCiPe Endpoint Ecosystem Quality',
        'R_R':'ReCiPe Endpoint Resources',
        'R_Total':'ReCiPe Endpoint Total'
       }

short_titles={'CC': 'Climate Change',
        'TA': 'Terrestrial Acidification',
        'POF':'Photochemical Oxidant Formation',
        'PMF':'Particulate Matter Formation',
        'MD': 'Mineral Depletion',
        'HT':'Human Toxicity',
        'CED':'Cumulative Energy',
        'MET':'Marine Ecotoxicity',
        'ME':'Marine Eutrophication',
        'FD':'Fossil Depletion',
        'IR': 'Ionising Radiation',
        'OD':'Ozone Depletion',
        'FET':'Freshwater Ecotoxicity',
        'TET':'Terrestrial Ecotoxicity',
        'ALO': 'Agricultural Land Occupation',
        'NLT': 'Natural Land Transformation',
        'ULO': 'Urban Land Occupation',
        'WD': 'Water Depletion',
        'FE': 'Freshwater Eutrophication',
        'R_HH':'Endpoint Human Health',
        'R_EQ':'Endpoint Ecosystem Quality',
        'R_R':'Endpoint Resources',
        'R_Total':'Endpoint Total'
       }

two_line_titles = {'ALO': 'Agricultural Land\nOccupation',
 'CED': 'Cumulative Energy\nDemand',
 'FD': 'Fossil Depletion',
 'FE': 'Freshwater Eutrophication',
 'FET': 'Freshwater Ecotoxicity',
 'CC': 'Climate Change',
 'HT': 'Human Toxicity',
 'IR': 'Ionising Radiation',
 'MD': 'Mineral Depletion',
 'ME': 'Marine Eutrophication',
 'MET': 'Marine Ecotoxicity',
 'NLT': 'Natural Land\nTransformation',
 'OD': 'Ozone Depletion',
 'PMF': 'Particulate Matter\nFormation',
 'POF': 'Photochemical Oxidant\nFormation',
 'R_EQ': 'ReCiPe Endpoint\nEcosystem Quality',
 'R_HH': 'ReCiPe Endpoint\nHuman Health',
 'R_R': 'ReCiPe Endpoint\nResources',
 'R_Total': 'ReCiPe Endpoin\nTotal',
 'TA': 'Terrestrial Acidification',
 'TET': 'Terrestrial Ecotoxicity',
 'ULO': 'Urban Land\nOccupation',
 'WD': 'Water Depletion'}

units={'CC': 'kg CO$_2$ eq',
        'TA': 'kg SO$_2$ eq',
        'POF':'kg NMVOC',
        'PMF':'kg PM$_{10}$ eq',
        'MD': 'kg Fe eq',
        'HT':'kg 1,4 DB eq',
        'CED':'MJ',
        'MET':'kg 14-DCB eq',
        'ME': 'kg N eq',
        'FD':'kg oil eq',
        'IR': 'kg U235 eq',
        'OD':'kg CFC11 eq',
        'FET':'kg 14-DCB eq',
        'TET':'kg 14-DCB eq',
        'ALO': 'm$^2$yr',
        'NLT': 'm$^2$',
        'ULO': 'm$^2$yr',
        'WD': 'm$^3$ H$_2$O',
        'FE': 'kg P eq',
        'R_HH':'Points',
        'R_EQ':'Points',
        'R_R':'Points',
        'R_Total':'Points'
        }


category_group = ['CEDF', 'CEDB', 'CEDG', 'CEDN',
                  'CEDFr', 'CEDS', 'CEDH', 'CEDW']


titles_units={}
for cat in titles.keys():
    titles_units[cat] = titles[cat] + ' (' + units[cat] + ')'

cats_units={}
for cat in titles.keys():
    cats_units[cat] = cat + ' (' + units[cat] + ')'

def LCA_to_df(datasets, cats=['CC', 'R_Total'], amount=1, names=['name', 'location']):
    """Calcuate a LCA for a list of datasets for a list of methods
    and return a pandas dataframe.
    """
    results = {}
    index_dict = {}

    mets = cats
    # check for method shortcuts
    if set(cats).issubset(set(lcia_methods.keys())):
        mets = [lcia_methods[cat] for cat in cats]
    # create lca object
    if datasets and mets:
        lca = bw.LCA({datasets[0]: 1}, method=mets[0])
        lca.lci()
        lca.lcia()
    else:
        raise ValueError("No datasets or impact categories found." +
                         "Provide at least one dataset and one impact category.")

    for ds in datasets:
        index_dict[ds['code']] = tuple(ds[i] for i in names)

    for met in mets:
        met_name = met[1]
        print(met_name)
        lca.switch_method(met)
        results[met_name] = {}
        for dataset in pyprind.prog_bar(datasets):
            lca.redo_lcia({dataset: amount})
            results[met_name][dataset['code']] = lca.score

    # We group all energy into one category:
    for cat in category_group:
        if cat in results.keys():
            for key in results[cat].keys():
                if 'CED' not in results.keys():
                    results['CED'] = {}
                try:
                    results['CED'][key] += results[cat][key]
                except KeyError:
                    results['CED'][key] = results[cat][key]
            del results[cat]

    return pd.DataFrame(results).rename(index=index_dict)


def contribution_LCA_to_df(datasets,
                           cats=['CC', 'R_Total'],
                           amount=1,
                           names=['name', 'location']):
    """Calculate foreground contribution LCA of a list of datasets
    and return a multi-index dataframe.
    """
    results = {}
    codes = {}
    index_dict = {}

    if datasets and cats:
        lca = bw.LCA({datasets[0]: 1}, method=lcia_methods[cats[0]])
        lca.lci()
        lca.lcia()
    else:
        raise ValueError(
            "No datasets or impact categories found." +
            "Provide at least one dataset and one impact category.")

    for ds in datasets:
        index_dict[ds['code']] = tuple(ds[i] for i in names)

    for cat in cats:
        lca.switch_method(lcia_methods[cat])
        cf_dict = dict(bw.Method(lcia_methods[cat]).load())
        codes[cat] = {}
        results[cat] = {}
        for dataset in datasets:
            for exc in dataset.technosphere():
                existing_value = 0
                if (dataset['code'], exc.input['name']) in results[cat].keys():
                    existing_value = results[cat][(dataset['code'], exc.input['name'])]
                if exc['amount'] == 0:
                    continue
                if exc['input'] in codes[cat]:
                    results[cat][(dataset['code'], exc.input['name'])] = \
                        amount * codes[cat][exc['input']]*exc['amount'] + existing_value
                else:
                    lca.redo_lcia({exc.input: exc['amount']})
                    results[cat][(dataset['code'],exc.input['name'])] = \
                        lca.score * amount + existing_value
                    codes[cat][exc['input']] = (lca.score/exc['amount'])

            for exc in dataset.biosphere():
                # Not all flows are characterized
                if exc.input in cf_dict:
                    existing_value = 0
                    if (dataset['code'], exc.input['name']) in results[cat].keys():
                        existing_value = results[cat][(dataset['code'], exc.input['name'])]
                    results[cat][(dataset['code'],exc.input['name'])] = \
                        amount * exc['amount'] * cf_dict[exc.input] + existing_value

    for cat in category_group:
        if cat in results.keys():
            for key in results[cat].keys():
                if 'CED' not in results.keys():
                    results['CED'] = {}
                try:
                    results['CED'][key] += results[cat][key]
                except KeyError:
                    results['CED'][key] = results[cat][key]
            del results[cat]

    return pd.DataFrame(results).unstack().sort_index(axis=1).rename(index=index_dict)


def get_biosphere_factors(flows, lca, cats=['GWP', 'R_Total']):
    """Calcuate a LCA for a dict of biosphere flows for
    a list of methods and return a pandas dataframe.
    """
    results = {}

    for cat in cats:
        lca.switch_method(lcia_methods[cat])
        cf_dict = dict(bw.Method(lcia_methods[cat]).load())
        results[cat] = {}

        for name, exc in flows.items():
            # Not all flows are characterized
            if ('biosphere3', exc['code']) in cf_dict:
                results[cat][name] = cf_dict[('biosphere3', exc['code'])]

    # We usually prefer to group fossil and nuclear non renewable energy
    # into one category called non-renewable energy:
    if 'CEDF' in results.keys():
        results['CED'] = results['CEDF']
        del results['CEDF']

    for cat in category_group:
        if cat in results.keys():
            for key in results[cat].keys():
                if 'CED' not in results.keys():
                    results['CED'] = {}
                try:
                    results['CED'][key] += results[cat][key]
                except KeyError:
                    results['CED'][key] = results[cat][key]
            del results[cat]

    return pd.DataFrame(results)


def group_into_other(df, lim=0.01):
    """Group all processes that contribute less than a
    specified amount to the results for each LCIA method into 'other'.
    """
    norm = df.divide(df.sum(axis=1, level=0), axis=1, level=0).fillna(0)
    other_list = []
    for col in norm.columns:
        if norm[col].max() < lim and np.absolute(norm[col].min()) < lim:
            other_list.append(col)
    other = df[other_list].sum(axis=1, level=0)
    other.columns = pd.MultiIndex.from_product([other.columns, ['other']])
    return pd.concat([df.drop(other_list, axis=1), other], axis=1).sort_index(axis=1)


def cm2in(*tupl):
    inch = 2.54
    if isinstance(tupl[0], tuple):
        return tuple(i/inch for i in tupl[0])
    else:
        return tuple(i/inch for i in tupl)


def import_karma(ecoinvent_db, path_to_karma="../data/lci-Carma-CCS.xlsx"):
    db_name = "Carma CCS"
    if db_name not in bw.databases:
        sp = bw.ExcelImporter(path_to_karma)
        sp.apply_strategies()
        sp.match_database(fields=["name", "unit", "location"])
        if(ecoinvent_db == "ecoinvent_3.6"):
            # apply some updates to comply with ei 3.6
            new_technosphere_data = {
                'fields': ['name', 'reference product', 'location'],
                'data': [
                    (
                        ('market for water, decarbonised, at user', (), 'GLO'),
                        {
                            'name': ('market for water, decarbonised'),
                            'reference product': ('water, decarbonised'),
                            'location': ('DE'),
                        }
                    ),
                    (
                        ('market for water, completely softened, from decarbonised water, at user', (), 'GLO'),
                        {
                            'name': ('market for water, completely softened'),
                            'reference product': ('water, completely softened'),
                            'location': ('RER'),
                        }
                    ),
                    (
                        ('market for steam, in chemical industry', (), 'GLO'),
                        {
                            'location': ('RER'),
                            'reference product': ('steam, in chemical industry'),
                        }
                    ),
                    (
                        ('market for steam, in chemical industry', (), 'RER'),
                        {
                            'reference product': ('steam, in chemical industry'),
                        }
                    ),
                    (
                        ('zinc-lead mine operation', ('zinc concentrate',), 'GLO'),
                        {
                            'name': ('zinc mine operation'),
                            'reference product': ('bulk lead-zinc concentrate'),
                        }
                    ),
                    (
                        ('market for aluminium oxide', ('aluminium oxide',), 'GLO'),
                        {
                            'name': ('market for aluminium oxide, non-metallurgical'),
                            'reference product': ('aluminium oxide, non-metallurgical'),
                            'location': ('IAI Area, EU27 & EFTA'),
                        }
                    ),
                    (
                        ('platinum group metal mine operation, ore with high rhodium content', ('nickel, 99.5%',), 'ZA'),
                        {
                            'name': ('platinum group metal, extraction and refinery operations'),
                        }
                    )

                ]
            }

            Migration("migration_alois").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5 to 3.6"
            )
            sp.migrate("migration_alois")
            sp.match_database("ecoinvent_3.6", fields=['name','location','reference product','unit'])
            sp.statistics()

        sp.match_database(ecoinvent_db,
                          fields=["reference product", "name", "unit", "location"])
        sp.match_database(ecoinvent_db,
                          fields=["name", "unit", "location"])
        sp.statistics()
        sp.write_database()
        del sp
    else:
        print("Database {} already present.".format(db_name))


def add_non_fossil_co2_flows_to_ipcc_method():
    """Add non-fossil CO2 flows to the IPCC 2013 GWP 100a method."""
    ipcc = bw.Method(('IPCC 2013', 'climate change', 'GWP 100a'))
    gwp_data = ipcc.load()

    non_fossil = [x for x in ws.get_many(
        bw.Database("biosphere3"), ws.equals("name", "Carbon dioxide, non-fossil"))]
    print("Adding the following flows:")
    pprint(non_fossil)

    gwp_data.extend([(x.key, 1.) for x in non_fossil])

    co2_in_air = ws.get_one(bw.Database("biosphere3"),
                            ws.equals("name", 'Carbon dioxide, in air'))

    print("Adding {}.".format(co2_in_air))
    gwp_data.append((co2_in_air.key, -1.))

    method = bw.Method(('IPCC 2013', 'climate change', 'GWP 100a', 'Complete'))
    method.register()
    method.write(gwp_data)
    method.process()


def add_non_fossil_co2_flows_to_storage():
    """Add a new flow to the biosphere: Non-fossil CO2 to storage.
    Add this biosphere flow to LCIA methods where it is suitable.
    """
    from peewee import IntegrityError

    biosphere = bw.Database('biosphere3')
    new_flow = biosphere.new_activity(
        'CO2 to geological storage, non-fossil', **{
            'name': 'CO2 to geological storage, non-fossil',
            'unit': 'kilogram',
            'type': 'storage',
            'categories': ('geological storage',)})
    try:
        new_flow.save()
    except IntegrityError as e:
        print("Database Error (flow is likely to be present already): {}".format(e))

    print("Added new flow: {}".format(new_flow))

    co2_to_soil = [
        x for x in bw.Database("biosphere3") if (
            "Carbon dioxide, to soil or biomass stock" in str(x)
            and "('soil',)" in str(x))][0]
    print("Use {} as a template for the characterization factors.".format(co2_to_soil))

    for cat in lcia_methods:
        method = bw.Method(lcia_methods[cat])
        method_data = method.load()
        # first make sure we don't already have the flow included:
        if [x for x in method_data if new_flow.key[1] in x[0][1]]:
            print('Flow already present- you must have run this code already.')
            continue
        else:
            try:
                characterized_flow = [
                    x for x in method_data if co2_to_soil.key[1] in x[0][1]][0]
            except:
                continue

            method_data.extend([(new_flow.key, characterized_flow[1])])

            print('Flow added to method: {}'.format(method.name))
            print('Characterisation factor: {}'.format(characterized_flow[1]))

            orig_name = [x for x in method.name]
            new_method = bw.Method(tuple(orig_name + ['CO2 storage']))
            new_method.register()
            new_method.write(method_data)
            new_method.process()
