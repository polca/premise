"""Map ecoinvent activities to REMIND technologies or material flows in single module."""

# electricity sector mappings

powerplants = {

    #From Carma project
    'Biomass IGCC CCS':['Electricity, from CC plant, 100% SNG, truck 25km, post, pipeline 200km, storage 1000m/2025',
                    'Electricity, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m/2025',
                    'Electricity, at BIGCC power plant 450MW, pre, pipeline 200km, storage 1000m/2025'],

    #From Carma project
    'Biomass IGCC': ['Electricity, at BIGCC power plant 450MW, no CCS/2025'],


    #From Carma project
    'Coal IGCC':['Electricity, at power plant/hard coal, IGCC, no CCS/2025',
                'Electricity, at power plant/lignite, IGCC, no CCS/2025'],

    'Coal IGCC CCS':['Electricity, at power plant/hard coal, pre, pipeline 200km, storage 1000m/2025',
                      'Electricity, at power plant/lignite, pre, pipeline 200km, storage 1000m/2025',],

    #From Carma project
     'Coal PC CCS':[ 'Electricity, at power plant/hard coal, post, pipeline 200km, storage 1000m/2025',
                 'Electricity, at power plant/lignite, post, pipeline 200km, storage 1000m/2025'],

    #From Carma project
    'Gas CCS':['Electricity, at power plant/natural gas, pre, pipeline 200km, storage 1000m/2025',
                        'Electricity, at power plant/natural gas, post, pipeline 200km, storage 1000m/2025'],

    # only Biomass CHP available
    'Biomass CHP':['heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014',
                    'heat and power co-generation, wood chips, 6667 kW',
                    'heat and power co-generation, biogas, gas engine'],

    'Coal PC':['electricity production, hard coal',
                'electricity production, lignite',
              'electricity production, hard coal, conventional',
              'electricity production, hard coal, supercritical'],

    'Coal CHP': ['heat and power co-generation, hard coal',
                'heat and power co-generation, lignite'],


    'Gas OC':['electricity production, natural gas, conventional power plant'],

    'Gas CC': ['electricity production, natural gas, combined cycle power plant'],

    'Gas CHP': ['heat and power co-generation, natural gas, combined cycle power plant, 400MW electrical',
            'heat and power co-generation, natural gas, conventional power plant, 100MW electrical'],

    'Geothermal':['electricity production, deep geothermal'],

    'Hydro':['electricity production, hydro, reservoir, alpine region',
            'electricity production, hydro, reservoir, non-alpine region',
            'electricity production, hydro, reservoir, tropical region',
            'electricity production, hydro, run-of-river'],

    'Hydrogen':[],

    'Nuclear':['electricity production, nuclear, boiling water reactor',
                'electricity production, nuclear, pressure water reactor, heavy water moderated',
                'electricity production, nuclear, pressure water reactor'],

    'Oil':['electricity production, oil',
          'heat and power co-generation, oil'],

    'Solar CSP': ['electricity production, solar thermal parabolic trough, 50 MW',
               'electricity production, solar tower power plant, 20 MW'],

    'Solar PV':['electricity production, photovoltaic, 3kWp slanted-roof installation, multi-Si, panel, mounted',
                    'electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted',
                    'electricity production, photovoltaic, 570kWp open ground installation, multi-Si'],

    'Wind':['electricity production, wind, <1MW turbine, onshore',
            'electricity production, wind, 1-3MW turbine, onshore',
            'electricity production, wind, >3MW turbine, onshore',
            'electricity production, wind, 1-3MW turbine, offshore']
}

# Material flows in the technosphere

# these sets of activity names are generated with the filter functions below
materials = {
    'steel': {'market for steel, chromium steel 18/8',
              'market for steel, low-alloyed',
              'market for steel, unalloyed'},
    'concrete': {'market for concrete, 20MPa',
                 'market for concrete, 25MPa',
                 'market for concrete, 30-32MPa',
                 'market for concrete, 35MPa',
                 'market for concrete, 50MPa',
                 'market for concrete, for de-icing salt contact',
                 'market for concrete, high exacting requirements',
                 'market for concrete, normal',
                 'market for concrete, sole plate and foundation'},
    'copper': {'market for copper'},
    'aluminium': {'market for aluminium alloy, AlLi',
                  'market for aluminium alloy, AlMg3',
                  'market for aluminium alloy, metal matrix composite',
                  'market for aluminium, primary, cast alloy slab from continuous casting',
                  'market for aluminium, primary, ingot',
                  'market for aluminium, primary, liquid'},
    'electricity': {'market for electricity, for reuse in municipal waste incineration only',
                    'market for electricity, high voltage',
                    'market for electricity, high voltage, aluminium industry',
                    'market for electricity, high voltage, for Swiss Federal Railways',
                    'market for electricity, high voltage, for internal use in coal mining',
                    'market for electricity, high voltage, label-certified',
                    'market for electricity, low voltage',
                    'market for electricity, low voltage, label-certified',
                    'market for electricity, medium voltage',
                    'market for electricity, medium voltage, aluminium industry',
                    'market for electricity, medium voltage, label-certified'},
    'gas': {'market for natural gas, high pressure',
            'market for natural gas, liquefied',
            'market for natural gas, low pressure',
            'market for natural gas, unprocessed, at extraction',
            'market for natural gas, vented'},
    'diesel': {'market for diesel', 'market for diesel, low-sulfur'},
    'petrol': {'market for petrol, 15% ETBE additive by volume, with ethanol from biomass',
               'market for petrol, 4% ETBE additive by volume, with ethanol from biomass',
               'market for petrol, 5% ethanol by volume from biomass',
               'market for petrol, low-sulfur',
               'market for petrol, two-stroke blend',
               'market for petrol, unleaded'},
    'freight': {'market for transport, freight train',
                'market for transport, freight, aircraft',
                'market for transport, freight, aircraft with reefer, cooling',
                'market for transport, freight, aircraft with reefer, freezing',
                'market for transport, freight, conveyor belt',
                'market for transport, freight, inland waterways, barge',
                'market for transport, freight, inland waterways, barge tanker',
                'market for transport, freight, inland waterways, barge with reefer, cooling',
                'market for transport, freight, inland waterways, barge with reefer, freezing',
                'market for transport, freight, light commercial vehicle',
                'market for transport, freight, lorry 16-32 metric ton, EURO3',
                'market for transport, freight, lorry 16-32 metric ton, EURO4',
                'market for transport, freight, lorry 16-32 metric ton, EURO5',
                'market for transport, freight, lorry 16-32 metric ton, EURO6',
                'market for transport, freight, lorry 28 metric ton, vegetable oil methyl ester 100%',
                'market for transport, freight, lorry 3.5-7.5 metric ton, EURO3',
                'market for transport, freight, lorry 3.5-7.5 metric ton, EURO4',
                'market for transport, freight, lorry 3.5-7.5 metric ton, EURO5',
                'market for transport, freight, lorry 3.5-7.5 metric ton, EURO6',
                'market for transport, freight, lorry 7.5-16 metric ton, EURO3',
                'market for transport, freight, lorry 7.5-16 metric ton, EURO4',
                'market for transport, freight, lorry 7.5-16 metric ton, EURO5',
                'market for transport, freight, lorry 7.5-16 metric ton, EURO6',
                'market for transport, freight, lorry >32 metric ton, EURO3',
                'market for transport, freight, lorry >32 metric ton, EURO4',
                'market for transport, freight, lorry >32 metric ton, EURO5',
                'market for transport, freight, lorry >32 metric ton, EURO6',
                'market for transport, freight, lorry with reefer, cooling',
                'market for transport, freight, lorry with reefer, freezing',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO3, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO3, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO3, carbon dioxide, liquid refr(...)_11',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO3, carbon dioxide, liquid refri(...)_1',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO4, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO4, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO4, carbon dioxide, liquid refr(...)_10',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO4, carbon dioxide, liquid refri(...)_2',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO5, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO5, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO5, carbon dioxide, liquid refri(...)_3',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO5, carbon dioxide, liquid refri(...)_7',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO6, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO6, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO6, carbon dioxide, liquid refr(...)_13',
                'market for transport, freight, lorry with refrigeration machine, 3.5-7.5 ton, EURO6, carbon dioxide, liquid refri(...)_5',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO3, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO3, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO3, carbon dioxide, liquid refri(...)_14',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO3, carbon dioxide, liquid refrig(...)_4',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO4, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO4, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO4, carbon dioxide, liquid refri(...)_12',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO4, carbon dioxide, liquid refrig(...)_6',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO5, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO5, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO5, carbon dioxide, liquid refri(...)_16',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO5, carbon dioxide, liquid refrig(...)_8',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO6, R134a refrigerant, cooling',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO6, R134a refrigerant, freezing',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO6, carbon dioxide, liquid refri(...)_15',
                'market for transport, freight, lorry with refrigeration machine, 7.5-16 ton, EURO6, carbon dioxide, liquid refrig(...)_9',
                'market for transport, freight, lorry with refrigeration machine, cooling',
                'market for transport, freight, lorry with refrigeration machine, freezing',
                'market for transport, freight, lorry, unspecified',
                'market for transport, freight, sea, liquefied natural gas',
                'market for transport, freight, sea, transoceanic ship',
                'market for transport, freight, sea, transoceanic ship with reefer, cooling',
                'market for transport, freight, sea, transoceanic ship with reefer, freezing',
                'market for transport, freight, sea, transoceanic tanker',
                'market for transport, freight, train with reefer, cooling',
                'market for transport, freight, train with reefer, freezing'},
    'cement': {'market for cement, Portland',
               'market for cement, alternative constituents 21-35%',
               'market for cement, alternative constituents 6-20%',
               'market for cement, blast furnace slag 18-30% and 18-30% other alternative constituents',
               'market for cement, blast furnace slag 25-70%, US only',
               'market for cement, blast furnace slag 31-50% and 31-50% other alternative constituents',
               'market for cement, blast furnace slag 36-65%',
               'market for cement, blast furnace slag 5-25%, US only',
               'market for cement, blast furnace slag 66-80%',
               'market for cement, blast furnace slag 70-100%, US only',
               'market for cement, blast furnace slag 81-95%',
               'market for cement, pozzolana and fly ash 11-35%',
               'market for cement, pozzolana and fly ash 15-40%, US only',
               'market for cement, pozzolana and fly ash 36-55%',
               'market for cement, pozzolana and fly ash 5-15%, US only',
               'market for cement, unspecified'},
    'heat': {'market for heat, central or small-scale, Jakobsberg',
             'market for heat, central or small-scale, natural gas',
             'market for heat, central or small-scale, natural gas and heat pump, Jakobsberg',
             'market for heat, central or small-scale, natural gas, Jakobsberg',
             'market for heat, central or small-scale, other than natural gas',
             'market for heat, district or industrial, natural gas',
             'market for heat, district or industrial, other than natural gas',
             'market for heat, for reuse in municipal waste incineration only',
             'market for heat, from steam, in chemical industry',
             'market for heat, future'}
}


def act_fltr(db, fltr={}, mask={}, filter_exact=False, mask_exact=False):
    """Filter `db` for activities matching field contents given by `fltr` excluding strings in `mask`.

    Args:
      `db`: brightway database object.
      `fltr`: string, list of strings or dictionary.
        If a string is provided, it is used to match the name field from the start (*startswith*).
        If a list is provided, all strings in the lists are used and results are joined (*or*).
        A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
      `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
      `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.

    Returns:
      list of brightway activities
    """
    result = []

    # default field is name
    if type(fltr) == list or type(fltr) == str:
        fltr = {
            "name": fltr
        }
    if type(mask) == list or type(mask) == str:
        mask = {
            "name": mask
        }

    def like(a, b):
        if filter_exact:
            return a == b
        else:
            return a.startswith(b)

    def notlike(a, b):
        if mask_exact:
            return a != b
        else:
            return b not in a

    assert len(fltr) > 0, "Filter dict must not be empty."
    for field in fltr:
        condition = fltr[field]
        if type(condition) == list:
            for el in condition:
                # this is effectively connecting the statements by *or*
                result.extend([act for act in db if like(act[field], el)])
        else:
            result.extend([act for act in db if like(act[field], condition)])

    for field in mask:
        condition = mask[field]
        if type(condition) == list:
            for el in condition:
                # this is effectively connecting the statements by *and*
                result = [act for act in result if notlike(act[field], el)]
        else:
            result = [act for act in result if notlike(act[field], condition)]
    return result


material_filters = {
    "steel": {
        "fltr": "market for steel,",
        "mask": "hot rolled"},
    "concrete": {"fltr": "market for concrete,"},
    "copper": {
        "fltr": "market for copper",
        "filter_exact": True},
    "aluminium": {
        "fltr": ["market for aluminium, primary",
                 "market for aluminium alloy,"]},
    "electricity": {"fltr": "market for electricity"},
    "gas": {
        "fltr": "market for natural gas,",
        "mask": ["network", "burned"]},
    "diesel": {
        "fltr": "market for diesel",
        "mask": ["burned", "electric"]},
    "petrol": {
        "fltr": "market for petrol,",
        "mask": "burned"},
    "freight": {"fltr": "market for transport, freight"},
    "cement": {"fltr": "market for cement,"},
    "heat": {"fltr": "market for heat,"}
}


def generate_sets_from_filters(db):
    """Generate sets of activity names for technologies from the filter specifications."""
    techs = {tech: act_fltr(db, **fltr) for tech, fltr in material_filters.items()}
    return {tech: set([act["name"] for act in actlst]) for tech, actlst in techs.items()}








