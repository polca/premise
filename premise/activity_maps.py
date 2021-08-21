from . import DATA_DIR
import csv

REMIND_TO_ECOINVENT_EMISSION_FILEPATH = (DATA_DIR / "ecoinvent_to_gains_emission_mappping.csv")


class InventorySet:
    """
    Hosts different filter sets to for ecoinvent activities and exchanges.

    It stores:
    * material_filters: filters for activities related to materials.
    * powerplant_filters: filters for activities related to power generation technologies.
    * emissions_map: REMIND emission labels as keys, ecoinvent emission labels as values

    The functions :func:`generate_material_map` and :func:`generate_powerplant_map` can
    be used to extract the actual activity objects as dictionaries.
    These functions return the result of applying :func:`act_fltr` to the filter dictionaries.
    """

    material_filters = {
        "steel, primary": {"fltr": "steel production, converter", "mask": "hot rolled"},
        "steel, secondary": {"fltr": "steel production, electric",
                             "mask": {"name": "hot rolled", "reference product": "heat"}},
        "concrete": {"fltr": "market for concrete,"},
        "copper": {"fltr": "market for copper", "filter_exact": True},
        "aluminium": {
            "fltr": ["market for aluminium, primary", "market for aluminium alloy,"]
        },
        "electricity": {"fltr": "market for electricity"},
        "gas": {"fltr": "market for natural gas,", "mask": ["network", "burned"]},
        "diesel": {"fltr": "market for diesel", "mask": ["burned", "electric"]},
        "petrol": {"fltr": "market for petrol,", "mask": "burned"},
        "freight": {"fltr": "market for transport, freight"},
        "cement": {"fltr": "market for cement,"},
        "heat": {"fltr": "market for heat,"},
    }

    fuel_filters = {
        # Gaseous
        "natural gas": {"fltr": "market for natural gas,", "mask": ["network", "burned", "liquefied"]}, #OK
        "natural gas, high pressure": {"fltr": 'market for natural gas, high pressure'},#OK
        "natural gas, low pressure": {"fltr": 'market for natural gas, low pressure'},#OK
        "biomethane": {"fltr": ['Biomethane, gaseous'], 'mask': ['burned']},#OK
        "methane, synthetic, from coal": {"fltr": "Methane, synthetic, gaseous, 5 bar, from coal-based hydrogen"},#OK
        "methane, synthetic, from electrolysis": {"fltr": 'methane, from electrochemical methanation'},#OK

        #Liquids
        "diesel": {"fltr": ["diesel production, low", "diesel production, petroleum refinery"],
                   "mask": ["burned", "electric"]},#OK
        "petrol": {"fltr": "petrol production, low", "mask": "burned"},#OK
        "heavy fuel oil": {"fltr": 'market for heavy fuel oil', 'mask': ['burned']},#OK
        "light fuel oil": {"fltr": 'market for light fuel oil'},#OK
        "bioethanol": {"fltr": ['market for ethanol', 'Ethanol, from']},#OK
        "bioethanol, woody": {"fltr": ['Ethanol production, via fermentation, from forest',
                                       'Ethanol production, via fermentation, from eucalyptus',
                                       'Ethanol production, via fermentation, from poplar',
                                       'Ethanol production, via fermentation, from willow'], 'mask': ['expansion', 'economic']},#OK
        "bioethanol, grassy": {"fltr": ['Ethanol production, via fermentation, from switchgrass',
                                        'Ethanol production, via fermentation, from miscanthus',
                                        'Ethanol production, via fermentation, from sorghum',]
                               , 'mask': ['expansion', 'economic']},#OK
        "bioethanol, grain": {"fltr": ['Ethanol production, via fermentation, from wheat grains,',
                                       'Ethanol production, via fermentation, from corn,'], 'mask': ['expansion', 'economic', 'carbon']},#OK
        "bioethanol, sugar": {"fltr": ['Ethanol production, via fermentation, from sugarbeet',
                                       'Ethanol production, via fermentation, from sugarcane,'], 'mask': ['expansion', 'economic']},#OK
        "biodiesel, oil": {"fltr": 'Biodiesel production, via transesterification, from', 'mask': ['expansion', 'economic']},#OK
        "diesel, synthetic, from electrolysis, energy allocation": {"fltr": ['Diesel, synthetic, from electrolysis-based hydrogen, energy allocation']},#OK
        "diesel, synthetic, from natural gas, energy allocation": {
            "fltr": 'Diesel, synthetic, from natural gas-based hydrogen, energy allocation, at fuelling station'},  # OK
        "diesel, synthetic, from natural gas with CCS, energy allocation": {
            "fltr": 'Diesel, synthetic, from natural gas-based hydrogen, energy allocation, at fuelling station'},  # OK
        "diesel, synthetic, from biomethane, energy allocation": {
            "fltr": 'Diesel, synthetic, from biomethane-based hydrogen, energy allocation, at fuelling station'},  # OK
        "diesel, synthetic, from biomass, energy allocation": {
            "fltr": 'Diesel, synthetic, from biomass-based hydrogen, energy allocation, at fuelling station'},  # OK
        "diesel, synthetic, from biomass with CCS, energy allocation": {
            "fltr": 'Diesel, synthetic, from biomass-based hydrogen with CCS, energy allocation, at fuelling station'},  # OK
        "diesel, synthetic, from biomass with CCS, economic allocation": {
            "fltr": 'Diesel, synthetic, from biomass-based hydrogen with CCS, economic allocation, at fuelling station'},
        # OK
        "diesel, synthetic, from petroleum, energy allocation": {
            "fltr": 'Diesel, synthetic, from petroleum-based hydrogen, energy allocation, at fuelling station'},

        "petrol, synthetic, hydrogen":{"fltr":"gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, energy allocation, at fuelling station"},
        "petrol, synthetic, coal":{"fltr":"gasoline production, synthetic, from methanol, hydrogen from electrolysis, CO2 from DAC, energy allocation, at fuelling station"},
        "diesel, synthetic, hydrogen":{"fltr":"diesel production, synthetic, from electrolysis-based hydrogen, energy allocation, at fuelling station"},
        "diesel, synthetic, coal":{"fltr":"diesel production, synthetic, from electrolysis-based hydrogen, energy allocation, at fuelling station"},
        "diesel, synthetic, wood":{"fltr":"diesel production, synthetic, from woody biomass-based hydrogen, energy allocation, at fuelling station"},
        "diesel, synthetic, wood, with CCS":{"fltr":"diesel production, synthetic, from electrolysis-based hydrogen, energy allocation, at fuelling station"},
        "diesel, synthetic, grass":{"fltr":"diesel production, synthetic, from woody biomass-based hydrogen, energy allocation, at fuelling station"},
        "diesel, synthetic, grass, with CCS":{"fltr":"diesel production, synthetic, from electrolysis-based hydrogen, energy allocation, at fuelling station"},

        #Methanol
        "methanol, wood": {"fltr": "market for methanol, from biomass"},
        "methanol, grass": {"fltr": "market for methanol, from biomass"},

        #Solids
        "hard coal": {"fltr": 'market for hard coal', 'mask': ['factory', 'plant', 'briquettes', 'ash']}, #OK
        "lignite": {"fltr": 'market for lignite', 'mask': ['factory', 'plant', 'briquettes', 'ash']},#OK
        "petroleum coke": {"fltr": 'market for petroleum coke'},#OK
        "wood pellet": {"fltr": 'market for wood pellet', 'mask': ['factory']},#OK
        "waste": {"fltr": {'reference product': ['waste plastic, mixture']},
                  'mask': ['market for', 'treatment', 'market group']},#OK

        #Hydrogen
        "hydrogen, from petroleum": {"fltr": 'hydrogen production, gaseous, petroleum refinery operation'}, #OK
        "hydrogen, from electrolysis": {"fltr": 'Hydrogen, gaseous, 700 bar, from electrolysis'}, #OK
        "hydrogen, from biomass": {"fltr": ['Hydrogen, gaseous, 700 bar, from dual fluidised bed gasification of woody biomass',
                                                    'Hydrogen, gaseous, 700 bar, from gasification of woody biomass'], "mask": ["CCS"]}, #OK
        "hydrogen, from biomass with CCS": {"fltr": ['Hydrogen, gaseous, 700 bar, from dual fluidised bed gasification of woody biomass with CCS',
                                                    'Hydrogen, gaseous, 700 bar, from gasification of woody biomass in oxy-fired entrained flow gasifier, with CCS']}, #OK
        "hydrogen, from coal": {"fltr": 'Hydrogen, gaseous, 700 bar, from hard coal gasification'}, #OK
        "hydrogen, from natural gas": {"fltr": ['Hydrogen, gaseous, 700 bar, ATR of NG',
                                               'Hydrogen, gaseous, 700 bar, from SMR of NG'], 'mask': ['CCS']}, #OK
        "hydrogen, from natural gas with CCS": {"fltr": ['Hydrogen, gaseous, 700 bar, ATR of NG, with CCS',
                                               'Hydrogen, gaseous, 700 bar, from SMR of NG, with CCS']}, #OK
        "hydrogen, from biomethane": {"fltr": ['Hydrogen, gaseous, 700 bar, from SMR of biogas',
                                               'Hydrogen, gaseous, 700 bar, from ATR of biogas'], 'mask': ['CCS']}, #OK
        "hydrogen, from biomethane with CCS": {"fltr": ['Hydrogen, gaseous, 700 bar, from SMR of NG, with CCS',
                                                        'Hydrogen, gaseous, 700 bar, from ATR of biogas with CCS']}, #OK

        #Kerosene
        "kerosene, from petroleum": {"fltr":"kerosene production, petroleum refinery operation"}, #OK
        "kerosene, synthetic, from electrolysis, energy allocation": {"fltr":["Kerosene, synthetic, from electrolysis-based hydrogen, energy allocation",
                                                                              "Kerosene, synthetic, from MTO, hydrogen from electrolysis, energy allocation"]},
        "kerosene, synthetic, from electrolysis, economic allocation": {"fltr":["Kerosene, synthetic, from electrolysis-based hydrogen, economic allocation",
                                                                                "Kerosene, synthetic, from MTO, hydrogen from electrolysis, economic allocation"]},
        "kerosene, synthetic, from coal, energy allocation": {"fltr":["Kerosene, synthetic, from coal-based hydrogen, energy allocation, at fuelling station"]},
        "kerosene, synthetic, from coal, economic allocation": {"fltr":["Kerosene, synthetic, from coal-based hydrogen, economic allocation, at fuelling station"]},
        "kerosene, synthetic, from natural gas, energy allocation": {"fltr": ["Kerosene, synthetic, from natural gas-based hydrogen, energy allocation, at fuelling station"]},
        "kerosene, synthetic, from natural gas, economic allocation": {"fltr": ["Kerosene, synthetic, from natural gas-based hydrogen, economic allocation, at fuelling station"]},
        "kerosene, synthetic, from biomethane, energy allocation": {"fltr":["Kerosene, synthetic, from biomethane-based hydrogen, energy allocation, at fuelling station"]},
        "kerosene, synthetic, from biomethane, economic allocation": {"fltr":["Kerosene, synthetic, from biomethane-based hydrogen, economic allocation, at fuelling station"]},
        "kerosene, synthetic, from biomass, energy allocation": {"fltr":["Kerosene, synthetic, from biomass-based hydrogen, energy allocation, at fuelling station"]},
        "kerosene, synthetic, from biomass, economic allocation": {"fltr":["Kerosene, synthetic, from biomass-based hydrogen, economic allocation, at fuelling station"]},



    }

    powerplant_fuels = {
        "Biomass IGCC CCS": {
            "fltr": ["100% SNG, burned in CC plant, truck 25km, post, pipeline 200km, storage 1000m",
                     "Wood chips, burned in power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m",
                     "Hydrogen, gaseous, 25 bar, from dual fluidised bed gasification of woody biomass with CCS, at gasification plant"
                     ]
        },
        "Biomass IGCC": {
            "fltr": "Hydrogen, gaseous, 25 bar, from dual fluidised bed gasification of woody biomass, at gasification plant"
        },
        "Biomass ST": {
            "fltr": "Wood chips, burned in power plant 20 MW, truck 25km, no CCS"
        },
        "Biomass CHP": {
            "fltr": ["market for wood chips, wet, measured as dry mass"],
        },
        "Biomass CHP CCS": {
            "fltr": "heat and power co-generation, wood chips, 6667 kW"
        },
        "Coal PC": {
            "fltr": ["market for hard coal", "market for lignite"],
            "mask": ['factory', 'plant', 'briquettes', 'ash']
        },
        "Coal CHP": {
            "fltr": ["market for hard coal", "market for lignite"],
            "mask": ['factory', 'plant', 'briquettes', 'ash']

        },
        "Coal CHP CCS": {
            "fltr": ["heat and power co-generation, hard coal"]
        },
        "Coal IGCC": {
            "fltr": ["Hard coal, burned in power plant/IGCC, no CCS",
                     "Lignite, burned in power plant/IGCC, no CCS"
                     ]
        },
        "Coal IGCC CCS": {
            "fltr": ["Hard coal, burned in power plant/pre, pipeline 200km, storage 1000m",
                     "Lignite, burned in power plant/pre, pipeline 200km, storage 1000m"
                     ]
        },
        "Coal PC CCS": {
            "fltr": ["Hard coal, burned in power plant/post, pipeline 200km, storage 1000m",
                     "Lignite, burned in power plant/post, pipeline 200km, storage 1000m"
                     ]
        },

        "Gas OC": {
            "fltr": ["market for natural gas, high pressure",
                     "market for natural gas, medium pressure",
                     "market for natural gas, low pressure",
                     "market group for natural gas"
                     ],
            "mask": ['liquids', 'liquefied', 'unprocessed', 'station', 'burned', 'vented']
        },
        "Gas CC": {
            "fltr": ["market for natural gas, high pressure",
                     "market for natural gas, medium pressure",
                     "market for natural gas, low pressure"],
            "mask": ['liquids', 'liquefied', 'unprocessed', 'station', 'burned', 'vented']
        },
        "Gas CHP": {
            "fltr": ["market for natural gas, high pressure",
                     "market for natural gas, medium pressure",
                     "market for natural gas, low pressure",
                     "market for biogas"
                     ],
            "mask": ['liquids', 'liquefied', 'unprocessed', 'station', 'burned', 'vented']
        },
        "Gas CC CCS": {
            "fltr": ["Natural gas, in ATR H2-CC/pre, pipeline 200km, storage 1000m",
                     "Natural gas, burned in power plant/post, pipeline 200km, storage 1000m/RER"
                     ]
        },
        "Gas CHP CCS": {
            "fltr": "heat and power co-generation, natural gas, conventional power plant, 100MW electrical"
        },
        "Nuclear": {"fltr": ["market for uranium, enriched",
                             "market for nuclear fuel element, for pressure water reactor",
                             "market for nuclear fuel element, for boiling water reactor",
                             "market for uranium hexafluoride"
                             ]
                    },
        "Oil ST": {
            "fltr": "market for heavy fuel oil",
            "mask": ["burned"]
        },
        "Oil CC": {
            "fltr": "market for heavy fuel oil",
            "mask": ["burned"]
        },
        "Oil CC CCS": {
            "fltr": "heat and power co-generation, oil",
            "mask": ["burned"]
        },
        "Oil CHP": {
            "fltr": "market for heavy fuel oil",
            "mask": ["burned"]
        },
        "Oil CHP CCS": {
            "fltr": "heat and power co-generation, oil",
            "mask": ["burned"]
        }
    }

    powerplant_filters = {
        "Biomass IGCC CCS": {
            "fltr": [
                "electricity production, from CC plant, 100% SNG, truck 25km, post, pipeline 200km, storage 1000m",
                "electricity production, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m",
                "electricity production, at BIGCC power plant 450MW, pre, pipeline 200km, storage 1000m",
            ]
        },
        "Biomass IGCC": {
            "fltr": "electricity production, at BIGCC power plant 450MW, no CCS"
        },
        "Biomass ST": {
            "fltr": "electricity production, at wood burning power plant 20 MW, truck 25km, no CCS"
        },
        "Biomass CHP": {
            "fltr": [
                "heat and power co-generation, wood chips",
            ],
            "mask": {"reference product": "heat"}
        },
        "Biomass CHP CCS": {
            "fltr": ["electricity production, at co-generation power plant/wood, post, pipeline 200km, storage 1000m",
                     "electricity production, at co-generation power plant/wood, post, pipeline 400km, storage 3000m"]
        },
        "Coal PC": {
            "fltr": [
                "electricity production, hard coal",
                "electricity production, lignite",
            ],
            "mask": "mine",
        },
        "Coal CHP": {
            "fltr": [
                "heat and power co-generation, hard coal",
                "heat and power co-generation, lignite",
            ],
            "mask": {"reference product": "heat"}

        },
        "Coal CHP CCS": {
            "fltr": [
                "electricity production, at co-generation power plant/hard coal, oxy, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/hard coal, oxy, pipeline 400km, storage 3000m",
                "electricity production, at co-generation power plant/hard coal, post, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/hard coal, post, pipeline 400km, storage 1000m",
                "electricity production, at co-generation power plant/hard coal, post, pipeline 400km, storage 3000m",
                "electricity production, at co-generation power plant/hard coal, pre, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/hard coal, pre, pipeline 400km, storage 3000m",
            ]
        },
        "Coal IGCC": {
            "fltr": [
                "electricity production, at power plant/hard coal, IGCC, no CCS",
                "electricity production, at power plant/lignite, IGCC, no CCS",
            ]
        },
        "Coal IGCC CCS": {
            "fltr": [
                "electricity production, at power plant/hard coal, pre, pipeline 200km, storage 1000m",
                "electricity production, at power plant/lignite, pre, pipeline 200km, storage 1000m",
            ]
        },
        "Coal PC CCS": {
            "fltr": [
                "electricity production, at power plant/hard coal, post, pipeline 200km, storage 1000m",
                "electricity production, at power plant/lignite, post, pipeline 200km, storage 1000m",
            ]
        },

        "Gas OC": {
            "fltr": "electricity production, natural gas, conventional power plant"
        },
        "Gas CC": {
            "fltr": "electricity production, natural gas, combined cycle power plant"
        },
        "Gas CHP": {
            "fltr": [
                "heat and power co-generation, natural gas, combined cycle power plant, 400MW electrical",
                "heat and power co-generation, natural gas, conventional power plant, 100MW electrical",
                "heat and power co-generation, biogas",
            ],
            "mask": {"reference product": "heat"}
        },
        "Gas CC CCS": {
            "fltr": [
                "electricity production, at power plant/natural gas, pre, pipeline 200km, storage 1000m",
                "electricity production, at power plant/natural gas, post, pipeline 200km, storage 1000m",
            ]
        },
        "Gas CHP CCS": {
            "fltr": [
                "electricity production, at co-generation power plant/natural gas, post, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/natural gas, pre, pipeline 200km, storage 1000m",
            ]
        },
        "Geothermal": {"fltr": "electricity production, deep geothermal"},
        "Hydro": {
            "fltr": [
                "electricity production, hydro, reservoir",
                "electricity production, hydro, run-of-river",
            ]
        },
        "Nuclear": {"fltr": "electricity production, nuclear", "mask": "aluminium"},
        "Oil ST": {
            "fltr": [
                    "electricity production, oil"
            ],
            "mask": {"name": "aluminium", "reference product": "heat"}
        },
        # TODO: find batter fit than this. Nothing better available in ecoinvent.
        "Oil CC": {
            "fltr": [
                "electricity production, oil"
            ],
            "mask": {"name": "aluminium", "reference product": "heat"}
        },
        # TODO: find batter fit than this. Nothing better available in ecoinvent.
        "Oil CC CCS": {
            "fltr": [
                "electricity production, at co-generation power plant/oil, post, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/oil, pre, pipeline 200km, storage 1000m"
            ],
            "mask": {"name": "aluminium"}
        },
        "Oil CHP": {
            "fltr": [
                "heat and power co-generation, oil",
            ],
            "mask": {"name": "aluminium", "reference product": "heat"}
        },
        "Oil CHP CCS": {
            "fltr": [
                "electricity production, at co-generation power plant/oil, post, pipeline 200km, storage 1000m",
                "electricity production, at co-generation power plant/oil, pre, pipeline 200km, storage 1000m"
            ],
            "mask": {"name": "aluminium"}
        },
        "Solar CSP": {
            "fltr": [
                "electricity production, solar thermal parabolic trough, 50 MW",
                "electricity production, solar tower power plant, 20 MW",
            ]
        },
        "Solar PV Centralized": {"fltr": "electricity production, photovoltaic, 570kWp"},
        "Solar PV Residential": {"fltr": "electricity production, photovoltaic, 3kWp"},
        "Wind Onshore": {"fltr": ["electricity production, wind, <1MW turbine, onshore",
                                  "electricity production, wind, >3MW turbine, onshore",
                                  "electricity production, wind, 1-3MW turbine, onshore"
                                  ]
                         },
        "Wind Offshore": {"fltr": "electricity production, wind, 1-3MW turbine, offshore"},
    }

    def __init__(self, db):
        self.db = db

    def generate_material_map(self):
        """
        Filter ecoinvent processes related to different material demands.

        :return: dictionary with materials as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """

        return self.generate_sets_from_filters(self.material_filters)

    def generate_powerplant_map(self):
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_filters)

    def generate_powerplant_fuels_map(self):
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_fuels)

    def generate_fuel_map(self):
        """
        Filter ecoinvent processes related to fuel supply.

        :return: dictionary with fuel names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.fuel_filters)

    @staticmethod
    def get_remind_to_ecoinvent_emissions():
        """
        Retrieve the correspondence between REMIND and ecoinvent emission labels.
        :return: REMIND emission labels as keys and ecoinvent emission labels as values
        :rtype: dict
        """

        if not REMIND_TO_ECOINVENT_EMISSION_FILEPATH.is_file():
            raise FileNotFoundError(
                "The dictionary of emission labels correspondences could not be found."
            )

        csv_dict = {}

        with open(REMIND_TO_ECOINVENT_EMISSION_FILEPATH) as f:
            input_dict = csv.reader(f, delimiter=";")
            for row in input_dict:
                csv_dict[row[0]] = row[1]

        return csv_dict

    @staticmethod
    def act_fltr(db, fltr=None, mask=None, filter_exact=False, mask_exact=False):
        """Filter `db` for activities matching field contents given by `fltr` excluding strings in `mask`.
        `fltr`: string, list of strings or dictionary.
        If a string is provided, it is used to match the name field from the start (*startswith*).
        If a list is provided, all strings in the lists are used and results are joined (*or*).
        A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
        `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
        `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.

        :param db: A lice cycle inventory database
        :type db: brightway2 database object
        :param fltr: value(s) to filter with.
        :type fltr: Union[str, lst, dict]
        :param mask: value(s) to filter with.
        :type mask: Union[str, lst, dict]
        :param filter_exact: requires exact match when true.
        :type filter_exact: bool
        :param mask_exact: requires exact match when true.
        :type mask_exact: bool
        :return: list of activity data set names
        :rtype: list

        """
        if fltr is None:
            fltr = {}
        if mask is None:
            mask = {}
        result = []

        # default field is name
        if type(fltr) == list or type(fltr) == str:
            fltr = {"name": fltr}
        if type(mask) == list or type(mask) == str:
            mask = {"name": mask}

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

    def generate_sets_from_filters(self, filtr):
        """
        Generate a dictionary with sets of activity names for
        technologies from the filter specifications.

            :param filtr:
            :func:`activity_maps.InventorySet.act_fltr`.
        :return: dictionary with the same keys as provided in filter
            and a set of activity data set names as values.
        :rtype: dict
        """
        techs = {tech: self.act_fltr(self.db, **fltr) for tech, fltr in filtr.items()}
        return {
            tech: set([act["name"] for act in actlst]) for tech, actlst in techs.items()
        }
