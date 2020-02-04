from . import DATA_DIR
import csv

REMIND_TO_ECOINVENT_EMISSION_FILEPATH = (DATA_DIR / "remind_to_ecoinvent_emission_mappping.csv")


class InventorySet:
    """
    This class is used as a container for various dictionaries.

    It stores:
    * activities_map: ecoinvent commodities as keys, ecoinvent dataset names as values
    * powerplants_map: ecoinvent electricity technology as keys, dataset names as values
    * emissions_map: REMIND emission labels as keys, ecoinvent emission labels as values

    """


class InventorySet:
    def __init__(self, db):
        self.emissions_map = self.get_remind_to_ecoinvent_emissions()
        self.db = db

    def generate_material_map(self):
        """
        Filter ecoinvent processes related to different material demands.

        :return: dictionary with materials as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        material_filters = {
            "steel": {"fltr": "market for steel,", "mask": "hot rolled"},
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

        return self.generate_sets_from_filters(material_filters)

    def generate_powerplant_map(self):
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        powerplant_filters = {
            "Biomass IGCC CCS": {
                "fltr": [
                    "Electricity, from CC plant, 100% SNG, truck 25km, post, pipeline 200km, storage 1000m/2025",
                    "Electricity, at wood burning power plant 20 MW, truck 25km, post, pipeline 200km, storage 1000m/2025",
                    "Electricity, at BIGCC power plant 450MW, pre, pipeline 200km, storage 1000m/2025",
                ]
            },
            "Biomass IGCC": {
                "fltr": "Electricity, at BIGCC power plant 450MW, no CCS/2025"
            },
            "Coal IGCC": {
                "fltr": [
                    "Electricity, at power plant/hard coal, IGCC, no CCS/2025",
                    "Electricity, at power plant/lignite, IGCC, no CCS/2025",
                ]
            },
            "Coal IGCC CCS": {
                "fltr": [
                    "Electricity, at power plant/hard coal, pre, pipeline 200km, storage 1000m/2025",
                    "Electricity, at power plant/lignite, pre, pipeline 200km, storage 1000m/2025",
                ]
            },
            "Coal PC CCS": {
                "fltr": [
                    "Electricity, at power plant/hard coal, post, pipeline 200km, storage 1000m/2025",
                    "Electricity, at power plant/lignite, post, pipeline 200km, storage 1000m/2025",
                ]
            },
            "Gas CCS": {
                "fltr": [
                    "Electricity, at power plant/natural gas, pre, pipeline 200km, storage 1000m/2025",
                    "Electricity, at power plant/natural gas, post, pipeline 200km, storage 1000m/2025",
                ]
            },
            "Biomass CHP": {
                "fltr": [
                    "heat and power co-generation, wood chips",
                    "heat and power co-generation, biogas",
                ]
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
            "Oil": {
                "fltr": [
                    "electricity production, oil",
                    "heat and power co-generation, oil",
                ],
                "mask": "aluminium",
            },
            "Solar CSP": {
                "fltr": [
                    "electricity production, solar thermal parabolic trough, 50 MW",
                    "electricity production, solar tower power plant, 20 MW",
                ]
            },
            "Solar PV": {"fltr": "electricity production, photovoltaic"},
            "Wind": {"fltr": "electricity production, wind"},
        }
        return self.generate_sets_from_filters(powerplant_filters)

    def get_remind_to_ecoinvent_emissions(self):
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

    def act_fltr(self, db, fltr={}, mask={}, filter_exact=False, mask_exact=False):
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

        :param fltr: A dictionary with labels and filter conditions as given to
            :func:`activity_maps.InventorySet.act_fltr`.
        :return: dictionary with the same keys as provided in filter
            and a set of activity data set names as values.
        :rtype: dict
        """
        techs = {tech: self.act_fltr(self.db, **fltr) for tech, fltr in filtr.items()}
        return {
            tech: set([act["name"] for act in actlst]) for tech, actlst in techs.items()
        }
