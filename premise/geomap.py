from wurst.geo import geomatcher
from premise import DATA_DIR

REGION_MAPPING_FILEPATH = DATA_DIR / "regionmappingH12.csv"


class Geomap:
    """
    Map ecoinvent locations to REMIND regions and vice-versa.
    """

    def __init__(self, model):

        self.model = model

        if self.model == "remind":
            self.geo = self.get_REMIND_geomatcher()

        if self.model == "image":
            self.geo = self.get_IMAGE_geomatcher()

    @staticmethod
    def get_IMAGE_geomatcher():
        """
        Geographical boundaries for IMAGE regions are initally included in geomatcher.
        However, they are not properly labelled.

        """

        d_image_regions = {
            "BRA": "Brazil",
            "CAN": "Canada",
            "CEU": "Central Europe",
            "CHN": "China Region",
            "EAF": "Eastern Africa",
            "INDIA": "India",
            "INDO": "Indonesia Region",
            "JAP": "Japan",
            "KOR": "Korea Region",
            "ME": "Middle east",
            "MEX": "Mexico",
            "NAF": "Northern Africa",
            "OCE": "Oceania",
            "RCAM": "Central America",
            "RSAF": "Rest of Southern Africa",
            "RSAM": "Rest of South America",
            "RSAS": "Rest of South Asia",
            "RUS": "Russia Region",
            "SAF": "South Africa",
            "SEAS": "South Asia",
            "STAN": "Central Asia",
            "TUR": "Turkey",
            "UKR": "Ukraine region",
            "USA": "USA",
            "WAF": "Western Africa",
            "WEU": "Western Europe",
        }

        d_map = {("IMAGE", v): ("IMAGE", k) for k, v in d_image_regions.items()}

        new_def = dict()

        for k, v in geomatcher.items():
            if isinstance(k, tuple):
                if k[0] == "IMAGE" and k[1] in list(d_image_regions.values()):
                    new_def[d_map[k]] = v

        geo = geomatcher

        for k in list(geomatcher.keys()):
            if k[0] == "IMAGE" and k[1] in list(d_image_regions.values()):
                geomatcher.pop(k)

        geo.update(new_def)

        return geo

    @staticmethod
    def get_REMIND_geomatcher():
        """
        Load a geomatcher object from the `constructive_geometries`library and add definitions.
        It is used to find correspondences between REMIND and ecoinvent region names.
        :return: geomatcher object
        :rtype: wurst.geo.geomatcher
        """
        with open(REGION_MAPPING_FILEPATH) as f:
            f.readline()
            csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
            list_of_region = [(x[1], x[2]) for x in csv_list]

        # List of countries not found
        countries_not_found = ["CC", "CX", "GG", "JE", "BL"]

        rmnd_to_iso = {}
        iso_to_rmnd = {}

        # Build a dictionary that maps region names (used by REMIND) to ISO country codes
        # And a reverse dictionary that maps ISO country codes to region names
        for ISO, region in list_of_region:
            if ISO not in countries_not_found:
                try:
                    rmnd_to_iso[region].append(ISO)
                except KeyError:
                    rmnd_to_iso[region] = [ISO]

                iso_to_rmnd[region] = ISO

        geo = geomatcher
        geo.add_definitions(rmnd_to_iso, "REMIND")

        return geo

    def iam_to_ecoinvent_location(self, location, contained=False):
        """
        Find the corresponding ecoinvent region given an IAM region.

        :param location: name of a IAM region
        :type location: str
        :param contained: whether only geographies that are contained within the IAM region should be returned.
        By default, `contained` is False, meaning the function also returns geographies that intersects with IAM region.
        :type contained: bool
        :return: name(s) of an ecoinvent region
        :rtype: list
        """

        if location == "World":
            return ["GLO"]
        else:
            location = (self.model.upper(), location)

            ecoinvent_locations = []
            try:
                searchfunc = self.geo.contained if contained else self.geo.intersects
                for r in searchfunc(location):
                    if not isinstance(r, tuple):
                        ecoinvent_locations.append(r)
                    else:
                        if r[0] not in ("REMIND", "IMAGE"):
                            ecoinvent_locations.append(r[1])

                # TODO: Dirty trick. In the future, "CA" should be removed from "RNA". Also, "GLO" should not appear.
                if location == ("REMIND", "USA"):
                    ecoinvent_locations = [
                        e for e in ecoinvent_locations if "CA" not in e
                    ]

                if location in [("REMIND", "REF"), ("IMAGE", "RUS")]:
                    ecoinvent_locations = [
                        e for e in ecoinvent_locations if e not in [
                            "RER",
                            'Europe without Switzerland',
                            'Europe without Switzerland and France',
                            'RER w/o CH+DE',
                            'RER w/o AT+BE+CH+DE+FR+IT',
                            'RER w/o DE+NL+NO',
                            'Europe without NORDEL (NCPA)',
                        ]
                    ]

                # Current behaviour of `intersects` is to include "GLO" in all REMIND regions.
                if location != (self.model.upper(), "World"):
                    ecoinvent_locations = [e for e in ecoinvent_locations if e != "GLO"]

                return ecoinvent_locations
            except KeyError:
                print("Can't find location {} using the geomatcher.".format(location))
                return ["RoW"]

    def ecoinvent_to_iam_location(self, location):
        """
        Return an IAM region name for a 2-digit ISO country code given.
        Set rules in case two IAM regions are within the ecoinvent region.

        :param location: 2-digit ISO country code
        :type location: str
        :return: IAM region name
        :rtype: str
        """

        mapping = {
            "GLO": "World",
            "RoW": "CAZ" if self.model == "remind" else "World",
            "Europe without Austria": "EUR" if self.model == "remind" else "WEU",
            "Europe without Switzerland and Austria": "EUR" if self.model == "remind" else "WEU",
            "North America without Quebec": "USA",
            "RER w/o RU": "EUR" if self.model == "remind" else "WEU",

        }
        if location in mapping:
            return mapping[location]

        try:
            iam_location = [
                r[1]
                for r in self.geo.within(location)
                if r[0] == self.model.upper() and r[1] != "World"
            ]
        except KeyError:
            print("Cannot find the IAM location for {} from IAM model {}.".format(location, self.model))
            iam_location = ["World"]


        mapping = {
            ("AFR", "MEA"): "AFR",
            ("AFR", "SSA"): "AFR",
            ("EUR", "NEU"): "EUR",
            ("EUR", "REF"): "EUR",
            ("OAS", "CHA"): "OAS",
            ("OAS", "EUR"): "OAS",
            ("OAS", "IND"): "OAS",
            ("OAS", "JPN"): "OAS",
            ("OAS", "MEA"): "OAS",
            ("OAS", "REF"): "OAS",
            ("USA", "CAZ"): "USA",
        }

        # If we have more than one REMIND region
        if len(iam_location) > 1:
            # TODO: find a more elegant way to do that
            for key, value in mapping.items():
                # We need to find the most specific REMIND region
                if len(set(iam_location).intersection(set(key))) == 2:
                    iam_location.remove(value)
            return iam_location[0]
        elif len(iam_location) == 0:

            # There are a few ecoinvent regions that do not match well
            # with IMAGE regions

            if self.model == "image":

                d_ecoinvent_regions = {
                    "ENTSO-E": "WEU",
                    "RER": "WEU",
                    "RNA": "USA",
                    "RAS": "SEAS",
                    "RAF": "RSAF",
                    "Europe without Switzerland": "WEU",
                    "RLA": "RSAF",
                    "XK": "WEU",
                    "SS": "EAF",
                    "IAI Area, Africa": "WAF",
                    "UN-OCEANIA": "OCE",
                    "UCTE": "CEU",
                    "CU": "RCAM",
                    "IAI Area, Asia, without China and GCC": "RSAS",
                    "IAI Area, South America": "RSAM",
                    "IAI Area, EU27 & EFTA": "WEU",
                    "IAI Area, Russia & RER w/o EU27 & EFTA": "RUS"
                }

            else:
                d_ecoinvent_regions = {
                    "IAI Area, Russia & RER w/o EU27 & EFTA": "REF",
                }

            if location in d_ecoinvent_regions:
                return d_ecoinvent_regions[location]
            else:
                print("no location for {}".format(location))

            # It can also be that the location is already
            # an IAM location

            list_IAM_regions = [
                k[1]
                for k in list(self.geo.keys())
                if isinstance(k, tuple) and k[0].lower() == self.model.lower()
            ]

            if location in list_IAM_regions:
                return location

            # Or it could be an ecoinvent region
            try:
                iam_location = self.geo.intersects(("ecoinvent", location))
                iam_location = [i[1] for i in iam_location if i[0].lower() == self.model]
                return iam_location[0]

            except KeyError:
                print("no location for {}".format(location))
        else:
            return iam_location[0]

    def iam_to_GAINS_region(self, location):
        """
        Regions defined in GAINS emission data follows REMIND naming convention, but is different from IMAGE.
        :param location:
        :return:
        """

        d_map_region = {
            "BRA": "LAM",
            "CAN": "CAZ",
            "CEU": "EUR",
            "CHN": "CHA",
            "EAF": "SSA",
            "INDIA": "IND",
            "INDO": "OAS",
            "JAP": "JPN",
            "KOR": "OAS",
            "ME": "MEA",
            "MEX": "LAM",
            "NAF": "SSA",
            "OCE": "CAZ",
            "RCAM": "LAM",
            "RSAF": "SSA",
            "RSAM": "LAM",
            "RSAS": "OAS",
            "RUS": "REF",
            "SAF": "SSA",
            "SEAS": "OAS",
            "STAN": "MEA",
            "TUR": "MEA",
            "UKR": "REF",
            "USA": "USA",
            "WAF": "SSA",
            "WEU": "EUR",
            "World": "EUR",
        }

        if self.model == "remind":
            return "EUR" if location == "World" else location

        if self.model == "image":
            return d_map_region[location]

    def iam_to_iam_region(self, location):
        """
        When data is defined according to one IAM geography naming convention but needs to be used with another IAM.
        :param location:
        :return:
        """

        d_map_region = {
            "BRA": "LAM",
            "CAN": "CAZ",
            "CEU": "EUR",
            "CHN": "CHA",
            "EAF": "SSA",
            "INDIA": "IND",
            "INDO": "OAS",
            "JAP": "JPN",
            "KOR": "OAS",
            "ME": "MEA",
            "MEX": "LAM",
            "NAF": "SSA",
            "OCE": "CAZ",
            "RCAM": "LAM",
            "RSAF": "SSA",
            "RSAM": "LAM",
            "RSAS": "OAS",
            "RUS": "REF",
            "SAF": "SSA",
            "SEAS": "OAS",
            "STAN": "MEA",
            "TUR": "MEA",
            "UKR": "REF",
            "USA": "USA",
            "WAF": "SSA",
            "WEU": "EUR",
            "World": "EUR",
        }

        d_map_region_rev = {v: k for k, v in d_map_region.items()}

        if self.model == "image":
            return d_map_region[location]

        if self.model == "remind":
            return d_map_region_rev[location]
