import json

from wurst import geomatcher

from premise import DATA_DIR

REGION_MAPPING_FILEPATH = DATA_DIR / "regionmappingH12.csv"
ADDITIONAL_DEFINITIONS = DATA_DIR / "additional_definitions.json"


class Geomap:
    """
    Map ecoinvent locations to REMIND regions and vice-versa.
    """

    def __init__(self, model, current_regions=[]):

        self.model = model
        self.geo = geomatcher

        if not isinstance(current_regions, list):
            current_regions = list(current_regions)

        if len(current_regions) > 0:
            self.add_or_remove_regions(current_regions)

        self.iam_regions = [
            x[1]
            for x in list(self.geo.keys())
            if isinstance(x, tuple) and x[0] == self.model.upper()
        ]

    def add_or_remove_regions(self, regions):

        with open(ADDITIONAL_DEFINITIONS) as json_file:
            additional_regions = json.load(json_file)

        # add regions that do not have a topological definition in `wurst`
        for r in regions:
            if (self.model.upper(), r) not in self.geo.keys():
                self.geo.add_definitions({r: additional_regions[r]}, self.model.upper())

        for k in list(self.geo.keys()):
            if (
                isinstance(k, tuple)
                and k[0] == self.model.upper()
                and k[1] not in regions
            ):
                self.geo[k].clear()
                del self.geo[k]

    def iam_to_ecoinvent_location(self, location, contained=True):
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

        # First, it can be that the location is already
        # an IAM location

        list_IAM_regions = [
            k[1]
            for k in list(self.geo.keys())
            if isinstance(k, tuple) and k[0].lower() == self.model.lower()
        ]

        if location in list_IAM_regions:
            return location

        # Second, it can be an ecoinvent region
        mapping = {
            "Europe without Austria": "EUR" if self.model == "remind" else "WEU",
            "Europe without Switzerland and Austria": "EUR"
            if self.model == "remind"
            else "WEU",
            "Europe without Switzerland": "EUR" if self.model == "remind" else "WEU",
            "North America without Quebec": "USA",
            "RER w/o RU": "EUR" if self.model == "remind" else "WEU",
            "RER": "EUR" if self.model == "remind" else "WEU",
            "RoW": "World",
            "GLO": "World",
            "RNA": "USA",
            "SAS": "OAS" if self.model == "remind" else "SEAS",
            "IAI Area, EU27 & EFTA": "EUR" if self.model == "remind" else "WEU",
            "UN-OCEANIA": "CAZ" if self.model == "remind" else "OCE",
            "UN-SEASIA": "OAS" if self.model == "remind" else "SEAS",
            "RAF": "SSA" if self.model == "remind" else "RSAF",
            "RAS": "CHA" if self.model == "remind" else "CHN",
            "IAI Area, Africa": "SSA" if self.model == "remind" else "RSAF",
            "RER w/o CH+DE": "EUR" if self.model == "remind" else "WEU",
            "RER w/o DE+NL+RU": "EUR" if self.model == "remind" else "WEU",
            "IAI Area, Asia, without China and GCC": "OAS"
            if self.model == "remind"
            else "SEAS",
            "Europe, without Russia and Turkey": "EUR"
            if self.model == "remind"
            else "WEU",
            "WECC": "USA",
            "WEU": "EUR" if self.model == "remind" else "WEU",
            "UCTE": "EUR" if self.model == "remind" else "WEU",
            "UCTE without Germany": "EUR" if self.model == "remind" else "WEU",
            "NORDEL": "NEU" if self.model == "remind" else "WEU",
            "ENTSO-E": "EUR" if self.model == "remind" else "WEU",
            "RLA": "LAM" if self.model == "remind" else "RSAM",
            "IAI Area, South America": "LAM" if self.model == "remind" else "RSAM",
            "IAI Area, Russia & RER w/o EU27 & EFTA": "REF"
            if self.model == "remind"
            else "RUS",
            "IAI Area, North America": "USA",
            "OCE": "CAZ" if self.model == "remind" else "OCE",
            "US-PR": "USA",
            "US only": "USA",
            "APAC": "CHA" if self.model == "remind" else "CHN",
        }

        if location in mapping:
            if mapping[location] in self.iam_regions:
                return mapping[location]
            # likely a case of missing "EUR" region
            else:
                return "DEU"

        # If not, then we look for IAM regions that contain it
        iam_location = [
            r[1]
            for r in self.geo.within(location)
            if r[0] == self.model.upper() and r[1] != "World"
        ]

        # If not, then we look for IAM regions that intersects with it
        if len(iam_location) == 0:
            iam_location = [
                r[1]
                for r in self.geo.intersects(location)
                if r[0] == self.model.upper() and r[1] != "World"
            ]

        # If not, then we look for IAM regions that are contained in it
        if len(iam_location) == 0:
            iam_location = [
                r[1]
                for r in self.geo.contained(location)
                if r[0] == self.model.upper() and r[1] != "World"
            ]

        if len(iam_location) == 0:
            print(
                "Cannot find the IAM location for {} from IAM model {}.".format(
                    location, self.model
                )
            )
            return "World"

        elif len(iam_location) == 1:
            return iam_location[0]
        else:

            if all(x in iam_location for x in ["NEU", "EUR"]):
                return "NEU"
            if all(x in iam_location for x in ["OAS", "JPN"]):
                return "JPN"
            if all(x in iam_location for x in ["CAZ", "USA"]):
                return "CAZ"
            if all(x in iam_location for x in ["OAS", "IND"]):
                return "IND"
            if all(x in iam_location for x in ["OAS", "CHA"]):
                return "CHA"
            if all(x in iam_location for x in ["OAS", "MEA"]):
                return "MEA"
            if all(x in iam_location for x in ["OAS", "REF"]):
                return "REF"
            if all(x in iam_location for x in ["OAS", "EUR"]):
                return "EUR"
            if all(x in iam_location for x in ["REF", "EUR"]):
                return "REF"
            if all(x in iam_location for x in ["MEA", "SSA"]):
                return "MEA"

            # more than one region is found
            print(f"More than one region found for {location}:{iam_location}")

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

        d_map_region_image_to_remind = {
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
            "World": "World",
        }

        d_map_region_remind_to_image = {
            "LAM": "RSAM",
            "CAZ": "OCE",
            "EUR": "WEU",
            "CHA": "CHN",
            "SSA": "SAF",
            "IND": "INDIA",
            "OAS": "RSAS",
            "JPN": "JAP",
            "USA": "USA",
            "NEU": "WEU",
            "MEA": "ME",
            "REF": "RUS",
            "World": "World",
        }

        if self.model == "image":
            return d_map_region_image_to_remind[location]

        if self.model == "remind":
            return d_map_region_remind_to_image[location]
