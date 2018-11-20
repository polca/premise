"""Construct LCAs for REMIND technologies and regions."""

import brightway2 as bw
from helpers.eimod import geomatcher


def multi_lca_average(actvts, demand=1.):
    """ Perform LCA calculations for multiple technologies (activities).
        The demand is distributed evenly over all found activities (average).
    """

    share = 1./len(actvts)

    lca = bw.LCA({act: demand*share for act in actvts})
    lca.lci()

    return lca


def find_activities_by_name(techname, db):
    return [act for act in db if act["name"] == techname]


def find_activities_in_regions(techname, regions, db):
    actvts = find_activities_by_name(techname, db)
    if len(actvts) == 0:
        actvts = [act for act in db if act["name"] == techname and
                  act["location"] == "RoW"]
        if len(actvts) == 0:
            actvts = [act for act in db if act["name"] == techname and
                      act["location"] == "GLO"]
            if len(actvts) == 0:
                print("Could not find any activities matching {}".format(techname))
    return actvts


def lca_for_multiple_techs_and_regions(techs, regions, db, units_and_conversions={}):
    """ Perform LCA calculations for multiple technologies (activities) and regions.
        The demand is distributed evenly over all found activities (average).
    """
    if len(techs) == 0:
        return None
    # print("LCA for activities (from {}): {}".format(db.name, techs))
    actvts = [act for act in db if act["name"] in techs and
              act["location"] in regions]
    if len(actvts) == 0:
        actvts = [act for act in db if act["name"] in techs and
                  act["location"] == "RoW"]
        if len(actvts) == 0:
            actvts = [act for act in db if act["name"] in techs and
                      act["location"] == "GLO"]
            if len(actvts) == 0:
                print("Could not find any activities matching {}".format(techs))
                return None

    # set demand to portion
    # TODO: Somehow seperate heat and power generation for CHP
    if hasattr(actvts[0], "demand"):
        raise Exception("Activity object changed: demand attribute found.")

    share = 1./len(actvts)
    for act in actvts:
        if act["unit"] in units_and_conversions.keys():
            act.demand = share * units_and_conversions[act["unit"]]
        else:
            print("WARNING: Irregular units found for {}: {}.".format(act, act["unit"]))

    lca = bw.LCA({act: act.demand for act in actvts})
    lca.lci()

    return lca


def ei_locations_in_remind_region(region):
    regions = [
        el[1] if type(el) == tuple else el for el in geomatcher.contained(("REMIND", region))]
    if region == "EUR":
        regions.append("RER")
    return regions


def get_REMIND_database_name(scenario, year):
    return "_".join(["ecoinvent", "Remind", scenario, str(year)])
