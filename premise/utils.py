from . import DATA_DIR
import csv
import pandas as pd
from .export import *
import numpy as np
from scipy.sparse import coo_matrix

CO2_FUELS = DATA_DIR / "fuel_co2_emission_factor.txt"
LHV_FUELS = DATA_DIR / "fuels_lower_heating_value.txt"
CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"

REMIND_TO_FUELS = DATA_DIR / "steel" / "remind_fuels_correspondance.txt"
EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"

def eidb_label(model, scenario, year):
    return "ecoinvent_" + model + "_" + scenario + "_" + str(year)

def get_correspondance_remind_to_fuels():
    """
    Return a dictionary with REMIND fuels as keys and ecoinvent activity names and reference products as values.
    :return: dict
    :rtype: dict
    """
    d = {}
    with open(REMIND_TO_FUELS) as f:
        r = csv.reader(f, delimiter=";")
        for row in r:
            d[row[0]] = {"fuel name": row[1], "activity name": row[2], "reference product": row[3]}
    return d

def get_fuel_co2_emission_factors():
    """
    Return a dictionary with fuel names as keys and, as values:
    * CO_2 emission factor, in kg CO2 per MJ of lower heating value
    * share of biogenic CO2

    Source: https://www.plateformeco2.ch/portal/documents/10279/16917/IPCC+(2006),%20Guidelines+for+National+Greenhouse+Gas+Inventories.pdf/a3838a98-5ad6-4da5-82f3-c9430007a158

    :return: dict
    """
    d = {}
    with open(CO2_FUELS) as f:
        r = csv.reader(f, delimiter=";")
        for row in r:
            d[row[0]] = {"co2": float(row[1]), "bio_share": float(row[2])}

    return d

def get_lower_heating_values():
    """
    Loads a csv file into a dictionary. This dictionary contains lower heating values for a number of fuel types.
    Mostly taken from: https://www.engineeringtoolbox.com/fuels-higher-calorific-values-d_169.html

    :return: dictionary that contains lower heating values
    :rtype: dict
    """
    with open(LHV_FUELS) as f:
        d = dict(filter(None, csv.reader(f, delimiter=";")))
        d = {k: float(v) for k, v in d.items()}
        return d

def get_efficiency_ratio_solar_PV(year, power):
    """
    Return a dictionary with years as keys and efficiency ratios as values
    :return: dict
    """

    df = pd.read_csv(
        EFFICIENCY_RATIO_SOLAR_PV)

    return df.groupby(["power", "year"]) \
        .mean()["value"] \
        .to_xarray() \
        .interp(year=year, power=power, kwargs={"fill_value": "extrapolate"})

def get_clinker_ratio_ecoinvent(version):
    """
    Return a dictionary with (cement names, location) as keys and clinker-to-cement ratios as values,
    as found in ecoinvent.
    :return: dict
    """
    if version == 3.5:
        fp = CLINKER_RATIO_ECOINVENT_35
    else:
        fp = CLINKER_RATIO_ECOINVENT_36

    with open(fp) as f:
        d = {}
        for val in csv.reader(f):
            d[(val[0], val[1])] = float(val[2])
    return d

def get_clinker_ratio_remind(year):
    """
    Return an array with the average clinker-to-cement ratio per year and per region, as given by REMIND.
    :return: xarray
    :return:
    """
    df = pd.read_csv(
        CLINKER_RATIO_REMIND)

    return df.groupby(["region", "year"]) \
        .mean()["value"] \
        .to_xarray() \
        .interp(year=year)

def shape_matrix(data, shape):
    data = np.matrix(data)
    row = np.squeeze(data[:, 0]).tolist()[0]
    col = np.squeeze(data[:, 1]).tolist()[0]
    val = np.squeeze(data[:, 2]).tolist()[0]

    d = {}

    A = coo_matrix((val, (row, col)), shape=(shape, shape))
    return A.toarray()

def rev_index(inds):

    return {v: k for k, v in inds.items()}


def add_modified_tags(original, scenarios):
    """
    Add a tag to any activity that is new or that has been modified
    compared to the source database.
    :return:
    """

    list_d = []

    exp = Export(original)
    ind_A = create_index_of_A_matrix(original)
    rev_ind_A = rev_index(ind_A)
    coords_A = exp.create_A_matrix_coordinates()
    original_A = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}
    list_d.append(original_A)

    ind_B = create_index_of_B_matrix()
    rev_ind_B = rev_index(ind_B)
    coords_B = exp.create_B_matrix_coordinates()
    original_B = {(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B}

    for scenario in scenarios:
        ind_A = create_index_of_A_matrix(scenario["database"])
        rev_ind_A = rev_index(ind_A)
        exp = Export(scenario["database"], scenario["model"], scenario["pathway"], scenario["year"], "")
        coords_A = exp.create_A_matrix_coordinates()
        new_A = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}

        ind_B = create_index_of_B_matrix()
        rev_ind_B = rev_index(ind_B)
        coords_B = exp.create_B_matrix_coordinates()
        new_B = {(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B}

        list_changes = []
        list_changes_names = []
        list_new = []

        for x, y in new_A.items():
            if x not in original_A:
                list_new.append(x[0])
            else:
                if original_A[x] != new_A[x]:
                    list_changes.append(x)
                    list_changes_names.append(x[0])

        for x, y in new_B.items():
            if x in original_B:
                if original_B[x] != new_B[x]:
                    list_changes.append(x)
                    list_changes_names.append(x[0])

        list_changes_names = set(list_changes_names)

        for ds in scenario["database"]:
            if (ds["name"], ds["reference product"], ds["unit"], ds["location"]) in list_new:
                print("new", ds["name"])
                ds["modified"] = True

            if (ds["name"], ds["reference product"], ds["unit"], ds["location"]) in list_changes_names:
                for exc in ds["exchanges"]:
                    if exc["type"] == "technopshere":
                        if ((ds["name"], ds["reference product"], ds["unit"], ds["location"]),
                            (exc["name"], exc["product"], exc["unit"], exc["location"])) in list_changes:
                            exc["modified"] = True

                    if exc["type"] == "biosphere":
                        if len(exc["categories"]) > 1:
                            if ((ds["name"], ds["reference product"], ds["unit"], ds["location"]),
                                (exc["name"], exc["categories"][0], exc["categories"][1], exc["unit"])) in list_changes:
                                exc["modified"] = True
                        else:
                            if ((ds["name"], ds["reference product"], ds["unit"], ds["location"]),
                                (exc["name"], exc["categories"][0], "unspecified", exc["unit"])) in list_changes:
                                exc["modified"] = True

    return scenarios