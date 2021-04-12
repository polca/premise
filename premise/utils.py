from . import DATA_DIR
import csv
import pandas as pd
from .export import *
import numpy as np
from wurst import searching as ws
from datetime import date
import uuid
from itertools import chain

CO2_FUELS = DATA_DIR / "fuel_co2_emission_factor.txt"
LHV_FUELS = DATA_DIR / "fuels_lower_heating_value.txt"
CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"

STEEL_RECYCLING_SHARES = DATA_DIR / "steel" / "steel_recycling_shares.csv"
METALS_RECYCLING_SHARES = DATA_DIR / "metals" / "metals_recycling_shares.csv"

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
            d[row[0]] = {
                "fuel name": row[1],
                "activity name": row[2],
                "reference product": row[3],
            }
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

    df = pd.read_csv(EFFICIENCY_RATIO_SOLAR_PV, sep=",")

    return (
        df.groupby(["power", "year"])
        .mean()["value"]
        .to_xarray()
        .interp(year=year, power=power, kwargs={"fill_value": "extrapolate"})
    )


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
        for val in csv.reader(f, delimiter=","):
            d[(val[0], val[1])] = float(val[2])
    return d


def get_clinker_ratio_remind(year):
    """
    Return an array with the average clinker-to-cement ratio per year and per region, as given by REMIND.
    :return: xarray
    :return:
    """
    df = pd.read_csv(CLINKER_RATIO_REMIND, sep=",")

    return df.groupby(["region", "year"]).mean()["value"].to_xarray().interp(year=year)


def get_steel_recycling_rates(year):
    """
    Return an array with the average shares for primary (Basic oxygen furnace) and secondary (Electric furnace)
    steel production per year and per region, as given by: https://www.bir.org/publications/facts-figures/download/643/175/36?method=view
    for 2015-2019, further linearly extrapolated to 2020, 2030, 2040 and 2050.
    :return: xarray
    :return:
    """
    df = pd.read_csv(STEEL_RECYCLING_SHARES, sep=";")

    return (
        df.groupby(["region", "year", "type"])
        .mean()[["share", "world_share"]]
        .to_xarray()
        .interp(year=year)
    )

def get_metals_recycling_rates(year):
    """
    Return an array with the average shares for some metals,
    as given by: https://static-content.springer.com/esm/art%3A10.1038%2Fs43246-020-00095-x/MediaObjects/43246_2020_95_MOESM1_ESM.pdf
    for 2025, 2035, 2045.
    :return: xarray
    :return:
    """
    df = pd.read_csv(METALS_RECYCLING_SHARES, sep=";")

    return (
        df.groupby(["metal", "year", "type"])
        .mean()["share"]
        .to_xarray()
        .interp(year=year)
    )

def rev_index(inds):
    return {v: k for k, v in inds.items()}

def create_codes_and_names_of_A_matrix(db):
    """
    Create a dictionary a tuple (activity name, reference product,
    unit, location) as key, and its code as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (i["name"], i["reference product"], i["unit"], i["location"],): i["code"]
        for i in db
    }


def add_modified_tags(original_db, scenarios):
    """
    Add a `modified` label to any activity that is new
    Also add a `modified` label to any exchange that has been added
    or that has a different value than the source database.
    :return:
    """

    # Class `Export` to which the original database is passed
    exp = Export(original_db)
    # Collect a dictionary of activities {row/col index in A matrix: code}
    rev_ind_A = rev_index(create_codes_index_of_A_matrix(original_db))
    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()
    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}
    # Collect a dictionary with activities' names and correponding codes
    codes_names = create_codes_and_names_of_A_matrix(original_db)
    # Collect list of substances
    rev_ind_B = rev_index(create_codes_index_of_B_matrix())
    # Retrieve list of coordinates of the B matrix [activity index, substance index, value]
    coords_B = exp.create_B_matrix_coordinates()
    # Turn it into a dictionary {(activity code, substance code): value}
    original.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

    for s, scenario in enumerate(scenarios):
        print(f"Looking for differences in database {s + 1} ...")
        rev_ind_A = rev_index(create_codes_index_of_A_matrix(scenario["database"]))
        exp = Export(
            scenario["database"],
            scenario["model"],
            scenario["pathway"],
            scenario["year"],
            "",
        )
        coords_A = exp.create_A_matrix_coordinates()
        new = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}

        rev_ind_B = rev_index(create_codes_index_of_B_matrix())
        coords_B = exp.create_B_matrix_coordinates()
        new.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

        list_new = set(i[0] for i in original.keys()) ^ set(i[0] for i in new.keys())

        ds = (d for d in scenario["database"] if d["code"] in list_new)

        # Tag new activities
        for d in ds:
            d["modified"] = True

        # List codes that belong to activities that contain modified exchanges
        list_modified = (i[0] for i in new if i in original and new[i] != original[i])
        #
        # Filter for activities that have modified exchanges
        for ds in ws.get_many(
            scenario["database"],
            ws.either(*[ws.equals("code", c) for c in set(list_modified)]),
        ):
            # Loop through biosphere exchanges and check if
            # the exchange also exists in the original database
            # and if it has the same value
            # if any of these two conditions is False, we tag the exchange
            excs = (exc for exc in ds["exchanges"] if exc["type"] == "biosphere")
            for exc in excs:
                if (ds["code"], exc["input"][0]) not in original or new[
                    (ds["code"], exc["input"][0])
                ] != original[(ds["code"], exc["input"][0])]:
                    exc["modified"] = True
            # Same thing for technosphere exchanges,
            # except that we first need to look up the provider's code first
            excs = (exc for exc in ds["exchanges"] if exc["type"] == "technosphere")
            for exc in excs:
                if (
                    exc["name"],
                    exc["product"],
                    exc["unit"],
                    exc["location"],
                ) in codes_names:
                    exc_code = codes_names[
                        (exc["name"], exc["product"], exc["unit"], exc["location"])
                    ]
                    if new[(ds["code"], exc_code)] != original[(ds["code"], exc_code)]:
                        exc["modified"] = True
                else:
                    exc["modified"] = True

    return scenarios


def build_superstructure_db(origin_db, scenarios, db_name, fp):
    # Class `Export` to which the original database is passed
    exp = Export(db=origin_db, filepath=fp)

    # Collect a dictionary of activities
    # {(name, ref_prod, loc, db, unit):row/col index in A matrix}
    rev_ind_A = exp.rev_index(exp.create_names_and_indices_of_A_matrix())

    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()

    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] * -1 for x in coords_A}

    # Collect list of substances
    rev_ind_B = exp.rev_index(exp.create_names_and_indices_of_B_matrix())
    # Retrieve list of coordinates of the B matrix [activity index, substance index, value]
    coords_B = exp.create_B_matrix_coordinates()

    # Turn it into a dictionary {(activity name, ref prod, location, database, unit): value}
    original.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] * -1 for x in coords_B})

    modified = {}

    print("Looping through scenarios to detect changes...")

    for scenario in scenarios:

        exp = Export(
            db=scenario["database"],
            model=scenario["model"],
            scenario=scenario["pathway"],
            year=scenario["year"],
            filepath=fp,
        )

        new_rev_ind_A = exp.rev_index(exp.create_names_and_indices_of_A_matrix())
        new_coords_A = exp.create_A_matrix_coordinates()
        new = {
            (new_rev_ind_A[x[0]], new_rev_ind_A[x[1]]): x[2] * -1 for x in new_coords_A
        }

        new_coords_B = exp.create_B_matrix_coordinates()
        new.update(
            {(new_rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] * -1 for x in new_coords_B}
        )
        # List activities that are in teh new database but not in the original one
        # As well as exchanges that are present in both databases but with a different value
        list_modified = (i for i in new if i not in original or new[i] != original[i])
        # Also add activities from the original database that are not present in
        # the new one
        list_new = (i for i in original if i not in new)

        list_modified = chain(list_modified, list_new)

        for i in list_modified:
            if i not in modified:
                modified[i] = {"original": original.get(i, 0)}
                modified[i][
                    scenario["model"]
                    + " - "
                    + scenario["pathway"]
                    + " - "
                    + str(scenario["year"])
                ] = new.get(i, 0)
            else:
                modified[i][
                    scenario["model"]
                    + " - "
                    + scenario["pathway"]
                    + " - "
                    + str(scenario["year"])
                ] = new.get(i, 0)

    # some scenarios may have not been modified
    # and that means that exchanges might be absent
    # from `modified`
    # so we need to manually add them
    # and set the exchange value similar to that
    # of the original database

    list_scenarios = ["original"] + [s["model"]
                    + " - "
                    + s["pathway"]
                    + " - "
                    + str(s["year"]) for s in scenarios]

    for m in modified:
        for s in list_scenarios:
            if s not in modified[m].keys():
                # if it is a production exchange
                # the value should be -1
                if m[1] == m[0]:
                    modified[m][s] = -1
                else:
                    modified[m][s] = modified[m]["original"]

    columns = [
        "from activity name",
        "from reference product",
        "from location",
        "from categories",
        "from database",
        "from key",
        "to activity name",
        "to reference product",
        "to location",
        "to categories",
        "to database",
        "to key",
        "flow type"
    ]
    columns.append("original")
    columns.extend(
        [
            a["model"] + " - " + a["pathway"] + " - " + str(a["year"])
            for a in scenarios
        ]
    )

    print("Export a scenario difference file.")

    l_modified = [columns]

    for m in modified:
        # Avoid production flow types
        if not (m[1] == m[0] and any(v for v in modified[m].values() if v == -1)):

            if m[1][2] == "biosphere3":
                d = [
                    m[1][0],
                    "",
                    "",
                    m[1][1],
                    m[1][2],
                    "",
                    m[0][0],
                    m[0][1],
                    m[0][3],
                    "",
                    db_name,
                    "",
                    "biosphere"
                ]
            else:
                d = [
                    m[1][0],
                    m[1][1],
                    m[1][3],
                    "",
                    db_name,
                    "",
                    m[0][0],
                    m[0][1],
                    m[0][3],
                    "",
                    db_name,
                    "",
                    "technosphere"
                ]
            for s in list_scenarios:
                # we do not want a zero here,
                # as it would render the matrix undetermined
                if m[1] == m[0] and modified[m][s] == 0:
                    d.append(1)
                else:
                    d.append(modified[m][s])
            l_modified.append(d)

    if fp is not None:
        filepath = Path(fp)
    else:
        filepath = (
                DATA_DIR / "export" / "scenario diff files"
        )

    if not os.path.exists(filepath):
        os.makedirs(filepath)

    filepath = filepath / f"scenario_diff_{date.today()}.xlsx"

    pd.DataFrame(l_modified, columns=[""] * len(columns)).to_excel(
        filepath, index=False
    )

    print(f"Scenario difference file exported to {filepath}!")

    print("Adding extra exchanges to the original database...")

    dict_bio = exp.create_names_and_indices_of_B_matrix()

    for ds in origin_db:
        exc_to_add = []
        for exc in [
            e
            for e in modified
            if e[0]
            == (
                ds["name"],
                ds["reference product"],
                ds["database"],
                ds["location"],
                ds["unit"],
            ) and modified[e]["original"] == 0
        ]:
            if isinstance(exc[1][1], tuple):
                exc_to_add.append(
                    {
                        "amount": 0,
                        "input": (
                            "biosphere3",
                            exp.get_bio_code(
                                dict_bio[(exc[1][0], exc[1][1], exc[1][2], exc[1][3])]
                            ),
                        ),
                        "type": "biosphere",
                        "name": exc[1][0],
                        "unit": exc[1][3],
                        "categories": exc[1][1],
                    }
                )

            else:
                exc_to_add.append(
                    {
                        "amount": 0,
                        "type": "technosphere",
                        "product": exc[1][1],
                        "name": exc[1][0],
                        "unit": exc[1][4],
                        "location": exc[1][3],
                    }
                )

        if len(exc_to_add) > 0:
            ds["exchanges"].extend(exc_to_add)

    print("Adding extra activities to the original database...")

    list_act = [
        (a["name"], a["reference product"], a["database"], a["location"], a["unit"])
        for a in origin_db
    ]
    list_to_add = [
        m[0] for m in modified if modified[m]["original"] == 0 and m[0] not in list_act
    ]
    list_to_add = list(set(list_to_add))

    data = []
    for add in list_to_add:
        act_to_add = {
            "location": add[3],
            "name": add[0],
            "reference product": add[1],
            "unit": add[4],
            "database": add[2],
            "code": str(uuid.uuid4().hex),
            "exchanges": [],
        }

        acts = (act for act in modified if act[0] == add)

        for act in acts:
            if isinstance(act[1][1], tuple):
                # this is a biosphere flow
                act_to_add["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "loc": 0,
                        "amount": 0,
                        "type": "biosphere",
                        "input": (
                            "biosphere3",
                            exp.get_bio_code(
                                dict_bio[(act[1][0], act[1][1], act[1][2], act[1][3])]
                            ),
                        ),
                        "name": act[1][0],
                        "unit": act[1][3],
                        "categories": act[1][1],
                    }
                )

            else:

                if act[1] == act[0]:
                    act_to_add["exchanges"].append(
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": act[1][1],
                            "name": act[1][0],
                            "unit": act[1][4],
                            "location": act[1][3],
                        }
                    )

                else:

                    act_to_add["exchanges"].append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 0,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": act[1][1],
                            "name": act[1][0],
                            "unit": act[1][4],
                            "location": act[1][3],
                        }
                    )
        data.append(act_to_add)
    origin_db.extend(data)

    return origin_db
