import uuid
from copy import deepcopy
from datetime import date
from functools import lru_cache
from itertools import chain
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import xarray as xr
import yaml
from constructive_geometries import resolved_row
from wurst import log
from wurst import searching as ws
from wurst.searching import equals, get_many, reference_product
from wurst.transformations.uncertainty import rescale_exchange

from . import geomap
from .export import *

FUELS_PROPERTIES = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
CROPS_PROPERTIES = DATA_DIR / "fuels" / "crops_properties.yml"
CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"

STEEL_RECYCLING_SHARES = DATA_DIR / "steel" / "steel_recycling_shares.csv"
METALS_RECYCLING_SHARES = DATA_DIR / "metals" / "metals_recycling_shares.csv"

REMIND_TO_FUELS = DATA_DIR / "steel" / "remind_fuels_correspondance.txt"
EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"


def eidb_label(model: str, scenario: str, year: int) -> str:
    """
    Return a label to name a scenario.
    :param model: IAM model
    :param scenario: SSP/RCP scenario
    :param year: year
    :return: scenario label, str.
    """
    return f"ecoinvent_{model}_{scenario}_{year}"


@lru_cache(maxsize=None)
def get_fuel_properties() -> dict:
    """
    Loads a yaml file into a dictionary.
    This dictionary contains lower heating values
    CO2 emission factors, biogenic carbon share, for a number of fuel types.
    Mostly taken from ecoinvent and
    https://www.engineeringtoolbox.com/fuels-higher-calorific-values-d_169.html
    :return: dictionary that contains lower heating values
    :rtype: dict
    """

    with open(FUELS_PROPERTIES, "r") as stream:
        fuel_props = yaml.safe_load(stream)

    return fuel_props


def get_crops_properties() -> dict:
    """
    Return a dictionary with crop names as keys and IAM labels as values
    relating to land use change CO2 per crop type
    :return: dict
    """
    with open(CROPS_PROPERTIES, "r") as stream:
        crop_props = yaml.safe_load(stream)

    return crop_props


def get_efficiency_ratio_solar_PV() -> xr.DataArray:
    """
    Return an array with PV module efficiencies in function of year and technology.
    :return: xr.DataArray with PV module efficiencies
    """

    df = pd.read_csv(EFFICIENCY_RATIO_SOLAR_PV, sep=";")

    return df.groupby(["technology", "year"]).mean()["efficiency"].to_xarray()


def get_clinker_ratio_ecoinvent(version: str) -> Dict[Tuple[str, str], float]:
    """
    Return a dictionary with (cement names, location) as keys and clinker-to-cement ratios as values,
    as found in ecoinvent.
    :return: dict
    """
    if version == "3.5":
        fp = CLINKER_RATIO_ECOINVENT_35
    else:
        fp = CLINKER_RATIO_ECOINVENT_36

    with open(fp) as f:
        d = {}
        for val in csv.reader(f, delimiter=","):
            d[(val[0], val[1])] = float(val[2])
    return d


def get_clinker_ratio_remind(year: int) -> xr.DataArray:
    """
    Return an array with the average clinker-to-cement ratio per year and per region, as given by REMIND.
    :return: xarray
    :return:
    """
    df = pd.read_csv(CLINKER_RATIO_REMIND, sep=",")

    return df.groupby(["region", "year"]).mean()["value"].to_xarray().interp(year=year)


def rev_index(inds: dict) -> dict:
    return {v: k for k, v in inds.items()}


def create_codes_and_names_of_tech_matrix(db: List[dict]):
    """
    Create a dictionary a tuple (activity name, reference product,
    unit, location) as key, and its code as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (
            i["name"],
            i["reference product"],
            i["unit"],
            i["location"],
        ): i["code"]
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
    # Collect a dictionary with activities' names and corresponding codes
    codes_names = create_codes_and_names_of_tech_matrix(original_db)
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
    # {(name, ref_prod, loc, database, unit):row/col index in A matrix}
    rev_ind_A = exp.rev_index(exp.create_names_and_indices_of_A_matrix())

    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()

    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = dict()
    for x in coords_A:
        if (rev_ind_A[x[0]], rev_ind_A[x[1]]) in original:
            original[(rev_ind_A[x[0]], rev_ind_A[x[1]])] += x[2] * -1
        else:
            original[(rev_ind_A[x[0]], rev_ind_A[x[1]])] = x[2] * -1

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

        new = dict()
        for x in new_coords_A:
            if (new_rev_ind_A[x[0]], new_rev_ind_A[x[1]]) in new:
                new[(new_rev_ind_A[x[0]], new_rev_ind_A[x[1]])] += x[2] * -1
            else:
                new[(new_rev_ind_A[x[0]], new_rev_ind_A[x[1]])] = x[2] * -1

        new_coords_B = exp.create_B_matrix_coordinates()
        new.update(
            {(new_rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] * -1 for x in new_coords_B}
        )
        # List activities that are in the new database but not in the original one
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

    list_scenarios = ["original"] + [
        s["model"] + " - " + s["pathway"] + " - " + str(s["year"]) for s in scenarios
    ]

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
        "flow type",
        "original",
    ]
    columns.extend(
        [a["model"] + " - " + a["pathway"] + " - " + str(a["year"]) for a in scenarios]
    )

    print("Export a scenario difference file.")

    l_modified = [columns]

    for m in modified:
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
                "biosphere",
            ]
        elif m[1] == m[0] and any(v < 0 for v in modified[m].values()):
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
                "production",
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
                "technosphere",
            ]

        for s in list_scenarios:
            # we do not want a zero here,
            # as it would render the matrix undetermined
            if m[1] == m[0] and modified[m][s] == 0:
                d.append(1)
            elif m[1] == m[0] and modified[m][s] < 0:
                d.append(modified[m][s] * -1)
            else:
                d.append(modified[m][s])
        l_modified.append(d)

    if fp is not None:
        filepath = Path(fp)
    else:
        filepath = DATA_DIR / "export" / "scenario diff files"

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
            )
            and modified[e]["original"] == 0
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

    acts_to_add = []
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
        excs_to_add = []
        for act in acts:
            if isinstance(act[1][1], tuple):
                # this is a biosphere flow
                excs_to_add.append(
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
                    excs_to_add.append(
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

                    excs_to_add.append(
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
        act_to_add["exchanges"].extend(excs_to_add)
        acts_to_add.append(act_to_add)

    origin_db.extend(acts_to_add)

    return origin_db


def relink_technosphere_exchanges(
    ds,
    data,
    model,
    cache,
    exclusive=True,
    drop_invalid=False,
    biggest_first=False,
    contained=True,
):
    """Find new technosphere providers based on the location of the dataset.
    Designed to be used when the dataset's location changes, or when new datasets are added.
    Uses the name, reference product, and unit of the exchange to filter possible inputs. These must match exactly. Searches in the list of datasets ``data``.
    Will only search for providers contained within the location of ``dataset``, unless ``contained`` is set to ``False``, all providers whose location intersects the location of ``dataset`` will be used.
    A ``RoW`` provider will be added if there is a single topological face in the location of ``dataset`` which isn't covered by the location of any providing activity.
    If no providers can be found, `relink_technosphere_exchanes` will try to add a `RoW` or `GLO` providers, in that order, if available. If there are still no valid providers, a ``InvalidLink`` exception is raised, unless ``drop_invalid`` is ``True``, in which case the exchange will be deleted.
    Allocation between providers is done using ``allocate_inputs``; results seem strange if ``contained=False``, as production volumes for large regions would be used as allocation factors.
    Input arguments:
        * ``dataset``: The dataset whose technosphere exchanges will be modified.
        * ``data``: The list of datasets to search for technosphere product providers.
        * ``model``: The IAM model
        * ``exclusive``: Bool, default is ``True``. Don't allow overlapping locations in input providers.
        * ``drop_invalid``: Bool, default is ``False``. Delete exchanges for which no valid provider is available.
        * ``biggest_first``: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant is ``exclusive`` is ``True``.
        * ``contained``: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.
        * ``iam_regions``: List, lists IAM regions, if additional ones need to be defined.
    Modifies the dataset in place; returns the modified dataset."""

    MESSAGE = "Relinked technosphere exchange of {}/{}/{} from {}/{} to {}/{}."
    DROPPED = "Dropped technosphere exchange of {}/{}/{}; no valid providers."
    new_exchanges = []
    technosphere = lambda x: x["type"] == "technosphere"

    geomatcher = geomap.Geomap(model=model)

    list_loc = [k if isinstance(k, str) else k[1] for k in geomatcher.geo.keys()]

    for exc in filter(technosphere, ds["exchanges"]):

        try:
            e = cache[ds["location"]][
                (exc["name"], exc["product"], exc["location"], exc["unit"])
            ]

            if isinstance(e, tuple):
                name, prod, unit, loc = e

                new_exchanges.append(
                    {
                        "name": name,
                        "product": prod,
                        "unit": unit,
                        "location": loc,
                        "type": "technosphere",
                        "amount": exc["amount"],
                    }
                )

            else:

                new_exchanges.extend(
                    [
                        {
                            "name": i[0],
                            "product": i[1],
                            "unit": i[3],
                            "location": i[2],
                            "type": "technosphere",
                            "amount": exc["amount"] * i[-1],
                        }
                        for i in e
                    ]
                )

        except KeyError:
            possible_datasets = [
                x for x in get_possibles(exc, data) if x["location"] in list_loc
            ]
            possible_locations = [obj["location"] for obj in possible_datasets]

            if ds["location"] in possible_locations:
                exc["location"] = ds["location"]
                new_exchanges.append(exc)
                continue

            possible_locations = [
                (model.upper(), p) if p in geomatcher.iam_regions else p
                for p in possible_locations
            ]

            if len(possible_datasets) > 0:

                with resolved_row(possible_locations, geomatcher.geo) as g:
                    func = g.contained if contained else g.intersects

                    if ds["location"] in geomatcher.iam_regions:
                        location = (model.upper(), ds["location"])
                    else:
                        location = ds["location"]

                    gis_match = func(
                        location,
                        include_self=True,
                        exclusive=exclusive,
                        biggest_first=biggest_first,
                        only=possible_locations,
                    )

                kept = [
                    ds
                    for loc in gis_match
                    for ds in possible_datasets
                    if ds["location"] == loc
                ]

                if kept:
                    missing_faces = geomatcher.geo[location].difference(
                        set.union(*[geomatcher.geo[obj["location"]] for obj in kept])
                    )
                    if missing_faces and "RoW" in possible_locations:
                        kept.extend(
                            [
                                obj
                                for obj in possible_datasets
                                if obj["location"] == "RoW"
                            ]
                        )
                elif "RoW" in possible_locations:
                    kept = [
                        obj for obj in possible_datasets if obj["location"] == "RoW"
                    ]

                if not kept and "GLO" in possible_locations:
                    kept = [
                        obj for obj in possible_datasets if obj["location"] == "GLO"
                    ]

                if not kept and any(
                    x in possible_locations for x in ["RER", "EUR", "WEU"]
                ):
                    kept = [
                        obj
                        for obj in possible_datasets
                        if obj["location"] in ["RER", "EUR", "WEU"]
                    ]

                if not kept:
                    if drop_invalid:
                        log(
                            {
                                "function": "relink_technosphere_exchanges",
                                "message": DROPPED.format(
                                    exc["name"], exc["product"], exc["unit"]
                                ),
                            },
                            ds,
                        )
                        continue
                    else:
                        new_exchanges.append(exc)
                        continue

                allocated, share = allocate_inputs(exc, kept)

                for obj in allocated:
                    log(
                        {
                            "function": "relink_technosphere_exchanges",
                            "message": MESSAGE.format(
                                exc["name"],
                                exc["product"],
                                exc["unit"],
                                exc["amount"],
                                ds["location"],
                                obj["amount"],
                                obj["location"],
                            ),
                        },
                        ds,
                    )

                new_exchanges.extend(allocated)

                if ds["location"] in cache:
                    cache[ds["location"]][
                        (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    ] = [
                        (e["name"], e["product"], e["location"], e["unit"], s)
                        for e, s in zip(allocated, share)
                    ]

                else:

                    cache[ds["location"]] = {
                        (exc["name"], exc["product"], exc["location"], exc["unit"],): [
                            (e["name"], e["product"], e["location"], e["unit"], s)
                            for e, s in zip(allocated, share)
                        ]
                    }
            else:
                new_exchanges.append(exc)
                # add to cache
                if ds["location"] in cache:
                    cache[ds["location"]][
                        (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    ] = (
                        exc["name"],
                        exc["product"],
                        exc["location"],
                        exc["unit"],
                    )

                else:
                    cache[ds["location"]] = {
                        (exc["name"], exc["product"], exc["location"], exc["unit"],): (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    }

    ds["exchanges"] = [
        exc for exc in ds["exchanges"] if exc["type"] != "technosphere"
    ] + new_exchanges

    return cache, ds


def allocate_inputs(exc, lst):
    """Allocate the input exchanges in ``lst`` to ``exc``, using production volumes where possible, and equal splitting otherwise.
    Always uses equal splitting if ``RoW`` is present."""
    has_row = any((x["location"] in ("RoW", "GLO") for x in lst))
    pvs = [reference_product(o).get("production volume") or 0 for o in lst]
    if all((x > 0 for x in pvs)) and not has_row:
        # Allocate using production volume
        total = sum(pvs)
    else:
        # Allocate evenly
        total = len(lst)
        pvs = [1 for _ in range(total)]

    def new_exchange(exc, location, factor):
        cp = deepcopy(exc)
        cp["location"] = location
        return rescale_exchange(cp, factor)

    return [
        new_exchange(exc, obj["location"], factor / total)
        for obj, factor in zip(lst, pvs)
    ], [p / total for p in pvs]


def get_possibles(exchange, data):
    """Filter a list of datasets ``data``, returning those with the save name, reference product, and unit as in ``exchange``.
    Returns a generator."""
    key = (exchange["name"], exchange["product"], exchange["unit"])
    list_exc = []
    for ds in data:
        if (ds["name"], ds["reference product"], ds["unit"]) == key:
            list_exc.append(ds)
    return list_exc


def default_global_location(database):
    """Set missing locations to ```GLO``` for datasets in ``database``.
    Changes location if ``location`` is missing or ``None``. Will add key ``location`` if missing."""
    for ds in get_many(database, *[equals("location", None)]):
        ds["location"] = "GLO"
    return database
