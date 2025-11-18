"""
This module contains classes for validating datasets after they have been transformed.
"""

import csv
import math

import numpy as np
import pandas as pd
import yaml

from .filesystem_constants import DATA_DIR
from .geomap import Geomap
from .logger import create_logger
from .utils import rescale_exchanges, get_uuids
from .inventory_imports import get_classifications, get_biosphere_code
import country_converter as coco
import wurst.searching as ws

logger = create_logger("validation")


def load_electricity_keys():
    # load electricity keys from data/utils/validation/electricity.yaml

    with open(DATA_DIR / "utils/validation/electricity.yaml", encoding="utf-8") as f:
        electricity_keys = yaml.safe_load(f)

    return electricity_keys


def load_waste_keys():
    # load waste keys from data/utils/validation/waste flows.yaml

    with open(DATA_DIR / "utils/validation/waste flows.yaml", encoding="utf-8") as f:
        waste_keys = yaml.safe_load(f)

    return waste_keys


def load_waste_flows_exceptions():
    # load waste flows exceptions.yaml from data/utils/validation/waste flows exceptions.yaml

    with open(
        DATA_DIR / "utils/validation/waste flows exceptions.yaml", encoding="utf-8"
    ) as f:
        waste_flows_exceptions = yaml.safe_load(f)

    return waste_flows_exceptions


def load_circular_exceptions():
    # load circular exceptions.yaml from data/utils/validation/circular exceptions.yaml.yaml

    with open(
        DATA_DIR / "utils/validation/circular exceptions.yaml", encoding="utf-8"
    ) as f:
        circular_exceptions = yaml.safe_load(f)

    return circular_exceptions


def load_car_exhaust_pollutants():
    fp = DATA_DIR / "transport" / "car" / "EF_HBEFA42_exhaust.csv"
    nested_dict = {}

    with open(str(fp), mode="r") as file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(file)

        # Iterate through each row in the CSV
        for row in csv_reader:
            powertrain = row["powertrain"]
            euro_class = row["euro_class"]
            component = row["component"]
            value = float(row["value"])

            # Build the nested dictionary
            if powertrain not in nested_dict:
                nested_dict[powertrain] = {}

            if euro_class not in nested_dict[powertrain]:
                nested_dict[powertrain][euro_class] = {}

            nested_dict[powertrain][euro_class][component] = value

    return nested_dict


def load_truck_exhaust_pollutants():
    fp = DATA_DIR / "transport" / "truck" / "EF_HBEFA42_exhaust.csv"
    nested_dict = {}

    with open(str(fp), mode="r") as file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(file)

        # Iterate through each row in the CSV
        for row in csv_reader:
            powertrain = row["powertrain"]
            euro_class = row["euro_class"]
            component = row["component"]
            size = row["size"]
            value = float(row["value"])

            # Build the nested dictionary
            if powertrain not in nested_dict:
                nested_dict[powertrain] = {}

            if euro_class not in nested_dict[powertrain]:
                nested_dict[powertrain][euro_class] = {}

            if size not in nested_dict[powertrain][euro_class]:
                nested_dict[powertrain][euro_class][size] = {}

            nested_dict[powertrain][euro_class][size][component] = value

    return nested_dict


def clean_up(exc):
    """Remove keys from ``exc`` that are not in the schema."""

    FORBIDDEN_FIELDS_TECH = [
        "categories",
    ]

    FORBIDDEN_FIELDS_BIO = ["location", "product"]

    for field in list(exc.keys()):
        if exc[field] is None or exc[field] == "None":
            del exc[field]
            continue

        if exc["type"] == "biosphere" and field in FORBIDDEN_FIELDS_BIO:
            del exc[field]
        if exc["type"] == "technosphere" and field in FORBIDDEN_FIELDS_TECH:
            del exc[field]

    return exc


def _load_mining_shares_mapping_for_validation():
    """
    Minimal local loader to avoid importing metals.py and creating a cycle.
    Mirrors the basic behavior of load_mining_shares_mapping used for checks.
    """
    fp = DATA_DIR / "metals" / "mining_shares_mapping.xlsx"
    df = pd.read_excel(fp, sheet_name="Shares_mapping")
    df.columns = df.columns.str.replace("Year ", "", regex=False)
    return df


def convert_numpy_generics_to_float(
    records, *, in_place: bool = False, convert_keys: bool = False
) -> list:
    """
    Walk a list of dictionaries and convert any NumPy scalar (np.generic)
    to a Python float. Nested dicts/lists/tuples/sets are handled.

    :param records: List of dictionaries to sanitize.
    :param in_place: If True, modify the input records in place. If False,
        return a new sanitized copy.
    :param convert_keys: If True, also convert dictionary keys that are
        NumPy scalars to floats (and then to strings for JSON safety).
    :return: Sanitized list of dictionaries.
    """

    def _to_float_if_np_scalar(x):
        # Only convert NumPy scalars; leave arrays and other types alone.
        if isinstance(x, np.generic):
            # This will turn np.bool_(True) -> 1.0 and np.int64(3) -> 3.0
            # which is what the user requested (everything to float).
            return float(x)
        return x

    def _sanitize(obj):
        # Convert NumPy scalar immediately
        if isinstance(obj, np.generic):
            return _to_float_if_np_scalar(obj)

        # Dicts
        if isinstance(obj, dict):
            if in_place:
                # Potentially rewrite keys if requested
                if convert_keys:
                    # Rebuild only if any key needs conversion
                    needs_rebuild = any(isinstance(k, np.generic) for k in obj.keys())
                    if needs_rebuild:
                        new_obj = {}
                        for k, v in obj.items():
                            new_k = _to_float_if_np_scalar(k)
                            if isinstance(new_k, float):
                                # Keys must be strings for JSON, so cast to str
                                new_k = str(new_k)
                            new_obj[new_k] = _sanitize(v)
                        obj.clear()
                        obj.update(new_obj)
                    else:
                        for k in list(obj.keys()):
                            obj[k] = _sanitize(obj[k])
                else:
                    for k in list(obj.keys()):
                        obj[k] = _sanitize(obj[k])
                return obj
            else:
                if convert_keys:
                    new_obj = {}
                    for k, v in obj.items():
                        new_k = _to_float_if_np_scalar(k)
                        if isinstance(new_k, float):
                            new_k = str(new_k)  # JSON-safe key
                        new_obj[new_k] = _sanitize(v)
                    return new_obj
                else:
                    return {k: _sanitize(v) for k, v in obj.items()}

        # Lists
        if isinstance(obj, list):
            if in_place:
                for i in range(len(obj)):
                    obj[i] = _sanitize(obj[i])
                return obj
            else:
                return [_sanitize(v) for v in obj]

        # Tuples: return same type
        if isinstance(obj, tuple):
            return tuple(_sanitize(v) for v in obj)

        # Sets: keep as set (note: not JSON-serializable by default)
        if isinstance(obj, set):
            return {_sanitize(v) for v in obj}

        # Anything else (including numpy arrays) left as-is
        return obj

    return _sanitize(records)


class BaseDatasetValidator:
    """
    Base class for validating datasets after they have been transformed.
    """

    def __init__(
        self,
        model,
        scenario,
        year,
        regions,
        database,
        original_database=None,
        db_name=None,
        biosphere_name=None,
        version=None,
        system_model="cutoff",
    ):
        self.original_database = original_database
        self.database = database
        self.model = model
        self.scenario = scenario
        self.year = year
        self.regions = regions
        self.db_name = db_name
        self.geo = Geomap(model)
        self.minor_issues_log = []
        self.major_issues_log = []
        self.biosphere_name = biosphere_name
        self.biosphere_codes = get_biosphere_code(version)
        self.classifications = get_classifications()

    def check_matrix_squareness(self):
        """
        Check if the number of products equals the number of activities
        """

        products, activities = [], []

        for ds in self.database:
            activities.append(
                (ds["name"], ds["reference product"], ds["unit"], ds["location"])
            )
            for e in ds["exchanges"]:
                if e["type"] == "production":
                    products.append((e["name"], e["product"], e["unit"], e["location"]))

        if len(list(set(activities))) != len(list(set(products))):
            print(
                f"WARNING: matrix is not square: {len(list(set(activities)))} activities, {len(list(set(products)))} products."
            )

    def check_uncertainty(self):
        MANDATORY_UNCERTAINTY_FIELDS = {
            2: {"loc", "scale"},
            3: {"loc", "scale"},
            4: {"minimum", "maximum"},
            5: {"loc", "minimum", "maximum"},
            6: {"loc", "minimum", "maximum"},
            7: {"minimum", "maximum"},
            8: {"loc", "scale", "shape"},
            9: {"loc", "scale", "shape"},
            10: {"loc", "scale", "shape"},
            11: {"loc", "scale", "shape"},
            12: {"loc", "scale", "shape"},
        }

        for ds in self.database:
            for exc in ds["exchanges"]:
                if int(exc.get("uncertainty type", 0)) not in [0, 1]:

                    if not all(
                        f in exc
                        for f in MANDATORY_UNCERTAINTY_FIELDS[
                            int(exc["uncertainty type"])
                        ]
                    ):
                        message = (
                            f"Exchange {exc['name']} has incomplete uncertainty data."
                        )
                        self.log_issue(ds, "incomplete uncertainty data", message)

                    try:
                        if exc.get("uncertainty type", 0) == 2 and "loc" not in exc:
                            if exc["amount"] < 0:
                                exc["loc"] = float(math.log(exc["amount"] * -1))
                                exc["negative"] = True
                            else:
                                exc["loc"] = float(math.log(exc["amount"]))

                        if exc.get("uncertainty type", 0) == 3 and "loc" not in exc:
                            exc["loc"] = float(exc["amount"])

                        if exc.get("uncertainty type", 0) == 5:
                            if "loc" not in exc:
                                print(
                                    f"'loc' not found in exchange {exc['name']} in dataset {ds['name']}{ds['location']}"
                                )
                                exc["loc"] = float(exc["amount"])
                            if exc["minimum"] > exc["loc"]:
                                message = (
                                    f"Exchange {exc['name']} - {exc['location']} has a minimum value greater than the loc value."
                                    f"Min: {exc['minimum']}, Max: {exc['maximum']}, Loc: {exc['loc']}"
                                )
                                self.log_issue(
                                    ds,
                                    "uncertainty minimum greater than loc",
                                    message,
                                    issue_type="minor",
                                )

                                # fix it
                                exc["minimum"] = exc["loc"]
                            if exc["maximum"] < exc["loc"]:
                                message = (
                                    f"Exchange {exc['name']} - {exc['location']} has a maximum value lower than the loc value."
                                    f"Min: {exc['minimum']}, Max: {exc['maximum']}, Loc: {exc['loc']}"
                                )
                                self.log_issue(
                                    ds,
                                    "uncertainty maximum less than loc",
                                    message,
                                    issue_type="minor",
                                )

                                # fix it
                                exc["maximum"] = exc["loc"]

                    except KeyError:
                        print(f"Issue with exchange {exc}")
                        raise

    def check_datasets_integrity(self):
        # Verify no unintended loss of datasets
        original_activities = [
            (ds["name"], ds["reference product"], ds["location"])
            for ds in self.original_database
        ]

        new_activities = [
            (ds["name"], ds["reference product"], ds["location"])
            for ds in self.database
        ]

        for ds in original_activities:
            if ds not in new_activities:
                message = f"Dataset {ds} was lost during transformation"
                self.log_issue(
                    {"name": ds[0], "reference product": ds[1], "location": ds[2]},
                    "lost dataset",
                    message,
                    issue_type="major",
                )

        # Ensure no datasets have null or empty values for required keys
        required_activity_keys = [
            "name",
            "location",
            "reference product",
            "unit",
            "exchanges",
        ]
        for dataset in self.database:
            for key in required_activity_keys:
                if key not in dataset or not dataset[key]:
                    message = f"Dataset {dataset.get('name', 'Unknown')} is missing required key: {key}"
                    self.log_issue(dataset, "missing key", message, issue_type="major")

            # Making sure that every technosphere exchange has a `product` field
            for exc in dataset.get("exchanges", []):
                if exc["type"] == "technosphere" and exc.get("product") is None:
                    # find it in new_activities based on the name and location
                    # of the exchange
                    candidate = [
                        x
                        for x in new_activities
                        if x[0] == exc["name"] and x[2] == exc["location"]
                    ]
                    if len(candidate) == 1:
                        exc["product"] = candidate[0][1]
                    elif len(candidate) > 1:
                        message = f"Exchange {exc['name']} in {dataset['name']} has multiple possible products: {candidate}."
                        self.log_issue(
                            dataset,
                            "multiple exchange products",
                            message,
                            issue_type="major",
                        )
                    else:
                        message = f"Exchange {exc['name']} in {dataset['name']} is missing the 'product' key."
                        self.log_issue(
                            dataset,
                            "missing exchange product",
                            message,
                            issue_type="major",
                        )

        # remove empty fields
        self.database = [
            {k: v for k, v in dataset.items() if v is not None}
            for dataset in self.database
        ]

    def check_for_orphaned_datasets(self):
        # check the presence of orphan datasets
        consumed_datasets = {
            (exc["name"], exc["product"], exc["location"])
            for ds in self.database
            for exc in ds.get("exchanges", [])
            if exc["type"] == "technosphere"
        }
        for dataset in self.database:
            key = (dataset["name"], dataset["reference product"], dataset["location"])
            if key not in consumed_datasets and not any(
                x not in dataset["name"] for x in ["market for", "market group for"]
            ):
                message = f"Orphaned dataset found: {dataset['name']}"
                self.log_issue(dataset, "orphaned dataset", message)

    def check_new_location(self):
        original_locations = set([ds["location"] for ds in self.original_database])
        new_locations = set([ds["location"] for ds in self.database])

        for loc in new_locations:
            if loc not in original_locations:
                if loc not in self.regions:
                    try:
                        self.geo.ecoinvent_to_iam_location(loc)
                    except ValueError:
                        message = f"New unregistered location found: {loc}"
                        self.log_issue({"location": loc}, "new location", message)

    def validate_dataset_structure(self):
        # Check that all datasets have a list of exchanges and each exchange has a type
        for dataset in self.database:
            if not isinstance(dataset.get("exchanges"), list):
                message = (
                    f"Dataset {dataset['name']} does not have a list of exchanges."
                )
                self.log_issue(
                    dataset, "missing exchanges", message, issue_type="major"
                )

            for exchange in dataset.get("exchanges", []):
                if "type" not in exchange:
                    message = f"Exchange in dataset {dataset['name']} is missing the 'type' key."
                    self.log_issue(
                        dataset, "missing exchange type", message, issue_type="major"
                    )

                if not isinstance(exchange["amount"], float):
                    exchange["amount"] = float(exchange["amount"])

            # if list of exchanges is 2, and the two exchanges are identical
            if len(dataset.get("exchanges", [])) == 2:
                if (
                    dataset["exchanges"][0]["name"],
                    dataset["exchanges"][0].get("product"),
                    dataset["exchanges"][0].get("location"),
                ) == (
                    dataset["exchanges"][1]["name"],
                    dataset["exchanges"][1].get("product"),
                    dataset["exchanges"][1].get("location"),
                ):
                    message = f"Dataset {dataset['name']} has two identical exchanges."
                    self.log_issue(
                        dataset, "identical exchanges", message, issue_type="major"
                    )

    def verify_data_consistency(self):
        # Check for negative amounts in exchanges that should only have positive amounts
        WASTE_KEYS = load_waste_keys()
        non_negative_exchanges = load_waste_flows_exceptions()

        for dataset in self.database:
            for exchange in dataset.get("exchanges", []):
                if exchange.get("amount", 0) < 0 and exchange["type"] == "production":
                    # check that `name` and `product` field of `exchange`
                    # do not contain substring in `WASTE_KEYS`
                    if not any([x in exchange["name"].lower() for x in WASTE_KEYS]):
                        if not any(
                            [x in exchange["product"].lower() for x in WASTE_KEYS]
                        ):
                            message = f"Dataset {dataset['name']} has a negative production amount."
                            self.log_issue(dataset, "negative production", message)

                if (
                    any(item in WASTE_KEYS for item in exchange["name"].split())
                    and exchange["type"] == "technosphere"
                    and exchange["unit"]
                    not in ["megajoule", "kilowatt hour", "ton kilometer"]
                    and exchange["name"] not in non_negative_exchanges
                ):
                    if exchange.get("amount", 0) > 0:
                        message = f"Positive technosphere amount for a possible waste exchange {exchange['name']}, {exchange['amount']}."
                        self.log_issue(dataset, "positive waste", message)

    def check_relinking_logic(self):
        # Verify that technosphere exchanges link to existing datasets
        dataset_names = {
            (ds["name"], ds["reference product"], ds["location"])
            for ds in self.database
        }
        for dataset in self.database:
            for exchange in dataset.get("exchanges", []):
                if (
                    exchange["type"] == "technosphere"
                    and (
                        exchange["name"],
                        exchange["product"],
                        exchange["location"],
                    )
                    not in dataset_names
                ):
                    message = f"Dataset {dataset['name']} in {dataset['location']} links to a non-existing dataset: {exchange['name']} in {exchange['location']}."
                    self.log_issue(
                        dataset, "non-existing dataset", message, issue_type="major"
                    )

    def check_for_duplicates(self):
        """Check for the presence of duplicates"""

        activities = [
            (x["name"].lower(), x["reference product"].lower(), x["location"])
            for x in self.database
        ]

        if len(activities) != len(set(activities)):
            seen = set()
            self.database = [
                x
                for x in self.database
                if (x["name"].lower(), x["reference product"].lower(), x["location"])
                not in seen
                and not seen.add(
                    (x["name"].lower(), x["reference product"].lower(), x["location"])
                )
            ]

            # log duplicates
            for x in set(activities):
                if activities.count(x) > 1:
                    message = f"Duplicate found (and removed): {x}"
                    self.log_issue(
                        {"name": x[0], "reference product": x[1], "location": x[2]},
                        "duplicate",
                        message,
                        issue_type="major",
                    )

    def check_for_circular_references(self):
        circular_exceptions = load_circular_exceptions()
        for dataset in self.database:
            key = (dataset["name"], dataset["reference product"], dataset["location"])
            for exchange in dataset["exchanges"]:
                if (
                    exchange["type"] == "technosphere"
                    and (
                        exchange["name"],
                        exchange.get("product"),
                        exchange.get("location"),
                    )
                    == key
                ):
                    if (
                        exchange["amount"] >= 0.2
                        and dataset["name"] not in circular_exceptions
                    ):
                        message = f"Dataset {dataset['name']} potentially has a circular reference to itself."
                        self.log_issue(dataset, "circular reference", message)

    def check_database_name(self):

        uuids = get_uuids(self.database)

        for ds in self.database:
            ds["database"] = self.db_name
            # ds["code"] = uuids[(ds["name"], ds["reference product"], ds["location"])]
            for exc in ds["exchanges"]:
                if exc["type"] in ["production", "technosphere"]:
                    if "input" in exc:
                        del exc["input"]
                if exc["type"] == "biosphere":
                    # check that the first item of the code field
                    # corresponds to biosphere_name
                    if "input" in exc:
                        if exc["input"][0] != self.biosphere_name:
                            exc["input"] = (self.biosphere_name, exc["input"][1])
                    else:
                        exc["input"] = (
                            self.biosphere_name,
                            self.biosphere_codes[
                                exc["name"],
                                exc["categories"][0],
                                (
                                    exc["categories"][1]
                                    if len(exc["categories"]) > 1
                                    else "unspecified"
                                ),
                                exc["unit"],
                            ],
                        )

                # if exc["type"] == "technosphere":
                #    exc["input"] = (
                #        self.db_name,
                #        uuids[exc["name"], exc["product"], exc["location"]],
                #    )

    def remove_unused_fields(self):
        """
        Remove fields which have no values from each dataset in database.
        """

        for dataset in self.database:
            for key in list(dataset.keys()):
                if not dataset[key]:
                    del dataset[key]

    def correct_fields_format(self):
        """
        Correct the format of some fields.
        """

        for dataset in self.database:
            if "parameters" in dataset:
                if not isinstance(dataset["parameters"], list):
                    dataset["parameters"] = [dataset["parameters"]]
            if "categories" in dataset:
                if not isinstance(dataset["categories"], tuple):
                    dataset["categories"] = tuple(dataset["categories"])

            for exc in dataset["exchanges"]:
                # check that `amount` is of type `float`
                if np.isnan(exc["amount"]):
                    ValueError(
                        f"Amount is NaN in exchange {exc} in dataset {dataset['name'], dataset['location']}"
                    )
                if not isinstance(exc["amount"], float):
                    exc["amount"] = float(exc["amount"])

            # remove fields that are None
            for key, value in list(dataset.items()):
                if value is None:
                    del dataset[key]

        # we also want to remove any numpy generics
        # that would prevent json serialization
        self.database = convert_numpy_generics_to_float(self.database)

    def check_amount_format(self):
        """
        Check that the `amount` field is of type `float`.
        """

        for dataset in self.database:
            for exc in dataset["exchanges"]:
                if not isinstance(exc["amount"], float):
                    exc["amount"] = float(exc["amount"])

                if isinstance(exc["amount"], (np.float64, np.ndarray)):
                    exc["amount"] = float(exc["amount"])

            for k, v in dataset.items():
                if isinstance(v, dict):
                    for i, j in v.items():
                        if isinstance(j, (np.float64, np.ndarray)):
                            v[i] = float(v[i])

            for e in dataset["exchanges"]:
                for k, v in e.items():
                    if isinstance(v, (np.float64, np.ndarray)):
                        e[k] = float(e[k])

    def reformat_parameters(self):
        for ds in self.database:
            if "parameters" in ds:
                if not isinstance(ds["parameters"], list):
                    if isinstance(ds["parameters"], dict):
                        ds["parameters"] = [
                            {"name": k, "amount": v}
                            for k, v in ds["parameters"].items()
                        ]
                    else:
                        ds["parameters"] = [ds["parameters"]]
                else:
                    if isinstance(ds["parameters"][0], dict):
                        ds["parameters"] = [
                            {"name": k, "amount": v}
                            for o in ds["parameters"]
                            for k, v in o.items()
                        ]

            for key, value in list(ds.items()):
                if not value:
                    del ds[key]

            ds["exchanges"] = [clean_up(exc) for exc in ds["exchanges"]]

    def add_missing_classifications(self):

        missing_classifications = []

        for ds in self.database:
            if "classifications" not in ds:
                if (ds["name"], ds["reference product"]) in self.classifications:
                    ds["classifications"] = [
                        (
                            "ISIC rev.4 ecoinvent",
                            self.classifications[(ds["name"], ds["reference product"])][
                                "ISIC rev.4 ecoinvent"
                            ],
                        ),
                        (
                            "CPC",
                            self.classifications[(ds["name"], ds["reference product"])][
                                "CPC"
                            ],
                        ),
                    ]
                else:
                    missing_classifications.append(
                        [ds["name"], ds["reference product"]]
                    )

        with open("missing_classifications.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "reference product"])
            for row in missing_classifications:
                writer.writerow(row)

    def log_issue(self, dataset, reason, message, issue_type="minor"):

        if issue_type == "minor":
            log = self.minor_issues_log
        else:
            log = self.major_issues_log

        log.append(
            {
                "name": dataset.get("name"),
                "reference product": dataset.get("reference product"),
                "location": dataset.get("location"),
                "severity": issue_type,
                "reason": reason,
                "message": message,
            }
        )

    def save_log(self):
        # Save the validation log
        for entry in self.minor_issues_log + self.major_issues_log:
            logger.info(
                f"{self.model}|{self.scenario}|{self.year}|"
                f"{entry['name']}|{entry['reference product']}|"
                f"{entry['location']}|{entry['severity']}|{entry['reason']}|{entry['message']}"
            )

    def run_all_checks(self):
        # Run all checks
        print("Running all checks...")
        self.check_datasets_integrity()
        self.check_matrix_squareness()
        self.validate_dataset_structure()
        self.verify_data_consistency()
        self.check_relinking_logic()
        self.check_new_location()
        self.check_for_orphaned_datasets()
        self.check_for_duplicates()
        self.check_for_circular_references()
        self.check_database_name()
        self.remove_unused_fields()
        self.correct_fields_format()
        self.check_amount_format()
        self.reformat_parameters()
        self.add_missing_classifications()
        self.check_uncertainty()
        self.save_log()
        if len(self.minor_issues_log) > 0:
            print("Minor anomalies found: check the change report.")
        if len(self.major_issues_log) > 0:
            print("---> MAJOR anomalies found: check the change report.")
            raise ValueError


class BatteryValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data

    def check_battery_capacity(self):
        # Check that the battery capacity is within the expected range
        for ds in ws.get_many(
            self.database,
            ws.contains("name", "market for battery capacity"),
            ws.equals("location", "GLO"),
            ws.equals("unit", "kilowatt hour"),
            ws.either(
                *[
                    ws.contains("name", s)
                    for s in ["(MIX)", "(LFP)", "(NCx)", "(PLiB)", "(CONT", "(TC"]
                ]
            ),
        ):
            # check that sum of technosphere exchanges sum to 1
            total = sum(
                [
                    exc["amount"]
                    for exc in ds["exchanges"]
                    if exc["type"] == "technosphere"
                ]
            )
            if not np.isclose(total, 1.0, rtol=1e-3):
                message = f"Total exchange amount is {total}, not 1.0"
                self.log_issue(
                    ds, "Incorrect market shares", message, issue_type="major"
                )

        for ds in ws.get_many(
            self.database,
            ws.contains("name", "market for battery capacity, "),
            ws.equals("location", "GLO"),
            ws.equals("unit", "kilowatt hour"),
            ws.exclude(
                ws.either(
                    *[
                        ws.contains("name", s)
                        for s in ["MIX", "LFP", "NCx", "PLiB", "TC", "CONT"]
                    ]
                )
            ),
        ):
            # check that um of technosphere exchanges equal zero
            total = sum(
                [
                    exc["amount"]
                    for exc in ds["exchanges"]
                    if exc["type"] == "technosphere"
                ]
            )
            if not np.isclose(total, 0.0, rtol=1e-3):
                message = f"Total exchange amount is {total}, not 0.0"
                self.log_issue(
                    ds,
                    "EoL not balanced with battery input",
                    message,
                    issue_type="major",
                )

            # also check that sum of positive technosphere exchanges
            # have a correct value
            total = sum(
                [
                    exc["amount"]
                    for exc in ds["exchanges"]
                    if exc["type"] == "technosphere" and exc["amount"] > 0
                ]
            )
            # this sum needs to be at least 2
            if total < 2:
                message = f"Total exchange amount is {total}, not at least 2"
                self.log_issue(
                    ds, "Energy density too high", message, issue_type="major"
                )

    def run_battery_checks(self):
        self.check_battery_capacity()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during battery update: check the change report."
            )


class HeatValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data

    def check_heat_markets_input(self):

        # Check that the sum of heat inputs in
        # the market for heat is equal to 1

        for ds in ws.get_many(
            self.database,
            ws.contains("name", "market for heat"),
            ws.equals("unit", "megajoule"),
        ):
            total = sum(
                [
                    exc["amount"]
                    for exc in ds["exchanges"]
                    if exc["type"] == "technosphere" and exc["unit"] == "megajoule"
                ]
            )
            if not np.isclose(total, 1.0, rtol=1e-3):
                message = f"Total exchange amount is {total}, not 1.0"
                self.log_issue(
                    ds, "Incorrect market shares", message, issue_type="major"
                )

    def check_heat_conversion_efficiency(self):

        # Check that the heat conversion efficiency
        # is within the expected range

        for ds in self.database:
            if (
                "heat" in ds["name"]
                and ds["unit"] == "megajoule"
                and not any(
                    x in ds["name"]
                    for x in [
                        "heat pump",
                        "heat recovery",
                        "heat storage",
                        "treatment of",
                        "market for",
                        "market group for",
                    ]
                )
                and ds["location"] in self.regions
            ):

                expected_co2 = 0.0

                energy = sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if exc["unit"] == "megajoule" and exc["type"] == "technosphere"
                    ]
                )
                energy += sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if exc["unit"] == "megajoule"
                        and exc["type"] == "biosphere"
                        and exc["name"].startswith("Energy")
                    ]
                )

                # add input of coal
                coal = sum(
                    [
                        exc["amount"] * 26.4
                        for exc in ds["exchanges"]
                        if "hard coal" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += coal * 0.098

                # add input of coal briquettes
                briquettes = sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if "briquettes" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "megajoule"
                    ]
                )

                expected_co2 += briquettes * 0.098

                # add input of natural gas
                nat_gas = sum(
                    [
                        exc["amount"] * (36 if exc["unit"] == "cubic meter" else 47.5)
                        for exc in ds["exchanges"]
                        if "natural gas" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] in ["cubic meter", "kilogram"]
                    ]
                )

                expected_co2 += nat_gas * 0.06

                # add input of liquefied natural gas

                lpg = sum(
                    [
                        exc["amount"] * (36 if exc["unit"] == "cubic meter" else 47.5)
                        for exc in ds["exchanges"]
                        if "liquefied petroleum gas" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] in ["cubic meter", "kilogram"]
                    ]
                )

                expected_co2 += lpg * 0.0631

                # add input of diesel
                diesel = sum(
                    [
                        exc["amount"] * 42.6
                        for exc in ds["exchanges"]
                        if "diesel" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += diesel * 0.0732

                # add input of light fuel oil
                light_fue_oil = sum(
                    [
                        exc["amount"] * 41.8
                        for exc in ds["exchanges"]
                        if "light fuel oil" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += light_fue_oil * 0.0686

                # add input of heavy fuel oil
                heavy_fuel_oil = sum(
                    [
                        exc["amount"] * 41.8
                        for exc in ds["exchanges"]
                        if "heavy fuel oil" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += heavy_fuel_oil * 0.0739

                # add input of biomass
                biomass = sum(
                    [
                        exc["amount"] * 18
                        for exc in ds["exchanges"]
                        if any(x in exc["name"] for x in ["biomass", "wood", "timber"])
                        and "ethanol" not in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += biomass * 0.112

                # add input of methane
                methane = sum(
                    [
                        exc["amount"] * (36 if exc["unit"] == "cubic meter" else 47.5)
                        for exc in ds["exchanges"]
                        if "methane" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] in ["kilogram", "cubic meter"]
                    ]
                )

                expected_co2 += methane * 0.06

                # add input of biogas
                biogas = sum(
                    [
                        exc["amount"] * 22.7
                        for exc in ds["exchanges"]
                        if "biogas" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "cubic meter"
                    ]
                )

                expected_co2 += biogas * 0.058

                # add input of methanol
                methanol = sum(
                    [
                        exc["amount"] * 20
                        for exc in ds["exchanges"]
                        if "methanol" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                expected_co2 += methanol * 0.069

                # add input of hydrogen
                hydrogen = sum(
                    [
                        exc["amount"] * 120
                        for exc in ds["exchanges"]
                        if "hydrogen" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                # add input of electricity
                electricity = sum(
                    [
                        exc["amount"] * 3.6
                        for exc in ds["exchanges"]
                        if "electricity" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilowatt hour"
                    ]
                )

                energy_input = sum(
                    [
                        energy,
                        coal,
                        briquettes,
                        nat_gas,
                        lpg,
                        diesel,
                        light_fue_oil,
                        heavy_fuel_oil,
                        biomass,
                        methane,
                        biogas,
                        hydrogen,
                        methanol,
                        electricity,
                    ]
                )

                efficiency = 1 / energy_input

                if efficiency > 1.15 and not any(
                    x in ds["name"]
                    for x in ["co-generation", "allocated", "allocation"]
                ):
                    message = f"Heat conversion efficiency is {efficiency:.2f}, expected to be less than 1.15."
                    self.log_issue(
                        ds, "heat conversion efficiency", message, issue_type="major"
                    )

                if efficiency > 3.0 and "co-generation" in ds["name"]:
                    message = f"Heat conversion efficiency is {efficiency:.2f}, expected to be less than 3.0. Corrected to 3.0."
                    self.log_issue(ds, "heat conversion efficiency", message)

                    scaling_factor = efficiency / 3.0
                    rescale_exchanges(ds, scaling_factor)
                    expected_co2 *= scaling_factor

                co2 = sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if exc["name"].startswith("Carbon dioxide")
                        and exc["type"] == "biosphere"
                    ]
                )

                if not math.isclose(co2, expected_co2, rel_tol=0.2):
                    if "co-generation" not in ds["name"]:
                        message = f"CO2 emissions are {co2:.3f}, expected to be {expected_co2:.3f}."
                        self.log_issue(ds, "CO2 emissions", message, issue_type="major")

    def run_heat_checks(self):
        self.check_heat_markets_input()
        self.check_heat_conversion_efficiency()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during heat update: check the change report."
            )


class TransportValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data
        self.euro_class_map = {
            "EURO-III": 3,
            "EURO-IV": 4,
            "EURO-V": 5,
            "EURO-VI": 6,
            "EURO-2": 2,
            "EURO-3": 3,
            "EURO-4": 4,
            "EURO-5": 5,
            "EURO-6": 6.2,
            "EURO-6ab": 6.0,
        }
        self.exhaust = None

    def validate_and_normalize_exchanges(self):
        for act in [
            a
            for a in self.database
            if a["name"].startswith("transport, ") and ", unspecified" in a["name"]
        ]:
            total = sum(
                exc["amount"]
                for exc in act["exchanges"]
                if exc["type"] == "technosphere"
            )
            if not np.isclose(total, 1.0, rtol=1e-3):
                message = f"Total exchange amount is {total}, not 1.0"
                self.log_issue(
                    act, "total exchange amount not 1.0", message, issue_type="major"
                )

    def check_vehicles(self):
        for act in [
            a
            for a in self.database
            if a["name"].startswith("transport, ") and ", unspecified" in a["name"]
        ]:
            # check that all transport exchanges are differently named
            names = [
                exc["name"] for exc in act["exchanges"] if exc["type"] == "technosphere"
            ]
            if len(names) != len(set(names)):
                message = "Duplicate transport exchanges"
                self.log_issue(
                    act, "duplicate transport exchanges", message, issue_type="major"
                )

    def calculate_fuel_consumption(self, ds):
        fuel_consumption = sum(
            x["amount"] * 43
            for x in ds["exchanges"]
            if x["name"].startswith(("market for diesel", "market for petrol"))
            and x["unit"] == "kilogram"
        )
        if fuel_consumption == 0:
            fuel_consumption = sum(
                x["amount"] * 47.5
                for x in ds["exchanges"]
                if "natural gas" in x["name"] and x["unit"] == "kilogram"
            )
        return fuel_consumption

    def calculate_actual_emission(self, ds, pollutant):
        return sum(
            x["amount"]
            for x in ds["exchanges"]
            if pollutant.lower() in x["name"].lower()
            and x["type"] == "biosphere"
            and x.get("categories", [None])[0] == "air"
        )

    def validate_emissions(self, ds, actual, expected, pollutant):
        if actual == 0.0:
            message = f"No emission factor found for {pollutant}."
            self.log_issue(ds, f"no emission factor for {pollutant}", message)
            return

        if not math.isclose(actual, expected, rel_tol=0.5):
            new_actual = np.clip(actual, 0.9 * expected, 1.1 * expected)
            if not 0.5 < new_actual / actual < 2:
                message = f"Emission factor for {pollutant} has been corrected from {actual} to {new_actual}."
                self.log_issue(ds, f"incorrect emission factor", message)

            for exc in ds["exchanges"]:
                if pollutant.lower() in exc["name"].lower():
                    exc["amount"] *= new_actual / actual

    def check_pollutant_emissions(self, vehicle_name):

        # Pre-filter datasets
        relevant_ds = [
            ds
            for ds in self.database
            if ds["name"].startswith(vehicle_name)
            and ds["location"] in self.regions
            and any(
                fuel in ds["name"] for fuel in ["gasoline", "diesel", "compressed gas"]
            )
        ]

        for ds in relevant_ds:
            powertrain = ds["name"].split(", ")[-3]
            euro_class = next((x for x in self.euro_class_map if x in ds["name"]), None)

            size = None
            if vehicle_name == "transport, freight, lorry":
                size = ds["name"].split(", ")[4].replace(" gross weight", "")

            fuel_consumption = self.calculate_fuel_consumption(ds)

            if powertrain in self.exhaust:
                if str(self.euro_class_map[euro_class]) in self.exhaust[powertrain]:
                    if size is None:
                        for pollutant, expected_value in self.exhaust[powertrain][
                            str(self.euro_class_map[euro_class])
                        ].items():
                            expected_value /= 1000  # g/MJ to kg/MJ
                            expected_value *= fuel_consumption
                            actual = self.calculate_actual_emission(ds, pollutant)
                            self.validate_emissions(
                                ds, actual, expected_value, pollutant
                            )
                    else:
                        for pollutant, expected_value in self.exhaust[powertrain][
                            str(self.euro_class_map[euro_class])
                        ][size].items():
                            expected_value /= 1000
                            expected_value *= fuel_consumption
                            actual = self.calculate_actual_emission(ds, pollutant)
                            self.validate_emissions(
                                ds, actual, expected_value, pollutant
                            )

    def check_vehicle_efficiency(
        self,
        vehicle_name,
        fossil_minimum=0.0,
        fossil_maximum=0.5,
        elec_minimum=0.01,
        elec_maximum=0.35,
    ):
        # check that the efficiency of the car production datasets
        # is within the expected range

        for ds in self.database:
            if "plugin" in ds["name"]:
                continue

            if ds["name"].startswith(vehicle_name) and ds["location"] in self.regions:
                electricity_consumption = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith("market group for electricity")
                        or x["name"].startswith("market for electricity")
                        and x["type"] == "technosphere"
                    ]
                )
                if electricity_consumption > 0:
                    if (
                        electricity_consumption < elec_minimum
                        or electricity_consumption > elec_maximum
                    ):
                        message = f"Electricity consumption per 100 km is incorrect: {electricity_consumption * 100}."
                        self.log_issue(
                            ds,
                            "electricity consumption too high",
                            message,
                            issue_type="major",
                        )

                fuel_consumption = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith("market for diesel")
                        or x["name"].startswith("market for petrol")
                        and x["type"] == "technosphere"
                    ]
                )

                if fuel_consumption == 0:

                    fuel_consumption += sum(
                        [
                            x["amount"]
                            * (47.5 if x["unit"] == "kilogram" else 36)
                            / 42.6
                            for x in ds["exchanges"]
                            if x["name"].startswith("market for natural gas")
                            or x["name"].startswith("market group for natural gas")
                            and x["type"] == "technosphere"
                        ]
                    )

                if fuel_consumption > 0:
                    if (
                        fuel_consumption < fossil_minimum
                        or fuel_consumption > fossil_maximum
                    ):
                        message = f"Fuel consumption per 100 km is incorrect: {fuel_consumption * 100}."
                        self.log_issue(
                            ds,
                            "fuel consumption incorrect",
                            message,
                            issue_type="major",
                        )

                        # sum the amounts of Carbon dioxide (fossil and non-fossil)
                        # and make sure it is roughly equal to 3.15 kg CO2/kg diesel
                        co2 = sum(
                            [
                                x["amount"]
                                for x in ds["exchanges"]
                                if x["name"].startswith("Carbon dioxide")
                                and x["type"] == "biosphere"
                                and x.get("categories", [None])[0] == "air"
                            ]
                        )
                        if not math.isclose(co2, 3.15 * fuel_consumption, rel_tol=0.1):
                            message = f"CO2 emissions per km are incorrect: {co2} instead of {3.15 * fuel_consumption}."
                            self.log_issue(
                                ds,
                                "CO2 emissions incorrect",
                                message,
                                issue_type="major",
                            )

    def run_vehicle_checks(self):
        self.validate_and_normalize_exchanges()
        self.check_vehicles()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during transport update: check the change report."
            )


class TruckValidation(TransportValidation):

    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database, iam_data)
        self.exhaust = load_truck_exhaust_pollutants()

    def run_checks(self):
        self.run_vehicle_checks()
        self.check_vehicle_efficiency(
            vehicle_name="transport, freight, lorry",
            fossil_minimum=0.0,
            fossil_maximum=0.5,
            elec_minimum=0.01,
            elec_maximum=0.9,
        )
        self.check_pollutant_emissions(vehicle_name="transport, freight, lorry")
        self.save_log()


class CarValidation(TransportValidation):

    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database, iam_data)
        self.exhaust = load_car_exhaust_pollutants()

    def run_checks(self):
        self.run_vehicle_checks()
        self.check_pollutant_emissions(vehicle_name="transport, passenger car")
        self.check_vehicle_efficiency(
            vehicle_name="transport, passenger car",
            fossil_minimum=0.02,
            fossil_maximum=0.1,
            elec_minimum=0.1,
            elec_maximum=0.35,
        )
        self.save_log()


class ElectricityValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data

    def check_electricity_market_composition(self):
        def _is_electricity(exc, dataset):
            return (
                exc["type"] == "technosphere"
                and exc["unit"] == "kilowatt hour"
                and "electricity" in exc.get("product")
                and exc["name"] != dataset["name"]
            )

        # checks that inputs in an electricity markets equal more or less to 1
        for dataset in self.database:
            if (
                dataset["name"].lower().startswith("market group for electricity")
                and dataset["location"] in self.regions
                and dataset["location"] != "World"
            ):
                total = sum(
                    [
                        x["amount"] if _is_electricity(x, dataset) else 0
                        for x in dataset["exchanges"]
                    ]
                )
                if total < 0.99 or total > 1.15:
                    message = f"Electricity market inputs sum to {total}."
                    self.log_issue(
                        dataset,
                        "electricity market not summing to 1",
                        message,
                        issue_type="major",
                    )

    def check_old_datasets(self):
        # ensure old electricity markets only have
        # one technosphere exchange
        # linking to the newly created markets

        for dataset in self.database:
            if (
                (
                    dataset["name"].lower().startswith("market group for electricity")
                    or dataset["name"].lower().startswith("market for electricity")
                )
                and dataset["location"] not in self.regions
                and not any(
                    i.lower() in dataset["name"].lower()
                    for i in [
                        "aluminium",
                        "railways",
                        "reuse",
                        "cobalt",
                        "renewable",
                        "coal",
                    ]
                )
            ):
                input_exc = [
                    x for x in dataset["exchanges"] if x["type"] == "technosphere"
                ]
                if len(input_exc) != 1:
                    message = f"Electricity market has {len(input_exc)} inputs."
                    self.log_issue(
                        dataset,
                        "old electricity market has more than one input",
                        message,
                        issue_type="major",
                    )
                else:
                    if (
                        not input_exc[0]["name"].startswith(
                            "market group for electricity"
                        )
                        or not input_exc[0]["location"] in self.regions
                    ):
                        message = "Electricity market input is incorrect."
                        self.log_issue(
                            dataset,
                            "incorrect old electricity market input",
                            message,
                            issue_type="major",
                        )

                    # check the location of the input
                    # matches the location of the dataset
                    # according to the geo-linking rules
                    self.check_geo_linking(
                        input_exc[0]["location"], dataset["location"]
                    )

    def check_geo_linking(self, exc_loc, dataset_loc):
        # check that the location of the input
        # matches the location of the dataset
        # according to the ecoinvent-IAM geo-linking rules
        if dataset_loc in ["RER", "Europe without Switzerland", "FR"]:
            if exc_loc not in ["EUR", "WEU", "EU-15"]:
                message = "Electricity market input has incorrect location."
                self.log_issue(
                    {"location": dataset_loc},
                    "incorrect old electricity market input",
                    message,
                )
        if exc_loc != self.geo.ecoinvent_to_iam_location(dataset_loc):
            message = "Electricity market input has incorrect location."
            self.log_issue(
                {"location": dataset_loc},
                "incorrect old electricity market input",
                message,
            )

    def check_electricity_mix(self):
        # check that the electricity mix in the market datasets
        # corresponds to the IAM scenario projection
        vars = [
            x
            for x in self.iam_data.electricity_mix.coords["variables"].values
            if x.lower().startswith("hydro")
        ]

        if self.year in self.iam_data.electricity_mix.coords["year"].values:

            hydro_share = self.iam_data.electricity_mix.sel(
                variables=vars, year=self.year
            ).sum(dim="variables") / self.iam_data.electricity_mix.sel(
                variables=[
                    v
                    for v in self.iam_data.electricity_mix.variables.values
                    if v.lower() != "solar pv residential"
                ],
                year=self.year,
            ).sum(
                dim="variables"
            )
        else:
            hydro_share = self.iam_data.electricity_mix.sel(variables=vars).interp(
                year=self.year
            ).sum(dim="variables") / self.iam_data.electricity_mix.sel(
                variables=[
                    v
                    for v in self.iam_data.electricity_mix.variables.values
                    if v.lower() != "solar pv residential"
                ],
            ).interp(
                year=self.year
            ).sum(
                dim="variables"
            )

        for ds in self.database:
            if (
                ds["name"] == "market group for electricity, high voltage"
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                hydro_sum = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith("electricity production, hydro")
                    ]
                )

                # check that hydro_sum is roughly equal to the IAM hydro share
                share = hydro_share.sel(region=ds["location"]).values.item(0)
                if math.isclose(hydro_sum, share, rel_tol=0.1) is False:
                    message = f"Electricity market hydro share is incorrect: {hydro_sum} instead of {share}."
                    self.log_issue(
                        ds,
                        "incorrect electricity market hydro share",
                        message,
                        issue_type="major",
                    )

        # check that the sum of photovoltaic electricity input and
        # input from medium voltage electricity in the low voltage
        # market is superior to 1

        for ds in self.database:
            if (
                ds["name"] == "market group for electricity, low voltage"
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                pv_sum = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith("electricity production, photovoltaic")
                    ]
                )
                mv_sum = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith(
                            "market group for electricity, medium voltage"
                        )
                    ]
                )

                if pv_sum + mv_sum < 1:
                    message = f"Electricity market PV and MV share is incorrect: {pv_sum + mv_sum} instead of > 1."
                    self.log_issue(
                        ds,
                        "incorrect electricity market PV and MV share",
                        message,
                        issue_type="major",
                    )

    def check_efficiency(self):
        # check efficiency of electricity production datasets
        # against expected values

        electricity_vars = load_electricity_keys()

        LHV = electricity_vars["LHV"]
        fuel_CO2 = electricity_vars["CO2"]
        exceptions = electricity_vars["exceptions"]
        units = electricity_vars["fuel units"]
        electricity_datasets = electricity_vars["electricity datasets"]
        efficiencies = electricity_vars["efficiencies"]

        for ds in self.database:
            if (
                ds["unit"] == "kilowatt hour"
                and any(ds["name"].startswith(x) for x in electricity_datasets)
                and not any(x in ds["name"].lower() for x in exceptions)
            ):
                fuel_energy, co2 = 0.0, 0.0

                for e in ds["exchanges"]:
                    if e["type"] == "technosphere" and e["amount"] > 0:
                        # check that the name contains a key of LHV
                        if (
                            any(x in e["name"].lower() for x in LHV)
                            and e["unit"] in units
                            and not any(
                                y in e["name"]
                                for y in ["plant", "flue gas", "carbon dioxide"]
                            )
                            and e["amount"] > 0
                        ):
                            key = [x for x in LHV if x in e["name"].lower()]
                            # choose the longest item in the list
                            key = max(key, key=len)
                            fuel_energy += e["amount"] * LHV[key]
                            co2 += float(e["amount"] * fuel_CO2[key] * LHV[key])

                    if (
                        e["name"].startswith("carbon dioxide, captured")
                        and e["type"] == "technosphere"
                    ):
                        co2 -= e["amount"]

                actual_co2 = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["name"].startswith("Carbon dioxide")
                        and x["type"] == "biosphere"
                        and x.get(
                            "categories",
                            [
                                None,
                            ],
                        )[0]
                        == "air"
                    ]
                )

                if fuel_energy > 0:
                    efficiency = 3.6 / fuel_energy

                    eff = efficiencies[
                        [x for x in efficiencies if x in ds["name"].lower()][0]
                    ]

                    if not eff["min"] <= efficiency <= eff["max"]:
                        message = f"Current eff.: {efficiency}. Min: {eff['min']}. Max: {eff['max']}."
                        self.log_issue(
                            ds,
                            "electricity efficiency possibly incorrect",
                            message,
                        )

                if not math.isclose(co2, actual_co2, rel_tol=0.2):
                    message = f"Current CO2: {actual_co2}. Expected: {co2}."
                    self.log_issue(
                        ds,
                        "CO2 emissions possibly incorrect",
                        message,
                    )

    def run_electricity_checks(self):
        self.check_electricity_market_composition()
        self.check_old_datasets()
        self.check_electricity_mix()
        self.check_efficiency()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during electricity update: check the change report."
            )


class FuelsValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data

    def check_fuel_market_composition(self):
        # check that the fuel markets inputs
        # equal to 1

        fuel_market_names = [
            "market for petrol, low-sulfur",
            "market for diesel, low-sulfur",
            "market for natural gas, high pressure",
            "market for hydrogen, gaseous",
            "market for kerosene",
            "market for liquefied petroleum gas",
        ]

        for ds in self.database:
            if (
                any(ds["name"].startswith(x) for x in fuel_market_names)
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                if ds["unit"] == "cubic meter":

                    total = sum(
                        [
                            (
                                x["amount"]
                                if x["unit"] == "cubic meter"
                                else x["amount"] / 0.716
                            )
                            for x in ds["exchanges"]
                            if x["type"] == "technosphere"
                            and x["unit"] in ("kilogram", "cubic meter")
                        ]
                    )

                else:
                    total = sum(
                        [
                            x["amount"]
                            for x in ds["exchanges"]
                            if x["type"] == "technosphere"
                            and x["unit"] in ("kilogram", "cubic meter")
                        ]
                    )
                if total < 0.99 or total > 2:
                    message = f"Fuel market inputs sum to {total}."
                    self.log_issue(
                        ds,
                        "fuel market inputs do not sum to 1",
                        message,
                        issue_type="major",
                    )

    def check_empty_fuel_markets(self):

        fuel_market_names = [
            "market for petrol, low-sulfur",
            "market for diesel, low-sulfur",
            "market for hydrogen, gaseous, low pressure",
        ]

        for ds in self.database:
            if (
                any(ds["name"].startswith(x) for x in fuel_market_names)
                and ds["location"] not in self.regions
            ):
                if not all(
                    e["location"] in self.regions
                    for e in ds["exchanges"]
                    if e["type"] == "technosphere"
                ):
                    if (
                        len(
                            [
                                d
                                for d in self.database
                                if d["name"] == ds["name"]
                                and d["location"] in self.regions
                            ]
                        )
                        > 0
                    ):

                        message = f"Inputs may have incorrect location."
                        self.log_issue(
                            ds,
                            "Non-regionalized inputs",
                            message,
                            issue_type="major",
                        )

    def check_electrolysis_electricity_input(self):
        # check that the input of electricity for hydrogen production
        # is within the expected range

        for ds in self.database:
            if (
                ds["name"].startswith("hydrogen production")
                and "electrolysis" in ds["name"]
                and ds["location"] in self.regions
            ):
                electricity = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere" and x["unit"] == "kilowatt hour"
                    ]
                )
                if electricity < 40 or electricity > 60:
                    message = (
                        f"Electricity use for hydrogen production is {electricity}."
                    )
                    self.log_issue(
                        ds,
                        "electricity use for hydrogen production",
                        message,
                        issue_type="major",
                    )

    def checking_linking(self):

        fuel_market_names = [
            "market for petrol, low-sulfur",
            "market group for petrol, low-sulfur",
            "market for diesel, low-sulfur",
            "market group for diesel, low-sulfur",
        ]

        for ds in self.database:
            if ds["location"] not in ["RoW", "GLO", "World"]:
                for e in ds["exchanges"]:
                    if e["type"] == "technosphere" and any(
                        e["name"].startswith(x) for x in fuel_market_names
                    ):
                        # check that the location of the input
                        # matches the location of the dataset
                        # according to the geo-linking rules
                        if ds["location"] in self.regions:
                            if e["location"] != ds["location"]:
                                if e["location"] != "World":
                                    message = f"Fuel market input {e['name']} in {e['location']} has incorrect location for dataset {ds['name']} in {ds['location']}."
                                    self.log_issue(
                                        ds,
                                        "incorrect fuel market input location",
                                        message,
                                        issue_type="major",
                                    )
                        else:
                            # check that the location of the input
                            if e["location"] != self.geo.ecoinvent_to_iam_location(
                                ds["location"]
                            ):
                                message = f"Fuel market input {e['name']} in {e['location']} has incorrect location for dataset {ds['name']} in {ds['location']}."
                                self.log_issue(
                                    ds,
                                    "incorrect fuel market input location",
                                    message,
                                    issue_type="major",
                                )

    def run_fuel_checks(self):
        self.check_fuel_market_composition()
        self.check_empty_fuel_markets()
        self.check_electrolysis_electricity_input()
        self.checking_linking()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during fuels update: check the change report."
            )


class SteelValidation(BaseDatasetValidator):
    def __init__(
        self, model, scenario, year, regions, database, iam_data, system_model
    ):
        super().__init__(model, scenario, year, regions, database, system_model)
        self.iam_data = iam_data
        self.system_model = system_model

    def check_steel_markets(self):
        # check that the steel markets inputs
        # equal to 1

        for ds in self.database:
            if (
                ds["name"].startswith("market for steel, ")
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere" and x["unit"] == "kilogram"
                    ]
                )
                if total < 0.99 or total > 1.1:
                    message = f"Steel market inputs sum to {total}."
                    self.log_issue(
                        ds,
                        "steel market inputs do not sum to 1",
                        message,
                        issue_type="major",
                    )

            # check that the inputs of EAF steel matches the IAM projections
            if (
                ds["name"].startswith("market for steel")
                and any(x in ds["name"] for x in ["low-alloyed", "unalloyed"])
                and ds["location"] in self.regions
            ):
                if ds["location"] == "World":
                    continue

                eaf_steel = 0
                if "steel - secondary" in self.iam_data.steel_technology_mix.variables:
                    if (
                        self.year
                        in self.iam_data.steel_technology_mix.coords["year"].values
                    ):

                        eaf_steel = (
                            self.iam_data.steel_technology_mix.sel(
                                variables="steel - secondary",
                                region=ds["location"],
                                year=self.year,
                            )
                        ).values.item(0)
                    else:
                        eaf_steel = (
                            self.iam_data.steel_technology_mix.sel(
                                variables="steel - secondary", region=ds["location"]
                            )
                            .interp(year=self.year)
                            .values.item(0)
                        )

                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere"
                        and x["unit"] == "kilogram"
                        and x["name"].startswith("steel production, electric")
                    ]
                )

                if eaf_steel > 0:
                    if self.system_model != "consequential":
                        # check that the total is roughly equal to the IAM projection
                        if math.isclose(total, eaf_steel, rel_tol=0.02) is False:
                            message = f"Input of secondary steel incorrect: {total} instead of {eaf_steel}."
                            self.log_issue(
                                ds,
                                "incorrect secondary steel market input",
                                message,
                                issue_type="major",
                            )
                    else:
                        # make sure the amount of secondary steel is 0
                        if total > 0.01:
                            message = f"Input of secondary steel is {total}."
                            self.log_issue(
                                ds,
                                "incorrect secondary steel market input. Should be zero.",
                                message,
                                issue_type="major",
                            )

    def check_empty_markets(self):

        market_names = [
            "market for steel, low-alloyed",
            "market for steel, unalloyed",
        ]

        for ds in self.database:
            if (
                any(ds["name"].startswith(x) for x in market_names)
                and ds["location"] not in self.regions
            ):
                assert all(
                    e["location"] in self.regions
                    for e in ds["exchanges"]
                    if e["type"] == "technosphere"
                ), f"Steel market {ds['name']} in {ds['location']} has exchanges with locations not in the IAM regions list."

    def checking_linking(self):

        fuel_market_names = [
            "market for steel, low-alloyed",
            "market for steel, unalloyed",
        ]

        for ds in self.database:
            for e in ds["exchanges"]:
                if e["type"] == "technosphere" and any(
                    e["name"].startswith(x) for x in fuel_market_names
                ):
                    # check that the location of the input
                    # matches the location of the dataset
                    # according to the geo-linking rules
                    assert e["location"] == self.geo.ecoinvent_to_iam_location(
                        ds["location"]
                    ), f"Steel market input {e['name']} in {e['location']} has incorrect location for dataset {ds['name']} in {ds['location']}."

    def check_pig_iron_input(self):
        """
        Check that the input of "market for pig iron" in "steel production, converter, low-alloyed"
        has the correct location.
        """

        for ds in self.database:
            if (
                ds["name"].startswith(
                    "steel production, blast furnace-basic oxygen furnace"
                )
                and ds["location"] in self.regions
            ):
                pig_iron = [
                    x
                    for x in ds["exchanges"]
                    if "pig iron production" in x["name"]
                    and x["type"] == "technosphere"
                    and x["amount"] > 0
                ]
                if not pig_iron:
                    message = "No input of pig iron found."
                    self.log_issue(
                        ds,
                        "no input of pig iron",
                        message,
                        issue_type="major",
                    )
                else:
                    if pig_iron[0]["location"] != ds["location"]:
                        message = f"Input of pig iron has incorrect location: {pig_iron[0]['location']}."
                        self.log_issue(
                            ds,
                            "incorrect pig iron input location",
                            message,
                            issue_type="major",
                        )

    def check_steel_energy_use(self):
        # check that low-alloyed steel produced by EAF
        # use at least 0.4 MWh electricity per kg of steel

        for ds in self.database:
            if (
                ds["name"].startswith("steel production, electric")
                and "steel" in ds["reference product"]
                and ds["location"] in self.regions
            ):
                electricity = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere" and x["unit"] == "kilowatt hour"
                    ]
                )
                # if electricity use is inferior to 0.39 MWh/kg
                # or superior to 0.8 MWh/kg, log a warning

                if electricity < 0.39:
                    message = f"Electricity use for steel production is too low: {electricity}."
                    self.log_issue(
                        ds,
                        "electricity use for EAF steel production too low",
                        message,
                        issue_type="major",
                    )

                if electricity > 0.8:
                    message = f"Electricity use for EAF steel production is too high: {electricity}."
                    self.log_issue(
                        ds,
                        "electricity use for steel production too high",
                        message,
                        issue_type="major",
                    )
        # check pig iron production datasets
        # against expected values

        for ds in self.database:
            if (
                ds["name"].startswith("pig iron production")
                and ds["location"] in self.regions
            ):
                energy = sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if exc["unit"] == "megajoule" and exc["type"] == "technosphere"
                    ]
                )
                # add input of coal
                energy += sum(
                    [
                        exc["amount"] * 26.4
                        for exc in ds["exchanges"]
                        if "hard coal" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                # add input of natural gas
                energy += sum(
                    [
                        exc["amount"] * 36
                        for exc in ds["exchanges"]
                        if "natural gas" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "cubic meter"
                    ]
                )

                # add electricity inputs
                energy += sum(
                    [
                        exc["amount"] * 3.6
                        for exc in ds["exchanges"]
                        if exc["type"] == "technosphere"
                        and exc["unit"] == "kilowatt hour"
                    ]
                )

                # add hydrogen inputs
                energy += sum(
                    [
                        exc["amount"] * 120
                        for exc in ds["exchanges"]
                        if "hydrogen" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                    ]
                )

                if energy < 8.0:
                    message = (
                        f"Energy use for pig iron production is too low: {energy}."
                    )
                    self.log_issue(
                        ds,
                        "energy use for pig iron production too low",
                        message,
                        issue_type="major",
                    )

    def run_steel_checks(self):
        self.check_steel_markets()
        self.check_empty_markets()
        self.check_steel_energy_use()
        self.check_pig_iron_input()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during steel update: check the change report."
            )


class CementValidation(BaseDatasetValidator):
    def __init__(self, model, scenario, year, regions, database, iam_data):
        super().__init__(model, scenario, year, regions, database)
        self.iam_data = iam_data

    def check_cement_markets(self):
        # check that the cement markets inputs
        # equal to 1

        for ds in self.database:
            if (
                ds["name"].startswith("market for cement, ")
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere"
                        and x["unit"] == "kilogram"
                        and "cement" in x["name"].lower()
                    ]
                )
                if total < 0.99 or total > 1.1:
                    message = f"Cement market inputs sum to {total}."
                    self.log_issue(
                        ds,
                        "cement market inputs do not sum to 1",
                        message,
                        issue_type="major",
                    )

        for ds in self.database:
            if (
                ds["name"].startswith("market for clinker, ")
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere"
                        and x["unit"] == "kilogram"
                        and "clinker" in x["name"].lower()
                    ]
                )
                if total < 0.99 or total > 1.1:
                    message = f"Clinker market inputs sum to {total}."
                    self.log_issue(
                        ds,
                        "clinker market inputs do not sum to 1",
                        message,
                        issue_type="major",
                    )

    def check_empty_markets(self):

        market_names = [
            "market for clinker",
        ]

        for ds in self.database:
            if (
                any(ds["name"].startswith(x) for x in market_names)
                and ds["location"] not in self.regions
            ):
                assert all(
                    e["location"] in self.regions
                    for e in ds["exchanges"]
                    if e["type"] == "technosphere"
                ), (
                    f"Clinker market {ds['name']} in {ds['location']} has exchanges with "
                    f"locations not in the IAM regions list."
                )

    def checking_linking(self):

        fuel_market_names = [
            "market for clinker",
        ]

        for ds in self.database:
            for e in ds["exchanges"]:
                if e["type"] == "technosphere" and any(
                    e["name"].startswith(x) for x in fuel_market_names
                ):
                    # check that the location of the input
                    # matches the location of the dataset
                    # according to the geo-linking rules
                    assert e["location"] == self.geo.ecoinvent_to_iam_location(
                        ds["location"]
                    ), f"Clinker market input {e['name']} in {e['location']} has incorrect location for dataset {ds['name']} in {ds['location']}."

    def check_clinker_energy_use(self):
        # check that clinker production datasets
        # use at least 3 MJ/kg clinker

        for ds in self.database:
            if (
                ds["name"].startswith("clinker production")
                and ds["location"] in self.regions
                and "clinker" in ds["reference product"]
            ):
                energy = sum(
                    [
                        exc["amount"]
                        for exc in ds["exchanges"]
                        if exc["unit"] == "megajoule" and exc["type"] == "technosphere"
                    ]
                )

                # add input of coal
                energy += sum(
                    [
                        exc["amount"] * 26.4
                        for exc in ds["exchanges"]
                        if "hard coal" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                        and exc["amount"] > 0
                    ]
                )

                # add input of heavy and light fuel oil
                energy += sum(
                    [
                        exc["amount"] * 41.9
                        for exc in ds["exchanges"]
                        if "fuel oil" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                        and exc["amount"] > 0
                    ]
                )

                # add lignite
                energy += sum(
                    [
                        exc["amount"] * 10.5
                        for exc in ds["exchanges"]
                        if "lignite" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                        and exc["amount"] > 0
                    ]
                )

                # add petcoke
                energy += sum(
                    [
                        exc["amount"] * 35.2
                        for exc in ds["exchanges"]
                        if "petroleum coke" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                        and exc["amount"] > 0
                    ]
                )

                # add input of natural gas
                energy += sum(
                    [
                        exc["amount"] * 36
                        for exc in ds["exchanges"]
                        if "natural gas" in exc["name"]
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "cubic meter"
                        and exc["amount"] > 0
                    ]
                )

                # add input of waste plastic, mixture
                energy += sum(
                    [
                        exc["amount"] * 17 * -1
                        for exc in ds["exchanges"]
                        if exc.get("product") == "waste plastic, mixture"
                        and exc["type"] == "technosphere"
                        and exc["unit"] == "kilogram"
                        and exc["amount"] < 0
                    ]
                )

                if energy < 2.99:
                    message = f"Energy use for clinker production is too low: {energy}."
                    self.log_issue(
                        ds,
                        "energy use for clinker production too low",
                        message,
                        issue_type="minor",
                    )

    def run_cement_checks(self):
        self.check_cement_markets()
        self.check_empty_markets()
        self.check_clinker_energy_use()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during cement update: check the change report."
            )


class BiomassValidation(BaseDatasetValidator):
    def __init__(
        self, model, scenario, year, regions, database, iam_data, system_model
    ):
        super().__init__(model, scenario, year, regions, database, system_model)
        self.iam_data = iam_data
        self.system_model = system_model

    def check_biomass_markets(self):
        # check that the biomass markets inputs
        # equal to 1

        for ds in self.database:
            if (
                ds["name"].startswith(
                    "market for lignocellulosic biomass, used as fuel"
                )
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere" and x["unit"] == "kilogram"
                    ]
                )
                if total < 0.99 or total > 1.1:
                    message = f"Biomass market inputs sum to {total}."
                    self.log_issue(
                        ds,
                        "biomass market inputs do not sum to 1",
                        message,
                        issue_type="major",
                    )

    def checking_linking(self):

        regions = self.iam_data.biomass_mix.coords["region"].values

        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.equals("unit", u) for u in ["kilowatt hour", "megajoule"]]),
            ws.either(
                *[ws.contains("name", n) for n in ["electricity", "heat", "power"]]
            ),
            ws.either(
                *[
                    ws.contains("name", n)
                    for n in [
                        "biomass",
                        "wood",
                    ]
                ]
            ),
            ws.exclude(
                ws.either(
                    *[
                        ws.contains("name", n)
                        for n in [
                            "treatment",
                            "untreated",
                            "logs",
                            "solar",
                            "storage",
                            "methanol",
                            "hydrogen",
                        ]
                    ]
                )
            ),
        ):

            if (
                dataset["location"] in regions
                or self.geo.ecoinvent_to_iam_location(dataset["location"]) in regions
            ):
                loc = (
                    dataset["location"]
                    if dataset["location"] in regions
                    else self.geo.ecoinvent_to_iam_location(dataset["location"])
                )
                if self.iam_data.biomass_mix.sel(region=loc).sum() > 0:
                    assert (
                        len(
                            [
                                e
                                for e in dataset["exchanges"]
                                if e["type"] == "technosphere"
                                and e["name"]
                                == "market for lignocellulosic biomass, used as fuel"
                            ]
                        )
                        >= 1
                    ), (
                        f"Dataset {dataset['name']} in {dataset['location']} "
                        f"should have one or more exchanges to "
                        f"'market for lignocellulosic biomass, used as fuel'. "
                        f"Currently has {len([e for e in dataset['exchanges'] if e['type'] == 'technosphere' and e['name'] == 'market for lignocellulosic biomass, used as fuel'])}."
                    )

    def check_residual_biomass_share(self):
        # check that the share of residual biomass
        # in the biomass market is equal to the IAM projections

        is_consequential = self.system_model == "consequential"

        for ds in self.database:
            if (
                ds["name"] == "market for lignocellulosic biomass, used as fuel"
                and ds["location"] in self.regions
                and ds["location"] != "World"
            ):
                if self.year in self.iam_data.biomass_mix.coords["year"].values:
                    if not is_consequential:
                        expected_share = self.iam_data.biomass_mix.sel(
                            variables="biomass - residual",
                            region=ds["location"],
                            year=self.year,
                        ).values.item(0)
                    else:
                        expected_share = 0.0
                else:
                    if not is_consequential:
                        expected_share = (
                            self.iam_data.biomass_mix.sel(
                                variables="biomass - residual",
                                region=ds["location"],
                            )
                            .interp(year=self.year)
                            .values.item(0)
                        )
                    else:
                        expected_share = 0.0

                residual_biomass = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere"
                        and x["unit"] == "kilogram"
                        and "residue" in x["name"].lower()
                    ]
                )
                total = sum(
                    [
                        x["amount"]
                        for x in ds["exchanges"]
                        if x["type"] == "technosphere" and x["unit"] == "kilogram"
                    ]
                )
                # check that the total is roughly equal to the IAM projection
                if (
                    math.isclose(residual_biomass / total, expected_share, rel_tol=0.01)
                    is False
                ):
                    message = f"Residual biomass share incorrect: {residual_biomass / total} instead of {expected_share}."
                    self.log_issue(
                        ds,
                        "incorrect residual biomass share",
                        message,
                        issue_type="major",
                    )

    def run_biomass_checks(self):
        self.check_biomass_markets()
        self.checking_linking()
        self.check_residual_biomass_share()
        self.save_log()

        if len(self.major_issues_log) > 0:
            print(
                "---> MAJOR anomalies found during biomass update: check the change report."
            )


class MetalsValidation(BaseDatasetValidator):
    def __init__(
        self, model, scenario, year, regions, database, iam_data, system_model
    ):
        super().__init__(model, scenario, year, regions, database, system_model)
        self.iam_data = iam_data
        self.system_model = system_model

    def run_metals_checks(self):
        self.check_market_balance()
        self.check_split_yaml_consistency()
        self.check_interpolation()
        self.check_excel_shares_preserved()
        self.save_log()

        if self.major_issues_log:
            print(
                "---> MAJOR anomalies found during metals update: check the change report."
            )

    def check_market_balance(self):
        """
        Check that the inputs of the metals markets sum to 1
        """
        for metal in self.metals_list:
            try:
                name = f"market for {metal}"
                ds = ws.get_one(
                    self.database,
                    ws.equals("name", name),
                    ws.equals("location", "World"),
                    ws.equals("unit", "kilogram"),
                )
            except Exception:
                continue

            total_kg_inputs = sum(
                e["amount"]
                for e in ds["exchanges"]
                if e["type"] == "technosphere" and e["unit"] == "kilogram"
            )
            if not np.isclose(total_kg_inputs, 1.0, rtol=1e-3):
                message = f"Metal market inputs sum to {total_kg_inputs}."
                self.log_issue(
                    ds,
                    "metal market inputs do not sum to 1",
                    message,
                    issue_type="major",
                )

    def check_split_yaml_consistency(self):
        """
        Check that the split YAML files for metals sum to 1 for each metal
        """
        for metal, data in self.prim_sec_split.items():
            for year in data["shares"]["primary"]:
                primary = data["shares"]["primary"].get(year, 0)
                secondary = data["shares"]["secondary"].get(year, 0)
                total = primary + secondary
                if not np.isclose(total, 1.0, rtol=1e-3):
                    message = (
                        f"Metal {metal} shares for year {year} do not sum to 1: "
                        f"primary={primary}, secondary={secondary}, total={total}."
                    )
                    self.log_issue(
                        {"name": metal, "year": year},
                        "metal shares do not sum to 1",
                        message,
                        issue_type="major",
                    )

    def check_interpolation(self):
        """
        Check that the interpolation of metal shares is consistent
        """
        test_cases = [
            ({2020: 0.8, 2050: 0.5}, 2020, 0.8),
            ({2020: 0.8, 2050: 0.5}, 2050, 0.5),
            ({2020: 0.8, 2050: 0.5}, 2035, 0.65),
            ({2020: 1.0}, 2040, 1.0),
        ]

        for shares, year, expected in test_cases:
            result = self.interpolate_by_year(year, shares)
            if not np.isclose(result, expected, rtol=1e-3):
                message = (
                    f"Interpolation for year {year} with shares {shares} "
                    f"expected {expected}, got {result}."
                )
                self.log_issue(
                    {"year": year, "shares": shares},
                    "interpolation error",
                    message,
                    issue_type="major",
                )

    def check_excel_shares_preserved(self):
        """
        Verify that the shares from Excel are preserved in the final markets.
        This should catch normalization bugs
        """

        mining_shares_df = _load_mining_shares_mapping_for_validation()

        country_codes = dict(
            zip(
                mining_shares_df["Country"].unique(),
                coco.convert(mining_shares_df["Country"].unique(), to="ISO2"),
            )
        )
        country_codes["France (French Guiana)"] = "GF"

        # Group df by metal
        for metal in mining_shares_df["Metal"].unique():
            metal_df = mining_shares_df[mining_shares_df["Metal"] == metal]

            # Find the 'World' market
            try:
                market = ws.get_one(
                    self.database,
                    ws.equals("name", f"market for {metal}"),
                    ws.equals("location", "World"),
                    ws.equals("unit", "kilogram"),
                )
            except:
                continue

            # Find year
            year_cols = sorted([int(col) for col in metal_df.columns if col.isdigit()])
            if not year_cols:
                print(f"WARNING: No year columns found for {metal}")
                continue
            min_year, max_year = year_cols[0], year_cols[-1]
            year_to_use = max(min_year, min(self.year, max_year))
            year_col = str(year_to_use)

            # APPLY THE SAME FILTERING AS metals.py:
            metal_df_filtered = metal_df[metal_df["Work done"] == "Yes"].copy()
            metal_df_filtered = metal_df_filtered[
                metal_df_filtered[year_col] >= 0.01
            ].copy()

            # Add up the shares in the excel by country (across all the different datasets)
            country_totals_excel = metal_df_filtered.groupby("Country")[year_col].sum()
            # Normalize the shares
            total_excel = country_totals_excel.sum()
            if total_excel > 0:
                expected_shares = country_totals_excel / total_excel
            else:
                continue

            # Get the shares from the market so we can compare
            actual_shares = {}
            for exc in market["exchanges"]:
                if (
                    exc["type"] == "technosphere"
                    and exc.get("location")
                    and exc["unit"] == "kilogram"
                ):
                    loc = exc["location"]
                    if loc not in actual_shares:
                        actual_shares[loc] = 0
                    actual_shares[loc] += exc["amount"]

            # Get primary share for this metal
            primary_share = self.get_primary_share_for_metal(metal)

            # Compare for significant producers
            for country_long in country_totals_excel.index:
                country_short = country_codes.get(country_long)
                if not country_short:
                    continue

                expected = expected_shares.get(country_long, 0) * primary_share
                actual = actual_shares.get(country_short, 0)

                if (
                    expected > 0.01 and self.system_model != "consequential"
                ):  # Only check significant shares
                    relative_error = (
                        abs(actual - expected) / expected
                        if expected > 0
                        else float("inf")
                    )

                    if relative_error > 0.3:  # More than 30% error
                        message = (
                            f"{country_long} should have {expected:.2%} of {metal} market "
                            f"(Excel sum: {country_totals_excel.get(country_long, 0):.2%}, "
                            f"normalized: {expected_shares.get(country_long, 0):.2%}, "
                            f"with primary share {primary_share:.2%}), but has {actual:.2%}."
                        )
                        self.log_issue(
                            market,
                            "metal market share mismatch",
                            message,
                            issue_type="major",
                        )

    def get_primary_share_for_metal(self, metal):
        """
        Get the primary share for a given metal from the prim_sec_split data.
        """
        if not self.prim_sec_split or metal not in self.prim_sec_split:
            return 1.0  # Default to 100% primary if not found

        entry = self.prim_sec_split[metal]
        primary_shares = entry["shares"]["primary"]

        # Use interpolate_by_year if available
        if self.interpolate_by_year:
            return self.interpolate_by_year(self.year, primary_shares)

        # Fallback to simple lookup
        if self.year in primary_shares:
            return primary_shares[self.year]
        elif 2020 in primary_shares:
            return primary_shares[2020]
        else:
            return 1.0
