import json
from os import path, listdir
import pandas as pd
import numpy as np
from bw2io.extractors import ExcelExtractor


# read the mapping json
with open("cpc_mapping.json", "r") as file:
    cpc_mapping = json.load(file)
    cpc_mapping.pop("_example CPC class")  # drop the example to demonstrate file format

# path to inventory files
inventories_folder = path.abspath(path.join(path.dirname(path.abspath(__file__)), "converted_files"))  # replace with ".." later

# get all files from the inventories folder that are actual inventories
files = [f for f in listdir(inventories_folder)
         if path.isfile(path.join(inventories_folder, f))
         and f.startswith("lci") and f.endswith(".xlsx")]
files = sorted(files, key=str.casefold)  # sort for consistency

# we use file sizes to estimate progress, not perfect, but better than judging by file number
file_sizes = [path.getsize(path.join(inventories_folder, f)) for f in files]
total_files_size = sum(file_sizes)

def extract_inventory_from_sheet(sheet):
    """extract data from the sheet as list

    returns an excel sheet in the form of a list, with activities as dicts

    returns:
    [
    [non activity data, nan, nan]
    {"Activity":
        [metadata, nan, nan]
        ...
        [Exchanges, nan, nan]
        [...]
    ]
    """
    l = []
    act = {}

    in_act = False
    in_exc = False
    for i, row in enumerate(sheet):

        if i == 0 and str(row[0]).lower() == "skip":
            # if the cell A1 is 'skip', then skip the sheet
            return [], True

        if row[0] == None or (str(row[0]).title() != "Activity" and not in_act):
            # this is non-activity data

            if in_act and in_exc:
                # this is between activities, write the last activity
                l.append(act)
                in_act = False
                in_exc = False
                act = {}

            l.append(row)

        elif str(row[0]).title() == "Activity" or in_act:
            # this is the start of an activity
            in_act = True

            if str(row[0]).title() != "Exchanges" and not in_exc:
                # this is metadata
                act[row[0]] = row[1:]
            elif str(row[0]).title() == "Exchanges" or in_exc:
                # these are exhanges
                if not in_exc:
                    in_exc = True
                    act["Exchanges"] = [row]
                else:
                    act["Exchanges"].append(row)
    if in_act and in_exc:
        # add the last activity
        l.append(act)

    return l, False

def assign_cpc(inventory, classification_mapping):
    """Assign the first classification that matches all criteria for this activity"""
    def find_classification(act):
        def check_filters(filters) -> bool:
            """Check a series of filters"""
            def check_filter(fltr) -> bool:
                """Check one filter"""
                for field, items in fltr.items():
                    # items is a list of items that field MUST contain
                    if not act.get(field, False):
                        # this field isn't present for this activity, skip
                        continue

                    for item in items:
                        # check if item[1] is in the field, and if it should be (item[0])
                        if not item[0] == (item[1].lower() in act[field][0].lower()):
                            return False
                return True
            ### end of check_filter

            for fltr in filters:
                if check_filter(fltr):
                    return True
            return False  # all of the filters failed
        ### end of check_filters

        for classification, filters in classification_mapping.items():
            # iteratively check every classification
            if check_filters(filters):
                return True, classification

        return False, ""
    ### end of find_classification

    # check each classification and its filters
    failed = []
    for activity in inventory:

        if isinstance(activity, dict):  # activities are dicts, non-dict can be skipped
            add, classification = find_classification(activity)
            if add and classification != "NO CLASSIFICATION":  # we found a classification
                classification_entry = [np.nan] * len(activity["Activity"])
                classification_entry[0] = "CPC::" + classification

                old_classification = activity.get("classifications", False)
                if old_classification and old_classification[0] != classification_entry[0]:  # a different classification was found
                    failed.append(activity)
                    print(">>> FAIL, different classification found for:")
                    print(f"{activity['reference product'][0]} | {activity['Activity'][0]} | {activity['unit'][0]}")
                    print(f"Old: {old_classification[0].split('::')[1]} | New {classification}")
                    print(" + Update the classification mapping so that this activity mapping is not changed")
                    print(" + NEW value will be added to the activity\n")

                activity["classifications"] = classification_entry
            elif add and classification == "NO CLASSIFICATION":  # we found assigned no classification
                continue
            else:  # we found no classification for this activity, this should not happen and mapping should be updated
                failed.append(activity)
                print(">>> FAIL, no classification found for:")
                print(f"{activity['reference product'][0]} | {activity['Activity'][0]} | {activity['unit'][0]}")
                print(" + Update the classification mapping so that this activity is mapped\n")

    return inventory, failed

def inventory_to_df(export_list):
    exp = []
    for row in export_list:
        if not isinstance(row, dict):
            exp.append(row)

        else:
            # write activity
            exp.append(["Activity"] + row["Activity"])
            del row["Activity"]

            # write metadata
            for k, v in row.items():
                if k != "Exchanges":
                    exp.append([k] + v)

            # write exchanges
            for exc in row["Exchanges"]:
                exp.append(exc)

    return pd.DataFrame(exp)

def write_sheet(df, file_path, sheet_name):
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
        workBook = writer.book
        try:
            workBook.remove(workBook[sheet_name])
        finally:
            df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

c_size = 0
for i, file_name in enumerate(files):
    # iterate over files
    c_size += file_sizes[i]
    print(f"Adding/verifying classifications in {file_name} | {round(c_size/total_files_size * 100, 1)}% ({i+1}/{len(files)})")
    file_path = path.join(inventories_folder, file_name)

    # extract the file
    sheets = ExcelExtractor.extract(file_path)  # [(sheet_name, sheet data), ...]

    # iterate over sheets
    for sheet_name, sheet in sheets:
        # convert to inventory list
        inventory, skip = extract_inventory_from_sheet(sheet)
        if skip:
            print(f"  sheet: '{sheet_name}' has skip instruction")
            continue
        n_processes = len([d for d in inventory if isinstance(d, dict)])

        if len(sheets) > 1:
            print(f"  {n_processes} processes in sheet: '{sheet_name}'")

        # assign classifications
        new_inventory, failed = assign_cpc(inventory, cpc_mapping)

        if len(failed) > 0:
            print(f">>> {len(failed)}/{n_processes} FAILED processes in {file_name}, {sheet_name}")
            user = input("continue? [y/N]")
            if user.lower() != "y":
                raise BaseException
        # convert back and write to excel
        new_df = inventory_to_df(new_inventory)
        write_sheet(new_df, file_path, sheet_name)








