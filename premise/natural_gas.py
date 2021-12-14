"""
natural_gas.py contains `update_ng_production_ds()` which replaces
original natural gas extraction datasets with those provided by
ESU services.
"""


def update_ng_production_ds(database):
    """
    Relink updated datasets for natural gas extraction from
    http://www.esu-services.ch/fileadmin/download/publicLCI/meili-2021-LCI%20for%20the%20oil%20and%20gas%20extraction.pdf
    to high pressure natural gas markets.
    :param database: wurst database
    :return: wurst database, with updated natural gas extraction datasets
    """

    print("Update natural gas production datasets...")

    countries = ["DE", "DZ", "GB", "NG", "NL", "NO", "RU", "US"]

    names = ["natural gas production", "petroleum and gas production"]

    for ds in database:
        amount = {}
        to_remove = []
        for exc in ds["exchanges"]:
            if (
                any(i in exc["name"] for i in names)
                and "natural gas, high pressure"
                and exc["location"] in countries
                and exc["type"] == "technosphere"
            ):
                if exc["location"] in amount:
                    amount[exc["location"]] += exc["amount"]
                else:
                    amount[exc["location"]] = exc["amount"]
                to_remove.append((exc["name"], exc["product"], exc["location"]))

        if amount:
            ds["exchanges"] = [
                e
                for e in ds["exchanges"]
                if (e["name"], e.get("product"), e.get("location")) not in to_remove
            ]

            for loc in amount:
                ds["exchanges"].append(
                    {
                        "name": "natural gas, at production",
                        "product": "natural gas, high pressure",
                        "location": loc,
                        "unit": "cubic meter",
                        "amount": amount[loc],
                        "type": "technosphere"
                    }
                )

    return database
