"""Preserve export linking rules independently of inventory normalization."""

from copy import deepcopy

import pytest

from premise.export import check_geographical_linking


def scenario_with_supplier(
    activity_updates=None, exchange_updates=None, locations=("CH",)
):
    exchange = {
        "type": "technosphere",
        "name": "fuel supply",
        "product": "fuel",
        "location": "DE",
        "amount": 2,
        "input": ("source", "supplier-code"),
        **(exchange_updates or {}),
    }
    activity = {
        "name": "heat production",
        "reference product": "heat",
        "location": "CH",
        "exchanges": [exchange],
        **(activity_updates or {}),
    }
    return {
        "database": [activity],
        "index": {
            (exchange["name"], exchange["product"]): [
                {"location": location} for location in locations
            ]
        },
    }


@pytest.mark.parametrize(
    "activity,exchange,locations,expected_location",
    [
        ({}, {}, ("CH", "US"), "CH"),
        ({"location": "GLO"}, {}, ("GLO",), "DE"),
        ({"location": "RoW"}, {}, ("RoW",), "DE"),
        ({"location": "World"}, {}, ("World",), "DE"),
        ({"name": "market for heat"}, {}, ("CH",), "DE"),
        ({"name": "Market for heat"}, {}, ("CH",), "CH"),
        ({"name": "heat import"}, {}, ("CH",), "DE"),
        ({"name": "heat mixing"}, {}, ("CH",), "DE"),
        ({"name": "heat transport"}, {}, ("CH",), "DE"),
        ({}, {"name": "fuel import"}, ("CH",), "DE"),
        ({}, {"name": "fuel mix"}, ("CH",), "DE"),
        ({}, {"name": "fuel transport"}, ("CH",), "DE"),
        ({}, {"type": "production"}, ("CH",), "DE"),
        ({}, {"type": "biosphere"}, ("CH",), "DE"),
        ({}, {"location": "CH"}, (), "CH"),
        ({}, {}, (), "DE"),
        ({}, {}, ("US",), "DE"),
        ({"name": "fuel supply", "reference product": "fuel"}, {}, ("CH",), "DE"),
        ({"name": "fuel supply", "reference product": "heat"}, {}, ("CH",), "CH"),
    ],
)
def test_geographical_linking_only_changes_eligible_exchange_locations(
    activity, exchange, locations, expected_location
):
    scenario = scenario_with_supplier(activity, exchange, locations)
    expected = deepcopy(scenario)
    expected["database"][0]["exchanges"][0]["location"] = expected_location

    assert check_geographical_linking(scenario, []) is scenario
    assert scenario == expected


def test_original_activity_identity_is_preserved_even_with_a_local_supplier():
    scenario = scenario_with_supplier()
    expected = deepcopy(scenario)
    original = [
        {
            key: value
            for key, value in scenario["database"][0].items()
            if key != "exchanges"
        }
    ]

    check_geographical_linking(scenario, original)

    assert scenario == expected


def test_absent_supplier_index_does_not_infer_suppliers_from_the_inventory():
    scenario = scenario_with_supplier()
    del scenario["index"]
    expected = deepcopy(scenario)

    check_geographical_linking(scenario, [])

    assert scenario == expected


def test_supplier_locations_are_reused_within_a_call_but_refresh_between_calls():
    class CountingProviders(list):
        scans = 0

        def __iter__(self):
            self.scans += 1
            return super().__iter__()

    scenario = scenario_with_supplier()
    second = deepcopy(scenario["database"][0])
    second["location"] = "US"
    scenario["database"].append(second)
    key = ("fuel supply", "fuel")
    providers = CountingProviders([{"location": "CH"}, {"location": "US"}])
    scenario["index"][key] = providers

    check_geographical_linking(scenario, [])

    assert providers.scans == 1
    assert [ds["exchanges"][0]["location"] for ds in scenario["database"]] == [
        "CH",
        "US",
    ]
    for ds in scenario["database"]:
        ds["exchanges"][0]["location"] = "DE"
    providers[:] = [{"location": "US"}]

    check_geographical_linking(scenario, [])

    assert providers.scans == 2
    assert [ds["exchanges"][0]["location"] for ds in scenario["database"]] == [
        "DE",
        "US",
    ]
