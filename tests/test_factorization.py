import copy

from premise.factorization import FactorizationIdentityRegistry
from premise.inventory.inventory_store import IndexedInventoryList


def activity(code, *, amount=1.0):
    return {
        "database": "scenario",
        "code": code,
        "name": "market for hydrogen",
        "reference product": "hydrogen",
        "location": "CHA",
        "unit": "kilogram",
        "exchanges": [
            {
                "database": "scenario",
                "code": code,
                "input": ("scenario", code),
                "name": "market for hydrogen",
                "product": "hydrogen",
                "location": "CHA",
                "unit": "kilogram",
                "type": "production",
                "amount": amount,
            }
        ],
    }


def test_baseline_identity_uses_source_key_and_does_not_mutate_payload():
    first = activity("source-code")
    second = activity("source-code")
    expected_first = copy.deepcopy(first)
    expected_second = copy.deepcopy(second)
    first_registry = FactorizationIdentityRegistry()
    second_registry = FactorizationIdentityRegistry()

    first_registry.register_baseline([first])
    second_registry.register_baseline([second])

    first_identity = first_registry.payload_for([first])["identities"]["source-code"]
    second_identity = second_registry.payload_for([second])["identities"]["source-code"]
    assert first_identity == second_identity
    assert first == expected_first
    assert second == expected_second


def test_creator_sector_distinguishes_semantically_identical_added_markets():
    electricity = activity("electricity-market")
    fuels = activity("fuels-market")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([])

    with registry.session("electricity"):
        registry.register_created(electricity)
    with registry.session("fuels"):
        registry.register_created(fuels)

    identities = registry.payload_for([electricity, fuels])["identities"]
    assert identities["electricity-market"] != identities["fuels-market"]


def test_indexed_inventory_additions_register_in_active_session():
    baseline = activity("baseline")
    added = activity("added")
    database = IndexedInventoryList([baseline])
    registry = FactorizationIdentityRegistry()
    registry.register_baseline(database)

    with registry.session("electricity"):
        database.append(added)

    payload = registry.payload_for(database)
    assert payload["missing_count"] == 0
    assert set(payload["identities"]) == {"baseline", "added"}


def test_created_identity_is_reproducible_across_builds():
    identities = []
    for code in ("random-a", "random-b"):
        dataset = activity(code)
        registry = FactorizationIdentityRegistry()
        registry.register_baseline([])
        with registry.session("electricity"):
            registry.register_created(dataset)
        identities.append(registry.payload_for([dataset])["identities"][code])

    assert identities[0] == identities[1]


def test_sidecar_resolves_duplicate_provider_metadata_without_mutating_exchanges():
    consumer = activity("consumer")
    consumer["name"] = "hydrogen user"
    consumer["reference product"] = "service"
    consumer["exchanges"].append(
        {
            "name": "market for hydrogen",
            "product": "hydrogen",
            "location": "CHA",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 0.5,
        }
    )
    first_provider = activity("first-provider")
    second_provider = activity("second-provider")
    database = [consumer, first_provider, second_provider]
    expected = copy.deepcopy(database)
    registry = FactorizationIdentityRegistry()
    registry.register_baseline(database)

    payload = registry.payload_for(database)

    first_identity = payload["identities"]["first-provider"]
    assert payload["provider_identities"]["consumer"]["1"] == first_identity
    assert payload["ambiguous_provider_count"] == 1
    assert database == expected


def test_identities_survive_inventory_materialization_to_new_objects():
    original = activity("source-code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([original])
    materialized = copy.deepcopy(original)

    payload = registry.payload_for([materialized])

    assert payload["missing_count"] == 0
    assert payload["identities"]["source-code"]
