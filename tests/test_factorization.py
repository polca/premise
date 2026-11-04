import copy

from premise import NewDatabase
from premise.factorization import FactorizationIdentityRegistry, exchange_identity_key
from premise.inventory.inventory_store import (
    IndexedInventoryList,
    create_inventory_store,
)


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


def test_baseline_identity_uses_code_when_compact_payload_omits_database():
    first = activity("source-code")
    second = activity("source-code")
    first.pop("database")
    second.pop("database")
    first_registry = FactorizationIdentityRegistry()
    second_registry = FactorizationIdentityRegistry()

    first_registry.register_baseline([first])
    second_registry.register_baseline([second])

    assert (
        first_registry.payload_for([first])["identities"]["source-code"]
        == second_registry.payload_for([second])["identities"]["source-code"]
    )


def test_keyless_baseline_identity_tracks_role_not_exchange_amount():
    first = activity("first")
    second = activity("second", amount=2.0)
    for dataset in (first, second):
        dataset.pop("database")
        dataset.pop("code")
    first_registry = FactorizationIdentityRegistry()
    second_registry = FactorizationIdentityRegistry()

    first_registry.register_baseline([first])
    second_registry.register_baseline([second])

    assert first_registry.register_created(
        first, sector="ignored"
    ) == second_registry.register_created(second, sector="ignored")


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
    exchange_key = exchange_identity_key(consumer["exchanges"][1], 0)
    assert payload["provider_identities"]["consumer"][exchange_key] == first_identity
    assert payload["ambiguous_provider_count"] == 1
    assert database == expected


def test_sidecar_preserves_atomic_exchange_identities_after_materialization():
    original = activity("source-code")
    original["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([original])
    first = registry.payload_for([original])
    materialized = copy.deepcopy(original)
    second = registry.payload_for([materialized])

    assert first["exchange_identities"] == second["exchange_identities"]
    assert len(first["exchange_identities"]["source-code"]) == 2


def test_added_exchange_identity_depends_on_content_not_python_object_address():
    source = activity("source-code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    added = {
        "name": "first input",
        "product": "first product",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 0.5,
    }
    source["exchanges"].append(added)

    first = registry.payload_for([source])
    first_key = exchange_identity_key(added, 0)
    first_identity = first["exchange_identities"]["source-code"][first_key]

    # Reusing and mutating the same dictionary simulates the object-address
    # reuse which occurs when sector updates remove and create many exchanges.
    added.update(name="second input", product="second product")
    second = registry.payload_for([source])
    second_key = exchange_identity_key(added, 0)
    second_identity = second["exchange_identities"]["source-code"][second_key]

    assert second_identity != first_identity


def test_identities_survive_inventory_materialization_to_new_objects():
    original = activity("source-code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([original])
    materialized = copy.deepcopy(original)

    payload = registry.payload_for([materialized])

    assert payload["missing_count"] == 0
    assert payload["identities"]["source-code"]


def test_untracked_legacy_addition_gets_recovered_identity():
    added = activity("legacy-addition")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([])
    with registry.session("electricity"):
        pass

    payload = registry.payload_for([added])

    assert payload["missing_count"] == 0
    assert payload["identities"]["legacy-addition"]


def test_compact_store_addition_merges_into_existing_sidecar():
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([])
    store = create_inventory_store([], backend="compact")

    with registry.session("emissions"), store.transaction("add") as transaction:
        transaction.add_activity(activity("compact-addition"))

    payload = registry.merge_registered_identities(
        {
            "schema_version": registry.schema_version,
            "identities": {},
            "missing_count": 0,
        }
    )

    assert payload["missing_count"] == 0
    assert payload["identities"]["compact-addition"]


def test_new_database_returns_a_copy_of_factorization_metadata():
    database = NewDatabase.__new__(NewDatabase)
    database.factorization_mode = True
    database.scenarios = [
        {
            "_factorization_identities": {
                "schema_version": FactorizationIdentityRegistry.schema_version,
                "identities": {"source-code": "stable-id"},
            }
        }
    ]

    metadata = database.factorization_metadata()
    metadata["identities"]["source-code"] = "changed"

    assert (
        database.scenarios[0]["_factorization_identities"]["identities"]["source-code"]
        == "stable-id"
    )


def test_new_database_finalizes_atomic_sidecar_on_materialized_output():
    source = activity("source-code")
    materialized = copy.deepcopy(source)
    definition = {"model": "remind", "pathway": "SSP2-NPi", "year": 2050}
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    database = NewDatabase.__new__(NewDatabase)
    database.factorization_mode = True
    database.scenarios = [definition]
    database._factorization_registries = {
        database._scenario_identity(definition): registry
    }

    payload = database.factorization_metadata_for([materialized])

    assert payload["exchange_identities"]["source-code"]
    assert (
        database.factorization_metadata()["exchange_identities"]
        == payload["exchange_identities"]
    )
