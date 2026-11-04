import copy

from premise import NewDatabase
from premise.factorization import (
    FACTORIZATION_EXCHANGE_LINEAGE_FIELD,
    FactorizationIdentityRegistry,
    current_factorization_activity_identity,
    exchange_identity_key,
    register_factorization_shared_operation,
)
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


def test_factorization_registry_collects_invariant_shared_operations():
    registry = FactorizationIdentityRegistry()
    operation = {
        "operator": "allocate-fuel-carbon-origin",
        "location": "EUR",
        "non_fossil_share": 0.25,
    }

    with registry.session("fuels"):
        register_factorization_shared_operation(operation)
        register_factorization_shared_operation(operation)

    assert registry.payload_for([])["shared_operations"] == [
        {"sector": "fuels", **operation}
    ]


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


def test_code_free_baseline_identity_survives_materialization_with_code():
    source = activity("source")
    source.pop("database")
    source.pop("code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    expected = registry.identity_for(source)
    materialized = copy.deepcopy(source)
    materialized["code"] = "materialized-code"

    payload = registry.payload_for([materialized])

    assert payload["identities"]["materialized-code"] == expected


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


def test_creator_sector_does_not_change_cross_run_logical_identity():
    identities = []
    for sector in ("final energy", "fuels"):
        dataset = activity(f"{sector}-random-code")
        registry = FactorizationIdentityRegistry()
        registry.register_baseline([])
        with registry.session(sector):
            identities.append(registry.register_created(dataset))

    assert identities[0] == identities[1]


def test_transient_creation_does_not_shift_delivered_logical_identity():
    direct = activity("direct")
    direct_registry = FactorizationIdentityRegistry()
    direct_registry.register_baseline([])
    with direct_registry.session("final energy"):
        direct_registry.register_created(direct)
    expected = direct_registry.payload_for([direct])["identities"]["direct"]

    transient = activity("transient")
    delivered = activity("delivered")
    rebuilt_registry = FactorizationIdentityRegistry()
    rebuilt_registry.register_baseline([])
    with rebuilt_registry.session("fuels"):
        rebuilt_registry.register_created(transient)
        rebuilt_registry.register_created(delivered)

    assert (
        rebuilt_registry.payload_for([delivered])["identities"]["delivered"] == expected
    )


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


def test_created_identity_is_reused_when_compact_copy_gains_a_code():
    created = activity("")
    created.pop("code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([])
    with registry.session("fuels"):
        first = registry.register_created(created)
        materialized = copy.deepcopy(created)
        materialized["code"] = "compact-code"
        second = registry.register_created(materialized)

    assert first == second
    assert registry.payload_for([materialized])["identities"]["compact-code"] == first


def test_compact_activity_record_resolves_baseline_identity_by_storage_id():
    source = activity("source-code")
    store = create_inventory_store([source], backend="compact")
    record = store.activity(next(store.iter_activity_ids()))
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([record])

    fresh_record = store.activity(record.activity_id)
    with registry.session("final energy"):
        identity = current_factorization_activity_identity(fresh_record)

    assert identity == registry.identity_for(record)


def test_derived_identity_is_sector_independent_and_uses_source_lineage():
    source = activity("source-code")
    other_source = activity("other-source-code")
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source, other_source])

    final_energy_proxy = activity("final-random-code")
    fuels_proxy = activity("fuels-random-code")

    with registry.session("final energy"):
        final_identity = registry.register_derived(
            final_energy_proxy,
            source=source,
            operation="regionalized-proxy",
            qualifier="EUR",
        )
    with registry.session("fuels"):
        fuels_identity = registry.register_derived(
            fuels_proxy,
            source=source,
            operation="regionalized-proxy",
            qualifier="EUR",
        )
        other_identity = registry.register_derived(
            activity("other-random-code"),
            source=other_source,
            operation="regionalized-proxy",
            qualifier="EUR",
        )

    assert final_identity == fuels_identity
    assert other_identity != final_identity


def test_derived_activity_preserves_inherited_exchange_lineage():
    identities = []
    for sector in ("final energy", "fuels"):
        source = activity("source-code")
        registry = FactorizationIdentityRegistry()
        registry.register_baseline([source])
        derived = copy.deepcopy(source)
        derived["code"] = f"{sector}-random-code"

        with registry.session(sector):
            registry.register_derived(
                derived,
                source=source,
                operation="regionalized-proxy",
                qualifier="EUR",
            )
        derived["location"] = "EUR"
        derived["exchanges"][0]["location"] = "EUR"
        payload = registry.payload_for([derived])
        identities.append(
            next(iter(payload["exchange_identities"][derived["code"]].values()))
        )
        assert FACTORIZATION_EXCHANGE_LINEAGE_FIELD not in derived["exchanges"][0]

    assert identities[0] == identities[1]


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


def test_new_database_exports_activity_ownership_from_provenance():
    source = activity("source-code")
    definition = {"model": "remind", "pathway": "SSP2-NPi", "year": 2050}
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    database = NewDatabase.__new__(NewDatabase)
    database.factorization_mode = True
    database.scenarios = [definition]
    database.build_id = "ownership-test"
    database._factorization_registries = {
        database._scenario_identity(definition): registry
    }
    collector = database._get_provenance_collector()
    collector.record(
        scenario_identity=database._scenario_identity(definition),
        sector="fuels",
        transformation="fuels",
        activity={"code": "source-code"},
        reason_code="fuels.market",
        explanation="Fuel market rebuilt from immutable IAM weights.",
        status="modified",
    )

    payload = database.factorization_metadata_for([source])
    identity = payload["identities"]["source-code"]

    assert payload["activity_ownership"][identity] == [
        {
            "sector": "fuels",
            "status": "modified",
            "reason_code": "fuels.market",
        }
    ]


def test_new_database_exports_code_free_ownership_by_stable_identity():
    source = activity("source-code")
    definition = {"model": "remind", "pathway": "SSP2-NPi", "year": 2050}
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    identity = registry.identity_for(source)
    database = NewDatabase.__new__(NewDatabase)
    database.factorization_mode = True
    database.scenarios = [definition]
    database.build_id = "ownership-test"
    database._factorization_registries = {
        database._scenario_identity(definition): registry
    }
    database._get_provenance_collector().record(
        scenario_identity=database._scenario_identity(definition),
        sector="final energy",
        transformation="final energy",
        activity={
            "code": None,
            "factorization identity": identity,
            "name": source["name"],
        },
        reason_code="final energy.empty",
        explanation="Original activity replaced by regional providers.",
        status="empty",
    )

    payload = database.factorization_metadata_for([source])

    assert payload["activity_ownership"][identity][0]["status"] == "empty"


def test_new_database_reconciles_code_free_ownership_after_materialization():
    source = activity("source-code")
    definition = {"model": "remind", "pathway": "SSP2-NPi", "year": 2050}
    registry = FactorizationIdentityRegistry()
    registry.register_baseline([source])
    database = NewDatabase.__new__(NewDatabase)
    database.factorization_mode = True
    database.scenarios = [definition]
    database.build_id = "ownership-test"
    database._factorization_registries = {
        database._scenario_identity(definition): registry
    }
    database._get_provenance_collector().record(
        scenario_identity=database._scenario_identity(definition),
        sector="final energy",
        transformation="final energy",
        activity={
            "code": None,
            "factorization identity": "pre-materialization-identity",
            "name": source["name"],
            "product": source["reference product"],
            "location": source["location"],
            "unit": source["unit"],
        },
        reason_code="final energy.empty",
        explanation="Original activity replaced by regional providers.",
        status="empty",
    )

    payload = database.factorization_metadata_for([source])
    identity = payload["identities"]["source-code"]

    assert payload["activity_ownership"][identity][0]["status"] == "empty"
