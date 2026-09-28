import copy
import math
import random

import pytest

import premise
import premise.new_database as new_database_module
from premise.inventory_store import CompactInventoryStore, InventoryStore
from premise.new_database import NewDatabase
from premise.validation_framework import (
    ActivitySelector,
    InventoryGraphValidator,
    PremiseValidationError,
    ValidationIntent,
    ValidationPhaseResult,
    ValidationReport,
    ValidationRuleResult,
    ValidationSuppression,
    inventory_activity_fingerprints,
    inventory_baseline_snapshot,
    inventory_cycle_signatures,
    inventory_store_fingerprint,
)

SUPPLIER_KEY = ("fuel production", "fuel", "CH")
MARKET_KEY = ("market for fuel", "fuel", "CH")


@pytest.fixture
def valid_inventory():
    return [
        {
            "name": SUPPLIER_KEY[0],
            "reference product": SUPPLIER_KEY[1],
            "location": SUPPLIER_KEY[2],
            "unit": "kilogram",
            "database": "test-db",
            "code": "supplier",
            "exchanges": [
                {
                    "name": SUPPLIER_KEY[0],
                    "product": SUPPLIER_KEY[1],
                    "location": SUPPLIER_KEY[2],
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                }
            ],
        },
        {
            "name": MARKET_KEY[0],
            "reference product": MARKET_KEY[1],
            "location": MARKET_KEY[2],
            "unit": "kilogram",
            "database": "test-db",
            "code": "market",
            "exchanges": [
                {
                    "name": MARKET_KEY[0],
                    "product": MARKET_KEY[1],
                    "location": MARKET_KEY[2],
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                },
                {
                    "name": SUPPLIER_KEY[0],
                    "product": SUPPLIER_KEY[1],
                    "location": SUPPLIER_KEY[2],
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": 1.0,
                    "input": ("test-db", "supplier"),
                },
            ],
        },
    ]


def validate(inventory, **kwargs):
    return InventoryGraphValidator(
        CompactInventoryStore(inventory, scenario_identity=("image", "path", 2050)),
        scenario_identity=("image", "path", 2050),
        source_fingerprint="source",
        iam_fingerprint="iam",
        version="3.12",
        **kwargs,
    ).validate()


def test_validation_contract_types_are_public():
    assert premise.ValidationIssue is not None
    assert premise.ValidationPhaseResult is ValidationPhaseResult
    assert premise.ValidationReport is ValidationReport
    assert premise.PremiseValidationError is PremiseValidationError


def rule_issues(report, rule_id):
    return tuple(issue for issue in report.issues if issue.rule_id == rule_id)


def test_valid_graph_is_accepted_and_validation_is_read_only(valid_inventory):
    store = CompactInventoryStore(valid_inventory)
    before = inventory_store_fingerprint(store)

    certificate = InventoryGraphValidator(
        store,
        source_fingerprint="source",
        iam_fingerprint="iam",
        version="3.12",
    ).certify()

    assert certificate.report.valid
    assert inventory_store_fingerprint(store) == before


def test_baseline_snapshot_combines_fingerprints_and_cycle_audit(valid_inventory):
    store = CompactInventoryStore(valid_inventory)

    fingerprints, cycles = inventory_baseline_snapshot(store)

    assert dict(fingerprints) == dict(inventory_activity_fingerprints(store))
    assert cycles == inventory_cycle_signatures(store)


@pytest.mark.parametrize(
    ("mutate", "rule_id"),
    [
        (lambda data: data[0].pop("unit"), "GRAPH.REQUIRED_ACTIVITY_FIELDS"),
        (
            lambda data: data[0]["exchanges"][0].update(type="invalid"),
            "GRAPH.EXCHANGE_TYPE",
        ),
        (
            lambda data: data[1]["exchanges"][1].pop("product"),
            "GRAPH.REQUIRED_EXCHANGE_FIELDS",
        ),
        (
            lambda data: data[0]["exchanges"][0].update(amount=math.nan),
            "GRAPH.FINITE_NUMERIC",
        ),
        (
            lambda data: data[0]["exchanges"][0].update(
                {"uncertainty type": 5, "loc": 1.0, "minimum": 2.0, "maximum": 3.0}
            ),
            "GRAPH.UNCERTAINTY",
        ),
        (
            lambda data: data[0]["exchanges"][0].update(product="wrong"),
            "GRAPH.PRODUCTION_REFERENCE",
        ),
        (
            lambda data: data[0]["exchanges"][0].update(amount=0.0),
            "GRAPH.REFERENCE_PRODUCTION_AMOUNT",
        ),
        (
            lambda data: data[1]["exchanges"][1].update(
                name="missing provider", input=("test-db", "missing")
            ),
            "GRAPH.PROVIDER_EXISTS",
        ),
        (
            lambda data: data[1]["exchanges"][1].update(unit="megajoule"),
            "GRAPH.PROVIDER_PRODUCT_UNIT",
        ),
        (
            lambda data: data[1]["exchanges"][1].update(amount=-0.1),
            "GRAPH.NEGATIVE_MARKET_SHARE",
        ),
        (
            lambda data: data[1]["exchanges"].append(
                copy.deepcopy(data[1]["exchanges"][1])
            ),
            "GRAPH.DUPLICATE_SUPPLIER",
        ),
    ],
)
def test_direct_graph_rule_rejections(valid_inventory, mutate, rule_id):
    mutate(valid_inventory)
    report = validate(valid_inventory)

    assert rule_issues(report, rule_id)
    with pytest.raises(PremiseValidationError):
        report.raise_for_errors()


def test_infinite_uncertainty_parameter_is_rejected(valid_inventory):
    valid_inventory[0]["exchanges"][0].update(
        {"uncertainty type": 3, "loc": 1.0, "scale": math.inf}
    )

    report = validate(valid_inventory)

    assert rule_issues(report, "GRAPH.UNCERTAINTY")


def test_ambiguous_provider_without_resolving_identifier_is_rejected(valid_inventory):
    duplicate = copy.deepcopy(valid_inventory[0])
    duplicate["code"] = "supplier-duplicate"
    valid_inventory.append(duplicate)
    valid_inventory[1]["exchanges"][1].pop("input")

    report = validate(valid_inventory)

    assert rule_issues(report, "GRAPH.PROVIDER_AMBIGUOUS")


def test_stale_input_is_rejected_even_when_semantic_provider_exists(valid_inventory):
    valid_inventory[1]["exchanges"][1]["input"] = ("test-db", "removed-code")

    report = validate(valid_inventory)

    assert rule_issues(report, "GRAPH.STALE_SUPPLIER")


def test_repeated_provider_with_a_distinct_exchange_record_is_supported(
    valid_inventory,
):
    duplicate = copy.deepcopy(valid_inventory[1]["exchanges"][1])
    duplicate["amount"] = 0.25
    valid_inventory[1]["exchanges"].append(duplicate)

    report = validate(valid_inventory)

    assert not rule_issues(report, "GRAPH.DUPLICATE_SUPPLIER")


def test_exact_input_identifier_supports_duplicate_semantic_activity_keys(
    valid_inventory,
):
    duplicate = copy.deepcopy(valid_inventory[0])
    duplicate["code"] = "supplier-duplicate"
    valid_inventory.append(duplicate)

    report = validate(valid_inventory)

    assert not rule_issues(report, "GRAPH.PROVIDER_AMBIGUOUS")


def test_wrong_geographic_fallback_is_rejected(valid_inventory):
    global_supplier = copy.deepcopy(valid_inventory[0])
    global_supplier["location"] = "GLO"
    global_supplier["code"] = "supplier-global"
    global_supplier["exchanges"][0]["location"] = "GLO"
    valid_inventory.append(global_supplier)
    valid_inventory[1]["exchanges"][1].update(
        {"location": "GLO", "input": ("test-db", "supplier-global")}
    )

    intent = ValidationIntent(
        transformation="geographic relinking",
        affected_activity_keys=frozenset({MARKET_KEY}),
        expected_match_count=1,
        intended_suppliers={MARKET_KEY: ((SUPPLIER_KEY, 1.0),)},
    )
    report = validate(valid_inventory, intent=intent)

    assert rule_issues(report, "GRAPH.GEOGRAPHIC_FALLBACK")


def test_new_cycle_is_rejected_but_baseline_cycle_is_retained(valid_inventory):
    valid_inventory[0]["exchanges"].append(
        {
            "name": MARKET_KEY[0],
            "product": MARKET_KEY[1],
            "location": MARKET_KEY[2],
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 0.1,
            "input": ("test-db", "market"),
        }
    )
    store = CompactInventoryStore(valid_inventory)
    cycles = inventory_cycle_signatures(store)

    rejected = InventoryGraphValidator(store).validate()
    retained = InventoryGraphValidator(store, baseline_cycles=cycles).validate()

    assert rule_issues(rejected, "GRAPH.NEW_FORBIDDEN_CYCLE")
    assert not rule_issues(retained, "GRAPH.NEW_FORBIDDEN_CYCLE")


def test_zero_match_target_and_expected_coverage_are_explicit(valid_inventory):
    intent = ValidationIntent(
        transformation="electricity",
        affected_activity_keys=frozenset({("missing", "electricity", "EUR")}),
        expected_match_count=1,
        expected_regions=("EUR",),
        expected_technologies=("solar photovoltaic",),
    )

    report = validate(valid_inventory, intent=intent)

    cardinality = next(
        result
        for result in report.rule_results
        if result.rule_id == "GRAPH.RULE_TARGET_CARDINALITY"
    )
    assert cardinality.checked_object_count == 0
    assert cardinality.expected == 1
    assert rule_issues(report, "GRAPH.RULE_TARGET_CARDINALITY")
    assert len(rule_issues(report, "METHOD.EXPECTED_COVERAGE")) == 2


def test_expected_coverage_is_limited_to_declared_targets(valid_inventory):
    intent = ValidationIntent(
        transformation="electricity",
        affected_activity_keys=frozenset({MARKET_KEY}),
        expected_match_count=1,
        expected_technologies=(SUPPLIER_KEY[0],),
    )

    report = validate(valid_inventory, intent=intent)

    assert rule_issues(report, "METHOD.EXPECTED_COVERAGE")


def test_consequential_average_algorithm_and_wrong_supplier_vector_are_rejected(
    valid_inventory,
):
    intent = ValidationIntent(
        transformation="fuels",
        affected_activity_keys=frozenset({MARKET_KEY}),
        expected_match_count=1,
        algorithm="average production-volume mix",
        intended_suppliers={MARKET_KEY: ((SUPPLIER_KEY, 0.25),)},
    )

    report = validate(valid_inventory, system_model="consequential", intent=intent)

    assert rule_issues(report, "METHOD.CONSEQUENTIAL_ALGORITHM")
    assert rule_issues(report, "METHOD.SUPPLIER_VECTOR")


def test_correct_marginal_supplier_vector_is_accepted(valid_inventory):
    intent = ValidationIntent(
        transformation="electricity",
        affected_activity_keys=frozenset({MARKET_KEY}),
        expected_match_count=1,
        algorithm="marginal-mix",
        intended_suppliers={MARKET_KEY: ((SUPPLIER_KEY, 1.0),)},
    )

    report = validate(valid_inventory, system_model="consequential", intent=intent)

    assert not rule_issues(report, "METHOD.CONSEQUENTIAL_ALGORITHM")
    assert not rule_issues(report, "METHOD.SUPPLIER_VECTOR")


def test_collateral_mutation_addition_and_loss_are_rejected(valid_inventory):
    baseline_store = CompactInventoryStore(valid_inventory)
    fingerprints = inventory_activity_fingerprints(baseline_store)
    mutated = copy.deepcopy(valid_inventory)
    mutated[0]["comment"] = "unexpected"
    mutated.pop(1)
    added = copy.deepcopy(mutated[0])
    added.update(name="unexpected activity", code="unexpected")
    added["exchanges"][0]["name"] = "unexpected activity"
    mutated.append(added)
    intent = ValidationIntent(
        transformation="steel",
        affected_activity_keys=frozenset({MARKET_KEY}),
        baseline_fingerprints=fingerprints,
    )

    report = validate(mutated, intent=intent)

    scope_issues = rule_issues(report, "GRAPH.TRANSFORMATION_SCOPE")
    assert len(scope_issues) == 3


def test_suppression_is_narrow_versioned_and_keeps_issue_visible(valid_inventory):
    valid_inventory[1]["exchanges"][1]["amount"] = -0.1
    suppression = ValidationSuppression(
        rule_id="GRAPH.NEGATIVE_MARKET_SHARE",
        selector=ActivitySelector(name=MARKET_KEY[0], location="CH"),
        versions=("3.12",),
        system_models=("cutoff",),
        explanation="Synthetic reviewed exception.",
    )
    store = CompactInventoryStore(valid_inventory)

    report = InventoryGraphValidator(
        store,
        version="3.12",
        system_model="cutoff",
        suppressions=(suppression,),
    ).validate()

    issue = rule_issues(report, "GRAPH.NEGATIVE_MARKET_SHARE")[0]
    assert issue.suppressed
    assert issue.suppression_explanation == "Synthetic reviewed exception."
    assert issue in report.suppressed_issues
    assert issue not in report.errors


def test_activity_selector_supports_anchored_name_patterns():
    selector = ActivitySelector(
        name_pattern=r"^market group for electricity, high voltage$"
    )

    assert selector.matches({"name": "market group for electricity, high voltage"})
    assert not selector.matches(
        {"name": "market group for electricity, medium voltage"}
    )


def test_certificate_cache_invalidation_and_checkpoint_reuse(valid_inventory, tmp_path):
    store = CompactInventoryStore(valid_inventory, scenario_identity="scenario")
    validator = InventoryGraphValidator(
        store, source_fingerprint="s", iam_fingerprint="i"
    )

    first = validator.certify()
    second = validator.certify()
    checkpoint = store.checkpoint(tmp_path / "validated.inventory-store")
    reopened = InventoryStore.open(checkpoint)
    reopened_certificate = InventoryGraphValidator(
        reopened, source_fingerprint="s", iam_fingerprint="i"
    ).certify()

    assert not first.report.reused
    assert second.report.reused
    assert reopened_certificate.report.reused

    with store.transaction("invalid mutation") as transaction:
        transaction.patch_exchange(0, {"amount": math.nan})
    invalid = InventoryGraphValidator(
        store, source_fingerprint="s", iam_fingerprint="i"
    ).certify(raise_on_error=False)
    assert invalid.cache_key != first.cache_key
    assert rule_issues(invalid.report, "GRAPH.FINITE_NUMERIC")


def test_report_aggregates_phases_and_keeps_export_phase_transient():
    rule = ValidationRuleResult(
        rule_id="EXPORT.SCHEMA",
        severity="error",
        applicability="applicable",
        checked_object_count=1,
    )
    graph = ValidationReport(
        scenario_identity="scenario",
        store_generation=1,
        ruleset_version=2,
        certificate_key="key",
        rule_results=(),
    )
    combined = graph.with_phase(
        ValidationPhaseResult(
            phase_id="export:brightway", kind="export", rule_results=(rule,)
        )
    )

    assert combined.get_phase("export:brightway") is not None
    assert combined.semantic_only().get_phase("export:brightway") is None
    assert ValidationReport.from_dict(combined.to_dict()).phase_results == (
        combined.phase_results
    )


def test_validation_intent_roundtrip_preserves_tuple_keyed_contracts():
    intent = ValidationIntent(
        transformation="electricity",
        affected_activity_keys=frozenset({MARKET_KEY}),
        intended_suppliers={MARKET_KEY: ((SUPPLIER_KEY, 1.0),)},
        baseline_fingerprints={MARKET_KEY: "abc"},
    )

    assert ValidationIntent.from_dict(intent.to_dict()) == intent


def test_seeded_randomized_invalid_amount_mutations_are_deterministic(valid_inventory):
    randomizer = random.Random(42)
    invalid_values = [math.nan, math.inf, -math.inf, "not-a-number"]
    observed = []
    for _ in range(20):
        inventory = copy.deepcopy(valid_inventory)
        activity = randomizer.randrange(len(inventory))
        exchange = randomizer.randrange(len(inventory[activity]["exchanges"]))
        inventory[activity]["exchanges"][exchange]["amount"] = randomizer.choice(
            invalid_values
        )
        report = validate(inventory)
        observed.append(bool(rule_issues(report, "GRAPH.FINITE_NUMERIC")))

    assert observed == [True] * 20


def configured_new_database(valid_inventory):
    database = object.__new__(NewDatabase)
    database._validation_enabled = True
    database._validation_reports = {}
    database._validation_baseline_cycles = frozenset()
    database.source = "test-db"
    database.version = "3.12"
    database.system_model = "cutoff"
    database._compact_source_checkpoint = None
    database._source_inventory_store = CompactInventoryStore(
        valid_inventory, scenario_identity="source"
    )
    database.database_cache_filepath = None
    database.inventories_cache_filepath = None
    database.scenarios = []
    return database


def test_new_database_persists_and_exposes_completed_report(
    valid_inventory, tmp_path, monkeypatch
):
    database = configured_new_database(valid_inventory)
    definition = {"model": "image", "pathway": "path", "year": 2050}
    database.scenarios = [definition]
    runtime = definition.copy()
    runtime["_inventory_working_copy"] = copy.deepcopy(valid_inventory)
    monkeypatch.setattr(new_database_module, "DIR_CACHED_FILES", tmp_path)

    live_store = database._store_updated_scenario(definition, runtime, persist=True)
    report = database.get_validation_report()

    assert report.valid
    assert definition["_validation_report"]["certificate_key"] == report.certificate_key
    checkpoint = definition["_inventory_checkpoint"]
    handoff = definition["_inventory_export_handoff"]
    assert handoff is live_store
    assert handoff.materialize(restore_metadata=True) == InventoryStore.open(
        checkpoint
    ).materialize(restore_metadata=True)
    manifest = (checkpoint / "manifest.json").read_text(encoding="utf-8")
    assert '"validation_certificate"' in manifest


def test_covered_update_uses_incremental_certificate_without_graph_scan(
    valid_inventory, monkeypatch
):
    database = configured_new_database(valid_inventory)
    definition = {"model": "image", "pathway": "path", "year": 2050}
    database.scenarios = [definition]
    phase = ValidationPhaseResult(
        phase_id="sector:electricity:contract",
        kind="sector",
        rule_results=(
            ValidationRuleResult(
                rule_id="METHOD.ELECTRICITY.TARGET_COVERAGE",
                severity="error",
                applicability="applicable",
                checked_object_count=1,
            ),
        ),
    )
    runtime = definition.copy()
    runtime["_inventory_working_copy"] = copy.deepcopy(valid_inventory)
    runtime["_validation_phase_results"] = [phase.to_dict()]

    monkeypatch.setattr(
        InventoryGraphValidator,
        "validate",
        lambda self: pytest.fail("covered updates must not run a full graph scan"),
    )

    database._store_updated_scenario(definition, runtime, persist=False)

    report = database.get_validation_report()
    assert report.valid
    assert report.get_phase("graph:incremental") is not None
    assert report.get_phase("sector:electricity:contract") == phase


def test_mutating_incrementally_certified_store_forces_exhaustive_validation(
    valid_inventory,
):
    database = configured_new_database(valid_inventory)
    definition = {"model": "image", "pathway": "path", "year": 2050}
    database.scenarios = [definition]
    phase = ValidationPhaseResult(
        phase_id="sector:electricity:contract",
        kind="sector",
        rule_results=(),
    )
    runtime = definition.copy()
    runtime["_inventory_working_copy"] = copy.deepcopy(valid_inventory)
    runtime["_validation_phase_results"] = [phase.to_dict()]
    store = database._store_updated_scenario(definition, runtime, persist=False)

    with store.transaction("custom invalid mutation") as transaction:
        transaction.patch_exchange(0, {"amount": math.nan})

    with pytest.raises(PremiseValidationError):
        database.get_validation_report()


def test_exhaustive_report_is_available_after_incremental_update(valid_inventory):
    database = configured_new_database(valid_inventory)
    definition = {"model": "image", "pathway": "path", "year": 2050}
    database.scenarios = [definition]
    phase = ValidationPhaseResult(
        phase_id="sector:electricity:contract",
        kind="sector",
        rule_results=(),
    )
    runtime = definition.copy()
    runtime["_inventory_working_copy"] = copy.deepcopy(valid_inventory)
    runtime["_validation_phase_results"] = [phase.to_dict()]
    database._store_updated_scenario(definition, runtime, persist=False)

    report = database.get_validation_report(exhaustive=True)

    assert report.valid
    assert report.get_phase("graph:full") is not None
    assert report.get_phase("graph:incremental") is None
    assert report.get_phase("sector:electricity:contract") == phase


def test_invalid_build_fails_before_replacing_or_checkpointing_scenario(
    valid_inventory, tmp_path, monkeypatch
):
    database = configured_new_database(valid_inventory)
    definition = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "last known good": True,
    }
    database.scenarios = [definition]
    runtime = definition.copy()
    invalid = copy.deepcopy(valid_inventory)
    invalid[0]["exchanges"][0]["amount"] = math.nan
    runtime["_inventory_working_copy"] = invalid
    monkeypatch.setattr(new_database_module, "DIR_CACHED_FILES", tmp_path)

    with pytest.raises(PremiseValidationError):
        database._store_updated_scenario(definition, runtime, persist=True)

    assert definition == {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "last known good": True,
    }
    assert not tuple(tmp_path.glob("*.inventory-store"))
