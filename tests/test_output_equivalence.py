import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "benchmarks" / "compare_build_outputs.py"
SPEC = importlib.util.spec_from_file_location("compare_build_outputs", SCRIPT)
equivalence = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(equivalence)


def dataset(amount=1.0, code="random-code", exchange_order=False):
    exchanges = [
        {
            "name": "input",
            "product": "input product",
            "location": "GLO",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": amount,
            "input": ("database", "random-input-code"),
        },
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
            "input": ("biosphere", "random-flow-code"),
        },
    ]
    if exchange_order:
        exchanges.reverse()
    return {
        "name": "market for test",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "database": "random-database-name",
        "code": code,
        "comment": "Must remain identical",
        "classifications": [("CPC", "123")],
        "exchanges": exchanges,
    }


def test_canonical_dataset_ignores_only_storage_identity_and_order():
    left = equivalence.canonical_dataset(dataset())
    right = equivalence.canonical_dataset(
        dataset(code="another-random-code", exchange_order=True)
    )

    assert left == right


def test_canonical_dataset_detects_numerical_and_metadata_changes():
    original = equivalence.canonical_dataset(dataset())
    amount_changed = equivalence.canonical_dataset(dataset(amount=1.0000000000001))
    metadata_changed_dataset = dataset()
    metadata_changed_dataset["comment"] = "Changed"
    metadata_changed = equivalence.canonical_dataset(metadata_changed_dataset)

    assert original != amount_changed
    assert original != metadata_changed


def test_semantic_snapshot_is_independent_of_dataset_codes(tmp_path):
    left = equivalence.write_semantic_snapshot(
        [dataset(code="left")], tmp_path / "left", write_details=False
    )
    right = equivalence.write_semantic_snapshot(
        [dataset(code="right", exchange_order=True)],
        tmp_path / "right",
        write_details=False,
    )

    assert left["semantic_sha256"] == right["semantic_sha256"]


def test_equivalence_build_requires_a_fixed_python_hash_seed(monkeypatch):
    monkeypatch.delenv("PYTHONHASHSEED", raising=False)

    try:
        equivalence.fixed_python_hash_seed()
    except RuntimeError as error:
        assert "PYTHONHASHSEED" in str(error)
    else:
        raise AssertionError("An unseeded equivalence build must be rejected")

    monkeypatch.setenv("PYTHONHASHSEED", "0")
    assert equivalence.fixed_python_hash_seed() == "0"
