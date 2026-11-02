"""Assertions for declared inventory classifications."""


def assert_declared_classifications(importer, dataset):
    """Classifications are populated only when the import registry declares them."""
    from premise.inventory_imports import get_classification_entry

    expected = get_classification_entry(
        importer.classifications, dataset["name"], dataset["reference product"]
    )
    if expected:
        assert dict(dataset["classifications"]) == expected
