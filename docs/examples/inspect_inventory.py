"""An executable InventoryStore example requiring no ecoinvent or IAM data."""

from premise import CompactInventoryStore


def main():
    store = CompactInventoryStore(
        [
            {
                "name": "demonstration process",
                "reference product": "demonstration product",
                "location": "CH",
                "unit": "kilogram",
                "database": "documentation-example",
                "code": "example",
                "exchanges": [
                    {
                        "name": "demonstration process",
                        "product": "demonstration product",
                        "location": "CH",
                        "unit": "kilogram",
                        "type": "production",
                        "amount": 1,
                    }
                ],
            }
        ]
    )
    activity = store.find_one({"location": "CH"})
    with store.transaction("example:comment") as transaction:
        transaction.patch_activity(activity.id, {"comment": "Reviewed"})
    assert store.activity(activity.id)["comment"] == "Reviewed"
    assert "comment" not in activity  # Previously returned snapshots stay unchanged.
    print("Read, query and atomic edit completed.")


if __name__ == "__main__":
    main()
