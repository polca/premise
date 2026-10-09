import hashlib
import json

from dev.stock_vintage.generate_lifecycle_fixture import generate


def test_conditional_retirement_resource_survives_actual_export(tmp_path):
    expected = generate(tmp_path)
    root = tmp_path / "trails_temp"
    descriptor = json.loads((root / "datapackage.json").read_text())
    raw = (root / "stock_vintage.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == descriptor["stock_vintage"]["sha256"]
    resource = json.loads(raw)
    assert len(resource["bindings"]) == 4
    assert resource["profiles"][0]["years"] == expected["manufacture"]
    assert resource["profiles"][1]["years"] == expected["retirement"]
    for record in expected["retirement"]:
        assert min(record["event_years"]) > record["service_year"]
        assert sum(record["weights"]) == 1
    assert all(row["balance_residual"] == 0 for row in expected["stock_balances"])
