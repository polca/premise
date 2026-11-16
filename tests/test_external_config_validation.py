"""Configuration validation retains the existing inventory-import decisions."""

import pytest
import yaml
from datapackage import Package
from premise.external_data_validation import check_config_file
from schema import SchemaError


@pytest.mark.parametrize(
    "existing_flags,needs_imports",
    [
        pytest.param((), None, id="empty-pathways-rejected"),
        ((True,), False),
        ((False,), True),
        ((None,), True),
        ((True, True), False),
        ((True, False), True),
        ((True, None), True),
    ],
)
def test_import_decision_for_existing_and_missing_providers(
    tmp_path, existing_flags, needs_imports
):
    pathways = {}
    for i, flag in enumerate(existing_flags):
        alias = {"name": f"plant {i}", "reference product": "electricity"}
        if flag is not None:
            alias["exists in original database"] = flag
        pathways[f"pathway {i}"] = {
            "production volume": {"variable": f"production {i}"},
            "ecoinvent alias": alias,
        }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"production pathways": pathways}))
    before = path.read_bytes()
    data = Package(
        {"resources": [{"name": "config", "path": "config.yaml"}]},
        base_path=str(tmp_path),
    )

    if needs_imports is None:
        with pytest.raises(SchemaError, match="production pathways"):
            check_config_file(data)
    else:
        assert check_config_file(data) is needs_imports
    assert path.read_bytes() == before
