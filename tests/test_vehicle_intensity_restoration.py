import io

import pandas as pd
import pytest

from scripts.restore_vehicle_intensities import append_intensity_rows

HEADER = "Model;Scenario;Region;Variable;Unit;2020;2050\n"
SERVICE = "REMIND;SSP2;EUR;ES|Transport|Car|BEV;billion pkm/yr;0;10\n"
ENERGY = "REMIND;SSP2;EUR;FE|Transport|Car|Electricity;EJ/yr;0;0.01\n"
INTENSITY = "REMIND;SSP2;EUR;Energy intensity|Car|BEV;MJ/vehkm;1;0.8\n"
PAYLOAD = (HEADER + SERVICE + ENERGY).encode()


def original(extra="", service=SERVICE):
    return pd.read_csv(
        io.StringIO(HEADER + service + ENERGY + INTENSITY + extra), sep=";"
    )


def restore(source, payload=PAYLOAD):
    return append_intensity_rows(payload, source, {"Energy intensity|Car|BEV"})


def test_restoration_preserves_all_existing_bytes_and_zero_activity_intensity():
    source = original("REMIND;SSP2;EUR;Unrelated;EJ/yr;5;8\n")
    before = source.copy(deep=True)
    result, record = restore(source)
    assert result.startswith(PAYLOAD)
    parsed = pd.read_csv(io.BytesIO(result), sep=";").set_index("Variable")
    assert parsed.loc["Energy intensity|Car|BEV", "2020"] == 1
    assert "Unrelated" not in parsed.index
    assert record["appended_rows"] == 1
    pd.testing.assert_frame_equal(source, before)


def test_restoration_is_idempotent():
    first, _ = restore(original())
    second, record = restore(original(), first)
    assert first == second
    assert record["appended_rows"] == 0


def test_another_scenario_version_is_rejected():
    with pytest.raises(ValueError, match="version mismatch"):
        restore(original(service=SERVICE.replace(";0;10", ";0;12")))


def test_another_pathway_is_rejected():
    source = original()
    source["Scenario"] = "another pathway"
    with pytest.raises(ValueError, match="Scenario"):
        restore(source)


def test_conflicting_existing_intensity_is_not_overwritten():
    with pytest.raises(ValueError, match="conflict"):
        restore(original(), PAYLOAD + INTENSITY.replace(";1;0.8", ";1;0.5").encode())


def test_existing_intensity_with_another_unit_is_not_duplicated():
    with pytest.raises(ValueError, match="units conflict"):
        restore(original(), PAYLOAD + INTENSITY.replace("MJ/vehkm", "MJ/pkm").encode())


def test_duplicate_identity_is_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        restore(original(extra=INTENSITY))


def test_non_vehicle_intensity_unit_is_rejected():
    source = original()
    source.loc[source.Variable == "Energy intensity|Car|BEV", "Unit"] = "MJ/pkm"
    with pytest.raises(ValueError, match="MJ/vehicle-km"):
        restore(source)


def test_absent_region_is_not_created_from_other_region():
    source = original(extra=INTENSITY.replace(";EUR;", ";USA;"))
    result, record = restore(source)
    assert set(pd.read_csv(io.BytesIO(result), sep=";").Region) == {"EUR"}
    assert record["appended_rows"] == 1
