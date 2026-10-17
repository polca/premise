"""Keep production routes while excluding specialty grades from generic steel."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr
import yaml

from premise.filesystem_constants import DATA_DIR
from premise.steel import Steel, is_specialty_steel
import premise.steel as steel_module
from premise.activity_maps import get_mapping

SUPPORTED = yaml.safe_load(
    (DATA_DIR.parent / "iam_variables_mapping/constants.yaml").read_text()
)["SUPPORTED_EI_VERSIONS"]


def activity(name, product, location="R1", niobium=0):
    return {
        "name": name,
        "reference product": product,
        "unit": "kilogram",
        "location": location,
        "production volume": 1,
        "exchanges": [
            dict(
                name=name,
                product=product,
                location=location,
                unit="kilogram",
                type="production",
                amount=1,
            ),
            *(
                [
                    dict(
                        name="ferroniobium supply",
                        product="ferroniobium",
                        location="GLO",
                        unit="kilogram",
                        type="technosphere",
                        amount=niobium,
                    )
                ]
                if niobium
                else []
            ),
        ],
    }


@pytest.mark.parametrize(
    "grade",
    [
        "rolled",
        "removed",
        "residue",
        "grain oriented",
        "bearing",
        "case hardening",
        "case-hardening",
        "case–hardened",
        "high strength low alloy",
        "High-Strength Low-Alloyed",
        "HSLA",
        "weathering A",
        "weathering B",
        "chromium steel 18/8",
        "stainless",
        "tool",
        "spring",
        "electrical",
        "silicon",
        "maraging",
    ],
)
@pytest.mark.parametrize("field", ["name", "reference product"])
def test_specialty_exclusions_cover_name_product_and_spelling(grade, field):
    ds = activity("steel production, electric", "steel, carbon")
    ds[field] = f"steel, {grade}"
    assert is_specialty_steel(ds)


@pytest.mark.parametrize(
    "name,product",
    [
        ("steel production, electric, low-alloyed", "steel, low-alloyed"),
        ("steel production, electric arc furnace, carbon", "steel, carbon, 100% scrap"),
        (
            "steel production, electric arc furnace, structural",
            "steel, structural, 100% scrap",
        ),
        ("steel production, converter, unalloyed", "steel, unalloyed"),
        (
            "steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, unalloyed",
            "steel, unalloyed",
        ),
    ],
)
def test_ordinary_steel_and_carbon_capture_routes_remain_eligible(name, product):
    assert not is_specialty_steel(activity(name, product))


def model(version="3.12", system_model="cutoff"):
    obj = object.__new__(Steel)
    obj.version, obj.system_model = version, system_model
    obj.year, obj.regions = 2050, ["R1", "R2", "World"]
    obj.iam_to_ecoinvent_loc = {"R1": ["RER"], "R2": ["US"]}
    obj.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda loc: "R1")
    obj.iam_data = SimpleNamespace(
        production_volumes=xr.DataArray(
            [[[60], [40]], [[20], [80]]],
            dims=("region", "variables", "year"),
            coords={
                "region": ["R1", "R2"],
                "variables": ["steel - primary", "steel - secondary"],
                "year": [2050],
            },
        )
    )
    primary, secondary = [], []
    for region in ("R1", "R2"):
        for grade in ("unalloyed", "low-alloyed"):
            primary.append(
                activity(
                    f"steel production, converter, {grade}", f"steel, {grade}", region
                )
            )
        for name, product in [
            ("steel production, electric, low-alloyed", "steel, low-alloyed"),
            (
                "steel production, electric arc furnace, carbon",
                "steel, carbon, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, structural",
                "steel, structural, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, high strength low alloy",
                "steel, high strength low alloy, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, bearing",
                "steel, bearing, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, case hardening",
                "steel, case hardening, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, weathering A",
                "steel, weathering A, 100% scrap",
            ),
            (
                "steel production, electric arc furnace, weathering B",
                "steel, weathering B, 100% scrap",
            ),
        ]:
            secondary.append(
                activity(
                    name,
                    product,
                    region,
                    niobium=0.003 if "high strength" in name else 0,
                )
            )
    obj.steel_map = {"steel - primary": primary, "steel - secondary": secondary}
    obj.database = primary + secondary
    for grade in ("unalloyed", "low-alloyed", "bearing", "high strength low alloy"):
        obj.database.append(
            activity(f"market for steel, {grade}", f"steel, {grade}", "RER")
        )
    obj.add_to_index = lambda ds: None
    obj.is_in_index = lambda ds, location: True
    obj.write_log = lambda *args: None
    obj.emptied = []
    obj.empty_original_datasets = lambda **kw: obj.emptied.extend(kw["datasets"])
    return obj


@pytest.mark.parametrize("version", SUPPORTED)
@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
def test_real_market_builder_preserves_routes_and_specialty_datasets(
    version, system_model
):
    obj = model(version, system_model)
    before_mapping = deepcopy(obj.steel_map)
    specialty_before = [deepcopy(ds) for ds in obj.database if is_specialty_steel(ds)]
    obj.create_steel_markets()
    assert obj.steel_map == before_mapping
    assert [ds for ds in obj.database if is_specialty_steel(ds)] == specialty_before
    assert {ds["reference product"] for ds in obj.emptied} == {
        "steel, unalloyed",
        "steel, low-alloyed",
    }
    new = [ds for ds in obj.database if ds.get("regionalized")]
    assert len(new) == 6  # two regions and their World mix, for both generic markets
    for ds in new:
        flows = [e for e in ds["exchanges"] if e["type"] == "technosphere"]
        assert sum(e["amount"] for e in flows) == pytest.approx(1)
        assert not any(
            is_specialty_steel({"name": e["name"], "reference product": e["product"]})
            for e in flows
        )
        if ds["location"] == "World":
            continue
        converter = sum(e["amount"] for e in flows if "converter" in e["name"])
        electric = sum(e["amount"] for e in flows if "electric" in e["name"])
        expected = 0.6 if ds["location"] == "R1" else 0.2
        assert converter == pytest.approx(expected if system_model == "cutoff" else 1)
        assert electric == pytest.approx(
            1 - expected if system_model == "cutoff" else 0
        )
        if ds["reference product"] == "steel, unalloyed" and system_model == "cutoff":
            assert {e["product"] for e in flows if "electric" in e["name"]} == {
                "steel, low-alloyed",
                "steel, carbon, 100% scrap",
                "steel, structural, 100% scrap",
            }


def test_empty_electric_route_is_not_silently_reallocated_to_converter():
    obj = model()
    obj.steel_map["steel - secondary"] = [
        ds for ds in obj.steel_map["steel - secondary"] if is_specialty_steel(ds)
    ]
    before = deepcopy(obj.database)
    with pytest.raises(
        ValueError, match="No activity found for technology steel - secondary"
    ):
        obj.create_steel_markets()
    assert obj.database == before


def test_yaml_controls_exclusions_without_changing_production_route_aliases(
    tmp_path, monkeypatch
):
    original = yaml.safe_load(steel_module.STEEL_TECHS.read_text())
    changed = deepcopy(original)
    changed["steel - secondary"]["generic_market_exclusions"].append("structural")
    path = tmp_path / "steel.yaml"
    path.write_text(yaml.safe_dump(changed))
    structural = activity(
        "steel production, electric arc furnace, structural",
        "steel, structural, 100% scrap",
    )
    assert not is_specialty_steel(structural)
    monkeypatch.setattr(steel_module, "STEEL_TECHS", path)
    assert is_specialty_steel(structural)
    assert get_mapping(path, "ecoinvent_aliases") == get_mapping(
        DATA_DIR.parent / "iam_variables_mapping/steel.yaml", "ecoinvent_aliases"
    )
    assert changed.keys() == original.keys()  # no synthetic reporting technology


@pytest.mark.parametrize("value", [None, [], "bearing", [True], [""]])
def test_invalid_yaml_exclusions_fail_instead_of_admitting_specialties(
    tmp_path, monkeypatch, value
):
    changed = yaml.safe_load(steel_module.STEEL_TECHS.read_text())
    if value is None:
        changed["steel - secondary"].pop("generic_market_exclusions")
    else:
        changed["steel - secondary"]["generic_market_exclusions"] = value
    path = tmp_path / "steel.yaml"
    path.write_text(yaml.safe_dump(changed))
    monkeypatch.setattr(steel_module, "STEEL_TECHS", path)
    with pytest.raises(ValueError, match="generic_market_exclusions"):
        is_specialty_steel(activity("steel production, electric", "steel, carbon"))
