import gc
import os
import shutil
from copy import deepcopy

import bw2calc
import bw2data
import bw2io
import pytest
from dotenv import load_dotenv

import premise.export as export_module
from premise import NewDatabase, clear_inventory_cache
from premise.utils import delete_all_pickles
from lcia_regression import assert_lcia_regression_scores, get_lcia_regression_method

load_dotenv()

ei_user = os.environ.get("EI_USERNAME")
ei_pass = os.environ.get("EI_PASSWORD")
key = os.environ.get("IAM_FILES_KEY", "")
pytestmark = pytest.mark.skipif(
    not (ei_user and ei_pass and key), reason="ecoinvent credentials are unavailable"
)
# convert to bytes
key = key.encode()

ei_version = "3.12"
system_model = "cutoff"
project_name = f"ecoinvent-{ei_version}-{system_model}"

scenarios = [
    {"model": "remind", "pathway": "SSP3-rollBack", "year": 2050},
    {"model": "image", "pathway": "SSP2-VLHO", "year": 2050},
    {"model": "tiam-ucl", "pathway": "SSP2-RCP19", "year": 2050},
]


@pytest.fixture(scope="module")
def updated_ei312_cutoff():
    """Build the shared scenarios once and keep reusable compact checkpoints."""

    bw2data.projects.set_current(project_name)
    clear_inventory_cache()

    if project_name not in bw2data.databases:
        bw2io.import_ecoinvent_release(
            version=ei_version,
            system_model=system_model,
            username=ei_user,
            password=ei_pass,
            biosphere_name=f"ecoinvent-{ei_version}-biosphere",
        )

    bio_db = [db for db in bw2data.databases if "biosphere" in db][0]

    ndb = NewDatabase(
        scenarios=scenarios,
        source_db=project_name,
        source_version=ei_version,
        key=key,
        system_model=system_model,
        biosphere_name=bio_db,
        generate_reports=False,
        inventory_backend="compact",
    )

    ndb.update(persist=True)
    checkpoints = tuple(scenario["_inventory_checkpoint"] for scenario in ndb.scenarios)
    manifests = tuple(
        (checkpoint / "manifest.json").read_bytes() for checkpoint in checkpoints
    )
    applied_functions = tuple(
        deepcopy(scenario.get("applied functions")) for scenario in ndb.scenarios
    )

    try:
        yield ndb, tuple(zip(checkpoints, manifests, applied_functions))
    finally:
        del ndb
        gc.collect()
        delete_all_pickles()
        for checkpoint in checkpoints:
            shutil.rmtree(checkpoint, ignore_errors=True)


def assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots):
    """Ensure an exporter did not consume or rewrite the shared checkpoints."""

    assert len(ndb.scenarios) == len(checkpoint_snapshots)
    for scenario, (checkpoint, manifest, applied_functions) in zip(
        ndb.scenarios, checkpoint_snapshots
    ):
        assert scenario["_inventory_checkpoint"] == checkpoint
        assert checkpoint.is_dir()
        assert (checkpoint / "manifest.json").read_bytes() == manifest
        assert scenario.get("applied functions") == applied_functions


@pytest.mark.slow
def test_brightway(updated_ei312_cutoff):
    ndb, checkpoint_snapshots = updated_ei312_cutoff

    database_names = ["test1", "test2", "test3"]
    ndb.write_db_to_brightway(database_names)
    assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots)

    from bw2data import __version__

    print(f"Using Brightway2 data version: {__version__}")

    print(f"Length of databases: {len(bw2data.Database('test1'))}")

    case_key = project_name
    assert_lcia_regression_scores(case_key, database_names[:2])
    method = get_lcia_regression_method(case_key)

    lca = bw2calc.LCA({bw2data.Database("test1").random(): 1}, method)
    lca.lci()
    lca.lcia()
    assert isinstance(lca.score, float)
    print(lca.score)

    try:
        # uses BW2
        mclca = bw2calc.MonteCarloLCA({bw2data.Database("test1").random(): 1}, method)
    except AttributeError:
        # uses BW25
        mclca = bw2calc.LCA(
            {bw2data.Database("test1").random(): 1}, method, use_distributions=True
        )

    results = [lca.score for _ in zip(range(10), mclca)]
    assert all(isinstance(result, float) for result in results)
    print(results)


@pytest.mark.slow
def test_simapro_export(updated_ei312_cutoff, tmp_path, monkeypatch):
    ndb, checkpoint_snapshots = updated_ei312_cutoff
    output_dir = tmp_path / "simapro"
    monkeypatch.chdir(tmp_path)

    ndb.write_db_to_simapro(filepath=str(output_dir))

    assert len(tuple(output_dir.glob("simapro_export_*.csv"))) == len(scenarios)
    assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots)


@pytest.mark.slow
def test_openlca_export(updated_ei312_cutoff, tmp_path, monkeypatch):
    ndb, checkpoint_snapshots = updated_ei312_cutoff
    output_dir = tmp_path / "openlca"
    monkeypatch.chdir(tmp_path)

    ndb.write_db_to_olca(filepath=str(output_dir))

    assert len(tuple(output_dir.glob("simapro_export_*.csv"))) == len(scenarios)
    assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots)


@pytest.mark.slow
def test_datapackage_export(updated_ei312_cutoff, tmp_path, monkeypatch):
    ndb, checkpoint_snapshots = updated_ei312_cutoff
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        export_module,
        "DIR_DATAPACKAGE",
        tmp_path / "export" / "datapackage",
    )
    monkeypatch.setattr(
        export_module,
        "DIR_DATAPACKAGE_TEMP",
        tmp_path / "export" / "temp",
    )

    ndb.write_datapackage(name="datapackage")

    assert (tmp_path / "export" / "datapackage" / "datapackage.zip").is_file()
    assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots)


@pytest.mark.slow
def test_superstructure_export(updated_ei312_cutoff, tmp_path, monkeypatch):
    ndb, checkpoint_snapshots = updated_ei312_cutoff
    database_name = "superstructure"
    monkeypatch.chdir(tmp_path)

    ndb.write_superstructure_db_to_brightway(
        database_name,
        filepath=str(tmp_path / "scenario-differences"),
    )
    assert_persisted_scenarios_unchanged(ndb, checkpoint_snapshots)

    method = next(
        (
            method
            for method in bw2data.methods
            if any("GWP" in str(part).upper() for part in method)
        ),
        None,
    )
    assert method is not None, "No GWP LCIA method is registered in this project."

    lca = bw2calc.LCA({bw2data.Database(database_name).random(): 1}, method)
    lca.lci()
    lca.lcia()
    assert isinstance(lca.score, float)
    print(lca.score)
