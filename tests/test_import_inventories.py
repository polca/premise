# content of test_activity_maps.py
import pytest
from rmnd_lca.inventory_imports import \
    BaseInventoryImport, CarmaCCSInventory, BiofuelInventory
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "rmnd_lca" / "data"
FILEPATH_CARMA_INVENTORIES = (DATA_DIR / "lci-Carma-CCS.xlsx")
FILEPATH_BIO_INVENTORIES = (DATA_DIR / "lci-biodiesel_Cozzolini_2018.xlsx")


def get_db():
    db = [{
            'name':'fake activity',
            'reference product': 'fake product',
            'location':'IAI Area, Africa',
            'unit':'kilogram',
            'exchanges': [
                {'name' : 'fake activity',
                 'product': 'fake product',
                 'amount': 1,
                 'type': 'production',
                 'unit':'kilogram',
                 'input':('dummy_db', '6543541'),},
                {'name' : '1,4-Butanediol',
                 'categories': ('air', 'urban air close to ground'),
                 'amount': 1,
                 'type': 'biosphere',
                 'unit':'kilogram',
                 'input':('dummy_bio', '123'),
                },
            ]
        }]
    version = 3.5
    return db, version


def test_file_exists():
    db, version = get_db()
    with pytest.raises(FileNotFoundError) as wrapped_error:
        BaseInventoryImport(db, version, "testfile")
    assert wrapped_error.type == FileNotFoundError


def test_biosphere_dict():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(db, version, testpath)
    assert dbc.biosphere_dict[
               (
                   '1,4-Butanediol',
                   'air',
                   'urban air close to ground',
                   'kilogram'
               )] == '38a622c6-f086-4763-a952-7c6b3b1c42ba'

    testpath.unlink()

def test_biosphere_dict_2():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(db, version, testpath)

    for act in dbc.db:
        for exc in act['exchanges']:
            if exc['type'] == 'biosphere':
                assert dbc.biosphere_dict[(
                    exc['name'],
                    exc['categories'][0],
                    exc['categories'][1],
                    exc['unit']
                )] == '38a622c6-f086-4763-a952-7c6b3b1c42ba'

    testpath.unlink()


def test_load_carma():
    db, version = get_db()
    carma = CarmaCCSInventory(db, version, FILEPATH_CARMA_INVENTORIES)
    assert len(carma.import_db.data) == 146

def test_load_biofuel():
    db, version = get_db()
    bio = BiofuelInventory(db, version, FILEPATH_BIO_INVENTORIES)
    assert len(bio.import_db.data) == 27
