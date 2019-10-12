# content of test_activity_maps.py
from bw2data.database import DatabaseChooser
import pytest
from rmnd_lca.clean_datasets import DatabaseCleaner


def get_dict():
    dummy_db = {
                ('dummy_db', '6543541'): {
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
                }
            }
    dummy_bio = {
                ('dummy_bio', '123'): {
                    'name' : '1,4-Butanediol',
                    'categories': ('air', 'urban air close to ground'),
                    'unit':'kilogram',
                }
            }
    return dummy_db, dummy_bio
    


def test_presence_db():
    with pytest.raises(NameError) as wrapped_error:
        DatabaseCleaner("bla")
    assert wrapped_error.type == NameError

def test_validity_db():
    dummy_db, dummy_bio = get_dict()
    db_bio = DatabaseChooser('dummy_bio')
    db_bio.write(dummy_bio)
    
    db_act = DatabaseChooser('dummy_db')
    db_act.write(dummy_db)
    
    dbc = DatabaseCleaner("dummy_db")
    assert dbc.db[0]['name'] == 'fake activity'

def test_biosphere_dict():
    dummy_db, dummy_bio = get_dict()
    db_bio = DatabaseChooser('dummy_bio')
    db_bio.write(dummy_bio)
    
    db_act = DatabaseChooser('dummy_db')
    db_act.write(dummy_db)
    
    dbc = DatabaseCleaner("dummy_db")
    assert dbc.biosphere_dict[
               (
                   '1,4-Butanediol',
                   'air',
                   'urban air close to ground',
                   'kilogram'
               )] == '38a622c6-f086-4763-a952-7c6b3b1c42ba'


def test_biosphere_dict_2():
    dummy_db, dummy_bio = get_dict()
    db_bio = DatabaseChooser('dummy_bio')
    db_bio.write(dummy_bio)
    
    db_act = DatabaseChooser('dummy_db')
    db_act.write(dummy_db)
    dbc = DatabaseCleaner("dummy_db")
    for act in dbc.db:
        for exc in act['exchanges']:
            if exc['type'] == 'biosphere':
                assert dbc.biosphere_dict[(
                    exc['name'],
                    exc['categories'][0],
                    exc['categories'][1],
                    exc['unit']
                )] == '38a622c6-f086-4763-a952-7c6b3b1c42ba'