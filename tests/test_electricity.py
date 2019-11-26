# content of test_electricity.py
import pytest
from rmnd_lca import DATA_DIR
from rmnd_lca.electricity import Electricity
from rmnd_lca.data_collection import RemindDataCollection
from pathlib import Path

REGION_MAPPING_FILEPATH = (DATA_DIR /  "regionmappingH12.csv")
PRODUCTION_PER_TECH = (DATA_DIR / "electricity_production_volumes_per_tech.csv")
LOSS_PER_COUNTRY = (DATA_DIR / "losses_per_country.csv")
LHV_FUELS = (DATA_DIR / "fuels_lower_heating_value.txt")

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

rdc = RemindDataCollection('BAU', 2012, DATA_DIR / "Remind output files")
db, _ = get_db()
el = Electricity(db, rdc, 'BAU', 2012)

def test_losses():
    assert len(el.losses) == 174
    assert el.losses['AL']['Production volume'] == 7630

def test_fuels_lhv():
    assert float(el.fuels_lhv['hard coal']) == 20.1

def test_activities_map():
    s = el.activities_map['steel']
    assert isinstance(s, set)

def test_powerplant_map():
    s = el.powerplant_map['Biomass IGCC CCS']
    assert isinstance(s, set)

def test_emissions_map():
    s = el.emissions_map['Sulfur dioxide']
    assert  isinstance(s,str)
