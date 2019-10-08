# content of test_activity_maps.py
from bw2data.database import DatabaseChooser
import pytest
from rmnd_lca.clean_datasets import DatabaseCleaner

db = DatabaseChooser('dummy_db')
db.write({
    ('dummy_db', '6543541'): {
        'name':'fake activity',
        'location':'GLO',
        'unit':'kilogram',
    }
})

def test_presence_db():
    with pytest.raises(NameError) as wrapped_error:
        DatabaseCleaner("bla")
    assert wrapped_error.type == NameError