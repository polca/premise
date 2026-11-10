"""Renamed exports keep SQL and serialized exchange ownership consistent."""

from copy import deepcopy
import os
from pathlib import Path
import subprocess
import sys

import pytest

from premise.export.export_payload import normalize_exchange_owners


def test_owner_normalization_keeps_signed_fields_and_historical_audits():
    old = {
        "output": ["old", "owner"],
        "input": ["supplier", "code"],
        "amount": -2,
        "type": "technosphere",
        "formula": "-dose",
        "uncertainty type": 3,
        "loc": -2,
        "scale": 0.1,
    }
    data = [
        {
            "code": "new-owner",
            "exchanges": [deepcopy(old)],
            "premise heat consumer": {
                "substitutions": [{"source_exchange": deepcopy(old)}]
            },
        }
    ]
    assert normalize_exchange_owners(data, "target") is False
    actual = data[0]["exchanges"][0]
    assert actual == {**old, "output": ("target", "new-owner")}
    assert (
        data[0]["premise heat consumer"]["substitutions"][0]["source_exchange"] == old
    )
    assert normalize_exchange_owners(data, "target") is False
    data[0]["exchanges"].append({"amount": 0, "type": "technosphere"})
    assert normalize_exchange_owners(data, "target") is True
    assert data[0]["exchanges"][-1]["output"] == ("target", "new-owner")


@pytest.mark.parametrize("fast", [False, True])
def test_written_rows_roundtrip_through_bulk_and_live_Brightway_APIs(tmp_path, fast):
    env = dict(
        os.environ,
        BRIGHTWAY2_DIR=str(tmp_path / "bw"),
        PYTHONPATH=str(Path(__file__).resolve().parents[1]),
    )
    Path(env["BRIGHTWAY2_DIR"]).mkdir()
    script = r"""
import copy
import bw2data as bd
from premise.export.brightway25 import write_brightway_database
bd.projects.set_current('owner-roundtrip')
bd.Database('bio').write({('bio','flow'):dict(name='Emission',unit='kilogram',categories=('air',),type='emission')})
historical=dict(input=('source','a'),output=('source','b'),amount=-2,type='technosphere',name='Supplier',product='Supplier',unit='kilogram',location='CH',formula='-dose',**{'uncertainty type':3,'loc':-2,'scale':.1})
data=[dict(database='source',code='a',name='Supplier',unit='kilogram',location='CH',exchanges=[dict(input=('source','a'),output=('source','a'),type='production',amount=1,name='Supplier',product='Supplier',unit='kilogram',location='CH'),dict(input=('bio','flow'),output=('source','a'),type='biosphere',amount=.2,name='Emission',unit='kilogram',categories=('air',))],**{'reference product':'Supplier'}),dict(database='source',code='b',name='Consumer',unit='kilogram',location='CH',exchanges=[dict(input=('source','b'),output=('source','b'),type='production',amount=1,name='Consumer',product='Consumer',unit='kilogram',location='CH'),copy.deepcopy(historical)],**{'reference product':'Consumer','premise heat consumer':{'substitutions':[{'source_exchange':copy.deepcopy(historical)}]}})]
# Reproduce prepared exports: activity namespaces and active inputs are already
# renamed, so the writer cannot rely on change_db_name to repair output blobs.
for a in data:
 a['database']='target'
 for e in a['exchanges']:
  if e['input'][0]=='source':e['input']=('target',e['input'][1])
write_brightway_database(data,'target',fast=FAST,check_internal=True)
loaded=bd.Database('target').load()
assert len(loaded)==2
for key,a in loaded.items():
 assert len(a['exchanges'])==len(list(bd.get_node(database=key[0],code=key[1]).exchanges()))==2
 for e in a['exchanges']:assert tuple(e['output'])==key
consumer=loaded[('target','b')]
assert consumer['premise heat consumer']['substitutions'][0]['source_exchange']==historical
e=next(e for e in consumer['exchanges'] if e['type']=='technosphere')
assert e['input']==('target','a') and e['amount']==-2 and e['formula']=='-dose' and e['scale']==.1
assert 'source' not in bd.databases
""".replace("FAST", repr(fast))
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
