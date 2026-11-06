"""Audit actual Brightway relational data in an isolated, license-free project."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("stale_serialized_output", [False, True])
def test_brightway_dependencies_scores_and_stale_export_metadata(
    tmp_path, stale_serialized_output
):
    if importlib.util.find_spec("bw2calc") is None:
        pytest.skip("Brightway calculation stack is unavailable")
    root = Path(__file__).resolve().parents[1]
    bw = tmp_path / "brightway"
    bw.mkdir()
    env = dict(
        os.environ,
        BRIGHTWAY2_DIR=str(bw),
        PYTHONPATH=str(root),
        OPENBLAS_NUM_THREADS="1",
    )
    script = r"""
import copy,json
import bw2data as bd
from premise import AuditConfig,audit_brightway_database,AuditCase,audit_campaign,AuditReport
from premise._bw2_backend_compat import ExchangeDataset
bd.projects.set_current('physical-audit-test')
flows={
 ('bio','ir'):dict(name='Iridium',unit='kilogram',categories=('natural resource','in ground'),type='natural resource'),
 ('bio','co2'):dict(name='Carbon dioxide, fossil',unit='kilogram',categories=('air',),type='emission'),
 ('bio','water'):dict(name='Water, river',unit='cubic meter',categories=('natural resource','in water'),type='natural resource'),
 ('bio','land'):dict(name='Occupation, industrial area',unit='square meter-year',categories=('natural resource','land'),type='natural resource'),
}
bd.Database('bio').write(flows)
producer=('upstream','metal')
bd.Database('upstream').write({producer:dict(name='iridium production, economic allocation, proxy',
 **{'reference product':'iridium'},unit='kilogram',location='ZA',exchanges=[
 dict(input=producer,type='production',amount=2),
 *[dict(input=('bio',k),type='biosphere',amount=a) for k,a in [('ir',2),('co2',.4),('water',1),('land',2)]]] )})
consumer=('foreground','market')
bd.Database('foreground').write({consumer:dict(name='market for iridium',
 **{'reference product':'iridium'},location='GLO',unit='kilogram',exchanges=[
 dict(input=consumer,type='production',amount=1),dict(input=producer,type='technosphere',amount=3,name='iridium')])})
if STALE:
 for row in ExchangeDataset.select().where(ExchangeDataset.output_database=='foreground'):
  row.data=dict(row.data,output=('obsolete-export','stale-code'));row.save()
 assert all(not n['exchanges'] for n in bd.Database('foreground').load().values())
before_databases=set(bd.databases);before_methods=set(bd.methods)
before=[dict(e) for n in bd.Database('foreground') for e in n.exchanges()]
report=audit_brightway_database('foreground',config=AuditConfig(sample_size=10,system_model='cutoff'))
key=('market for iridium','iridium','GLO','kilogram')
assert not report.has_errors,[f.to_dict() for f in report.findings]
assert report.metadata['activities']==2 and len(report.sample)==1
assert abs(report.score(key,'metals.Iridium')-3)<1e-10
assert abs(report.score(key,'water.freshwater')-1.5)<1e-10
assert abs(report.score(key,'land.occupation')-3)<1e-10
# The processed database stores this amount at float32 precision.
assert abs(report.score(key,'co2.fossil')-.6)<1e-7
assert report.metadata['brightway_crosscheck']['failures']==0
assert report.metadata['provenance']['product_named_exchanges']==1
assert set(bd.databases)==before_databases and set(bd.methods)==before_methods
assert before==[dict(e) for n in bd.Database('foreground') for e in n.exchanges()]
campaign=audit_campaign([
 AuditCase('discovery','foreground',AuditConfig(sample_size=1)),
 AuditCase('control','foreground',AuditConfig(sample_size=1,include_default_contracts=False)),
],output='campaign',crosscheck_size=1)
assert campaign['control'].metadata['reference_cohort']['available_reference_matches']==1
assert abs(AuditReport.read('campaign/discovery').score(key,'metals.Iridium')-3)<1e-10
for row in ExchangeDataset.select().where(ExchangeDataset.output_database=='upstream'):
 if row.input_database=='bio' and row.input_code=='ir':
  row.data=dict(row.data,amount=0);row.save()
changed=audit_brightway_database('foreground',config=AuditConfig(sample_size=1),crosscheck_size=1)
assert 'GRAPH.PROCESSED_AMOUNT' in {f.rule_id for f in changed.findings}
for row in ExchangeDataset.select().where(ExchangeDataset.output_database=='foreground'):
 if row.type=='technosphere':
  row.data=dict(row.data,name='copper');row.save()
wrong=audit_brightway_database('foreground',config=AuditConfig(sample_size=1),crosscheck_size=0)
assert 'GRAPH.PROVIDER_IDENTITY' in {f.rule_id for f in wrong.findings}
print('BRIGHTWAY_AUDIT_OK')
""".replace("STALE", repr(stale_serialized_output))
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "BRIGHTWAY_AUDIT_OK" in result.stdout
