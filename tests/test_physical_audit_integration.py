"""Opt-in numerical smoke test of an existing licensed generated database."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.slow
@pytest.mark.skipif(
    not all(
        os.environ.get(k)
        for k in (
            "PREMISE_AUDIT_DATABASE",
            "PREMISE_AUDIT_PROJECT",
            "PREMISE_AUDIT_BRIGHTWAY_DIR",
        )
    ),
    reason="Set explicit physical-audit database/project/directory to run the licensed audit",
)
def test_existing_generated_database_physical_audit(tmp_path):
    env = dict(
        os.environ,
        BRIGHTWAY2_DIR=os.environ["PREMISE_AUDIT_BRIGHTWAY_DIR"],
        PYTHONPATH=str(Path(__file__).resolve().parents[1]),
        OPENBLAS_NUM_THREADS="1",
    )
    script = """
import os,json
import bw2data as bd
from premise import AuditConfig,audit_brightway_database
bd.projects.set_current(os.environ['PREMISE_AUDIT_PROJECT'])
r=audit_brightway_database(os.environ['PREMISE_AUDIT_DATABASE'],
 config=AuditConfig(sample_size=int(os.environ.get('PREMISE_AUDIT_SAMPLE_SIZE','5000')),
                    explanation_limit=10,
                    version=os.environ.get('PREMISE_AUDIT_VERSION'),
                    system_model=os.environ.get('PREMISE_AUDIT_SYSTEM_MODEL')),
 progress=lambda event: print(json.dumps(event),flush=True))
r.write('physical-audit')
assert r.metadata['scoring_status']=='completed'
assert r.metadata['brightway_crosscheck']['failures']==0,r.metadata['brightway_crosscheck']
assert not any(f.category in {'numerical','structure'} and f.issue.severity=='error' for f in r.findings)
assert not any(f.rule_id.startswith('CONTRACT.') for f in r.findings)
print(json.dumps(dict(sampled=len(r.sample),physical_findings=len(r.findings))))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, result.stdout + result.stderr
