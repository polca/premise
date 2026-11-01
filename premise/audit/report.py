"""Portable audit artifacts containing results, not complete inventories."""

from collections import Counter
import csv
import html
import json
from pathlib import Path
import numpy as np


def csv_rows(path, rows, fields=None):
    rows = list(rows)
    if not rows and fields is None:
        path.write_text("", encoding="utf-8")
        return
    fields = fields or list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    k: (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (dict, list, tuple))
                        else v
                    )
                    for k, v in row.items()
                }
            )


def write_report(report, directory):
    destination = Path(directory)
    destination.mkdir(parents=True, exist_ok=False)
    data = report.to_dict()
    np.savez_compressed(
        destination / "scores.npz",
        total=report.total,
        direct=report.direct,
        positive=report.positive,
        negative=report.negative,
    )
    (destination / "audit.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )
    csv_rows(destination / "sample.csv", report.sample)
    csv_rows(destination / "coverage.csv", report.coverage)
    csv_rows(destination / "rule-coverage.csv", report.rule_coverage)
    csv_rows(
        destination / "findings.csv",
        data["findings"],
        fields=(
            list(data["findings"][0])
            if data["findings"]
            else ["rule_id", "severity", "message"]
        ),
    )
    csv_rows(destination / "screen.csv", report.screen)
    csv_rows(destination / "selected-contributions.csv", report.explanations)
    csv_rows(destination / "manual-review.csv", report.review_sample)
    groups = Counter()
    for finding in report.findings:
        suppliers = sorted(
            {tuple(s["provider"]) for s in finding.evidence.get("suppliers", ())}
        )
        basis = "shared fuel suppliers" if suppliers else "reported activity or cause"
        key = (
            json.dumps(suppliers, ensure_ascii=False)
            if suppliers
            else finding.root_cause
        )
        groups[(finding.rule_id, basis, key)] += 1
    csv_rows(
        destination / "root-causes.csv",
        [
            dict(
                rule=rule,
                grouping_basis=basis,
                investigation_group=root,
                finding_count=count,
                qualification="Investigation cluster; a shared supplier is not proof of a common defect",
            )
            for (rule, basis, root), count in groups.most_common()
        ],
    )
    with (destination / "scores.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        columns = [
            "name",
            "reference_product",
            "location",
            "unit",
            "panel",
            "stratum",
            "reference_output",
            "demand",
        ]
        columns += [
            f"{spec['key']} | {scope} [{spec['unit']}]"
            for spec in report.indicators
            for scope in ("total", "direct", "positive", "negative")
        ]
        writer.writerow(columns)
        for i, row in enumerate(report.sample):
            values = [
                *row["activity"],
                row["panel"],
                row["stratum"],
                row["reference_output"],
                row["demand"],
            ]
            values += [
                float(getattr(report, scope)[i, j])
                for j in range(len(report.indicators))
                for scope in ("total", "direct", "positive", "negative")
            ]
            writer.writerow(values)
    # Explicit textContent rendering and escaped JSON protect reports containing
    # arbitrary activity names. No CDN, network dependency or licensed LCI dump.
    browser_data = dict(
        indicators=report.indicators,
        sample=report.sample,
        scores=report.total.tolist(),
        findings=data["findings"],
        explanations=report.explanations,
        review=report.review_sample,
    )
    payload = (
        json.dumps(browser_data, ensure_ascii=False, allow_nan=False)
        .replace("<", "\\u003c")
        .replace("&", "\\u0026")
    )
    metadata = html.escape(json.dumps(report.metadata, indent=2, ensure_ascii=False))
    page = """<!doctype html><html lang="en"><meta charset="utf-8"><title>Premise physical inventory audit</title>
<style>body{font:15px system-ui;max-width:1500px;margin:30px auto;padding:0 20px;color:#17202a}table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:7px;border-bottom:1px solid #ddd;text-align:left}input,select{padding:8px;margin:5px}pre{white-space:pre-wrap}tr.error{background:#fff0ed}canvas{max-width:100%;border:1px solid #ddd}#scroll{max-height:600px;overflow:auto}</style>
<h1>Physical inventory audit</h1><p>Numerical checks, declared physical contracts and conservative coverage screens. Unflagged does not mean scientifically certified. Scores use +1 reference unit; inspect negative production signs. CO2 origin, water withdrawal/returns and land occupation/transformation remain separate.</p>
<input id="search" placeholder="Search activity, rule or category"><select id="view"><option value="findings">Findings</option><option value="scores">Sampled scores</option></select><select id="indicator"></select>
<p id="count"></p><canvas id="plot" width="1200" height="160"></canvas><div id="scroll"><table><thead id="head"></thead><tbody id="rows"></tbody></table></div>
<details open><summary>Selected evidence and contributions</summary><pre id="detail">Click a table row to inspect its evidence.</pre></details>
<details><summary>Build and numerical metadata</summary><pre>METADATA</pre></details>
<script type="application/json" id="data">PAYLOAD</script><script>
const data=JSON.parse(document.getElementById('data').textContent),get=id=>document.getElementById(id);
data.indicators.forEach((v,i)=>{const o=document.createElement('option');o.value=i;o.textContent=v.key+' ['+v.unit+']';get('indicator').append(o)});
function cells(parent,values,tag='td'){values.forEach(value=>{const c=document.createElement(tag);c.textContent=value??'';parent.append(c)})}
function render(){const q=get('search').value.toLowerCase(),j=Number(get('indicator').value),scoreView=get('view').value==='scores';get('rows').replaceChildren();get('head').replaceChildren();const h=document.createElement('tr');cells(h,scoreView?['Activity','Location','Reference unit','Panel','Score']:['Rule','Severity','Confidence','Activity','Observed','Expected','Explanation'],'th');get('head').append(h);
const rows=scoreView?data.sample.map((s,i)=>({values:[s.activity[0],s.activity[2],s.activity[3],s.panel,data.scores[i][j]],text:JSON.stringify(s),severity:'',record:s})):data.findings.map(f=>({values:[f.rule_id,f.severity,f.confidence,(f.activity||[]).join(' | '),JSON.stringify(f.actual),JSON.stringify(f.expected),f.message],text:JSON.stringify(f),severity:f.severity,record:f}));const selected=rows.filter(r=>r.text.toLowerCase().includes(q));get('count').textContent=selected.length+' matching rows; displaying first 1000';selected.slice(0,1000).forEach(r=>{const tr=document.createElement('tr');tr.className=r.severity;cells(tr,r.values);tr.onclick=()=>{const key=JSON.stringify(r.record.activity);get('detail').textContent=JSON.stringify({record:r.record,contributions:data.explanations.filter(e=>JSON.stringify(e.activity)===key),review:data.review.filter(e=>JSON.stringify(e.activity)===key)},null,2)};get('rows').append(tr)});
const canvas=get('plot'),ctx=canvas.getContext('2d');ctx.clearRect(0,0,canvas.width,canvas.height);const ys=data.scores.map(r=>Math.sign(r[j])*Math.log10(1+Math.abs(r[j]))),max=ys.reduce((m,v)=>Math.max(m,Math.abs(v)),1);ctx.fillStyle='#235b8b';ys.forEach((y,i)=>{ctx.fillRect(5+i/Math.max(1,ys.length-1)*1190,80-y/max*70,2,2)});ctx.fillStyle='#333';ctx.fillText('Sample index vs signed log10(1 + |score|); use the table for exact values',10,155)}
['search','view','indicator'].forEach(id=>get(id).addEventListener('input',render));render();</script></html>"""
    (destination / "report.html").write_text(
        page.replace("METADATA", metadata).replace("PAYLOAD", payload), encoding="utf-8"
    )
    return destination
