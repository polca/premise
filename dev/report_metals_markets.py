#!/usr/bin/env python3
"""Make a readable table from the complete metal-market RELICS comparison."""

import argparse
import csv
import json
from pathlib import Path


def number(value):
    return None if value == "" else float(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument(
        "--resource-fix",
        action="store_true",
        help="Label the follow-up cobalt/zinc repair comparison",
    )
    args = parser.parse_args()
    rows = []
    joined = {}
    for system in ("cutoff", "consequential"):
        with (args.results / system / "own_resource.csv").open() as stream:
            for row in csv.DictReader(stream):
                row = {**row, "system model": system}
                for field in ("before", "after", "change", "percent change"):
                    row[field] = number(row[field])
                rows.append(row)
                key = (row["market product"], row["indicator"])
                target = joined.setdefault(
                    key, {"market product": key[0], "indicator": key[1]}
                )
                for field in (
                    "before",
                    "after",
                    "percent change",
                    "status",
                    "before location",
                    "after location",
                ):
                    target[f"{system} {field}"] = row[field]
    combined = [joined[key] for key in sorted(joined)]
    with (args.results / "comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=list(combined[0]), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(combined)
    data = json.dumps(rows).replace("<", "\\u003c")
    document = """<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Metal-market RELICS changes</title>
<style>
body{font:16px/1.5 system-ui,sans-serif;margin:32px auto;padding:0 20px;max-width:1300px;color:#182431;background:#fbfcfe}
h1{font-size:28px;margin-bottom:8px}p{max-width:1000px}a{color:#175f91}.controls{display:flex;gap:18px;flex-wrap:wrap;align-items:end;margin:24px 0}
label{display:grid;gap:5px;font-size:14px}input,select{font:inherit;padding:9px;border:1px solid #b4bdc7;border-radius:5px}input{width:320px}
.table{overflow:auto;border:1px solid #d4dbe2;border-radius:6px}table{border-collapse:collapse;width:100%;font-size:14px;background:white}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #e5e9ef}thead{position:sticky;top:0;background:#eaf0f6}th{white-space:nowrap}
td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}.increase{color:#995000}.decrease{color:#155783}.note{font-size:13px;color:#4d5c6e}.status{max-width:220px;font-size:12px}
</style>
<h1>How the metals corrections changed RELICS scores</h1>
<p><strong>1 kg of each market product · IMAGE SSP2-M, 2050 · ecoinvent 3.12 · metals-only update</strong></p>
__STATUS__
<p>Before and after refer to the code corrections, with the same scenario and source inventory.
Values are kg of the named RELICS resource extracted per kg of market product, including its supply chain.
Ores and compounds are not normalized to a kilogram of contained metal. Mixed products have separate resource rows; the indicators are not summed.</p>
<p class="note">A decrease is a change in resource accounting and supply, not evidence of a real-world improvement.
The table selects the product-associated indicators. The complete matrices also include other resources used upstream.</p>
__DIAGNOSIS__
<p><a href="comparison.csv">Download this comparison (CSV)</a> ·
<a href="cutoff/all_indicators.csv">All 65 indicators: cutoff</a> ·
<a href="consequential/all_indicators.csv">All 65 indicators: consequential</a></p>
<div class="controls">
<label>System model<select id="system"><option value="cutoff">Cutoff</option><option value="consequential">Consequential</option></select></label>
<label>Find a market or resource<input id="search" type="search" placeholder="e.g. copper, neodymium, gold"></label>
<label>Order<select id="order"><option value="name">Market name</option><option value="relative">Largest absolute % change</option><option value="absolute">Largest absolute score change</option></select></label>
<span id="count" class="note"></span></div>
<div class="table"><table><thead><tr><th>Market product</th><th>Resource indicator</th><th>Before</th><th>After</th><th>Change</th><th>Location before → after</th><th>Coverage</th></tr></thead><tbody id="body"></tbody></table></div>
<p class="note">— means unavailable, not zero. Percentages are undefined when the before score is zero.
The selection prefers World, then GLO, then RoW. Where construction was skipped because a route was missing,
the comparison uses a surviving global source market if available; the location column makes this visible.</p>
<p class="note">There is no dedicated RELICS indicator here for bentonite, sand/quartz, uranium or lutetium.
For spodumene and dolomite the indicator is the mineral itself. Rare-earth and platinum-group concentrate rows show the available associated indicators; they do not establish complete composition or extraction coverage.</p>
<script type="application/json" id="data">__DATA__</script>
<script>
const data=JSON.parse(document.getElementById('data').textContent);
const fmt=x=>x===null?'—':(x===0?'0':(Math.abs(x)<0.0001||Math.abs(x)>=10000?x.toExponential(4):x.toPrecision(6)));
function render(){const q=document.getElementById('search').value.toLowerCase(),system=document.getElementById('system').value,order=document.getElementById('order').value;
const rows=data.filter(r=>r['system model']===system&&(r['market product']+' '+r.indicator).toLowerCase().includes(q));
const rank=x=>x===null?-Infinity:Math.abs(x);
rows.sort((a,b)=>order==='name'?a['market product'].localeCompare(b['market product'])||a.indicator.localeCompare(b.indicator):rank(b[order==='relative'?'percent change':'change'])-rank(a[order==='relative'?'percent change':'change']));
const body=document.getElementById('body');body.replaceChildren();
for(const r of rows){const tr=document.createElement('tr');const values=[r['market product'],r.indicator,fmt(r.before),fmt(r.after),r['percent change']===null?'—':(r['percent change']>0?'+':'')+r['percent change'].toFixed(2)+'%',(r['before location']||'—')+' → '+(r['after location']||'—'),r.status==='comparable'?'Available':r.status];
values.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if(i>=2&&i<=4)td.className='num';if(i===4&&r.change!==null)td.classList.add(r.change>0?'increase':'decrease');if(i===6)td.className='status';tr.appendChild(td);});body.appendChild(tr);}
document.getElementById('count').textContent=new Set(rows.map(r=>r['market product'])).size+' markets · '+rows.length+' resource rows';}
for(const id of ['system','search','order'])document.getElementById(id).addEventListener('input',render);render();
</script></html>"""
    if args.resource_fix:
        status = '<p><strong>Cobalt/zinc repair:</strong> Before is the build with missing extraction; after includes the resource-attribution fix. See <a href="README.rst">validation and scope</a>.</p>'
        diagnosis = "<p>The restored cobalt and zinc extraction changes supply-chain resource scores throughout the database. These increases repair missing accounting; they do not represent increased physical demand. Other methodological limitations remain, including consequential sulfur coverage.</p>"
    else:
        status = '<p><strong>Historical comparison before the cobalt/zinc repair.</strong> The <a href="../resource-fix/report.html">follow-up results</a> show the correction.</p>'
        diagnosis = "<p><strong>Attribution gaps found:</strong> cobalt falls to effectively zero and zinc falls by about 95%, although their markets remain almost entirely primary supply. Those decreases were accounting gaps, not resource savings. Consequential sulfur also needs separate attribution review.</p>"
    (args.results / "report.html").write_text(
        document.replace("__DATA__", data)
        .replace("__STATUS__", status)
        .replace("__DIAGNOSIS__", diagnosis)
    )
    print(
        f"Wrote {len(combined)} comparison rows for {len({r['market product'] for r in rows})} markets"
    )


if __name__ == "__main__":
    main()
