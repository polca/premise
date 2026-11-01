"""Disjoint reproducible panels with explicit inclusion reasons."""

from collections import defaultdict
import random


def stratum(key):
    name, product = key[0].casefold(), key[1].casefold()
    if name.startswith(("treatment", "recycling")) or "scrap" in product:
        return "waste_recycling"
    if any(s in name for s in ("battery", "electrolyzer", "electrolyser")):
        return "battery_electrolyser"
    if any(
        s in name for s in ("mine operation", "mining", "refinery", "metal production")
    ):
        return "mining_metals"
    if any(s in name for s in ("farming", "forestry", "cultivation")):
        return "agriculture_forestry"
    if name.startswith(("transport", "passenger car")) or "burned in" in name:
        return "transport_combustion"
    if any(
        s in name
        for s in ("hydrogen", "kerosene", "petrol", "diesel", "methanol", "syngas")
    ):
        return "fuels"
    if any(s in name for s in ("electricity", "heat production", "power plant")):
        return "electricity_heat"
    if "water" in name or any(s in product for s in ("acid", "chloride", "carbonate")):
        return "chemicals_water"
    return "other_manufacturing"


def choose_sample(graph, config, all_scores, indicators, eligible=None, findings=()):
    available = sorted(
        (
            i
            for i, k in enumerate(graph.keys)
            if k not in graph.ambiguous and (eligible is None or i in eligible)
        ),
        key=lambda i: graph.keys[i],
    )
    if config.explicit_sample:
        indices = []
        for key in config.explicit_sample:
            matches = graph.semantic.get(key, ())
            if len(matches) != 1 or matches[0] not in available:
                raise ValueError(
                    f"Explicit sample is absent, ambiguous or outside the target: {key}"
                )
            indices.append(matches[0])
        if len(indices) != len(set(indices)):
            raise ValueError("Explicit sample contains duplicate activities")
        return indices, [
            dict(
                activity=list(graph.keys[i]),
                panel="explicit",
                stratum=stratum(graph.keys[i]),
                inclusion_probability=None,
            )
            for i in indices
        ]
    count = min(config.sample_size, len(available))
    rng = random.Random(config.seed)
    uniform_count, stratified_count = round(count * 0.6), round(count * 0.3)
    uniform_count = min(uniform_count, count)
    stratified_count = min(stratified_count, count - uniform_count)
    uniform = rng.sample(available, uniform_count)
    selected = set(uniform)
    rows = {
        i: dict(
            activity=list(graph.keys[i]),
            panel="uniform",
            stratum=stratum(graph.keys[i]),
            inclusion_probability=uniform_count / len(available),
        )
        for i in uniform
    }
    groups = defaultdict(list)
    for i in available:
        if i not in selected:
            groups[(stratum(graph.keys[i]), graph.keys[i][3])].append(i)
    for values in groups.values():
        rng.shuffle(values)
    group_sizes = {k: len(v) for k, v in groups.items()}
    strata, quotas = [], defaultdict(int)
    while len(strata) < stratified_count:
        for k in sorted(groups):
            if groups[k] and len(strata) < stratified_count:
                i = groups[k].pop()
                strata.append(i)
                selected.add(i)
                quotas[k] += 1
    for i in strata:
        k = (stratum(graph.keys[i]), graph.keys[i][3])
        rows[i] = dict(
            activity=list(graph.keys[i]),
            panel="stratified",
            stratum=k[0],
            inclusion_probability=quotas[k] / group_sizes[k],
        )
    remaining = [i for i in available if i not in selected]
    # Risk ranking stays within sector/reference-unit groups. It does not
    # compare kg metal per train with kg metal per kg material as equal units.
    risk = defaultdict(float)
    for finding in findings:
        if finding.issue.activity_id is not None:
            risk[finding.issue.activity_id] += (
                50 if finding.issue.severity == "error" else 10
            )
    index = {v.key: j for j, v in enumerate(indicators)}
    for i in remaining:
        if "burned in" in graph.keys[i][0] and all_scores is not None:
            fossil = all_scores[index["co2.fossil"], i] if "co2.fossil" in index else 0
            bio = (
                all_scores[index["co2.non_fossil"], i]
                if "co2.non_fossil" in index
                else 0
            )
            if bio > 0 and fossil <= bio * 0.01:
                risk[i] += 10
    groups = defaultdict(list)
    for i in remaining:
        groups[(stratum(graph.keys[i]), graph.keys[i][3])].append(i)
    if all_scores is not None:
        for values in groups.values():
            for j in range(len(indicators)):
                ordered = sorted(
                    values, key=lambda i: abs(all_scores[j, i]), reverse=True
                )
                for i in ordered[: max(1, len(ordered) // 100)]:
                    risk[i] += 1
    tie = {i: rng.random() for i in remaining}
    risky = sorted(remaining, key=lambda i: (-risk[i], tie[i]))[: count - len(selected)]
    for i in risky:
        rows[i] = dict(
            activity=list(graph.keys[i]),
            panel="risk",
            stratum=stratum(graph.keys[i]),
            inclusion_probability=None,
            risk_rank_score=risk[i],
        )
    indices = uniform + strata + risky
    return indices, [rows[i] for i in indices]
