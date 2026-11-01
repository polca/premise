"""Read-only Brightway adapter and independent numerical cross-checks."""

from __future__ import annotations

import random

import numpy as np
from scipy.sparse import csr_matrix

from .engine import audit_database
from .graph import flow_identity, identity
from .indicators import default_indicators
from .models import AuditConfig, AuditFinding
from ..validation.validation_framework import ValidationIssue


def audit_brightway_database(
    database,
    *,
    config=None,
    indicators=None,
    crosscheck_size=250,
    provenance=None,
    progress=None,
):
    """Audit a written database in the caller's current Brightway project.

    ``database`` can be a name or a Brightway Database. This function neither
    changes project nor registers/rewrites databases or LCIA methods. It follows
    actual external input links, loads their metadata and includes dependencies.
    Supply ``crosscheck_size=0`` to omit the separate Brightway solve session.
    """
    import bw2data as bd
    import bw2calc as bc
    from ..core._bw2_backend_compat import ActivityDataset, ExchangeDataset

    if (
        isinstance(crosscheck_size, bool)
        or not isinstance(crosscheck_size, int)
        or crosscheck_size < 0
    ):
        raise ValueError("crosscheck_size must be a nonnegative integer")
    config = config or AuditConfig()
    indicators = tuple(indicators) if indicators is not None else default_indicators()
    name = database if isinstance(database, str) else database.name
    if name not in bd.databases:
        raise ValueError(f"Database does not exist in the current project: {name}")
    loaded = {}
    pending = [name]
    while pending:
        dependency = pending.pop()
        if dependency in loaded:
            continue
        if dependency not in bd.databases:
            raise ValueError(f"External database is unavailable: {dependency}")
        # Fast premise exports store authoritative links in relational columns.
        # SQLiteBackend.load() can silently lose exchanges when serialized
        # ``output`` metadata is stale. Read the same columns Brightway solves.
        data = {}
        for row in (
            ActivityDataset.select()
            .where(ActivityDataset.database == dependency)
            .iterator()
        ):
            data[(row.database, row.code)] = dict(
                row.data,
                database=row.database,
                code=row.code,
                exchanges=[],
                _bw_id=row.id,
            )
        for row in (
            ExchangeDataset.select()
            .where(ExchangeDataset.output_database == dependency)
            .iterator()
        ):
            key = row.output_database, row.output_code
            if key not in data:
                raise ValueError(f"Exchange output activity is unavailable: {key}")
            exc = dict(
                row.data,
                input=(row.input_database, row.input_code),
                output=key,
                type=row.type,
            )
            data[key]["exchanges"].append(exc)
        loaded[dependency] = data
        if progress is not None:
            progress(
                dict(phase="read_database", database=dependency, completed=len(data))
            )
        for node in data.values():
            for exc in node.get("exchanges", ()):
                link = exc.get("input")
                if link and link[0] not in loaded:
                    pending.append(link[0])
    processes, biosphere = {}, {}
    for data in loaded.values():
        for key, node in data.items():
            record = dict(node, database=key[0], code=key[1])
            if "reference product" in record or any(
                e.get("type") == "production" for e in record.get("exchanges", ())
            ):
                processes[key] = record
            else:
                biosphere[key] = record
    # Fill omitted descriptive fields from actual links; retain conflicting
    # fields so the graph auditor can detect stale metadata and wrong units.
    inventory = []
    product_named_exchanges = 0
    for key, node in processes.items():
        exchanges = []
        for original in node.get("exchanges", ()):
            exc = dict(original)
            kind = exc.get("type")
            supplier = (
                biosphere.get(tuple(exc.get("input", ())))
                if kind == "biosphere"
                else processes.get(tuple(exc.get("input", ())))
            )
            if kind == "production":
                supplier = node
            if supplier:
                # Native ecoinvent imports commonly label an intermediate
                # exchange with its product name. Premise uses the supplier
                # activity name. Both are valid descriptions of the same
                # explicit relational link; unrelated names remain findings.
                if kind in {"technosphere", "substitution"} and exc.get(
                    "name"
                ) == supplier.get("reference product"):
                    exc["_audit_original_product_name"] = exc["name"]
                    exc["name"] = supplier.get("name")
                    product_named_exchanges += 1
                for field in ("name", "unit"):
                    if exc.get(field) is None:
                        exc[field] = supplier.get(field)
                if kind == "biosphere":
                    exc.setdefault("categories", supplier.get("categories"))
                else:
                    exc.setdefault("product", supplier.get("reference product"))
                    exc.setdefault("location", supplier.get("location"))
            exchanges.append(exc)
        inventory.append(dict(node, exchanges=exchanges))
    supplied_provenance = dict(provenance or {})
    supplied_provenance.update(
        brightway_project=bd.projects.current,
        database=name,
        dependencies=sorted(set(loaded) - {name}),
        product_named_exchanges=product_named_exchanges,
    )
    lca = None

    def processed_matrices(graph):
        # Audit written scores with the actual processed coefficients, not an
        # independently rounded reconstruction of nominal SQL amounts. Keep
        # SQL quantities for independent source-ledger checks and compare the
        # two representations explicitly before using the processed matrices.
        nonlocal lca
        first_key = next(k for k in processes if k[0] == name)
        lca = bc.LCA({first_key: 1})
        lca.load_lci_data()
        modern = hasattr(lca, "dicts")
        product = lca.dicts.product if modern else lca.product_dict
        activity = lca.dicts.activity if modern else lca.activity_dict

        def lookup(node):
            return node["_bw_id"] if modern else (node["database"], node["code"])

        try:
            rows = [product[lookup(n)] for n in graph.nodes]
            cols = [activity[lookup(n)] for n in graph.nodes]
        except KeyError as exc:
            raise ValueError(
                "Processed graph does not cover its relational process identities"
            ) from exc
        A = lca.technosphere_matrix.tocsr()[rows, :][:, cols].tocsc()
        reverse_bio = (
            lca.dicts.biosphere.reversed
            if modern
            else {v: k for k, v in lca.biosphere_dict.items()}
        )
        by_id = {v["_bw_id"]: v for v in biosphere.values()}
        flow_rows = {f: i for i, f in enumerate(graph.flows)}
        rr, cc = [], []
        for row, identifier in reverse_bio.items():
            node = by_id[identifier] if modern else biosphere[identifier]
            fk = flow_identity(node)
            if fk in flow_rows:
                rr.append(flow_rows[fk])
                cc.append(row)
            elif lca.biosphere_matrix.getrow(row).nnz:
                raise ValueError(
                    "Processed biosphere contains a flow absent from relational exchanges"
                )
        remap = csr_matrix(
            (np.ones(len(rr)), (rr, cc)),
            shape=(len(graph.flows), lca.biosphere_matrix.shape[0]),
        )
        B = (remap @ lca.biosphere_matrix[:, cols]).tocsr()
        precision = {}
        for label, raw, processed in (
            ("technosphere", graph.A, A),
            ("biosphere", graph.B, B),
        ):
            difference = (processed - raw).tocoo()
            difference.eliminate_zeros()
            outside = 0
            magnitude = (
                graph.A_magnitude
                if label == "technosphere"
                else graph.B_positive - graph.B_negative
            )
            for r, c, delta in zip(difference.row, difference.col, difference.data):
                original = float(raw[r, c])
                actual = float(processed[r, c])
                envelope = (
                    5
                    * np.finfo(np.float32).eps
                    * max(float(magnitude[r, c]), abs(actual))
                    + 1e-14
                )
                if abs(delta) > envelope:
                    outside += 1
                    graph.problem(
                        "GRAPH.PROCESSED_AMOUNT",
                        int(c),
                        "Processed coefficient differs from the nominal exchange beyond a float32 rounding envelope",
                        actual=actual,
                        expected=original,
                    )
            precision[label] = dict(
                changed_coefficients=difference.nnz,
                outside_rounding_envelope=outside,
                max_absolute_difference=float(max(abs(difference.data), default=0)),
            )
        supplied_provenance["processed_precision"] = precision
        delta = B - graph.B
        graph.B_positive = graph.B_positive + delta.maximum(0)
        graph.B_negative = graph.B_negative + delta.minimum(0)
        graph.A = A
        graph.A.sort_indices()
        graph.B = B
        graph.scoring_inputs = []
        for i in range(len(graph.nodes)):
            self_input = any(p == i for p, a, e in graph.inputs[i])
            if not self_input:
                graph.production[i] = float(A[i, i])
            column = A.getcol(i).tocoo()
            entries = [
                (int(r), -float(v), {})
                for r, v in zip(column.row, column.data)
                if r != i
            ]
            if self_input:
                entries.append((i, float(graph.production[i] - A[i, i]), {}))
            graph.scoring_inputs.append(entries)

    report = audit_database(
        inventory,
        config=config,
        biosphere=biosphere,
        indicators=indicators,
        provenance=supplied_provenance,
        target_database=name,
        progress=progress,
        _matrix_adapter=processed_matrices,
    )
    if (
        report.metadata["scoring_status"] != "completed"
        or not crosscheck_size
        or not report.sample
    ):
        return report
    nodes = {identity(n): key for key, n in processes.items() if key[0] == name}
    count = min(crosscheck_size, len(report.sample))
    positions = sorted(
        random.Random(config.seed + 1).sample(range(len(report.sample)), count)
    )
    get_node = getattr(bd, "get_node", None)

    def activity(key):
        return get_node(key=key) if get_node else bd.get_activity(key)

    lca.lci(factorize=True)
    r, c, v = [], [], []
    reverse_biosphere = (
        lca.dicts.biosphere.reversed
        if hasattr(lca, "dicts")
        else {v: k for k, v in lca.biosphere_dict.items()}
    )
    for k, node_id in reverse_biosphere.items():
        flow = (
            get_node(id=node_id)
            if isinstance(node_id, int) and get_node
            else bd.get_activity(node_id)
        )
        fk = flow_identity(flow)
        for j, spec in enumerate(indicators):
            value = spec.factor(fk)
            if value:
                r.append(j)
                c.append(k)
                v.append(value)
    cf = csr_matrix((v, (r, c)), shape=(len(indicators), lca.biosphere_matrix.shape[0]))
    errors = np.zeros(len(indicators))
    failures = []
    for p in positions:
        key = tuple(report.sample[p]["activity"])
        node = activity(nodes[key])
        lca.redo_lci({node.id if hasattr(lca, "dicts") else node.key: 1})
        inventory = np.asarray(lca.inventory.sum(axis=1)).ravel()
        values = np.asarray(cf @ inventory).ravel()
        error = abs(values - report.total[p])
        errors = np.maximum(errors, error)
        tolerance = config.numerical_atol + config.numerical_rtol * np.maximum(
            abs(values), abs(report.total[p])
        )
        if not np.all(np.isfinite(values)) or np.any(error > tolerance):
            failures.append(
                AuditFinding(
                    ValidationIssue(
                        "NUMERICAL.BRIGHTWAY",
                        "error",
                        "Independent Brightway inventory aggregation disagrees with audit scores",
                        activity_key=key[:3],
                    ),
                    "numerical",
                    "high",
                    key,
                    evidence=dict(max_absolute_error=float(max(error))),
                )
            )
    report.findings += tuple(failures)
    report.metadata["brightway_crosscheck"] = dict(
        demands=count,
        indicator_scores=count * len(indicators),
        factorizations=1,
        max_absolute_error=errors.tolist(),
        failures=len(failures),
        methods_written=False,
    )
    if progress is not None:
        progress(dict(phase="brightway_crosschecks", completed=count, total=count))
    return report
