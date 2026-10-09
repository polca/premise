"""Scoped deterministic separation of embedded lifecycle exchanges.

Explicit paths identify a capital market/manufacturer subnetwork. Copies of
that subnetwork omit selected lifecycle exchanges; equivalent coefficients
connect the service caller to new lifecycle adapters. Original suppliers and
all unselected exchanges remain intact. No timing or lifetime is inferred here.
"""

from collections import defaultdict
from copy import deepcopy
import hashlib
import json
import math

from .stock_vintage import IDENTITY_FIELDS, identity


def _amount(exchange):
    value = float(exchange["amount"])
    if not math.isfinite(value):
        raise ValueError("Lifecycle path coefficients must be finite")
    return value


def _production(dataset):
    rows = [e for e in dataset["exchanges"] if e["type"] == "production"]
    if len(rows) != 1 or identity(rows[0], exchange=True) != identity(dataset):
        raise ValueError("Lifecycle splitting needs one matching reference production")
    amount = _amount(rows[0])
    if amount <= 0:
        raise ValueError("Reference production must be positive")
    return amount


def _edge(dataset, supplier):
    rows = [
        e
        for e in dataset["exchanges"]
        if e["type"] == "technosphere" and identity(e, exchange=True) == supplier
    ]
    if len(rows) != 1:
        raise ValueError(f"Lifecycle paths require one exact exchange: {supplier}")
    _amount(rows[0])
    return rows[0]


def _metadata(key):
    return dict(zip(IDENTITY_FIELDS, key))


def _relink(exchange, dataset, owner):
    for field in ("name", "unit", "location"):
        exchange[field] = dataset[field]
    exchange["product"] = dataset["reference product"]
    exchange.pop("reference product", None)
    exchange["input"] = (dataset["database"], dataset["code"])
    exchange["output"] = (owner["database"], owner["code"])
    for field in ("database", "code"):
        if field in exchange:
            exchange[field] = dataset[field]


def scope_capital_chain(database, *, caller, chain, context_id):
    """Copy one reviewed linear capital chain without moving any exchanges.

    This supports capital inventories with no selected embedded lifecycle port.
    Explicit zero-shift bindings describe internal market/manufacturer links;
    material and energy inputs retain their own background roles. A branched or
    cyclic selected chain is rejected rather than partially scoped.
    """
    if not isinstance(context_id, str) or not context_id.strip() or not chain:
        raise ValueError("A context and an explicit capital chain are required")
    lookup = {identity(d): d for d in database}
    if len(lookup) != len(database):
        raise ValueError("Ambiguous capital activity identity")
    keys = [identity(caller)] + [identity(d) for d in chain]
    if len(set(keys)) != len(keys) or any(k not in lookup for k in keys):
        raise ValueError("Capital chain must contain distinct existing activities")
    links = set(zip(keys, keys[1:]))
    for a, b in links:
        _edge(lookup[a], b)
    for key in keys:
        _production(lookup[key])
        for exchange in lookup[key]["exchanges"]:
            if exchange["type"] == "technosphere":
                supplier = identity(exchange, exchange=True)
                if supplier in keys and (key, supplier) not in links:
                    raise ValueError(
                        "Selected capital chain has an extra internal edge"
                    )
    copies = {}
    for i, key in enumerate(keys):
        original = lookup[key]
        role = "service" if i == 0 else "capital"
        copy = deepcopy(original)
        token = hashlib.sha256(
            json.dumps([context_id, "chain", key]).encode()
        ).hexdigest()[:24]
        copy["name"] = f"{original['name']} [stock context {context_id}: {role}]"
        copy["database"] = original.get("database", "stock-vintage")
        copy["code"] = f"stock-{token}"
        copy["stock_vintage_context"] = context_id
        copy["stock_vintage_role"] = role
        copy["stock_vintage_original_identity"] = _metadata(key)
        copy.pop("id", None)
        if identity(copy) in lookup:
            raise ValueError("Capital context already exists; choose a unique context")
        copies[key] = copy
    for key, copy in copies.items():
        for exchange in copy["exchanges"]:
            exchange["output"] = (copy["database"], copy["code"])
            if exchange["type"] == "production":
                _relink(exchange, copy, copy)
            elif exchange["type"] == "technosphere":
                supplier = identity(exchange, exchange=True)
                if (key, supplier) in links:
                    _relink(exchange, copies[supplier], copy)
    audit = {
        "context_id": context_id,
        "caller": _metadata(identity(copies[keys[0]])),
        "capital_root": _metadata(identity(copies[keys[1]])),
        "original_caller": _metadata(keys[0]),
        "original_root": _metadata(keys[1]),
        "preserved_capital_amount": _amount(_edge(lookup[keys[0]], keys[1])),
        "copied_capital_activities": len(chain),
        "lifted_exchanges": [],
        "lifecycle_adapters": 0,
        "zero_shift_bindings": [
            {
                "caller": _metadata(identity(copies[a])),
                "supplier": _metadata(identity(copies[b])),
            }
            for a, b in zip(keys[1:], keys[2:])
        ],
        "scope": "Explicit capital chain only; no exchanges removed or invented; deterministic equivalence",
    }
    return list(database) + list(copies.values()), audit


def split_embedded_lifecycle(database, *, caller, paths, context_id, uncertainty_mode):
    """Return a new inventory list and an auditable deterministic rewrite.

    ``paths`` contains identity lists starting at one capital supplier and ending
    at a lifecycle supplier. Intermediate activities are copied once. Different
    paths may share a prefix or merge. ``uncertainty_mode`` must explicitly be
    ``deterministic``: lifted products of coefficients do not preserve Monte
    Carlo correlations. Existing inputs are never mutated, including on error.
    """
    if uncertainty_mode != "deterministic":
        raise ValueError("Only deterministic lifecycle splitting is implemented")
    if not isinstance(context_id, str) or not context_id.strip():
        raise ValueError("A nonempty lifecycle context_id is required")
    if not paths:
        raise ValueError("At least one explicit lifecycle path is required")
    lookup = {}
    for dataset in database:
        key = identity(dataset)
        if key in lookup:
            raise ValueError(f"Ambiguous lifecycle activity identity: {key}")
        lookup[key] = dataset
    caller_key = identity(caller)
    if caller_key not in lookup:
        raise ValueError("Lifecycle caller is absent from the inventory")
    keys = [tuple(identity(record) for record in path) for path in paths]
    if any(len(path) < 2 or len(set(path)) != len(path) for path in keys):
        raise ValueError("Lifecycle paths need a supplier edge and cannot cycle")
    root = keys[0][0]
    if any(path[0] != root for path in keys):
        raise ValueError("One lifecycle rewrite must have one capital root")
    if any(caller_key in path for path in keys):
        raise ValueError("The service caller cannot be in the capital path")
    if any(key not in lookup for path in keys for key in path):
        raise ValueError("A lifecycle path activity is absent from the inventory")
    nodes = {key for path in keys for key in path[:-1]}
    cuts = {(path[-2], path[-1]) for path in keys}
    links = {(a, b) for path in keys for a, b in zip(path[:-2], path[1:-1])}
    if cuts & links or any(b in nodes for _, b in cuts):
        raise ValueError("A lifecycle endpoint cannot also be a copied capital node")
    for a, b in cuts | links:
        _edge(lookup[a], b)
    root_exchange = _edge(lookup[caller_key], root)
    production = {key: _production(lookup[key]) for key in nodes}
    caller_production = _production(lookup[caller_key])
    incoming = {key: 0 for key in nodes}
    successors = defaultdict(list)
    for a, b in links:
        successors[a].append(b)
        incoming[b] += 1
    queue = sorted(key for key, count in incoming.items() if count == 0)
    order = []
    while queue:
        key = queue.pop(0)
        order.append(key)
        for successor in sorted(successors[key]):
            incoming[successor] -= 1
            if incoming[successor] == 0:
                queue.append(successor)
    if len(order) != len(nodes) or order[0] != root:
        raise ValueError(
            "The copied capital subnetwork must be an acyclic rooted graph"
        )

    # Activity scales per unit of the capital root, including non-unit
    # productions. Keep this separate from the service-year amortisation.
    scales = defaultdict(float)
    scales[root] = 1.0 / production[root]
    for key in order:
        for successor in successors[key]:
            scales[successor] += (
                scales[key]
                * _amount(_edge(lookup[key], successor))
                / production[successor]
            )

    def copied_metadata(original, role):
        result = deepcopy(original)
        token = hashlib.sha256(
            json.dumps([context_id, role, identity(original)], sort_keys=True).encode()
        ).hexdigest()[:24]
        result["name"] = f"{original['name']} [stock context {context_id}: {role}]"
        result["code"] = f"stock-{token}"
        result["database"] = original.get("database", "stock-vintage")
        result["stock_vintage_context"] = context_id
        result["stock_vintage_role"] = role
        result["stock_vintage_original_identity"] = _metadata(identity(original))
        result.pop("id", None)
        return result

    clones = {key: copied_metadata(lookup[key], "capital") for key in order}
    if any(identity(clone) in lookup for clone in clones.values()):
        raise ValueError("Lifecycle context already exists; choose a unique context")
    rewritten_caller = copied_metadata(lookup[caller_key], "service")
    for exchange in rewritten_caller["exchanges"]:
        exchange["output"] = (rewritten_caller["database"], rewritten_caller["code"])
        if exchange["type"] == "production":
            _relink(exchange, rewritten_caller, rewritten_caller)
    _relink(_edge(rewritten_caller, root), clones[root], rewritten_caller)
    zero_shift = []
    for key, clone in clones.items():
        retained = []
        for exchange in clone["exchanges"]:
            exchange["output"] = (clone["database"], clone["code"])
            if exchange["type"] == "production":
                _relink(exchange, clone, clone)
            elif exchange["type"] == "technosphere":
                supplier = identity(exchange, exchange=True)
                if (key, supplier) in cuts:
                    continue
                if (key, supplier) in links:
                    _relink(exchange, clones[supplier], clone)
                    zero_shift.append(
                        {
                            "caller": _metadata(identity(clone)),
                            "supplier": _metadata(identity(clones[supplier])),
                        }
                    )
            retained.append(exchange)
        clone["exchanges"] = retained

    adapters, lifted = [], []
    for parent, supplier in sorted(cuts):
        original = _edge(lookup[parent], supplier)
        per_capital_unit = scales[parent] * _amount(original)
        amount = _amount(root_exchange) * per_capital_unit
        endpoint = lookup[supplier]
        endpoint = dict(
            endpoint,
            database=endpoint.get("database", "stock-vintage"),
            code=endpoint.get(
                "code", hashlib.sha256(repr(supplier).encode()).hexdigest()
            ),
        )
        # One adapter per cut keeps distinct lifecycle roles from colliding with
        # existing direct exchanges or cancelling signed credits at the caller.
        adapter = copied_metadata(
            endpoint,
            "lifecycle-" + hashlib.sha256(repr(parent).encode()).hexdigest()[:10],
        )
        adapter.pop("parameters", None)
        adapter["exchanges"] = []
        for kind, supplier_dataset in (
            ("production", adapter),
            ("technosphere", endpoint),
        ):
            exchange = {"type": kind, "amount": 1.0, "uncertainty type": 0}
            _relink(exchange, supplier_dataset, adapter)
            adapter["exchanges"].append(exchange)
        new_exchange = {"type": "technosphere", "amount": amount, "uncertainty type": 0}
        _relink(new_exchange, adapter, rewritten_caller)
        rewritten_caller["exchanges"].append(new_exchange)
        adapters.append(adapter)
        zero_shift.append(
            {"caller": _metadata(identity(adapter)), "supplier": _metadata(supplier)}
        )
        lifted.append(
            {
                "original_caller": _metadata(parent),
                "original_supplier": _metadata(supplier),
                "original_amount": _amount(original),
                "activity_scale_per_caller": _amount(root_exchange) * scales[parent],
                "coefficient_per_capital_unit": per_capital_unit,
                "amount_per_calling_dataset": amount,
                "amount_per_service_unit": amount / caller_production,
                "caller": _metadata(identity(rewritten_caller)),
                "supplier": _metadata(identity(adapter)),
                "original_uncertainty_type": original.get("uncertainty type", 0),
            }
        )
    names = [
        identity(row) for row in [rewritten_caller] + list(clones.values()) + adapters
    ]
    if len(set(names)) != len(names) or any(key in lookup for key in names):
        raise ValueError("Lifecycle rewrite creates a conflicting identity")
    result = list(database) + [rewritten_caller]
    result.extend(clones.values())
    result.extend(adapters)
    audit = {
        "context_id": context_id,
        "uncertainty_mode": "deterministic",
        "original_root": _metadata(root),
        "capital_root": _metadata(identity(clones[root])),
        "original_caller": _metadata(caller_key),
        "caller": _metadata(identity(rewritten_caller)),
        "preserved_capital_amount": _amount(root_exchange),
        "lifted_exchanges": lifted,
        "zero_shift_bindings": zero_shift,
        "copied_capital_activities": len(clones),
        "lifecycle_adapters": len(adapters),
        "scope": "Explicit path rewrite only; no timing, cohort, or scientific-role inference",
        "uncertainty_limit": "Lifted coefficient products preserve deterministic amounts, not Monte Carlo correlations",
    }
    return result, audit


def validate_lifecycle_anchor_audits(audits):
    """Require invariant lifted coefficients per capital unit across anchors.

    Common service-year amortisation can vary. An embedded lifecycle quantity
    that instead varies with manufacture year needs cohort-specific attribution;
    moving its service-year coefficient would change that model. Fail explicitly
    rather than treating the simplified marginal representation as equivalent.
    """
    if not audits:
        raise ValueError("Lifecycle anchor audits are required")
    reference = audits[0]

    def coefficients(audit):
        return {
            (identity(row["original_caller"]), identity(row["original_supplier"])): row[
                "coefficient_per_capital_unit"
            ]
            for row in audit["lifted_exchanges"]
        }

    expected = coefficients(reference)
    for audit in audits:
        if any(
            audit[key] != reference[key] for key in ("original_caller", "original_root")
        ):
            raise ValueError("Lifecycle anchor audits refer to different boundaries")
        actual = coefficients(audit)
        if actual.keys() != expected.keys() or any(
            not math.isclose(actual[key], expected[key], rel_tol=1e-12, abs_tol=1e-15)
            for key in expected
        ):
            raise ValueError(
                "Lifecycle coefficients vary by manufacture anchor; use cohort-specific quantities"
            )
    return {
        "anchors_checked": len(audits),
        "invariant_lifecycle_coefficients": len(expected),
        "service_amortisation_may_vary": True,
    }
