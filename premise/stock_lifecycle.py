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


def _net_caller_production(dataset):
    """Retain a service market's own-product losses in its copied diagonal."""
    key = identity(dataset)
    amount = _production(dataset) - math.fsum(
        _amount(e)
        for e in dataset["exchanges"]
        if e["type"] == "technosphere" and identity(e, exchange=True) == key
    )
    if amount <= 0:
        raise ValueError("Service caller must have positive net reference production")
    return amount


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
    caller_production = _net_caller_production(lookup[keys[0]])
    for a, b in links:
        _edge(lookup[a], b)
    for key in keys:
        _production(lookup[key])
        for exchange in lookup[key]["exchanges"]:
            if exchange["type"] == "technosphere":
                supplier = identity(exchange, exchange=True)
                if (
                    supplier in keys
                    and (key, supplier) not in links
                    and not key == supplier == keys[0]
                ):
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
                if (key, supplier) in links or key == supplier == keys[0]:
                    _relink(exchange, copies[supplier], copy)
    audit = {
        "context_id": context_id,
        "caller": _metadata(identity(copies[keys[0]])),
        "capital_root": _metadata(identity(copies[keys[1]])),
        "original_caller": _metadata(keys[0]),
        "original_root": _metadata(keys[1]),
        "preserved_capital_amount": _amount(_edge(lookup[keys[0]], keys[1])),
        "caller_net_reference_production": caller_production,
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


def split_embedded_lifecycle(
    database, *, caller, paths, context_id, uncertainty_mode, component_cuts=()
):
    """Return a new inventory list and an auditable deterministic rewrite.

    ``paths`` contains identity lists starting at one capital supplier and ending
    at a lifecycle supplier. Intermediate activities are copied once. Different
    paths may share a prefix or merge. ``uncertainty_mode`` must explicitly be
    ``deterministic``: lifted products of coefficients do not preserve Monte
    Carlo correlations. Existing inputs are never mutated, including on error.

    Optional ``component_cuts`` are exact caller/supplier pairs within the
    selected capital graph. These inputs are also lifted to the service caller,
    pointing to the scoped component whose own lifecycle ports have already
    been separated. This permits current-component manufacture and disposal
    to receive distinct dates without leaving embedded disposal in manufacture.
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
    internal_cuts = set()
    for pair in component_cuts:
        if len(pair) != 2:
            raise ValueError("Component cuts require explicit caller/supplier pairs")
        edge = tuple(identity(record) for record in pair)
        if edge not in links or edge in internal_cuts:
            raise ValueError(
                "Component cuts must be distinct edges in the scoped graph"
            )
        internal_cuts.add(edge)
    cuts |= internal_cuts
    for a, b in cuts | links:
        _edge(lookup[a], b)
    root_exchange = _edge(lookup[caller_key], root)
    production = {key: _production(lookup[key]) for key in nodes}
    caller_production = _net_caller_production(lookup[caller_key])
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
        elif (
            exchange["type"] == "technosphere"
            and identity(exchange, exchange=True) == caller_key
        ):
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
        # Internal component cuts deliver the scoped remainder, not the original
        # component that still embeds its lifetime disposal requirements.
        target = clones[supplier] if (parent, supplier) in internal_cuts else endpoint
        for kind, supplier_dataset in (
            ("production", adapter),
            ("technosphere", target),
        ):
            exchange = {"type": kind, "amount": 1.0, "uncertainty type": 0}
            _relink(exchange, supplier_dataset, adapter)
            adapter["exchanges"].append(exchange)
        new_exchange = {"type": "technosphere", "amount": amount, "uncertainty type": 0}
        _relink(new_exchange, adapter, rewritten_caller)
        rewritten_caller["exchanges"].append(new_exchange)
        adapters.append(adapter)
        zero_shift.append(
            {
                "caller": _metadata(identity(adapter)),
                "supplier": _metadata(identity(target)),
            }
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
                "boundary_kind": (
                    "component" if (parent, supplier) in internal_cuts else "lifecycle"
                ),
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
        "caller_net_reference_production": caller_production,
        "lifted_exchanges": lifted,
        "zero_shift_bindings": zero_shift,
        "copied_capital_activities": len(clones),
        "lifecycle_adapters": len(adapters),
        "scoped_nodes": [
            {
                "original": _metadata(key),
                "scoped": _metadata(identity(clones[key])),
                "activity_scale_per_calling_dataset": _amount(root_exchange)
                * scales[key],
                "activity_scale_per_capital_unit": scales[key],
            }
            for key in order
        ],
        "scope": "Explicit path rewrite only; no timing, cohort, or scientific-role inference",
        "uncertainty_limit": "Lifted coefficient products preserve deterministic amounts, not Monte Carlo correlations",
    }
    return result, audit


def scope_event_market(database, audit, *, caller, supplier, event_suppliers):
    """Scope one reviewed market and date its event providers without a new lag.

    The caller must already belong to this lifecycle context. This works for a
    direct service-year maintenance input or an adapter delivering a disposal
    market. Signed reference production and all market coefficients are retained.
    Only the explicitly named event-provider links receive zero-shift bindings;
    freight and independent capital keep their existing background roles.
    """
    lookup = {identity(d): d for d in database}
    if len(lookup) != len(database):
        raise ValueError("Ambiguous event-market identity")
    caller_key, supplier_key = identity(caller), identity(supplier)
    if caller_key not in lookup or supplier_key not in lookup:
        raise ValueError("Event market or its caller is absent")
    owner, original = lookup[caller_key], lookup[supplier_key]
    if owner.get("stock_vintage_context") != audit["context_id"]:
        raise ValueError("Event market requires a caller in the reviewed context")
    original_exchange = _edge(owner, supplier_key)
    providers = [identity(d) for d in event_suppliers]
    if not providers or len(set(providers)) != len(providers):
        raise ValueError("Event providers must be explicit, nonempty and unique")
    if any(k not in lookup or k in {caller_key, supplier_key} for k in providers):
        raise ValueError("Event providers must be distinct existing activities")
    for key in providers:
        _edge(original, key)
    productions = [e for e in original["exchanges"] if e["type"] == "production"]
    if (
        len(productions) != 1
        or identity(productions[0], exchange=True) != supplier_key
        or _amount(productions[0]) == 0
    ):
        raise ValueError("Event market requires one nonzero matching production")
    if any(
        e["type"] == "technosphere" and identity(e, exchange=True) == supplier_key
        for e in original["exchanges"]
    ):
        raise ValueError(
            "Event-market self-consumption needs a separate boundary review"
        )
    context = audit["context_id"]
    clone = deepcopy(original)
    clone["name"] = f"{original['name']} [stock context {context}: capital]"
    clone["code"] = (
        "stock-"
        + hashlib.sha256(
            json.dumps([context, "event-market", supplier_key]).encode()
        ).hexdigest()[:24]
    )
    clone["database"] = original.get("database", "stock-vintage")
    clone["stock_vintage_context"] = context
    clone["stock_vintage_role"] = "capital"
    clone["stock_vintage_original_identity"] = _metadata(supplier_key)
    clone.pop("id", None)
    if identity(clone) in lookup:
        raise ValueError("Event market already scoped in this context")
    for exchange in clone["exchanges"]:
        exchange["output"] = (clone["database"], clone["code"])
        if exchange["type"] == "production":
            _relink(exchange, clone, clone)
    owner_copy = deepcopy(owner)
    _relink(_edge(owner_copy, supplier_key), clone, owner_copy)
    result = [owner_copy if identity(d) == caller_key else d for d in database]
    result.append(clone)
    audit = deepcopy(audit)
    for binding in audit["zero_shift_bindings"]:
        if (
            identity(binding["caller"]) == caller_key
            and identity(binding["supplier"]) == supplier_key
        ):
            binding["supplier"] = _metadata(identity(clone))
    audit["zero_shift_bindings"].extend(
        {"caller": _metadata(identity(clone)), "supplier": _metadata(key)}
        for key in providers
    )
    audit["copied_capital_activities"] += 1
    audit.setdefault("event_markets", []).append(
        {
            "caller": _metadata(caller_key),
            "original_supplier": _metadata(supplier_key),
            "supplier": _metadata(identity(clone)),
            "amount_per_calling_dataset": _amount(original_exchange),
            "reference_production": _amount(productions[0]),
            "zero_shift_event_suppliers": [_metadata(key) for key in providers],
        }
    )
    return result, audit


def wrap_scoped_exchange(database, audit, *, supplier):
    """Give a direct scoped-caller exchange a positive-unit timing adapter.

    Keep the original signed coefficient. In particular, a negative waste input
    can be dated at a positive-unit adapter while the supplier's negative
    reference production is retained downstream. No waste quantity is inverted
    or converted to an unsigned amount as part of the rewrite.
    """
    lookup = {identity(d): d for d in database}
    if len(lookup) != len(database):
        raise ValueError("Ambiguous direct-exchange activity identity")
    caller_key, supplier_key = identity(audit["caller"]), identity(supplier)
    if caller_key not in lookup or supplier_key not in lookup:
        raise ValueError("Direct exchange boundary is absent")
    owner, target = lookup[caller_key], lookup[supplier_key]
    if owner.get("stock_vintage_context") != audit["context_id"]:
        raise ValueError("Direct exchange requires a scoped service caller")
    coefficient = _amount(_edge(owner, supplier_key))
    token = hashlib.sha256(
        json.dumps([audit["context_id"], "direct", supplier_key]).encode()
    ).hexdigest()[:24]
    adapter = deepcopy(target)
    adapter.update(
        name=f"{target['name']} [stock context {audit['context_id']}: lifecycle-direct-{token}]",
        code=f"stock-{token}",
        database=target.get("database", "stock-vintage"),
        stock_vintage_context=audit["context_id"],
        stock_vintage_role="lifecycle",
        exchanges=[],
    )
    for key in ("id", "parameters", "stock_vintage_original_identity"):
        adapter.pop(key, None)
    if identity(adapter) in lookup:
        raise ValueError("Direct exchange already wrapped in this context")
    for kind, node in (("production", adapter), ("technosphere", target)):
        exchange = {"type": kind, "amount": 1.0, "uncertainty type": 0}
        _relink(exchange, node, adapter)
        adapter["exchanges"].append(exchange)
    owner_copy = deepcopy(owner)
    _relink(_edge(owner_copy, supplier_key), adapter, owner_copy)
    result = [owner_copy if identity(d) == caller_key else d for d in database]
    result.append(adapter)
    audit = deepcopy(audit)
    for binding in audit["zero_shift_bindings"]:
        if (
            identity(binding["caller"]) == caller_key
            and identity(binding["supplier"]) == supplier_key
        ):
            binding["supplier"] = _metadata(identity(adapter))
    audit["zero_shift_bindings"].append(
        {"caller": _metadata(identity(adapter)), "supplier": _metadata(supplier_key)}
    )
    audit["lifecycle_adapters"] += 1
    audit.setdefault("direct_lifecycle", []).append(
        {
            "caller": _metadata(caller_key),
            "supplier": _metadata(identity(adapter)),
            "original_supplier": _metadata(supplier_key),
            "amount_per_calling_dataset": coefficient,
            "amount_per_service_unit": coefficient
            / audit["caller_net_reference_production"],
        }
    )
    return result, audit


def lift_scoped_biosphere(database, audit, selections):
    """Move reviewed direct biosphere quantities to separately timed adapters.

    Each selection provides a scoped ``source`` identity and a two-part ``flow``
    input key. Only nodes and activity scales from a preceding audited rewrite
    are accepted. This supports lifetime land occupation at the service date
    without moving installation land transformation or inventing land recovery.
    No timing classification is inferred here. Original inputs are not mutated.
    """
    lookup = {identity(d): d for d in database}
    if len(lookup) != len(database):
        raise ValueError("Ambiguous biosphere lifting inventory")
    nodes = {identity(row["scoped"]): row for row in audit["scoped_nodes"]}
    caller_key = identity(audit["caller"])
    if caller_key not in lookup:
        raise ValueError("Scoped biosphere caller is absent")
    selected, seen = [], set()
    for selection in selections:
        source = identity(selection["source"])
        flow = tuple(selection["flow"])
        if len(flow) != 2 or any(not isinstance(v, str) or not v for v in flow):
            raise ValueError("Biosphere flow requires a database/code key")
        if source not in nodes or source not in lookup or (source, flow) in seen:
            raise ValueError(
                "Biosphere selection requires a distinct scoped source/flow"
            )
        if lookup[source].get("stock_vintage_context") != audit["context_id"]:
            raise ValueError("Biosphere source is outside the audited context")
        matches = [
            e
            for e in lookup[source]["exchanges"]
            if e["type"] == "biosphere" and tuple(e["input"]) == flow
        ]
        if len(matches) != 1:
            raise ValueError("Biosphere selection needs one exact exchange")
        selected.append((source, flow, matches[0]))
        seen.add((source, flow))
    changes = {
        k: deepcopy(lookup[k]) for k in {caller_key} | {s for s, _, _ in selected}
    }
    caller = changes[caller_key]
    updated_audit = deepcopy(audit)
    lifted = updated_audit.setdefault("lifted_biosphere", [])
    adapters = []
    for source, flow, original in selected:
        node = nodes[source]
        amount = node["activity_scale_per_calling_dataset"] * _amount(original)
        token = hashlib.sha256(
            json.dumps([audit["context_id"], source, flow]).encode()
        ).hexdigest()[:24]
        adapter = {
            "name": f"{original['name']} [stock context {audit['context_id']}: biosphere-{token}]",
            "reference product": original["name"],
            "unit": original["unit"],
            "location": caller["location"],
            "database": caller["database"],
            "code": f"stock-{token}",
            "stock_vintage_context": audit["context_id"],
            "stock_vintage_role": "biosphere-adapter",
            "exchanges": [],
        }
        production = {"type": "production", "amount": 1.0, "uncertainty type": 0}
        _relink(production, adapter, adapter)
        biosphere = {
            k: deepcopy(original[k]) for k in ("name", "unit", "categories", "input")
        }
        biosphere.update(
            type="biosphere",
            amount=1.0,
            output=(adapter["database"], adapter["code"]),
            **{"uncertainty type": 0},
        )
        adapter["exchanges"] = [production, biosphere]
        exchange = {"type": "technosphere", "amount": amount, "uncertainty type": 0}
        _relink(exchange, adapter, caller)
        caller["exchanges"].append(exchange)
        changes[source]["exchanges"] = [
            e
            for e in changes[source]["exchanges"]
            if not (e["type"] == "biosphere" and tuple(e["input"]) == flow)
        ]
        adapters.append(adapter)
        lifted.append(
            {
                "original_source": node["original"],
                "scoped_source": node["scoped"],
                "flow_input": flow,
                "original_amount": _amount(original),
                "coefficient_per_capital_unit": node["activity_scale_per_capital_unit"]
                * _amount(original),
                "amount_per_calling_dataset": amount,
                "amount_per_service_unit": amount / _net_caller_production(caller),
                "caller": _metadata(identity(caller)),
                "supplier": _metadata(identity(adapter)),
            }
        )
    keys = [identity(d) for d in adapters]
    if len(set(keys)) != len(keys) or set(keys) & lookup.keys():
        raise ValueError("Biosphere lifting creates an existing adapter identity")
    return [changes.get(identity(d), d) for d in database] + adapters, updated_audit


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
        values = {
            (
                "technosphere",
                identity(row["original_caller"]),
                identity(row["original_supplier"]),
            ): row["coefficient_per_capital_unit"]
            for row in audit["lifted_exchanges"]
        }
        values.update(
            {
                (
                    "biosphere",
                    identity(row["original_source"]),
                    tuple(row["flow_input"]),
                ): row["coefficient_per_capital_unit"]
                for row in audit.get("lifted_biosphere", [])
            }
        )
        return values

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
