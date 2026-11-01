"""Build physical matrices without changing inventories or creating suppliers."""

from __future__ import annotations

from collections import defaultdict
import math

import numpy as np
from scipy.sparse import coo_matrix

from .models import AuditFinding
from ..validation.validation_framework import ValidationIssue


def categories(value):
    if isinstance(value, str):
        value = tuple(value.split("::"))
    result = tuple(value or ()) if isinstance(value, (tuple, list)) else ()
    if not all(isinstance(part, str) and part for part in result):
        return ()
    return result[:1] if len(result) == 2 and result[1] == "unspecified" else result


def unit(value):
    if not isinstance(value, str):
        return ""
    return {
        "kg": "kilogram",
        "m3": "cubic meter",
        "m2": "square meter",
        "m2a": "square meter-year",
        "m2*year": "square meter-year",
        "kWh": "kilowatt hour",
        "MJ": "megajoule",
    }.get(value, value)


def identity(node):
    def text(value):
        return value if isinstance(value, str) else ""

    return (
        text(node.get("name", "")),
        text(node.get("reference product") or node.get("product", "")),
        text(node.get("location", "")),
        unit(node.get("unit", "")),
    )


def flow_identity(node):
    return (
        node.get("name") if isinstance(node.get("name"), str) else "",
        categories(node.get("categories")),
        unit(node.get("unit", "")),
    )


class AuditGraph:
    def __init__(self, inventory, biosphere=None):
        if hasattr(inventory, "iter_materialized"):
            inventory = inventory.iter_materialized()
        self.nodes = list(inventory)
        self.keys = tuple(identity(n) for n in self.nodes)
        self.findings = []
        self.blocked = False
        self.links = {}
        self.semantic = defaultdict(list)
        for i, node in enumerate(self.nodes):
            self.semantic[self.keys[i]].append(i)
            if node.get("database") is not None and node.get("code") is not None:
                link = (node["database"], node["code"])
                if link in self.links:
                    self.problem(
                        "GRAPH.DUPLICATE_IDENTITY",
                        i,
                        "Duplicate database/code identity",
                        actual=link,
                        fatal=True,
                    )
                self.links[link] = i
            if not all(self.keys[i]):
                self.problem(
                    "GRAPH.ACTIVITY_FIELDS",
                    i,
                    "Missing activity identity or unit",
                    fatal=True,
                )
        # Duplicate semantic identities can be resolved by explicit links, but
        # cannot form an unambiguous reproducible activity sample.
        self.ambiguous = {k for k, values in self.semantic.items() if len(values) > 1}
        for key in sorted(self.ambiguous):
            self.problem(
                "GRAPH.AMBIGUOUS_IDENTITY",
                self.semantic[key][0],
                "Semantic identity is ambiguous; excluded from sampling",
                actual=list(key),
            )
        self.biosphere_catalog = biosphere
        self.flow_catalog = {}
        if biosphere is not None:
            for key, value in biosphere.items():
                self.flow_catalog[tuple(key)] = value
        self.production = np.ones(len(self.nodes))
        self.inputs = [[] for _ in self.nodes]
        self.raw_biosphere = [[] for _ in self.nodes]
        flow_indices, flows = {}, []
        ar, ac, av, br, bc, bv = [], [], [], [], [], []
        for i, node in enumerate(self.nodes):
            production = [
                e for e in node.get("exchanges", ()) if e.get("type") == "production"
            ]
            if len(production) != 1:
                self.problem(
                    "GRAPH.PRODUCTION_REFERENCE",
                    i,
                    "Expected exactly one reference production exchange",
                    actual=len(production),
                    fatal=True,
                )
                continue
            output = self.number(production[0].get("amount"), i)
            if output is None or output == 0:
                self.problem(
                    "GRAPH.PRODUCTION_REFERENCE",
                    i,
                    "Reference production must be finite and nonzero",
                    fatal=True,
                )
                continue
            product = production[0].get(
                "reference product", production[0].get("product", self.keys[i][1])
            )
            if (
                product != self.keys[i][1]
                or unit(production[0].get("unit", self.keys[i][3])) != self.keys[i][3]
            ):
                self.problem(
                    "GRAPH.PRODUCTION_REFERENCE",
                    i,
                    "Production product/unit disagrees with reference product",
                    fatal=True,
                )
            self.production[i] = output
            ar.append(i)
            ac.append(i)
            av.append(output)
            for exc in node.get("exchanges", ()):
                kind = exc.get("type")
                if kind == "production":
                    continue
                amount = self.number(exc.get("amount"), i)
                if amount is None:
                    continue
                if kind in {"technosphere", "substitution"}:
                    if kind == "substitution":
                        amount = -amount
                    provider = self.resolve(exc, i)
                    if provider is not None:
                        ar.append(provider)
                        ac.append(i)
                        av.append(-amount)
                        self.inputs[i].append((provider, amount, exc))
                elif kind == "biosphere":
                    description = exc
                    link = exc.get("input")
                    if biosphere is not None:
                        target = self.flow_catalog.get(tuple(link)) if link else None
                        if target is None:
                            self.problem(
                                "GRAPH.BIOSPHERE_LINK",
                                i,
                                "Biosphere link is absent from the supplied catalog",
                                actual=link,
                            )
                        else:
                            description = target
                            if unit(exc.get("unit")) != unit(target.get("unit")):
                                self.problem(
                                    "GRAPH.BIOSPHERE_UNIT",
                                    i,
                                    "Exchange and linked biosphere units disagree",
                                    actual=exc.get("unit"),
                                    expected=target.get("unit"),
                                )
                            old_name = flow_identity(exc)[0].removesuffix(", in ground")
                            new_name = flow_identity(target)[0].removesuffix(
                                ", in ground"
                            )
                            if old_name != new_name or categories(
                                exc.get("categories")
                            ) != categories(target.get("categories")):
                                self.problem(
                                    "GRAPH.BIOSPHERE_IDENTITY",
                                    i,
                                    "Exchange metadata disagrees with linked elementary flow",
                                    actual=flow_identity(exc),
                                    expected=flow_identity(target),
                                )
                    fk = flow_identity(description)
                    if not fk[0] or not fk[1] or not fk[2]:
                        self.problem(
                            "GRAPH.BIOSPHERE_IDENTITY",
                            i,
                            "Incomplete elementary flow identity",
                            actual=fk,
                        )
                    if fk not in flow_indices:
                        flow_indices[fk] = len(flows)
                        flows.append(fk)
                    r = flow_indices[fk]
                    br.append(r)
                    bc.append(i)
                    bv.append(amount)
                    self.raw_biosphere[i].append((r, amount, exc))
                else:
                    self.problem(
                        "GRAPH.EXCHANGE_TYPE",
                        i,
                        "Unsupported exchange type",
                        actual=kind,
                        fatal=True,
                    )
        n = len(self.nodes)
        self.A = coo_matrix((av, (ar, ac)), shape=(n, n), dtype=float).tocsc()
        self.A.sum_duplicates()
        self.A.sort_indices()
        self.A_magnitude = coo_matrix(
            ([abs(v) for v in av], (ar, ac)), shape=(n, n), dtype=float
        ).tocsr()
        self.B = coo_matrix((bv, (br, bc)), shape=(len(flows), n), dtype=float).tocsr()
        self.B.sum_duplicates()
        self.B.eliminate_zeros()
        self.B_positive = coo_matrix(
            ([max(v, 0) for v in bv], (br, bc)), shape=self.B.shape
        ).tocsr()
        self.B_negative = coo_matrix(
            ([min(v, 0) for v in bv], (br, bc)), shape=self.B.shape
        ).tocsr()
        self.flows = tuple(flows)

    def number(self, value, i):
        try:
            if isinstance(value, (bool, np.bool_)):
                raise ValueError
            result = float(value)
            if not math.isfinite(result):
                raise ValueError
            return result
        except (ValueError, TypeError):
            self.problem(
                "GRAPH.FINITE_NUMERIC",
                i,
                "Nonfinite or invalid exchange amount",
                actual=str(value),
                fatal=True,
            )
            return None

    def problem(self, rule, i, message, actual=None, expected=None, fatal=False):
        self.blocked |= fatal
        key = self.keys[i] if i < len(self.keys) else None
        issue = ValidationIssue(
            rule,
            "error",
            message,
            activity_id=i,
            activity_key=key[:3] if key else None,
            actual=actual,
            expected=expected,
        )
        self.findings.append(AuditFinding(issue, "structure", "high", key))

    def resolve(self, exc, consumer):
        link = exc.get("input")
        semantic = identity(exc)
        provider = self.links.get(tuple(link)) if link else None
        if provider is None:
            matches = self.semantic.get(semantic, ())
            if len(matches) != 1:
                self.problem(
                    "GRAPH.PROVIDER_EXISTS",
                    consumer,
                    "Missing or ambiguous technosphere provider",
                    actual=semantic,
                    fatal=True,
                )
                return None
            provider = matches[0]
            if link:
                self.problem(
                    "GRAPH.STALE_SUPPLIER",
                    consumer,
                    "Explicit supplier link is stale; semantic match used for diagnosis",
                    actual=link,
                    expected=self.keys[provider],
                )
        if semantic != self.keys[provider]:
            self.problem(
                "GRAPH.PROVIDER_IDENTITY",
                consumer,
                "Exchange identity/unit disagrees with linked provider",
                actual=semantic,
                expected=self.keys[provider],
            )
        return provider


class Factorization:
    """One numerical LU, reused for forward and transpose solves."""

    def __init__(self, matrix):
        self.A = matrix
        self.forward = self.transpose = 0
        try:
            from scikits import umfpack
        except ImportError:
            from scipy.sparse.linalg import splu

            self.factor = splu(matrix)
            self.backend = "scipy.splu"
            factor = self.factor
            self._solve = lambda rhs, transpose: factor.solve(
                rhs, "T" if transpose else "N"
            )
        else:
            self.factor = umfpack.UmfpackContext(
                "dl" if matrix.indices.dtype == np.int64 else "di"
            )
            self.factor.numeric(matrix)
            self.backend = "scikits.umfpack"
            factor = self.factor
            self._solve = lambda rhs, transpose: factor.solve(
                umfpack.UMFPACK_At if transpose else umfpack.UMFPACK_A, matrix, rhs
            )

    def solve(self, rhs, transpose=False):
        if transpose:
            self.transpose += 1
        else:
            self.forward += 1
        return self._solve(rhs, transpose)
