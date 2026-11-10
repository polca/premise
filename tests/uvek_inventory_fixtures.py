"""Synthetic linked native records shared by inventory regression tests."""


def record(identity, code):
    a = {**identity, "database": "native", "code": code}
    a["exchanges"] = [edge(a, 1, "production")]
    return a


def edge(a, amount, kind="technosphere"):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": kind,
        "amount": amount,
        "input": (a["database"], a["code"]),
    }
