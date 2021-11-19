from premise.framework import BasicOperation


class contains(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key] == val
        self._repr = f"<filter df[{key}] == {val}>"


class excludes(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key] != val
        self._repr = f"<filter df[{key}] != {val}>"
