from premise.framework import BasicOperation


class contains(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key].str.contains(val, regex=False)
        self._repr = f"<filter df[{key}].str.contains({val}, regex=False)>"


class does_not_contain(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: ~(df[key].str.contains(val, regex=False))
        self._repr = f"<filter ~(df[{key}].str.contains({val}, regex=False))>"


class equals(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key] == val
        self._repr = f"<filter df[{key}] == {val}>"
