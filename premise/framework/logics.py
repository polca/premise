from premise.framework import BasicOperation


class contains(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key].str.contains(val, regex=False, case=False)
        self._repr = f"<filter df[{key}].str.contains({val}, regex=False)>"


class does_not_contain(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: ~(df[key].str.contains(val, regex=False, case=False))
        self._repr = f"<filter ~(df[{key}].str.contains({val}, regex=False))>"


class equals(BasicOperation):
    def __init__(self, key, val):
        self._selector = lambda df: df[key] == val
        self._repr = f"<filter df[{key}] == {val}>"


class contains_any_from_list(BasicOperation):
    def __init__(self, key, list_of_val):
        self.key = key
        self.val = list_of_val
        self._selector = lambda df: df[key].isin(list_of_val)
        self._repr = f"<filter df[{key}].isin({list_of_val})>"

    def __invert__(self):
        return lambda df: ~df[self.key].isin(self.val)
