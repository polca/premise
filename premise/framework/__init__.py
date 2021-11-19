

class LogOps:
    """
    This class serves as an encapsulation for logical operations based 
    on selectors (arrays of True False values) which are under a
    delayed execution.
    """

    def __init__(self, a, b, kind):
        if kind == "and":
            self._expr = lambda df: a(df) * b(df)
            self._repr = f"{a} and {b}"
        elif kind == "or":
            self._expr = lambda df: a(df) + b(df)
            self._repr = f"{a} or {b}"
        else:
            raise ArithmeticError("no bool operator of or or and")

    def __call__(self, df):
        return self._expr(df)

    def __and__(self, other):
        return self.__class__(self, other, "and")

    def __or__(self, other):
        return self.__class__(self, other, "or")

    def __repr__(self):
        return self._repr

    def __bool__(self):
        raise ArithmeticError("do use | or &. 'and' and 'or' are not supported!")


class BasicOperation:
    """

    """

    def __call__(self, df):
        return self._selector(df)

    def __or__(self, other):
        return LogOps(self, other, "or")

    def __and__(self, other):
        return LogOps(self, other, "and")

    def __ior__(self, other):
        return LogOps(self, other, "or")

    def __iand__(self, other):
        return LogOps(self, other, "and")

    def __repr__(self):
        return str(self._repr)

    def __bool__(self):
        raise ArithmeticError("do use | or &. and and or are not supported!")
