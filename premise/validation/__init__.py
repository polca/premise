"""Public validation API; implementation modules load on demand."""

from importlib import import_module
from .._compat import install_public_module

install_public_module(__name__, __name__ + ".validation")


def __getattr__(name):
    module = import_module(".validation", __name__)
    if name == "__all__":
        value = [key for key in vars(module) if not key.startswith("_")]
        globals()[name] = value
        return value
    try:
        value = getattr(module, name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value
