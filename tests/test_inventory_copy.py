import copy
import gc
import weakref
from collections import OrderedDict

import numpy as np

from premise.inventory_copy import clone_inventory_dataset


def test_clone_preserves_cycles_aliases_and_custom_types():
    class Key(str):
        pass

    shared = {"values": [1, 2]}
    loop = []
    pair = (loop,)
    loop.append(pair)
    original = {
        Key("first"): shared,
        "second": shared,
        "ordered": OrderedDict([(2, shared), (1, loop)]),
        "cycle": pair,
    }
    cloned = clone_inventory_dataset(original)
    assert list(cloned) == list(original)
    assert type(next(iter(cloned))) is Key
    assert type(cloned["ordered"]) is OrderedDict
    assert cloned["first"] is cloned["second"] is cloned["ordered"][2]
    assert cloned["first"] is not shared
    assert cloned["cycle"][0][0] is cloned["cycle"]
    assert cloned["ordered"][1] is cloned["cycle"][0]
    cloned["first"]["values"].append(3)
    assert shared["values"] == [1, 2]


def test_clone_isolates_numpy_object_metadata_and_structured_scalars():
    shared = {"values": [1]}
    array = np.empty(1, dtype=object)
    array[0] = shared
    structured = np.array([(shared,)], dtype=[("item", object)])[0]
    original = {"array": array, "structured": structured, "shared": shared}
    expected = copy.deepcopy(original)
    cloned = clone_inventory_dataset(original)
    assert cloned["array"][0] is cloned["shared"]
    assert cloned["structured"]["item"] is cloned["shared"]
    assert cloned["shared"] == expected["shared"]
    cloned["shared"]["values"].append(2)
    assert shared["values"] == [1]


def test_clone_honors_custom_deepcopy_and_an_external_memo():
    class Metadata:
        def __deepcopy__(self, memo):
            result = type(self)()
            memo[id(self)] = result
            result.values = copy.deepcopy(self.values, memo)
            return result

    metadata = Metadata()
    metadata.values = [1]
    memo = {}
    first = clone_inventory_dataset({"metadata": metadata}, memo)
    second = clone_inventory_dataset([metadata, metadata.values], memo)
    assert first["metadata"] is second[0]
    assert first["metadata"].values is second[1]
    assert second[1] is not metadata.values


def test_numpy_scalar_subclass_keeps_its_copy_protocol():
    class Scalar(np.float64):
        def __deepcopy__(self, memo):
            result = type(self)(self)
            memo[id(self)] = result
            result.metadata = copy.deepcopy(self.metadata, memo)
            return result

    value = Scalar(1.0)
    value.metadata = [1]
    cloned = clone_inventory_dataset({"value": value})["value"]
    assert type(cloned) is Scalar
    assert cloned == value
    assert cloned.metadata == value.metadata
    assert cloned.metadata is not value.metadata


def test_copy_does_not_retain_source_until_cyclic_garbage_collection():
    class Metadata:
        pass

    original = Metadata()
    reference = weakref.ref(original)
    enabled = gc.isenabled()
    gc.disable()
    try:
        cloned = clone_inventory_dataset({"metadata": original})
        del original
        assert reference() is None
        assert isinstance(cloned["metadata"], Metadata)
    finally:
        if enabled:
            gc.enable()
