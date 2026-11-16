"""Protect scalar conversion, mutation and dictionary-key policies."""

import numpy as np
import pytest

from premise.validation.validation import convert_numpy_generics_to_float


@pytest.mark.parametrize("in_place", [False, True])
@pytest.mark.parametrize("convert_keys", [False, True])
def test_nested_scalars_convert_but_arrays_and_python_booleans_do_not(
    in_place, convert_keys
):
    array = np.array([1.0, 2.0])
    original = {
        "values": [np.int64(2), (np.float32(3.5), np.bool_(True))],
        "array": array,
    }
    records = [original, True]

    result = convert_numpy_generics_to_float(
        records, in_place=in_place, convert_keys=convert_keys
    )

    assert (result is records) is in_place
    assert (result[0] is original) is in_place
    assert result[0]["values"] == [2.0, (3.5, 1.0)]
    assert type(result[0]["values"][0]) is float
    assert type(result[0]["values"][1][1]) is float
    assert result[0]["array"] is array
    assert result[1] is True
    if not in_place:
        assert isinstance(original["values"][0], np.integer)


@pytest.mark.parametrize("in_place", [False, True])
def test_plain_float_keys_are_rewritten_only_when_copying(in_place):
    result = convert_numpy_generics_to_float(
        [{2.0: np.int64(1)}], in_place=in_place, convert_keys=True
    )

    assert list(result[0]) == ([2.0] if in_place else ["2.0"])
    assert list(result[0].values()) == [1.0]


@pytest.mark.parametrize("in_place", [False, True])
def test_numpy_key_triggers_float_key_conversion(in_place):
    result = convert_numpy_generics_to_float(
        [{2.0: np.int64(1), np.int64(3): np.bool_(False)}],
        in_place=in_place,
        convert_keys=True,
    )

    assert list(result[0]) == ["2.0", "3.0"]
    assert list(result[0].values()) == [1.0, 0.0]


@pytest.mark.parametrize("in_place", [False, True])
def test_mutable_aliases_are_retained_only_for_in_place_conversion(in_place):
    shared = {"value": np.int64(2)}
    result = convert_numpy_generics_to_float([shared, shared], in_place=in_place)

    assert (result[0] is result[1]) is in_place
    assert result == [{"value": 2.0}, {"value": 2.0}]


class ReversedIterationDict(dict):
    def __iter__(self):
        return reversed(list(self.keys()))


@pytest.mark.parametrize("dictionary_type", [dict, ReversedIterationDict])
def test_failed_in_place_conversion_keeps_prior_updates(dictionary_type):
    records = [dictionary_type(converted=np.int64(2), invalid=np.str_("invalid"))]

    with pytest.raises(ValueError):
        convert_numpy_generics_to_float(records, in_place=True)

    assert type(records[0]["converted"]) is float
    assert isinstance(records[0]["invalid"], np.str_)
