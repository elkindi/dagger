from enum import Enum, auto
from datetime import date, time, datetime


class ArrayType(Enum):
    EMPTY = auto()
    INT = auto()
    FLOAT = auto()
    BOOL = auto()
    STR = auto()
    DATE = auto()
    TIME = auto()
    DATETIME = auto()
    COMPOUND = auto()

    @classmethod
    def from_type(cls, _type):
        if not isinstance(_type, type):
            _type = type(_type)
        if _type not in [int, float, str, bool, date, time, datetime]:
            raise ValueError(
                "There is no array type corresponding to the type " +
                _type.__name__)
        if _type == int:
            return cls.INT
        elif _type == float:
            return cls.FLOAT
        elif _type == str:
            return cls.STR
        elif _type == bool:
            return cls.BOOL
        elif _type == date:
            return cls.DATE
        elif _type == time:
            return cls.TIME
        elif _type == datetime:
            return cls.DATETIME


def get_array_type(arr):

    if type(arr) not in [list, tuple, set, frozenset]:
        raise ValueError(
            "Argument must be one of the following types: list, tuple, set, frozenset"
        )
    if len(arr) == 0:
        return ArrayType.EMPTY
    else:
        for elem in arr:
            if type(elem) not in [int, float, str, bool, date, time, datetime]:
                raise ValueError(
                    "Array-like object must contain only scalar types")
    first = True
    arr_type = None
    for elem in arr:
        if first:
            first = False
            arr_type = ArrayType.from_type(elem)
        else:
            if ArrayType.from_type(elem) is not arr_type:
                return ArrayType.COMPOUND
    return arr_type


# get types of elements in compound array pased as tuple
def get_element_types(arr):
    types = []
    for elem in arr:
        types.append(type(elem).__name__)
    return types