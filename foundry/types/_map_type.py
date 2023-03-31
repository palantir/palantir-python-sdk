from typing import TYPE_CHECKING, Any, Optional, Union

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping

    from foundry.types._any_type import AnyType
    from foundry.types._array_type import ArrayType
    from foundry.types._binary_type import BinaryType
    from foundry.types._boolean_type import BooleanType
    from foundry.types._byte_type import ByteType
    from foundry.types._date_type import DateType
    from foundry.types._decimal_type import DecimalType
    from foundry.types._double_type import DoubleType
    from foundry.types._float_type import FloatType
    from foundry.types._integer_type import IntegerType
    from foundry.types._long_type import LongType
    from foundry.types._set_type import SetType
    from foundry.types._short_type import ShortType
    from foundry.types._string_type import StringType
    from foundry.types._struct_type import StructType
    from foundry.types._timestamp_type import TimestampType


class MapType:
    def __init__(
        self,
        key_type: Union[
            "AnyType",
            "ArrayType",
            "BinaryType",
            "BooleanType",
            "ByteType",
            "DateType",
            "DecimalType",
            "DoubleType",
            "FloatType",
            "IntegerType",
            "LongType",
            "MapType",
            "SetType",
            "ShortType",
            "StringType",
            "StructType",
            "TimestampType",
        ],
        value_type: Union[
            "AnyType",
            "ArrayType",
            "BinaryType",
            "BooleanType",
            "ByteType",
            "DateType",
            "DecimalType",
            "DoubleType",
            "FloatType",
            "IntegerType",
            "LongType",
            "MapType",
            "SetType",
            "ShortType",
            "StringType",
            "StructType",
            "TimestampType",
        ],
        session: Optional[PalantirSession] = None,
    ) -> None:
        self._session = session
        self._key_type = key_type
        self._value_type = value_type

    @property
    def key_type(
        self,
    ) -> Union[
        "AnyType",
        "ArrayType",
        "BinaryType",
        "BooleanType",
        "ByteType",
        "DateType",
        "DecimalType",
        "DoubleType",
        "FloatType",
        "IntegerType",
        "LongType",
        "MapType",
        "SetType",
        "ShortType",
        "StringType",
        "StructType",
        "TimestampType",
    ]:
        return self._key_type

    @property
    def value_type(
        self,
    ) -> Union[
        "AnyType",
        "ArrayType",
        "BinaryType",
        "BooleanType",
        "ByteType",
        "DateType",
        "DecimalType",
        "DoubleType",
        "FloatType",
        "IntegerType",
        "LongType",
        "MapType",
        "SetType",
        "ShortType",
        "StringType",
        "StructType",
        "TimestampType",
    ]:
        return self._value_type

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other) and self._key_type == other._key_type and (self._value_type == other._value_type)
        )

    def __repr__(self) -> str:
        return f"MapType(key_type={self.key_type}, value_type={self.value_type})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"keyType": self._key_type._asdict(), "valueType": self._value_type._asdict()}
