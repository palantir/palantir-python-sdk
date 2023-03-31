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
    from foundry.types._map_type import MapType
    from foundry.types._set_type import SetType
    from foundry.types._short_type import ShortType
    from foundry.types._string_type import StringType
    from foundry.types._struct_type import StructType
    from foundry.types._timestamp_type import TimestampType


class ColumnConstraint:
    def __init__(
        self,
        column_type: Union[
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
        name: str,
        nullable: Optional[bool] = None,
        session: Optional[PalantirSession] = None,
    ) -> None:
        self._session = session
        self._column_type = column_type
        self._name = name
        self._nullable = nullable

    @property
    def column_type(
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
        return self._column_type

    @property
    def name(self) -> str:
        return self._name

    @property
    def nullable(self) -> Optional[bool]:
        return self._nullable

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._column_type == other._column_type
            and (self._name == other._name)
            and (self._nullable == other._nullable)
        )

    def __repr__(self) -> str:
        return f"ColumnConstraint(column_type={self.column_type}, name={self.name}, nullable={self.nullable})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"columnType": self._column_type._asdict(), "name": self._name, "nullable": self._nullable}
