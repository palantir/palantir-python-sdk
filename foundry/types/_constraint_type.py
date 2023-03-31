from abc import ABC
from typing import TYPE_CHECKING, Optional, Union

from foundry.core.api import PalantirSession
from foundry.types._column_constraint import ColumnConstraint

if TYPE_CHECKING:
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


class ConstraintType(ABC):
    """A union of all the types of constraints on Tabular data."""

    @staticmethod
    def column_constraint(
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
    ) -> ColumnConstraint:
        return ColumnConstraint(column_type=column_type, name=name, nullable=nullable, session=session)
