from abc import ABC
from typing import TYPE_CHECKING, Iterable, Optional, Union

from foundry.core.api import PalantirSession
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

if TYPE_CHECKING:
    from foundry.types._set_type import SetType
    from foundry.types._short_type import ShortType
    from foundry.types._string_type import StringType
    from foundry.types._struct_field import StructField
    from foundry.types._struct_type import StructType
    from foundry.types._timestamp_type import TimestampType


class BaseType(ABC):
    """A union of all the primitive types used universally across Palantir
    products and platforms.

    These are the standard data types supported by most systems.
    """

    @staticmethod
    def any_type(session: Optional[PalantirSession] = None) -> AnyType:
        return AnyType(session=session)

    @staticmethod
    def array_type(
        item_type: Union[
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
    ) -> ArrayType:
        return ArrayType(item_type=item_type, session=session)

    @staticmethod
    def binary_type(session: Optional[PalantirSession] = None) -> BinaryType:
        return BinaryType(session=session)

    @staticmethod
    def boolean_type(session: Optional[PalantirSession] = None) -> BooleanType:
        return BooleanType(session=session)

    @staticmethod
    def byte_type(session: Optional[PalantirSession] = None) -> ByteType:
        return ByteType(session=session)

    @staticmethod
    def date_type(session: Optional[PalantirSession] = None) -> DateType:
        return DateType(session=session)

    @staticmethod
    def decimal_type(
        precision: Optional[int] = None, scale: Optional[int] = None, session: Optional[PalantirSession] = None
    ) -> DecimalType:
        return DecimalType(precision=precision, scale=scale, session=session)

    @staticmethod
    def double_type(session: Optional[PalantirSession] = None) -> DoubleType:
        return DoubleType(session=session)

    @staticmethod
    def float_type(session: Optional[PalantirSession] = None) -> FloatType:
        return FloatType(session=session)

    @staticmethod
    def integer_type(session: Optional[PalantirSession] = None) -> IntegerType:
        return IntegerType(session=session)

    @staticmethod
    def long_type(session: Optional[PalantirSession] = None) -> LongType:
        return LongType(session=session)

    @staticmethod
    def map_type(
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
    ) -> MapType:
        return MapType(key_type=key_type, value_type=value_type, session=session)

    @staticmethod
    def set_type(
        item_type: Union[
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
    ) -> SetType:
        return SetType(item_type=item_type, session=session)

    @staticmethod
    def short_type(session: Optional[PalantirSession] = None) -> ShortType:
        return ShortType(session=session)

    @staticmethod
    def string_type(session: Optional[PalantirSession] = None) -> StringType:
        return StringType(session=session)

    @staticmethod
    def struct_type(fields: Iterable["StructField"], session: Optional[PalantirSession] = None) -> StructType:
        return StructType(fields=fields, session=session)

    @staticmethod
    def timestamp_type(session: Optional[PalantirSession] = None) -> TimestampType:
        return TimestampType(session=session)
