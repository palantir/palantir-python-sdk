#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional, Tuple, Union, Dict

from palantir.core.types import ResourceIdentifier
from palantir.core.util import alias


@dataclass(frozen=True)
class DatasetLocator:
    rid: ResourceIdentifier
    branch_id: str
    start_transaction_rid: Optional[ResourceIdentifier] = None
    end_transaction_rid: Optional[ResourceIdentifier] = None

    def with_updated(
        self,
        *,
        dataset_rid: ResourceIdentifier = None,
        branch_id: str = None,
        start_transaction_rid: ResourceIdentifier = None,
        end_transaction_rid: ResourceIdentifier = None,
    ) -> "DatasetLocator":
        return DatasetLocator(
            dataset_rid or self.rid,
            branch_id or self.branch_id,
            start_transaction_rid or self.start_transaction_rid,
            end_transaction_rid or self.end_transaction_rid,
        )


@dataclass(frozen=True)
class FileLocator:
    dataset_rid: ResourceIdentifier
    end_ref: str
    logical_path: str
    start_transaction_rid: Optional[str] = None

    def with_updated(
        self,
        *,
        dataset_rid: ResourceIdentifier = None,
        end_ref: str = None,
        logical_path: str = None,
        start_transaction_rid: str = None,
    ):
        return FileLocator(
            dataset_rid or self.dataset_rid,
            end_ref or self.end_ref,
            logical_path or self.logical_path,
            start_transaction_rid or self.start_transaction_rid,
        )


class _AutoNameEnum(Enum):
    def _generate_next_value_(
        name, start, count, last_values
    ):  # pylint: disable = no-self-argument
        return name


class TransactionType(_AutoNameEnum):
    UPDATE = auto()
    APPEND = auto()
    DELETE = auto()
    SNAPSHOT = auto()


class TransactionStatus(_AutoNameEnum):
    OPEN = auto()
    COMMITTED = auto()
    ABORTED = auto()


field_types = alias(ignore_case=True)


@dataclass(frozen=True)
class FieldType(ABC):
    """
    The type of a :class:`Field`. When constructing a field, primitive field types (e.g., strings, integers) can be
    specified using string aliases to reduce code verbosity.

    See the table below for supported field types:

    Type      | FieldType                   | Python type           | Aliases
    --------- | --------------------------- | --------------------- | -------
    Array     | :class:`ArrayFieldType`     | list, tuple, or array |
    Boolean   | :class:`BooleanFieldType`   | bool                  | bool, boolean
    Binary    | :class:`BinaryFieldType`    | bytearray             | binary, bytes
    Byte      | :class:`ByteFieldType`      | int or long           | byte, int8
    Date      | :class:`DateFieldType`      | datetime.date         | date
    Decimal   | :class:`DecimalFieldType`   | decimal.Decimal       | decimal
    Double    | :class:`DoubleFieldType`    | float                 | double, float64
    Float     | :class:`FloatFieldType`     | float                 | float, float32
    Integer   | :class:`IntegerFieldType`   | int or long           | integer, int, int32
    Long      | :class:`LongFieldType`      | long                  | long, int64
    Map       | :class:`MapFieldType`       | dict                  |
    Short     | :class:`ShortFieldType`     | int or long           | short, int16
    String    | :class:`StringFieldType`    | string                | string, str
    Struct    | :class:`StructFieldType`    | list or tuple         |
    Timestamp | :class:`TimestampFieldType` | datetime.timestamp    | timestamp, datetime

    Examples
    --------
    >>> (Field("f1", StringFieldType(), True)
    ...      == Field("f1", "str", True))
    """


@dataclass(frozen=True)
class AtomicFieldType(FieldType):
    pass


@dataclass(frozen=True)
class ArrayFieldType(FieldType):
    element_type: FieldType


@field_types.alias("bool", "boolean")
@dataclass(frozen=True)
class BooleanFieldType(AtomicFieldType):
    pass


@field_types.alias("binary", "bytes")
@dataclass(frozen=True)
class BinaryFieldType(AtomicFieldType):
    pass


@field_types.alias("byte", "int8")
@dataclass(frozen=True)
class ByteFieldType(AtomicFieldType):
    pass


@field_types.alias("date")
@dataclass(frozen=True)
class DateFieldType(AtomicFieldType):
    pass


@field_types.alias("decimal")
@dataclass(frozen=True)
class DecimalFieldType(AtomicFieldType):
    precision: int = 10
    scale: int = 0


@field_types.alias("double", "float64")
@dataclass(frozen=True)
class DoubleFieldType(AtomicFieldType):
    pass


@field_types.alias("float", "float32")
@dataclass(frozen=True)
class FloatFieldType(AtomicFieldType):
    pass


@field_types.alias("int", "integer", "int32")
@dataclass(frozen=True)
class IntegerFieldType(AtomicFieldType):
    pass


@field_types.alias("long", "int64")
@dataclass(frozen=True)
class LongFieldType(AtomicFieldType):
    pass


@dataclass(frozen=True)
class MapFieldType(FieldType):
    key_type: FieldType
    value_type: FieldType


@field_types.alias("short", "int16")
@dataclass(frozen=True)
class ShortFieldType(AtomicFieldType):
    pass


@field_types.alias("str", "string")
@dataclass(frozen=True)
class StringFieldType(AtomicFieldType):
    pass


@dataclass(frozen=True)
class StructFieldType(FieldType):
    fields: List[FieldType]


@field_types.alias("datetime", "timestamp")
@dataclass(frozen=True)
class TimestampFieldType(AtomicFieldType):
    pass


class Field:
    """
    A field in a :class:`FoundrySchema`.

    Parameters
    ----------
    name : str
        The name of the field.
    type : :class:`FieldType`
        The :class:`FieldType` of the field. Can be provided as a string alias.
    nullable : bool, optional
        A flag indicating whether the field can be null (``None``) or not. Defaults to ``True``.
    metadata : dict, optional
        A dict from string to simple type containing additional metadata to store on the field.

    Examples
    --------
    >>> (Field("f1", StringFieldType(), True)
    ...      == Field("f1", "str", True))
    True
    >>> (Field("f1", StringFieldType(), True)
    ...      == Field("f2", "str", True))
    False
    """

    def __init__(
        self,
        name: Optional[str],
        field_type: Union[FieldType, str],
        nullable: bool = True,
        metadata: Dict[str, Any] = None,
    ):
        self.name = name
        self.type = (
            field_type
            if isinstance(field_type, FieldType)
            else field_types[field_type]()
        )
        self.nullable = nullable
        self.metadata = metadata

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, Field)
            and other.name == self.name
            and other.type == self.type
            and other.nullable == self.nullable
            and other.metadata == self.metadata
        )

    def __repr__(self) -> str:
        return f"Field(name='{self.name}', field_type={self.type}, nullable={self.nullable}, metadata={self.metadata})"


class FileFormat(_AutoNameEnum):
    AVRO = auto()
    CSV = auto()
    PARQUET = auto()
    SOHO = auto()


class FoundrySchema:
    """
    The schema of a Foundry dataset.

    Parameters
    ----------
    fields : list[Field]
        The fields in the schema. Fields may be provided as a ``tuple`` to reduce code verbosity.
    format : :class:`FileFormat`
        The dataset format. Format may be provided as the string name. Defaults to ``FileFormat.PARQUET``.
    metadata : dict, optional
        A dict from string to simple type containing additional metadata to store on the schema.

    Examples
    --------
    >>> (
    ...     FoundrySchema(
    ...         [Field("f1", StringFieldType(), True), Field("f2", IntegerFieldType(), False)],
    ...         FileFormat.PARQUET,
    ...         {"foo": "bar"},
    ...     )
    ...     == FoundrySchema([("f1", "str"), ("f2", "int", False)], metadata={"foo": "bar"})
    ... )
    True
    >>> (
    ...    FoundrySchema(
    ...        [Field("f1", StringFieldType(), True), Field("f2", IntegerFieldType(), False)],
    ...        FileFormat.PARQUET, {"foo": "bar"}
    ...    )
    ...    == FoundrySchema([("f1", "str"), ("f2", "int")], "avro")
    ... )
    False
    """

    def __init__(
        self,
        fields: List[
            Union[
                Field,
                Tuple[
                    Optional[str],
                    Union[FieldType, str],
                    Optional[bool],
                    Optional[Dict[str, Any]],
                ],
            ]
        ],
        file_format: Union[FileFormat, str] = FileFormat.PARQUET,
        metadata: Dict[str, Any] = None,
    ):
        self.fields: List[Field] = [
            field if isinstance(field, Field)
            # type suppression: mypy does not handle arg expansion well
            else Field(*field)  # type: ignore
            for field in fields
        ]
        self.format: FileFormat = (
            file_format
            if isinstance(file_format, FileFormat)
            else FileFormat[file_format.upper()]
        )
        self.metadata = metadata

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, FoundrySchema)
            and other.fields == self.fields
            and other.format == self.format
            and self.metadata == self.metadata
        )

    def __repr__(self) -> str:
        return f"FoundrySchema(file_format={repr(self.format)}, fields={[repr(field) for field in self.fields]})"
