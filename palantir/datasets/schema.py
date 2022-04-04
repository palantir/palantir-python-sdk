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

from datetime import date
from typing import TYPE_CHECKING, Any, Optional, Type

from .types import (
    ArrayFieldType,
    BooleanFieldType,
    ByteFieldType,
    Field,
    FileFormat,
    DateFieldType,
    DoubleFieldType,
    FloatFieldType,
    FoundrySchema,
    IntegerFieldType,
    LongFieldType,
    ShortFieldType,
    StringFieldType,
    TimestampFieldType,
)

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd


def pandas_to_foundry_schema(df: "pd.DataFrame") -> FoundrySchema:
    return FoundrySchema(
        fields=[_get_field(column, df[column]) for column in df.columns],
        file_format=FileFormat.PARQUET,
    )


def _get_field(  # pylint: disable=too-many-return-statements,too-many-branches
    name: Optional[str], obj: Any
) -> Field:
    import pandas as pd
    import numpy as np

    dtype = _get_generic_type(obj)

    if _is_nested_list(obj):
        return Field(
            name=name,
            field_type=ArrayFieldType(
                element_type=_get_field(None, pd.Series(obj[0])).type
            ),
        )
    if pd.api.types.is_bool_dtype(dtype):
        return Field(name, BooleanFieldType())
    if pd.api.types.is_integer_dtype(dtype):
        if dtype == np.int8:
            return Field(name, ByteFieldType())
        if dtype == np.int16:
            return Field(name, ShortFieldType())
        if dtype == np.int32:
            return Field(name, IntegerFieldType())
        if dtype == np.int64:
            return Field(name, LongFieldType())
    if pd.api.types.is_float_dtype(dtype):
        if dtype == np.float32:
            return Field(name, FloatFieldType())
        if dtype == np.float64:
            return Field(name, DoubleFieldType())
    if pd.api.types.is_datetime64_dtype(dtype):
        return Field(name, TimestampFieldType())
    if pd.api.types.is_object_dtype(dtype):
        if isinstance(obj[0], date):
            return Field(name, DateFieldType())
        if pd.api.types.is_string_dtype(dtype):
            return Field(name, StringFieldType())
    raise ValueError(f"Unsupported dtype: {dtype}")


def _get_generic_type(obj_or_dtype: Any) -> Type:
    import pandas as pd
    import numpy as np

    if obj_or_dtype is None:
        raise ValueError("cannot convert None to FoundryFieldSchema")

    if isinstance(obj_or_dtype, (np.dtype, pd.api.extensions.ExtensionDtype)):
        return obj_or_dtype.type
    if isinstance(obj_or_dtype, type):
        if issubclass(obj_or_dtype, pd.api.extensions.ExtensionDtype):
            return obj_or_dtype.type
        return np.dtype(obj_or_dtype).type

    # if we have an array-like
    if hasattr(obj_or_dtype, "dtype"):
        return _get_generic_type(obj_or_dtype.dtype)

    return np.dtype(type(obj_or_dtype)).type


def _is_nested_list(obj):
    import pandas as pd

    return (
        pd.api.types.is_list_like(obj)
        and hasattr(obj, "__len__")
        and len(obj) > 0
        and all(pd.api.types.is_list_like(item) or item is None for item in obj)
    )
