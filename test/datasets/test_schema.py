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

import numpy as np
import pandas as pd
import pytest
from expects import expect, equal

from palantir.datasets.schema import pandas_to_foundry_schema, _get_field
from palantir.datasets.types import (
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


class TestPandasSchemaConverter:
    @pytest.mark.parametrize(
        "series,field_type",
        [
            (pd.Series([True, False], dtype=bool), BooleanFieldType()),
            (pd.Series([True, False]), BooleanFieldType()),
            (pd.Series([1, 2], dtype=np.int8), ByteFieldType()),
            (pd.Series([1, 2], dtype=np.int16), ShortFieldType()),
            (pd.Series([1, 2], dtype=np.int32), IntegerFieldType()),
            (pd.Series([1, 2], dtype=np.int64), LongFieldType()),
            (pd.Series([1, 2], dtype=int), LongFieldType()),
            (pd.Series([1, 2]), LongFieldType()),
            (
                pd.Series([1, None], dtype=pd.Int8Dtype()),
                ByteFieldType(),
            ),
            (
                pd.Series([1, None], dtype=pd.Int16Dtype()),
                ShortFieldType(),
            ),
            (
                pd.Series([1, None], dtype=pd.Int32Dtype()),
                IntegerFieldType(),
            ),
            (
                pd.Series([1, None], dtype=pd.Int64Dtype()),
                LongFieldType(),
            ),
            (pd.Series([1.1, 2.2], dtype=np.float32), FloatFieldType()),
            (pd.Series([1.1, 2.2], dtype=np.float64), DoubleFieldType()),
            (pd.Series([1.1, 2.2], dtype=float), DoubleFieldType()),
            (pd.Series([1.1, 2.2]), DoubleFieldType()),
            (pd.Series(["one", "two"], dtype=str), StringFieldType()),
            (pd.Series(["one", "two"]), StringFieldType()),
            (pd.Series([pd.Timestamp(1), pd.Timestamp(2)]), TimestampFieldType()),
            (
                pd.Series([date(1970, 1, 1), date(1970, 1, 2)]),
                DateFieldType(),
            ),
        ],
    )
    def test_get_field_schema_simple_types(self, series, field_type):
        expect(_get_field("name", series)).to(
            equal(Field(name="name", field_type=field_type))
        )

    @pytest.mark.parametrize(
        "series,array_subtype",
        [
            (
                pd.Series([pd.Series([True, False], dtype=bool)]),
                BooleanFieldType(),
            ),
            (
                pd.Series([pd.Series([1, 2], dtype=np.int8)]),
                ByteFieldType(),
            ),
            (
                pd.Series([pd.Series([1, 2], dtype=np.int16)]),
                ShortFieldType(),
            ),
            (
                pd.Series([pd.Series([1, 2], dtype=np.int32)]),
                IntegerFieldType(),
            ),
            (
                pd.Series([pd.Series([1, 2], dtype=np.int64)]),
                LongFieldType(),
            ),
            (pd.Series([[1, 2]]), LongFieldType()),
            (
                pd.Series([pd.Series([1.1, 2.2], dtype=np.float32)]),
                FloatFieldType(),
            ),
            (
                pd.Series([pd.Series([1.1, 2.2], dtype=np.float64)]),
                DoubleFieldType(),
            ),
            (pd.Series([[1.1, 2.2]]), DoubleFieldType()),
            (pd.Series([["one", "two"]]), StringFieldType()),
            (
                pd.Series([[pd.Timestamp(1), pd.Timestamp(2)]]),
                TimestampFieldType(),
            ),
            (
                pd.Series([[date(1970, 1, 1), date(1970, 1, 2)]]),
                DateFieldType(),
            ),
        ],
    )
    def test_get_field_schema_list(self, series, array_subtype):
        expect(_get_field("name", series)).to(
            equal(
                Field(
                    name="name",
                    field_type=ArrayFieldType(element_type=array_subtype),
                )
            )
        )

    def test_from_pandas(self):
        df = pd.DataFrame(
            {
                "bools": [True, False],
                "ints": [1, 2],
                "floats": [1.1, 2.2],
                "strings": ["one", "two"],
                "timestamps": [pd.Timestamp(1), pd.Timestamp(2)],
            }
        )

        expected = FoundrySchema(
            fields=[
                Field(
                    name="bools",
                    field_type="bool",
                ),
                Field(name="ints", field_type="int64"),
                Field(
                    name="floats",
                    field_type="float64",
                ),
                Field(
                    name="strings",
                    field_type="str",
                ),
                Field(
                    name="timestamps",
                    field_type="datetime",
                ),
            ],
            file_format=FileFormat.PARQUET,
        )

        expect(pandas_to_foundry_schema(df)).to(equal(expected))
