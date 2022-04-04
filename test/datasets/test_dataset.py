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

import io

import pandas as pd
import pyarrow as pa
import pytest
from expects import expect, equal
from mockito import mock, when, verify

from palantir.core.types import ResourceIdentifier
from palantir.datasets.client import DatasetsClient
from palantir.datasets.core import Dataset, File, Transaction
from palantir.datasets.types import (
    DatasetLocator,
    TransactionType,
    FoundrySchema,
    TransactionStatus,
    FileLocator,
    Field,
    StringFieldType,
    LongFieldType,
)


class TestDataset:
    @pytest.fixture(autouse=True)
    def before(self):
        self.client = mock(DatasetsClient)
        self.locator = DatasetLocator(
            rid=ResourceIdentifier.from_string("ri.foundry.test.dataset.0"),
            branch_id="master",
            start_transaction_rid=ResourceIdentifier.from_string(
                "ri.foundry.test.transaction.0"
            ),
            end_transaction_rid=ResourceIdentifier.from_string(
                "ri.foundry.test.transaction.1"
            ),
        )
        self.dataset = Dataset(self.client, self.locator)

    def test_rid(self):
        expect(self.dataset.rid).to(equal(self.locator.rid))

    def test_branch(self):
        expect(self.dataset.branch).to(equal(self.locator.branch_id))

    def test_view(self):
        expected = (
            self.locator.start_transaction_rid,
            self.locator.end_transaction_rid,
        )
        expect(self.dataset.view).to(equal(expected))

    def test_list_files(self):
        file1 = File(
            dataset=self.dataset,
            path="file1",
            client=self.client,
        )
        file2 = File(
            dataset=self.dataset,
            path="file2",
            client=self.client,
        )

        def gen():
            yield file1
            yield file2

        when(self.client).list_files(dataset=self.dataset, path=None).thenReturn(gen())

        expect(list(self.dataset.list_files())).to(equal([file1, file2]))

    def test_list_files_with_path(self):
        file1 = File(
            dataset=self.dataset,
            path="path/file1",
            client=self.client,
        )
        file2 = File(
            dataset=self.dataset,
            path="path/file2",
            client=self.client,
        )

        def gen():
            yield file1
            yield file2

        when(self.client).list_files(dataset=self.dataset, path="path").thenReturn(
            gen()
        )

        expect(list(self.dataset.list_files(path="path"))).to(equal([file1, file2]))

    def test_file(self):
        expected = File(
            dataset=self.dataset,
            path="file_path",
            client=self.client,
        )
        expect(self.dataset.file("file_path")).to(equal(expected))

    def test_read_arrow(self):
        table = mock(pa.Table)
        when(self.client).read_dataset(self.locator).thenReturn(table)

        expect(self.dataset.read_arrow()).to(equal(table))

    def test_read_pandas(self):
        table = mock(pa.Table)
        df = mock(pd.DataFrame)
        when(table).to_pandas().thenReturn(df)
        when(self.client).read_dataset(self.locator).thenReturn(table)

        expect(self.dataset.read_pandas()).to(equal(df))

    def test_start_transaction(self):
        txn = mock(Transaction)
        when(self.client).start_transaction(
            self.dataset, TransactionType.UPDATE
        ).thenReturn(txn)
        expect(self.dataset.start_transaction()).to(equal(txn))

    def test_start_transaction_with_type(self):
        txn = mock(Transaction)
        when(self.client).start_transaction(
            self.dataset, TransactionType.SNAPSHOT
        ).thenReturn(txn)
        expect(self.dataset.start_transaction(TransactionType.SNAPSHOT)).to(equal(txn))

    def test_write_pandas(self):
        df = pd.DataFrame(
            {
                "numbers": [1, 2],
                "words": ["one", "two"],
            }
        )
        content: bytes
        with io.BytesIO() as buf:
            df.to_parquet(buf)
            buf.seek(0)
            content = buf.read()

        txn = Transaction(
            self.dataset,
            rid="ri.foundry.test.transaction.2",
            txn_type=TransactionType.SNAPSHOT,
            status=TransactionStatus.OPEN,
            client=self.client,
        )
        when(self.client).start_transaction(
            self.dataset, TransactionType.SNAPSHOT
        ).thenReturn(txn)
        when(self.client).put_file(
            FileLocator(
                dataset_rid=self.locator.rid,
                end_ref=txn.rid,
                logical_path="dataframe.parquet",
            ),
            content,
        ).thenReturn(None)
        when(self.client).commit_transaction(txn).thenReturn(txn)
        when(self.client).put_schema(
            self.dataset,
            FoundrySchema(
                fields=[
                    Field(name="numbers", field_type=LongFieldType()),
                    Field(name="words", field_type=StringFieldType()),
                ]
            ),
        ).thenReturn(None)

        self.dataset.write_pandas(df)

        verify(self.client).start_transaction(self.dataset, TransactionType.SNAPSHOT)
        verify(self.client).put_file(
            FileLocator(
                dataset_rid=self.locator.rid,
                end_ref=txn.rid,
                logical_path="dataframe.parquet",
            ),
            content,
        )
        verify(self.client).commit_transaction(txn)
        verify(self.client).put_schema(
            self.dataset,
            FoundrySchema(
                fields=[
                    Field(name="numbers", field_type=LongFieldType()),
                    Field(name="words", field_type=StringFieldType()),
                ]
            ),
        )
