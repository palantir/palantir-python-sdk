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

from os.path import relpath

import pandas as pd
import pyarrow as pa
import pytest
import urllib3
from dateutil.parser import isoparse
from expects import expect, equal, raise_error
from mockito import mock, verifyZeroInteractions, when, verify

from palantir.core.config import StaticTokenProvider, StaticHostnameProvider, AuthToken
from palantir.core.types import ResourceIdentifier, PalantirContext
from palantir.datasets.client import DatasetsClient, DatasetServices
from palantir.datasets.core import Transaction, File, Dataset
from palantir.datasets.rpc.catalog import (
    Dataset as ConjureDataset,
    Transaction as ConjureTransaction,
    TransactionStatus as ConjureTransactionStatus,
    Branch,
    CatalogService,
    TransactionRange,
    FileResource,
    FileMetadata,
    FileResourcesPage,
    StartTransactionRequest,
    TransactionType as ConjureTransactionType,
    CloseTransactionRequest,
)
from palantir.datasets.rpc.data_proxy import (
    DataProxyConcatenationService,
    DataProxyService,
    StartConcatenationTaskRequest,
    StartConcatenationTaskResponse,
    ConcatenationTaskStatusReport,
    ConcatenationTaskStatus,
    ConcatenationTaskQueued,
    ConcatenationTaskInProgress,
    ConcatenationTaskSuccess,
    ConcatenationTaskFailure,
)
from palantir.datasets.rpc.path import PathService, DecoratedResource
from palantir.datasets.rpc.schema import (
    SchemaService,
    FoundryFieldType,
    FoundryFieldSchema,
    FoundrySchema as ConjureFoundrySchema,
)
from palantir.datasets.rpc.sql import (
    QueryStatus,
    ReadyQueryStatus,
    RunningQueryStatus,
    SerializationProtocol,
    SqlDialect,
    SqlExecuteRequest,
    SqlExecuteResponse,
    SqlGetStatusResponse,
    SqlQuery,
    SqlQueryService,
)
from palantir.datasets.types import (
    DatasetLocator,
    FileLocator,
    TransactionType,
    TransactionStatus,
    FoundrySchema,
    Field,
    FileFormat,
    ArrayFieldType,
    LongFieldType,
)

FILE_LEN = 10
FILE_MODIFIED = isoparse("2020-01-01")
TRANSACTION_RID = "ri.t.t.t.0"


def get_file(path: str) -> FileResource:
    return FileResource(
        is_open=False,
        logical_path=path,
        physical_path="unused",
        time_modified="2020-01-01",
        transaction_rid=TRANSACTION_RID,
        file_metadata=FileMetadata(FILE_LEN),
    )


# pylint: disable=too-many-public-methods
class TestFoundryClient:

    AUTH_HEADER: str = "auth-header"
    DATASET_RID: ResourceIdentifier = ResourceIdentifier.from_string(
        "ri.foundry.main.dataset.0"
    )
    BRANCH_ID: str = "branch-id"
    END_TRANSACTION_RID: ResourceIdentifier = ResourceIdentifier.from_string(
        "ri.foundry.main.transaction.2"
    )
    START_TRANSACTION_RID: ResourceIdentifier = ResourceIdentifier.from_string(
        "ri.foundry.main.transaction.1"
    )
    LOCATOR: DatasetLocator = DatasetLocator(
        rid=DATASET_RID,
        branch_id=BRANCH_ID,
        start_transaction_rid=START_TRANSACTION_RID,
        end_transaction_rid=END_TRANSACTION_RID,
    )

    @pytest.fixture(autouse=True)
    def before(self):
        self.catalog_service: CatalogService = mock(CatalogService)
        self.data_proxy_service: DataProxyService = mock(DataProxyService)
        self.data_proxy_concatenation_service: DataProxyConcatenationService = mock(
            DataProxyConcatenationService
        )
        self.sql_query_service: SqlQueryService = mock(SqlQueryService)
        self.path_service: PathService = mock(PathService)
        self.schema_service: SchemaService = mock(SchemaService)
        services: DatasetServices = mock(DatasetServices)
        services.catalog_service = self.catalog_service  # noqa
        services.data_proxy_service = self.data_proxy_service  # noqa
        services.data_proxy_concatenation_service = (  # noqa
            self.data_proxy_concatenation_service
        )
        services.sql_query_service = self.sql_query_service  # noqa
        services.path_service = self.path_service  # noqa
        services.schema_service = self.schema_service  # noqa
        ctx: PalantirContext = PalantirContext(
            StaticHostnameProvider("unused"),
            StaticTokenProvider(AuthToken(self.AUTH_HEADER)),
        )
        services.ctx = ctx
        self.client: DatasetsClient = DatasetsClient(services)

        self.conjure_dataset: ConjureDataset = mock(ConjureDataset)
        self.conjure_dataset.rid = self.DATASET_RID  # noqa
        self.branch: Branch = mock(Branch)
        self.branch.rid = self.BRANCH_ID  # noqa

        self.txn: ConjureTransaction = mock(ConjureTransaction)
        self.txn.rid = str(self.END_TRANSACTION_RID)  # noqa
        self.txn.status = ConjureTransactionStatus.OPEN  # noqa
        self.txn.type = ConjureTransactionType.APPEND  # noqa

        self.locator: DatasetLocator = DatasetLocator(
            rid=self.DATASET_RID,
            branch_id=self.BRANCH_ID,
            start_transaction_rid=self.START_TRANSACTION_RID,
            end_transaction_rid=self.END_TRANSACTION_RID,
        )
        self.dataset: Dataset = Dataset(self.client, self.locator)

        when(self.catalog_service).get_dataset_view_range2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            end_ref=self.BRANCH_ID,
            include_open_exclusive_transaction=False,
        ).thenReturn(
            TransactionRange(
                str(self.END_TRANSACTION_RID), str(self.START_TRANSACTION_RID)
            )
        )

    def test_get_dataset_with_rid(self):
        expect(self.client.get_dataset(str(self.DATASET_RID))).to(
            equal(self.DATASET_RID)
        )
        verifyZeroInteractions(self.catalog_service)
        verifyZeroInteractions(self.path_service)

    def test_get_dataset_with_rid_with_invalid_service(self):
        invalid_dataset_rid = (
            "ri.invalid.main.dataset.00000000-0000-0000-0000-000000000000"
        )
        expect(lambda: self.client.get_dataset(invalid_dataset_rid)).to(
            raise_error(ValueError, f"'{invalid_dataset_rid}' is not a dataset rid")
        )
        verifyZeroInteractions(self.catalog_service)
        verifyZeroInteractions(self.path_service)

    def test_get_dataset_with_rid_with_invalid_type(self):
        invalid_dataset_rid = (
            "ri.foundry.main.invalid.00000000-0000-0000-0000-000000000000"
        )
        expect(lambda: self.client.get_dataset(invalid_dataset_rid)).to(
            raise_error(ValueError, f"'{invalid_dataset_rid}' is not a dataset rid")
        )
        verifyZeroInteractions(self.catalog_service)
        verifyZeroInteractions(self.path_service)

    def test_get_dataset_with_path(self):
        dataset_ref = "/path/to/dataset"
        decorated_resource = mock(DecoratedResource)
        decorated_resource.rid = self.DATASET_RID
        when(self.path_service).get_resource_by_path(
            auth_header=self.AUTH_HEADER, path=dataset_ref
        ).thenReturn(decorated_resource)

        expect(self.client.get_dataset(dataset_ref)).to(equal(self.DATASET_RID))
        verifyZeroInteractions(self.catalog_service)

    def test_get_dataset_when_dataset_does_not_exist(self):
        dataset_ref = "/path/to/dataset"
        decorated_resource = mock(DecoratedResource)
        decorated_resource.rid = self.DATASET_RID
        when(self.path_service).get_resource_by_path(
            auth_header=self.AUTH_HEADER, path=dataset_ref
        ).thenReturn(None)

        expect(self.client.get_dataset(dataset_ref)).to(equal(None))

    def test_get_transaction_range(self):
        expect(self.client.get_transaction_range(self.DATASET_RID, self.BRANCH_ID)).to(
            equal((str(self.START_TRANSACTION_RID), str(self.END_TRANSACTION_RID)))
        )

    def test_get_transaction_range_when_no_view(self):
        when(self.catalog_service).get_dataset_view_range2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            end_ref=self.BRANCH_ID,
            include_open_exclusive_transaction=False,
        ).thenReturn(None)

        expect(self.client.get_transaction_range(self.DATASET_RID, self.BRANCH_ID)).to(
            equal((None, None))
        )

    def test_get_transaction_range_include_open(self):
        when(self.catalog_service).get_dataset_view_range2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            end_ref=self.BRANCH_ID,
            include_open_exclusive_transaction=True,
        ).thenReturn(
            TransactionRange(
                str(self.END_TRANSACTION_RID), str(self.START_TRANSACTION_RID)
            )
        )

        expect(
            self.client.get_transaction_range(
                self.DATASET_RID, self.BRANCH_ID, include_open_transaction=True
            )
        ).to(equal((str(self.START_TRANSACTION_RID), str(self.END_TRANSACTION_RID))))

    def test_list_files(self):
        path = "path"
        when(self.catalog_service).get_dataset_view_files2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            start_transaction_rid=str(self.START_TRANSACTION_RID),
            end_ref=str(self.END_TRANSACTION_RID),
            logical_path=path,
            include_open_exclusive_transaction=False,
            page_size=2,
            page_start_logical_path=None,
            exclude_hidden_files=True,
        ).thenReturn(
            FileResourcesPage(
                values=[get_file("/path/one"), get_file("/path/two")],
                next_page_token="next-page-token",
            )
        )
        when(self.catalog_service).get_dataset_view_files2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            start_transaction_rid=str(self.START_TRANSACTION_RID),
            end_ref=str(self.END_TRANSACTION_RID),
            logical_path=path,
            include_open_exclusive_transaction=False,
            page_size=2,
            page_start_logical_path="next-page-token",
            exclude_hidden_files=True,
        ).thenReturn(FileResourcesPage(values=[get_file("/path/three")]))

        files = self.client.list_files(
            dataset=self.dataset,
            path=path,
            include_open_transaction=False,
            page_size=2,
        )
        expect(list(files)).to(
            equal(
                [
                    File(
                        dataset=self.dataset,
                        path="/path/one",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                    File(
                        dataset=self.dataset,
                        path="/path/two",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                    File(
                        dataset=self.dataset,
                        path="/path/three",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                ]
            )
        )

    def test_list_files_without_path(self):
        path = "path"
        when(self.catalog_service).get_dataset_view_files2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            start_transaction_rid=str(self.START_TRANSACTION_RID),
            end_ref=str(self.END_TRANSACTION_RID),
            logical_path=path,
            include_open_exclusive_transaction=False,
            page_size=2,
            page_start_logical_path=None,
            exclude_hidden_files=True,
        ).thenReturn(
            FileResourcesPage(
                values=[get_file("/path/one"), get_file("/path/two")],
                next_page_token="next-page-token",
            )
        )
        when(self.catalog_service).get_dataset_view_files2(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            start_transaction_rid=str(self.START_TRANSACTION_RID),
            end_ref=str(self.END_TRANSACTION_RID),
            logical_path=path,
            include_open_exclusive_transaction=False,
            page_size=2,
            page_start_logical_path="next-page-token",
            exclude_hidden_files=True,
        ).thenReturn(FileResourcesPage(values=[get_file("/path/three")]))

        files = self.client.list_files(
            dataset=self.dataset,
            path=path,
            include_open_transaction=False,
            page_size=2,
        )
        expect(list(files)).to(
            equal(
                [
                    File(
                        dataset=self.dataset,
                        path="/path/one",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                    File(
                        dataset=self.dataset,
                        path="/path/two",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                    File(
                        dataset=self.dataset,
                        path="/path/three",
                        modified=FILE_MODIFIED,
                        transaction_rid=ResourceIdentifier.from_string(TRANSACTION_RID),
                        length=FILE_LEN,
                        client=self.client,
                    ),
                ]
            )
        )

    def test_read_file(self):
        path = "path"
        binary_content = b"123456"
        http_response = mock(urllib3.HTTPResponse)
        when(http_response).read().thenReturn(binary_content)
        when(self.data_proxy_service).get_file_in_view(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            end_ref=str(self.END_TRANSACTION_RID),
            logical_path=path,
            start_transaction_rid=str(self.START_TRANSACTION_RID),
        ).thenReturn(http_response)

        _bytes = self.client.read_file(
            locator=FileLocator(
                dataset_rid=self.DATASET_RID,
                end_ref=str(self.END_TRANSACTION_RID),
                logical_path=path,
                start_transaction_rid=str(self.START_TRANSACTION_RID),
            )
        )
        expect(_bytes.read()).to(equal(binary_content))

    def test_put_file(self):
        path = "path"
        binary_content = b"123456"
        when(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=path,
            file_data=binary_content,
        ).thenReturn(None)

        self.client.put_file(
            locator=FileLocator(
                dataset_rid=self.DATASET_RID,
                end_ref=str(self.END_TRANSACTION_RID),
                logical_path=path,
                start_transaction_rid=str(self.START_TRANSACTION_RID),
            ),
            content=binary_content,
        )

        verify(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=path,
            file_data=binary_content,
        )

    def test_put_file_chunked(self):
        path = "path"
        megabyte = 1024 * 1024
        max_chunk_size = 50 * megabyte
        first_chunk = b"0" * max_chunk_size
        second_chunk = b"1" * max_chunk_size
        binary_content = first_chunk + second_chunk

        when(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.0",
            file_data=first_chunk,
        ).thenReturn(None)
        when(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.1",
            file_data=second_chunk,
        ).thenReturn(None)

        when(self.data_proxy_concatenation_service).start_concatenation_task(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=StartConcatenationTaskRequest(
                destination_path=relpath(path),
                source_paths=[f"{path}.0", f"{path}.1"],
            ),
        ).thenReturn(
            StartConcatenationTaskResponse(
                concatenation_task_id="concatenation-task-id"
            )
        )

        when(self.data_proxy_concatenation_service).get_concatenation_task_status(
            auth_header=self.AUTH_HEADER, concatenation_task_id="concatenation-task-id"
        ).thenReturn(
            ConcatenationTaskStatusReport(
                status=ConcatenationTaskStatus(queued=ConcatenationTaskQueued()),
                reported_at="reported-at",
            )
        ).thenReturn(
            ConcatenationTaskStatusReport(
                status=ConcatenationTaskStatus(
                    in_progress=ConcatenationTaskInProgress(
                        concatenated_files_count=0,
                        deleted_files_count=0,
                        total_files_count=0,
                    )
                ),
                reported_at="reported-at",
            )
        ).thenReturn(
            ConcatenationTaskStatusReport(
                status=ConcatenationTaskStatus(success=ConcatenationTaskSuccess()),
                reported_at="reported-at",
            )
        )

        self.client.put_file(
            locator=FileLocator(
                dataset_rid=self.DATASET_RID,
                end_ref=str(self.END_TRANSACTION_RID),
                logical_path=path,
                start_transaction_rid=str(self.START_TRANSACTION_RID),
            ),
            content=binary_content,
        )

        verify(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.0",
            file_data=first_chunk,
        )
        verify(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.1",
            file_data=second_chunk,
        )

    def test_put_file_chunked_failure(self):
        path = "path"
        megabyte = 1024 * 1024
        max_chunk_size = 50 * megabyte
        first_chunk = b"0" * max_chunk_size
        second_chunk = b"1" * max_chunk_size
        binary_content = first_chunk + second_chunk

        when(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.0",
            file_data=first_chunk,
        ).thenReturn(None)
        when(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.1",
            file_data=second_chunk,
        ).thenReturn(None)

        when(self.data_proxy_concatenation_service).start_concatenation_task(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=StartConcatenationTaskRequest(
                destination_path=relpath(path),
                source_paths=[f"{path}.0", f"{path}.1"],
            ),
        ).thenReturn(
            StartConcatenationTaskResponse(
                concatenation_task_id="concatenation-task-id"
            )
        )

        when(self.data_proxy_concatenation_service).get_concatenation_task_status(
            auth_header=self.AUTH_HEADER, concatenation_task_id="concatenation-task-id"
        ).thenReturn(
            ConcatenationTaskStatusReport(
                status=ConcatenationTaskStatus(
                    failure=ConcatenationTaskFailure(
                        concatenated_files_count=0,
                        deleted_files_count=0,
                        total_files_count=0,
                        error_message="error message",
                    )
                ),
                reported_at="reported-at",
            )
        )

        expect(
            lambda: self.client.put_file(
                locator=FileLocator(
                    dataset_rid=self.DATASET_RID,
                    end_ref=str(self.END_TRANSACTION_RID),
                    logical_path=path,
                    start_transaction_rid=str(self.START_TRANSACTION_RID),
                ),
                content=binary_content,
            )
        ).to(raise_error(ValueError, "error message"))

        verify(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.0",
            file_data=first_chunk,
        )
        verify(self.data_proxy_service).put_file(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            logical_path=f"{path}.1",
            file_data=second_chunk,
        )

    def test_start_transaction_with_append_type(self):
        when(self.catalog_service).start_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            request=StartTransactionRequest(branch_id=self.BRANCH_ID, record={}),
        ).thenReturn(self.txn)

        expect(self.client.start_transaction(self.dataset, TransactionType.APPEND)).to(
            equal(
                Transaction(
                    dataset=self.dataset,
                    rid=self.END_TRANSACTION_RID,
                    status=TransactionStatus.OPEN,
                    txn_type=TransactionType.APPEND,
                    client=self.client,
                )
            )
        )

    def test_start_transaction_with_update_type(self):
        txn2: ConjureTransaction = mock(ConjureTransaction)
        txn2.rid = self.txn.rid  # noqa
        txn2.status = self.txn.status  # noqa
        txn2.type = TransactionType.UPDATE  # noqa

        when(self.catalog_service).start_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            request=StartTransactionRequest(branch_id=self.BRANCH_ID, record={}),
        ).thenReturn(self.txn)
        when(self.catalog_service).set_transaction_type(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            txn_type=ConjureTransactionType.UPDATE,
        ).thenReturn(txn2)

        expect(self.client.start_transaction(self.dataset, TransactionType.UPDATE)).to(
            equal(
                Transaction(
                    dataset=self.dataset,
                    rid=self.END_TRANSACTION_RID,
                    status=TransactionStatus.OPEN,
                    txn_type=TransactionType.UPDATE,
                    client=self.client,
                )
            )
        )

    def test_commit_transaction(self):
        txn = Transaction(
            dataset=self.dataset,
            rid=self.END_TRANSACTION_RID,
            status=TransactionStatus.OPEN,
            txn_type=TransactionType.APPEND,
            client=self.client,
        )
        when(self.catalog_service).commit_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=CloseTransactionRequest(record={}),
        )
        self.client.commit_transaction(txn)

        verify(self.catalog_service).commit_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=CloseTransactionRequest(record={}),
        )

    def test_abort_transaction(self):
        txn = Transaction(
            dataset=self.dataset,
            rid=self.END_TRANSACTION_RID,
            status=TransactionStatus.OPEN,
            txn_type=TransactionType.APPEND,
            client=self.client,
        )
        when(self.catalog_service).abort_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=CloseTransactionRequest(record={}),
        )

        self.client.abort_transaction(txn)

        verify(self.catalog_service).abort_transaction(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            transaction_rid=str(self.END_TRANSACTION_RID),
            request=CloseTransactionRequest(record={}),
        )

    def test_put_schema(self):
        schema = FoundrySchema(
            fields=[
                Field("foo", "str"),
                Field("bar", "int32", nullable=False),
                Field("list_list", ArrayFieldType(ArrayFieldType(LongFieldType()))),
            ],
            file_format=FileFormat.PARQUET,
            metadata={"key": "value", "absentKey": None},
        )
        expected = ConjureFoundrySchema(
            field_schema_list=[
                FoundryFieldSchema(
                    name="foo",
                    field_type=FoundryFieldType.STRING,
                    custom_metadata={},
                    nullable=True,
                ),
                FoundryFieldSchema(
                    name="bar",
                    field_type=FoundryFieldType.INTEGER,
                    custom_metadata={},
                    nullable=False,
                ),
                FoundryFieldSchema(
                    name="list_list",
                    field_type=FoundryFieldType.ARRAY,
                    array_subtype=FoundryFieldSchema(
                        field_type=FoundryFieldType.ARRAY,
                        array_subtype=FoundryFieldSchema(
                            field_type=FoundryFieldType.LONG,
                            custom_metadata={},
                            nullable=True,
                        ),
                        custom_metadata={},
                        nullable=True,
                    ),
                    custom_metadata={},
                    nullable=True,
                ),
            ],
            data_frame_reader_class="com.palantir.foundry.spark.input.ParquetDataFrameReader",
            custom_metadata={"key": "value", "format": "parquet"},
        )
        when(self.schema_service).put_schema(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            branch_id=self.BRANCH_ID,
            end_transaction_rid=str(self.END_TRANSACTION_RID),
            schema=expected,
        )

        self.client.put_schema(self.dataset, schema)

        verify(self.schema_service).put_schema(
            auth_header=self.AUTH_HEADER,
            dataset_rid=str(self.DATASET_RID),
            branch_id=self.BRANCH_ID,
            end_transaction_rid=str(self.END_TRANSACTION_RID),
            schema=expected,
        )

    def test_read_dataset(self):
        query_id = "query_id"
        running = QueryStatus(running=RunningQueryStatus())
        ready = QueryStatus(ready=ReadyQueryStatus())

        table = pa.Table.from_pandas(
            pd.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["foo", "bar"])
        )
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink=sink, schema=table.schema)
        writer.write_table(table)
        writer.close()

        results = io.BytesIO()
        results.write(b"A")
        results.write(sink.getvalue())
        results.seek(0)

        when(self.sql_query_service).execute(
            auth_header=self.AUTH_HEADER,
            request=SqlExecuteRequest(
                dialect=SqlDialect.ANSI,  # type: ignore
                fallback_branch_ids=[],
                query=SqlQuery(
                    f'SELECT * FROM "{self.END_TRANSACTION_RID}"."{self.DATASET_RID}"'
                ),
                serialization_protocol=SerializationProtocol.ARROW,  # type: ignore
            ),
        ).thenReturn(
            SqlExecuteResponse(
                query_id=query_id,
                status=running,
            )
        )
        when(self.sql_query_service).get_status(
            auth_header=self.AUTH_HEADER,
            query_id=query_id,
        ).thenReturn(
            SqlGetStatusResponse(
                status=running,
            ),
            SqlGetStatusResponse(
                status=ready,
            ),
        )
        when(self.sql_query_service).get_results(
            auth_header=self.AUTH_HEADER,
            query_id=query_id,
        ).thenReturn(results)

        expect(self.client.read_dataset(locator=self.LOCATOR)).to(equal(table))

        verify(self.sql_query_service, times=2).get_status(
            auth_header=self.AUTH_HEADER,
            query_id=query_id,
        )
