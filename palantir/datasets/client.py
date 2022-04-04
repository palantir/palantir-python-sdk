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
from time import sleep
from typing import TYPE_CHECKING, Generator, Optional, Tuple, Any, Union, Dict

from dateutil.parser import isoparse

import palantir
from palantir.core.rpc import ConjureClient
from palantir.core.types import PalantirContext, ResourceIdentifier
from palantir.core.util import page_results
from palantir.datasets.rpc.catalog import (
    CatalogService,
    Transaction as ConjureTransaction,
    StartTransactionRequest,
    TransactionType as ConjureTransactionType,
    CloseTransactionRequest,
    TransactionRange,
    Dataset as ConjureDataset,
    CreateDatasetRequest,
    CreateBranchRequest,
)
from palantir.datasets.rpc.data_proxy import (
    DataProxyService,
    ConcatenationTaskStatusVisitor,
    ConcatenationTaskInProgress,
    ConcatenationTaskQueued,
    ConcatenationTaskFailure,
    ConcatenationTaskSuccess,
    StartConcatenationTaskRequest,
    StartConcatenationTaskResponse,
    DataProxyConcatenationService,
)
from palantir.datasets.rpc.path import PathService, DecoratedResource
from palantir.datasets.rpc.schema import (
    FoundrySchema as ConjureFoundrySchema,
    SchemaService,
    FoundryFieldSchema,
    FoundryFieldType,
)
from palantir.datasets.rpc.sql import (
    SqlQueryService,
    SqlDialect,
    SerializationProtocol,
    SqlExecuteRequest,
    SqlQuery,
    QueryStatusVisitor,
)
from palantir.datasets.types import (
    FileLocator,
    DatasetLocator,
    TransactionType,
    TransactionStatus,
    FileFormat,
    FieldType,
    DecimalFieldType,
    ArrayFieldType,
    MapFieldType,
    StructFieldType,
    BinaryFieldType,
    BooleanFieldType,
    ByteFieldType,
    DateFieldType,
    DoubleFieldType,
    FloatFieldType,
    IntegerFieldType,
    LongFieldType,
    ShortFieldType,
    StringFieldType,
    TimestampFieldType,
    FoundrySchema,
)

if TYPE_CHECKING:
    from palantir.datasets.core import File, Transaction, Dataset
    import pyarrow as pa


def _chunk(content, chunk_size):
    for offset in range(0, len(content), chunk_size):
        yield content[offset : offset + chunk_size]


class DatasetServices:
    def __init__(self, ctx: PalantirContext):
        self.factory = ConjureClient()
        self.ctx = ctx

    @property
    def catalog_service(self) -> CatalogService:
        return self.factory.service(
            CatalogService,
            f"https://{self.ctx.hostname}/foundry-catalog/api",
        )

    @property
    def data_proxy_service(self) -> DataProxyService:
        return self.factory.service(
            DataProxyService,
            f"https://{self.ctx.hostname}/foundry-data-proxy/api",
        )

    @property
    def data_proxy_concatenation_service(self) -> DataProxyConcatenationService:
        return self.factory.service(
            DataProxyConcatenationService,
            f"https://{self.ctx.hostname}/foundry-data-proxy/api",
        )

    @property
    def path_service(self) -> PathService:
        return self.factory.service(
            PathService, f"https://{self.ctx.hostname}/compass/api"
        )

    @property
    def schema_service(self) -> SchemaService:
        return self.factory.service(
            SchemaService, f"https://{self.ctx.hostname}/foundry-metadata/api"
        )

    @property
    def sql_query_service(self) -> SqlQueryService:
        return self.factory.service(
            SqlQueryService,
            f"https://{self.ctx.hostname}/foundry-sql-server/api",
        )


class DatasetsClient:
    def __init__(self, services: DatasetServices):
        self.services = services
        self.ctx = services.ctx

    @property
    def _catalog_service(self) -> CatalogService:
        return self.services.catalog_service

    @property
    def _data_proxy_service(self) -> DataProxyService:
        return self.services.data_proxy_service

    @property
    def _data_proxy_concatenation_service(self) -> DataProxyConcatenationService:
        return self.services.data_proxy_concatenation_service

    @property
    def _path_service(self) -> PathService:
        return self.services.path_service

    @property
    def _schema_service(self) -> SchemaService:
        return self.services.schema_service

    @property
    def _sql_query_service(self) -> SqlQueryService:
        return self.services.sql_query_service

    def get_dataset(self, dataset_ref: str) -> Optional[ResourceIdentifier]:
        """
        :param dataset_ref: A ResourceIdentifier or Compass Path
        :return: the ResourceIdentifier of the dataset, if it exists
        """
        rid = ResourceIdentifier.try_parse(dataset_ref)
        if rid is not None:
            if rid.service == "foundry" and rid.type == "dataset":
                return rid
            raise ValueError(f"'{rid}' is not a dataset rid")

        dataset_resource: Optional[
            DecoratedResource
        ] = self._path_service.get_resource_by_path(
            auth_header=self.ctx.auth_token,
            path=dataset_ref,
        )
        return dataset_resource.rid if dataset_resource else None

    def create_dataset(self, path: str, branch: str) -> "Dataset":
        dataset: ConjureDataset = self._catalog_service.create_dataset(
            auth_header=self.ctx.auth_token,
            request=CreateDatasetRequest(
                path=path,
                markings=[],
            ),
        )
        self._catalog_service.create_branch2(
            auth_header=self.ctx.auth_token,
            dataset_rid=dataset.rid,
            branch_id=branch,
            request=CreateBranchRequest(),
        )
        return palantir.datasets.core.Dataset(
            client=self,
            locator=DatasetLocator(
                rid=ResourceIdentifier.from_string(dataset.rid),
                branch_id=branch,
            ),
        )

    def get_transaction_range(
        self,
        dataset_rid: ResourceIdentifier,
        branch_id: str,
        include_open_transaction: bool = False,
    ) -> Union[Tuple[str, str], Tuple[None, None]]:
        transaction_range: Optional[
            TransactionRange
        ] = self._catalog_service.get_dataset_view_range2(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(dataset_rid),
            end_ref=branch_id,
            include_open_exclusive_transaction=include_open_transaction,
        )
        return (
            (
                transaction_range.start_transaction_rid,
                transaction_range.end_transaction_rid,
            )
            if transaction_range is not None
            else (None, None)
        )

    def list_files(
        self,
        dataset: "Dataset",
        path: str = None,
        include_open_transaction: bool = False,
        page_size: int = 100,
    ) -> Generator["File", None, None]:
        if dataset.locator.end_transaction_rid is None:
            return
        for file in page_results(
            values_extractor=lambda page: page.values,
            token_extractor=lambda page: page.next_page_token,
            page_supplier=lambda next_page_token: self._catalog_service.get_dataset_view_files2(
                auth_header=self.ctx.auth_token,
                dataset_rid=str(dataset.rid),
                start_transaction_rid=str(dataset.locator.start_transaction_rid)
                if dataset.locator.start_transaction_rid
                else None,
                end_ref=str(dataset.locator.end_transaction_rid)
                if dataset.locator.end_transaction_rid is not None
                else dataset.locator.branch_id
                if dataset.locator.branch_id
                else "master",
                logical_path=None if path is None else relpath(path),
                include_open_exclusive_transaction=include_open_transaction,
                page_size=page_size,
                page_start_logical_path=next_page_token,
                exclude_hidden_files=True,
            ),
        ):
            yield palantir.datasets.core.File(
                dataset=dataset,
                path=file.logical_path,
                modified=isoparse(file.time_modified),
                transaction_rid=ResourceIdentifier.from_string(file.transaction_rid),
                length=file.file_metadata.length
                if file.file_metadata is not None
                else None,
                client=self,
            )

    def read_file(self, locator: FileLocator) -> io.IOBase:
        return self._data_proxy_service.get_file_in_view(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(locator.dataset_rid),
            end_ref=locator.end_ref,
            logical_path=relpath(locator.logical_path),
            start_transaction_rid=locator.start_transaction_rid,
        )

    def start_transaction(
        self,
        dataset: "Dataset",
        txn_type: TransactionType,
    ) -> "Transaction":
        txn: ConjureTransaction = self._catalog_service.start_transaction(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(dataset.rid),
            request=StartTransactionRequest(branch_id=dataset.branch, record={}),
        )

        if txn_type != TransactionType.APPEND:
            txn = self._catalog_service.set_transaction_type(
                auth_header=self.ctx.auth_token,
                dataset_rid=str(dataset.rid),
                transaction_rid=txn.rid,
                txn_type=ConjureTransactionType(txn_type.name),
            )
        return palantir.datasets.core.Transaction(
            dataset=dataset,
            rid=ResourceIdentifier.from_string(txn.rid),
            status=TransactionStatus(txn.status.value),
            txn_type=TransactionType(txn.type.value),
            client=self,
        )

    def commit_transaction(self, txn: "Transaction") -> None:
        self._catalog_service.commit_transaction(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(txn.dataset.rid),
            transaction_rid=str(txn.rid),
            request=CloseTransactionRequest(record={}),
        )

    def abort_transaction(self, txn: "Transaction") -> None:
        self._catalog_service.abort_transaction(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(txn.dataset.rid),
            transaction_rid=str(txn.rid),
            request=CloseTransactionRequest(record={}),
        )

    def put_schema(self, dataset: "Dataset", schema: FoundrySchema) -> None:
        data_frame_reader_class, dataset_format = _get_data_frame_reader_class(
            schema.format
        )
        self._schema_service.put_schema(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(dataset.rid),
            branch_id=dataset.branch,
            end_transaction_rid=str(dataset.locator.end_transaction_rid)
            if dataset.locator.end_transaction_rid
            else None,
            schema=ConjureFoundrySchema(
                field_schema_list=[
                    _get_conjure_field_schema(
                        field.type, field.name, field.nullable, field.metadata
                    )
                    for field in schema.fields
                ],
                data_frame_reader_class=data_frame_reader_class,
                custom_metadata=_prune_absent_values(
                    dict(
                        {} if schema.metadata is None else schema.metadata,
                        **{"format": dataset_format},
                    )
                ),
            ),
        )

    def put_file(self, locator: FileLocator, content: bytes) -> None:
        megabyte = 1024 * 1024
        max_chunk_size = 50 * megabyte
        if len(content) < max_chunk_size:
            self._put_file(locator, content)
        else:
            self._put_file_chunked(locator, content, max_chunk_size)

    def _put_file(self, locator: FileLocator, content: bytes) -> None:
        self._data_proxy_service.put_file(
            auth_header=self.ctx.auth_token,
            dataset_rid=str(locator.dataset_rid),
            transaction_rid=locator.end_ref,
            logical_path=relpath(locator.logical_path),
            file_data=content,
        )

    def _put_file_chunked(
        self, locator: FileLocator, content: bytes, chunk_size: int
    ) -> None:
        chunk_paths = []
        for idx, chunk_content in enumerate(_chunk(content, chunk_size)):
            chunk_path = f"{relpath(locator.logical_path)}.{idx}"
            self._data_proxy_service.put_file(
                auth_header=self.ctx.auth_token,
                dataset_rid=str(locator.dataset_rid),
                transaction_rid=locator.end_ref,
                logical_path=chunk_path,
                file_data=chunk_content,
            )
            chunk_paths.append(chunk_path)

        response: StartConcatenationTaskResponse = (
            self._data_proxy_concatenation_service.start_concatenation_task(
                auth_header=self.ctx.auth_token,
                dataset_rid=str(locator.dataset_rid),
                transaction_rid=locator.end_ref,
                request=StartConcatenationTaskRequest(
                    destination_path=relpath(locator.logical_path),
                    source_paths=chunk_paths,
                ),
            )
        )

        def is_terminal():
            return self._data_proxy_concatenation_service.get_concatenation_task_status(
                auth_header=self.ctx.auth_token,
                concatenation_task_id=response.concatenation_task_id,
            ).status.accept(ConcatenationTaskTerminationVisitor())

        while not is_terminal():
            sleep(0.5)

    def read_dataset(
        self,
        locator: DatasetLocator,
    ) -> "pa.Table":
        query = SqlQuery(f'SELECT * FROM "{locator.branch_id}"."{locator.rid}"')
        response = self._sql_query_service.execute(
            auth_header=self.ctx.auth_token,
            request=SqlExecuteRequest(
                dialect=SqlDialect.ANSI,  # type: ignore
                fallback_branch_ids=[],
                query=query,
                serialization_protocol=SerializationProtocol.ARROW,  # type: ignore
            ),
        )
        query_status = response.status

        while not query_status.accept(_IsQueryStatusTerminalVisitor()):
            sleep(1)
            query_status = self._sql_query_service.get_status(
                auth_header=self.ctx.auth_token, query_id=response.query_id
            ).status

        stream = self._sql_query_service.get_results(
            auth_header=self.ctx.auth_token, query_id=response.query_id
        )

        # the BufferedReader will close the underlying stream when it is closed
        stream.auto_close = False  # type: ignore

        # N.B. we assume since the query is a simple select star that we are direct read eligible, if the stack is not
        # properly configured for direct read this will fail
        control = stream.read(1)  # control character that should be 'A'
        assert control == b"A"
        import pyarrow as pa

        return pa.ipc.open_stream(stream).read_all()


class _IsQueryStatusTerminalVisitor(QueryStatusVisitor):
    def canceled(self, _canceled) -> bool:
        return True

    def failed(self, failed) -> bool:
        raise ValueError(
            f"read failed. "
            f"failure reason: {failed.failure_reason}. "
            f"message: {failed.error_message} "
        )

    def ready(self, _ready) -> bool:
        return True

    def running(self, _running) -> bool:
        return False


class FileRef:
    def __init__(self, locator: FileLocator):
        self.locator = locator

    def get(self) -> io.BytesIO:
        pass


class ConcatenationTaskTerminationVisitor(ConcatenationTaskStatusVisitor):
    """
    Returns True if the task was successful, False if the task is not in a terminal state, and raises ValueError if
    the task has failed.
    """

    def success(self, success: ConcatenationTaskSuccess):
        return True

    def failure(self, failure: ConcatenationTaskFailure):
        raise ValueError(failure.error_message)

    def queued(self, queued: ConcatenationTaskQueued):
        return False

    def in_progress(self, in_progress: ConcatenationTaskInProgress):
        return False


def _get_data_frame_reader_class(file_format: FileFormat) -> Tuple[str, Optional[str]]:
    if file_format == FileFormat.AVRO:
        return "com.palantir.foundry.spark.input.AvroDataFrameReader", "avro"
    if file_format == FileFormat.CSV:
        return "com.palantir.foundry.spark.input.TextDataFrameReader", None
    if file_format == FileFormat.PARQUET:
        return "com.palantir.foundry.spark.input.ParquetDataFrameReader", "parquet"
    if file_format == FileFormat.SOHO:
        return "com.palantir.foundry.spark.input.DataSourceDataFrameReader", "soho"
    raise ValueError(f"unknown file format: {file_format}")


def _get_conjure_field_schema(
    field_type: FieldType,
    name: Optional[str] = None,
    nullable: bool = True,
    metadata: Dict[str, Any] = None,
) -> FoundryFieldSchema:
    foundry_field_type = _get_conjure_field_type(field_type)
    array_subtype = None
    map_key_type = None
    map_value_type = None
    sub_schemas = None
    precision = None
    scale = None

    if isinstance(field_type, DecimalFieldType):
        precision = field_type.precision
        scale = field_type.scale
    elif isinstance(field_type, ArrayFieldType):
        array_subtype = _get_conjure_field_schema(field_type.element_type)
    elif isinstance(field_type, MapFieldType):
        map_key_type = _get_conjure_field_schema(field_type.key_type, nullable=False)
        map_value_type = _get_conjure_field_schema(field_type.value_type)
    elif isinstance(field_type, StructFieldType):
        sub_schemas = [_get_conjure_field_schema(child) for child in field_type.fields]

    return FoundryFieldSchema(
        field_type=foundry_field_type,
        name=name,
        nullable=nullable,
        custom_metadata={} if metadata is None else metadata,
        array_subtype=array_subtype,
        map_key_type=map_key_type,
        map_value_type=map_value_type,
        sub_schemas=sub_schemas,
        precision=precision,
        scale=scale,
    )


def _get_conjure_field_type(field_type: FieldType) -> FoundryFieldType:
    sdk_to_conjure = {
        ArrayFieldType: FoundryFieldType.ARRAY,
        BinaryFieldType: FoundryFieldType.BINARY,
        BooleanFieldType: FoundryFieldType.BOOLEAN,
        ByteFieldType: FoundryFieldType.BYTE,
        DateFieldType: FoundryFieldType.DATE,
        DecimalFieldType: FoundryFieldType.DECIMAL,
        DoubleFieldType: FoundryFieldType.DOUBLE,
        FloatFieldType: FoundryFieldType.FLOAT,
        IntegerFieldType: FoundryFieldType.INTEGER,
        LongFieldType: FoundryFieldType.LONG,
        MapFieldType: FoundryFieldType.MAP,
        ShortFieldType: FoundryFieldType.SHORT,
        StringFieldType: FoundryFieldType.STRING,
        StructFieldType: FoundryFieldType.STRUCT,
        TimestampFieldType: FoundryFieldType.TIMESTAMP,
    }
    conjure_field_type = sdk_to_conjure.get(type(field_type))
    if conjure_field_type is not None:
        return conjure_field_type
    raise ValueError(f"Unknown FoundryFieldType: {field_type}")


def _prune_absent_values(dictionary):
    return {
        k: _prune_absent_values(v) if isinstance(v, dict) else v
        for k, v in dictionary.items()
        if v is not None
    }
