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

from typing import Optional, List, Dict, Any

from conjure_python_client import (
    Service,
    ConjureDecoder,
    ConjureEncoder,
    OptionalType,
    ConjureBeanType,
    ConjureFieldDefinition,
    ListType,
    DictType,
    ConjureEnumType,
)


class CatalogService(Service):
    def create_dataset(
        self, auth_header: str, request: "CreateDatasetRequest"
    ) -> "Dataset":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {}

        _json: Any = ConjureEncoder().default(request)

        _path = "/catalog/datasets"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), Dataset)

    def create_branch2(
        self,
        auth_header: str,
        branch_id: str,
        dataset_rid: str,
        request: "CreateBranchRequest",
    ) -> "Branch":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "branchId": branch_id,
        }

        _json: Any = ConjureEncoder().default(request)

        _path = "/catalog/datasets/{datasetRid}/branchesUnrestricted2/{branchId}"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), Branch)

    def get_dataset_view_range2(
        self,
        auth_header: str,
        dataset_rid: str,
        end_ref: str,
        include_open_exclusive_transaction: Optional[bool] = None,
        start_transaction_rid: Optional[str] = None,
    ) -> "Optional[TransactionRange]":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {
            "startTransactionRid": start_transaction_rid,
            "includeOpenExclusiveTransaction": include_open_exclusive_transaction,
        }

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "endRef": end_ref,
        }

        _json = None

        _path = "/catalog/datasets/{datasetRid}/views2/{endRef}/range"
        _path = _path.format(**_path_params)

        _response = self._request(
            "GET", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return (
            None
            if _response.status_code == 204
            else _decoder.decode(
                _response.json(),
                OptionalType(TransactionRange),
            )
        )

    def get_dataset_view_files2(
        self,
        auth_header: str,
        dataset_rid: str,
        end_ref: str,
        page_size: int,
        exclude_hidden_files: bool = None,
        include_open_exclusive_transaction: bool = None,
        logical_path: str = None,
        page_start_logical_path: str = None,
        start_transaction_rid: str = None,
    ):

        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {
            "startTransactionRid": start_transaction_rid,
            "logicalPath": logical_path,
            "pageSize": page_size,
            "pageStartLogicalPath": page_start_logical_path,
            "includeOpenExclusiveTransaction": include_open_exclusive_transaction,
            "excludeHiddenFiles": exclude_hidden_files,
        }

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "endRef": end_ref,
        }

        _json = None

        _path = "/catalog/datasets/{datasetRid}/views2/{endRef}/files"
        _path = _path.format(**_path_params)

        _response = self._request(
            "GET", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), FileResourcesPage)

    def start_transaction(
        self, auth_header: str, dataset_rid: str, request: "StartTransactionRequest"
    ) -> "Transaction":

        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
        }

        _json: Any = ConjureEncoder().default(request)

        _path = "/catalog/datasets/{datasetRid}/transactions"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), Transaction)

    def set_transaction_type(
        self,
        auth_header: str,
        dataset_rid: str,
        transaction_rid: str,
        txn_type: "TransactionType",
    ) -> "Transaction":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "transactionRid": transaction_rid,
        }

        _json: Any = ConjureEncoder().default(txn_type)

        _path = "/catalog/datasets/{datasetRid}/transactions/{transactionRid}"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), Transaction)

    def commit_transaction(
        self,
        auth_header: str,
        dataset_rid: str,
        request: "CloseTransactionRequest",
        transaction_rid: str,
    ):
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "transactionRid": transaction_rid,
        }

        _json: Any = ConjureEncoder().default(request)

        _path = "/catalog/datasets/{datasetRid}/transactions/{transactionRid}/commit"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

    def abort_transaction(
        self,
        auth_header: str,
        dataset_rid: str,
        request: "CloseTransactionRequest",
        transaction_rid: str,
    ):
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "transactionRid": transaction_rid,
        }

        _json: Any = ConjureEncoder().default(request)

        _path = "/catalog/datasets/{datasetRid}/transactions/{transactionRid}/abortWithMetadata"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )


class StartTransactionRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "branch_id": ConjureFieldDefinition("branchId", str),
            "provenance": ConjureFieldDefinition(
                "provenance", OptionalType(TransactionProvenance)
            ),
            "record": ConjureFieldDefinition("record", DictType(str, Any)),  # type: ignore
            "user_id": ConjureFieldDefinition("userId", OptionalType(UserId)),
        }

    __slots__: List[str] = ["_branch_id", "_provenance", "_record", "_user_id"]

    def __init__(
        self,
        branch_id: str,
        record: Dict[str, Any],
        provenance: "TransactionProvenance" = None,
        user_id: str = None,
    ):
        self._branch_id = branch_id
        self._provenance = provenance
        self._record = record
        self._user_id = user_id

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @property
    def provenance(self) -> Optional["TransactionProvenance"]:
        return self._provenance

    @property
    def record(self) -> Dict[str, Any]:
        return self._record

    @property
    def user_id(self) -> Optional[str]:
        return self._user_id


class CloseTransactionRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "record": ConjureFieldDefinition("record", DictType(str, Any)),  # type: ignore
            "provenance": ConjureFieldDefinition(
                "provenance", OptionalType(TransactionProvenance)
            ),
            "do_sever_inherited_permissions": ConjureFieldDefinition(
                "doSeverInheritedPermissions", OptionalType(bool)
            ),
        }

    __slots__: List[str] = ["_record", "_provenance", "_do_sever_inherited_permissions"]

    def __init__(
        self,
        record: Dict[str, Any],
        do_sever_inherited_permissions: bool = None,
        provenance: "TransactionProvenance" = None,
    ):
        self._record = record
        self._provenance = provenance
        self._do_sever_inherited_permissions = do_sever_inherited_permissions

    @property
    def record(self) -> Dict[str, Any]:
        return self._record

    @property
    def provenance(self) -> Optional["TransactionProvenance"]:
        return self._provenance

    @property
    def do_sever_inherited_permissions(self) -> Optional[bool]:
        return self._do_sever_inherited_permissions


class TransactionProvenance(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "provenance_records": ConjureFieldDefinition(
                "provenanceRecords", ListType(ProvenanceRecord)
            ),
            "non_catalog_provenance_records": ConjureFieldDefinition(
                "nonCatalogProvenanceRecords", ListType(NonCatalogProvenanceRecord)
            ),
        }

    __slots__: List[str] = ["_provenance_records", "_non_catalog_provenance_records"]

    def __init__(
        self,
        non_catalog_provenance_records: List["NonCatalogProvenanceRecord"],
        provenance_records: List["ProvenanceRecord"],
    ):
        self._provenance_records = provenance_records
        self._non_catalog_provenance_records = non_catalog_provenance_records

    @property
    def provenance_records(self) -> List["ProvenanceRecord"]:
        return self._provenance_records

    @property
    def non_catalog_provenance_records(self) -> List["NonCatalogProvenanceRecord"]:
        return self._non_catalog_provenance_records


class ProvenanceRecord(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "dataset_rid": ConjureFieldDefinition("datasetRid", str),
            "transaction_range": ConjureFieldDefinition(
                "transactionRange", OptionalType(TransactionRange)
            ),
            "schema_branch_id": ConjureFieldDefinition(
                "schemaBranchId", OptionalType(str)
            ),
            "schema_version_id": ConjureFieldDefinition(
                "schemaVersionId", OptionalType(str)
            ),
            "non_catalog_resources": ConjureFieldDefinition(
                "nonCatalogResources", ListType(str)
            ),
            "assumed_markings": ConjureFieldDefinition(
                "assumedMarkings", ListType(MarkingId)
            ),
        }

    __slots__: List[str] = [
        "_dataset_rid",
        "_transaction_range",
        "_schema_branch_id",
        "_schema_version_id",
        "_non_catalog_resources",
        "_assumed_markings",
    ]

    def __init__(
        self,
        assumed_markings: List[str],
        dataset_rid: str,
        non_catalog_resources: List[str],
        schema_branch_id: str = None,
        schema_version_id: str = None,
        transaction_range: "TransactionRange" = None,
    ):
        self._dataset_rid = dataset_rid
        self._transaction_range = transaction_range
        self._schema_branch_id = schema_branch_id
        self._schema_version_id = schema_version_id
        self._non_catalog_resources = non_catalog_resources
        self._assumed_markings = assumed_markings

    @property
    def dataset_rid(self) -> str:
        return self._dataset_rid

    @property
    def transaction_range(self) -> Optional["TransactionRange"]:
        return self._transaction_range

    @property
    def schema_branch_id(self) -> Optional[str]:
        return self._schema_branch_id

    @property
    def schema_version_id(self) -> Optional[str]:
        return self._schema_version_id

    @property
    def non_catalog_resources(self) -> List[str]:
        return self._non_catalog_resources

    @property
    def assumed_markings(self) -> List[str]:
        return self._assumed_markings


class NonCatalogProvenanceRecord(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "resources": ConjureFieldDefinition("resources", ListType(str)),
            "assumed_markings": ConjureFieldDefinition(
                "assumedMarkings", ListType(MarkingId)
            ),
            "dependency_type": ConjureFieldDefinition(
                "dependencyType", OptionalType(DependencyType)
            ),
        }

    __slots__: List[str] = ["_resources", "_assumed_markings", "_dependency_type"]

    def __init__(
        self,
        assumed_markings: List[str],
        resources: List[str],
        dependency_type: "DependencyType" = None,
    ):
        self._resources = resources
        self._assumed_markings = assumed_markings
        self._dependency_type = dependency_type

    @property
    def resources(self) -> List[str]:
        return self._resources

    @property
    def assumed_markings(self) -> List[str]:
        return self._assumed_markings

    @property
    def dependency_type(self) -> Optional["DependencyType"]:
        return self._dependency_type


class DependencyType(ConjureEnumType):
    SECURED = "SECURED"
    """SECURED"""
    UNSECURED = "UNSECURED"
    """UNSECURED"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class TransactionType(ConjureEnumType):

    UPDATE = "UPDATE"
    """UPDATE"""
    APPEND = "APPEND"
    """APPEND"""
    DELETE = "DELETE"
    """DELETE"""
    SNAPSHOT = "SNAPSHOT"
    """SNAPSHOT"""
    UNDEFINED = "UNDEFINED"
    """UNDEFINED"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class TransactionStatus(ConjureEnumType):

    OPEN = "OPEN"
    """OPEN"""
    COMMITTED = "COMMITTED"
    """COMMITTED"""
    ABORTED = "ABORTED"
    """ABORTED"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class Transaction(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "type": ConjureFieldDefinition("type", TransactionType),
            "status": ConjureFieldDefinition("status", TransactionStatus),
            "file_path_type": ConjureFieldDefinition("filePathType", FilePathType),
            "start_time": ConjureFieldDefinition("startTime", str),
            "close_time": ConjureFieldDefinition("closeTime", OptionalType(str)),
            "permission_path": ConjureFieldDefinition(
                "permissionPath", OptionalType(str)
            ),
            "record": ConjureFieldDefinition(
                "record", OptionalType(DictType(str, Any))  # type: ignore
            ),
            "attribution": ConjureFieldDefinition(
                "attribution", OptionalType(Attribution)
            ),
            "is_data_deleted": ConjureFieldDefinition("isDataDeleted", bool),
            "is_deletion_complete": ConjureFieldDefinition("isDeletionComplete", bool),
            "rid": ConjureFieldDefinition("rid", str),
            "provenance": ConjureFieldDefinition(
                "provenance", OptionalType(TransactionProvenance)
            ),
            "dataset_rid": ConjureFieldDefinition("datasetRid", str),
        }

    __slots__: List[str] = [
        "_type",
        "_status",
        "_file_path_type",
        "_start_time",
        "_close_time",
        "_permission_path",
        "_record",
        "_attribution",
        "_is_data_deleted",
        "_is_deletion_complete",
        "_rid",
        "_provenance",
        "_dataset_rid",
    ]

    def __init__(
        self,
        dataset_rid: str,
        file_path_type: "FilePathType",
        is_data_deleted: bool,
        is_deletion_complete: bool,
        rid: str,
        start_time: str,
        status: "TransactionStatus",
        type: TransactionType,  # pylint: disable=redefined-builtin
        attribution: "Attribution" = None,
        close_time: str = None,
        permission_path: str = None,
        provenance: TransactionProvenance = None,
        record: Dict[str, Any] = None,
    ):
        self._type = type
        self._status = status
        self._file_path_type = file_path_type
        self._start_time = start_time
        self._close_time = close_time
        self._permission_path = permission_path
        self._record = record
        self._attribution = attribution
        self._is_data_deleted = is_data_deleted
        self._is_deletion_complete = is_deletion_complete
        self._rid = rid
        self._provenance = provenance
        self._dataset_rid = dataset_rid

    @property
    def type(self) -> "TransactionType":
        return self._type

    @property
    def status(self) -> "TransactionStatus":
        return self._status

    @property
    def file_path_type(self) -> "FilePathType":
        return self._file_path_type

    @property
    def start_time(self) -> str:
        return self._start_time

    @property
    def close_time(self) -> Optional[str]:
        return self._close_time

    @property
    def permission_path(self) -> Optional[str]:
        return self._permission_path

    @property
    def record(self) -> Optional[Dict[str, Any]]:
        return self._record

    @property
    def attribution(self) -> Optional["Attribution"]:
        return self._attribution

    @property
    def is_data_deleted(self) -> bool:
        return self._is_data_deleted

    @property
    def is_deletion_complete(self) -> bool:
        return self._is_deletion_complete

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def provenance(self) -> Optional["TransactionProvenance"]:
        return self._provenance

    @property
    def dataset_rid(self) -> str:
        return self._dataset_rid


class FilePathType(ConjureEnumType):

    NO_FILES = "NO_FILES"
    """NO_FILES"""
    MANAGED_FILES = "MANAGED_FILES"
    """MANAGED_FILES"""
    REGISTERED_FILES = "REGISTERED_FILES"
    """REGISTERED_FILES"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""


class Attribution(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "user_id": ConjureFieldDefinition("userId", str),
            "time": ConjureFieldDefinition("time", str),
        }

    __slots__: List[str] = ["_user_id", "_time"]

    def __init__(self, time: str, user_id: str):
        self._user_id = user_id
        self._time = time

    @property
    def user_id(self) -> str:
        return self._user_id

    @property
    def time(self) -> str:
        return self._time


class TransactionRange(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "start_transaction_rid": ConjureFieldDefinition("startTransactionRid", str),
            "end_transaction_rid": ConjureFieldDefinition("endTransactionRid", str),
        }

    __slots__: List[str] = ["_start_transaction_rid", "_end_transaction_rid"]

    def __init__(self, end_transaction_rid: str, start_transaction_rid: str):
        self._start_transaction_rid = start_transaction_rid
        self._end_transaction_rid = end_transaction_rid

    @property
    def start_transaction_rid(self) -> str:
        return self._start_transaction_rid

    @property
    def end_transaction_rid(self) -> str:
        return self._end_transaction_rid


class FileResourcesPage(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "values": ConjureFieldDefinition("values", ListType(FileResource)),
            "next_page_token": ConjureFieldDefinition(
                "nextPageToken", OptionalType(str)
            ),
        }

    __slots__ = ["_values", "_next_page_token"]

    def __init__(self, values: "List[FileResource]", next_page_token: str = None):
        self._values = values
        self._next_page_token = next_page_token

    @property
    def values(self) -> "List[FileResource]":
        return self._values

    @property
    def next_page_token(self) -> Optional[str]:
        return self._next_page_token


class FileResource(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "logical_path": ConjureFieldDefinition("logicalPath", str),
            "physical_path": ConjureFieldDefinition("physicalPath", str),
            "physical_uri": ConjureFieldDefinition("physicalUri", OptionalType(str)),
            "transaction_rid": ConjureFieldDefinition("transactionRid", str),
            "file_metadata": ConjureFieldDefinition(
                "fileMetadata", OptionalType(FileMetadata)
            ),
            "is_open": ConjureFieldDefinition("isOpen", bool),
            "time_modified": ConjureFieldDefinition("timeModified", str),
        }

    __slots__ = [
        "_logical_path",
        "_physical_path",
        "_physical_uri",
        "_transaction_rid",
        "_file_metadata",
        "_is_open",
        "_time_modified",
    ]

    def __init__(
        self,
        is_open: bool,
        logical_path: str,
        physical_path: str,
        time_modified: str,
        transaction_rid: str,
        file_metadata: "FileMetadata" = None,
        physical_uri: str = None,
    ):
        self._logical_path = logical_path
        self._physical_path = physical_path
        self._physical_uri = physical_uri
        self._transaction_rid = transaction_rid
        self._file_metadata = file_metadata
        self._is_open = is_open
        self._time_modified = time_modified

    @property
    def logical_path(self) -> str:
        return self._logical_path

    @property
    def physical_path(self) -> str:
        return self._physical_path

    @property
    def physical_uri(self) -> Optional[str]:
        return self._physical_uri

    @property
    def transaction_rid(self) -> str:
        return self._transaction_rid

    @property
    def file_metadata(self) -> "Optional[FileMetadata]":
        return self._file_metadata

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def time_modified(self) -> str:
        return self._time_modified


class FileMetadata(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {"length": ConjureFieldDefinition("length", int)}

    __slots__ = ["_length"]

    def __init__(self, length: int):
        self._length = length

    @property
    def length(self) -> int:
        return self._length


class CreateDatasetRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "file_system_id": ConjureFieldDefinition("fileSystemId", OptionalType(str)),
            "path": ConjureFieldDefinition("path", str),
            "markings": ConjureFieldDefinition("markings", ListType(MarkingId)),
        }

    __slots__: List[str] = ["_file_system_id", "_path", "_markings"]

    def __init__(self, markings: List[str], path: str, file_system_id: str = None):
        self._file_system_id = file_system_id
        self._path = path
        self._markings = markings

    @property
    def file_system_id(self) -> Optional[str]:
        return self._file_system_id

    @property
    def path(self) -> str:
        return self._path

    @property
    def markings(self) -> List[str]:
        return self._markings


class Dataset(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "rid": ConjureFieldDefinition("rid", str),
            "file_system_id": ConjureFieldDefinition("fileSystemId", str),
        }

    __slots__: List[str] = ["_rid", "_file_system_id"]

    def __init__(self, file_system_id: str, rid: str):
        self._rid = rid
        self._file_system_id = file_system_id

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def file_system_id(self) -> str:
        return self._file_system_id


class CreateBranchRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "parent_ref": ConjureFieldDefinition("parentRef", OptionalType(str)),
            "parent_branch_id": ConjureFieldDefinition(
                "parentBranchId", OptionalType(str)
            ),
        }

    __slots__: List[str] = ["_parent_ref", "_parent_branch_id"]  # type

    def __init__(self, parent_branch_id: str = None, parent_ref: str = None):
        self._parent_ref = parent_ref
        self._parent_branch_id = parent_branch_id

    @property
    def parent_ref(self) -> Optional[str]:
        return self._parent_ref

    @property
    def parent_branch_id(self) -> Optional[str]:
        return self._parent_branch_id


class Branch(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "id": ConjureFieldDefinition("id", str),
            "rid": ConjureFieldDefinition("rid", str),
            "ancestor_branch_ids": ConjureFieldDefinition(
                "ancestorBranchIds", ListType(str)
            ),
            "creation_time": ConjureFieldDefinition("creationTime", str),
            "transaction_rid": ConjureFieldDefinition(
                "transactionRid", OptionalType(str)
            ),
        }

    __slots__: List[str] = [
        "_id",
        "_rid",
        "_ancestor_branch_ids",
        "_creation_time",
        "_transaction_rid",
    ]

    def __init__(
        self,
        ancestor_branch_ids: List[str],
        creation_time: str,
        id: str,  # pylint: disable=redefined-builtin,invalid-name
        rid: str,
        transaction_rid: str = None,
    ):
        self._id = id
        self._rid = rid
        self._ancestor_branch_ids = ancestor_branch_ids
        self._creation_time = creation_time
        self._transaction_rid = transaction_rid

    @property
    def id(self) -> str:  # pylint: disable=invalid-name
        return self._id

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def ancestor_branch_ids(self) -> List[str]:
        return self._ancestor_branch_ids

    @property
    def creation_time(self) -> str:
        return self._creation_time

    @property
    def transaction_rid(self) -> Optional[str]:
        return self._transaction_rid


MarkingId = str

UserId = str
