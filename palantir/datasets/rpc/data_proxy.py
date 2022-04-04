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
from abc import abstractmethod, ABC
from typing import Dict, Any, List, Optional

from conjure_python_client import (
    Service,
    ConjureBeanType,
    ConjureFieldDefinition,
    ConjureUnionType,
    ConjureDecoder,
    ConjureEncoder,
    ListType,
)
from requests import Response


class DataProxyService(Service):
    def get_file_in_view(
        self,
        auth_header: str,
        dataset_rid: str,
        end_ref: str,
        logical_path: str,
        start_transaction_rid: str = None,
    ) -> io.IOBase:

        _headers: Dict[str, Any] = {
            "Accept": "application/octet-stream",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {
            "startTransactionRid": start_transaction_rid,
        }

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "endRef": end_ref,
            "logicalPath": logical_path,
        }

        _json = None

        _path = "/dataproxy/datasets/{datasetRid}/views/{endRef}/{logicalPath}"
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            "GET",
            self._uri + _path,
            params=_params,
            headers=_headers,
            stream=True,
            json=_json,
        )

        _raw = _response.raw
        _raw.decode_content = True
        return _raw

    def put_file(
        self,
        auth_header: str,
        dataset_rid: str,
        file_data: Any,
        logical_path: str,
        transaction_rid: str,
        overwrite: Optional[bool] = None,
    ):
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/octet-stream",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {
            "logicalPath": logical_path,
            "overwrite": overwrite,
        }

        _path_params: Dict[str, Any] = {
            "datasetRid": dataset_rid,
            "transactionRid": transaction_rid,
        }
        _path = "/dataproxy/datasets/{datasetRid}/transactions/{transactionRid}/putFile"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, data=file_data
        )


class DataProxyConcatenationService(Service):
    """
    Used to start and monitor concatenation tasks.
    """

    def start_concatenation_task(
        self,
        auth_header: str,
        dataset_rid: str,
        request: "StartConcatenationTaskRequest",
        transaction_rid: str,
    ) -> "StartConcatenationTaskResponse":

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

        _path = "/concatenation-tasks/datasets/{datasetRid}/transactions/{transactionRid}/start"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), StartConcatenationTaskResponse)

    def get_concatenation_task_status(
        self, auth_header: str, concatenation_task_id: str
    ) -> "ConcatenationTaskStatusReport":

        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "concatenationTaskId": concatenation_task_id,
        }

        _json: Any = None

        _path = "/concatenation-tasks/tasks/{concatenationTaskId}/status-report"
        _path = _path.format(**_path_params)

        _response = self._request(
            "GET", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ConcatenationTaskStatusReport)


class StartConcatenationTaskRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "source_paths": ConjureFieldDefinition("sourcePaths", ListType(str)),
            "destination_path": ConjureFieldDefinition("destinationPath", str),
        }

    __slots__: List[str] = ["_source_paths", "_destination_path"]

    def __init__(self, destination_path: str, source_paths: List[str]):
        self._source_paths = source_paths
        self._destination_path = destination_path

    @property
    def source_paths(self) -> List[str]:
        return self._source_paths

    @property
    def destination_path(self) -> str:
        return self._destination_path


class StartConcatenationTaskResponse(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "concatenation_task_id": ConjureFieldDefinition(
                "concatenationTaskId", ConcatenationTaskId
            )
        }

    __slots__: List[str] = ["_concatenation_task_id"]

    def __init__(self, concatenation_task_id: str):
        self._concatenation_task_id = concatenation_task_id

    @property
    def concatenation_task_id(self) -> str:
        return self._concatenation_task_id


class ConcatenationTaskStatusReport(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "status": ConjureFieldDefinition("status", ConcatenationTaskStatus),
            "reported_at": ConjureFieldDefinition("reportedAt", str),
        }

    __slots__: List[str] = ["_status", "_reported_at"]

    def __init__(self, reported_at: str, status: "ConcatenationTaskStatus"):
        self._status = status
        self._reported_at = reported_at

    @property
    def status(self) -> "ConcatenationTaskStatus":
        return self._status

    @property
    def reported_at(self) -> str:
        return self._reported_at


class ConcatenationTaskStatus(ConjureUnionType):
    @classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "success": ConjureFieldDefinition("success", ConcatenationTaskSuccess),
            "failure": ConjureFieldDefinition("failure", ConcatenationTaskFailure),
            "queued": ConjureFieldDefinition("queued", ConcatenationTaskQueued),
            "in_progress": ConjureFieldDefinition(
                "inProgress", ConcatenationTaskInProgress
            ),
        }

    def __init__(self, success=None, failure=None, queued=None, in_progress=None):
        if (success is not None) + (failure is not None) + (queued is not None) + (
            in_progress is not None
        ) != 1:
            raise ValueError("a union must contain a single member")

        if success is not None:
            self._success = success
            self._type = "success"
        if failure is not None:
            self._failure = failure
            self._type = "failure"
        if queued is not None:
            self._queued = queued
            self._type = "queued"
        if in_progress is not None:
            self._in_progress = in_progress
            self._type = "inProgress"

    @property
    def type(self) -> str:
        return self._type

    @property
    def success(self) -> "ConcatenationTaskSuccess":
        return self._success  # type: ignore

    @property
    def failure(self) -> "ConcatenationTaskFailure":
        return self._failure  # type: ignore

    @property
    def queued(self) -> "ConcatenationTaskQueued":
        return self._queued  # type: ignore

    @property
    def in_progress(self) -> "ConcatenationTaskInProgress":
        return self._in_progress  # type: ignore

    def accept(self, visitor: "ConcatenationTaskStatusVisitor") -> Any:
        if not isinstance(visitor, ConcatenationTaskStatusVisitor):
            raise ValueError(
                f"{visitor.__class__.__name__} is not an instance of ConcatenationTaskStatusVisitor"
            )
        options = {
            "success": lambda: visitor.success(self.success),
            "failure": lambda: visitor.failure(self.failure),
            "queued": lambda: visitor.queued(self.queued),
            "inProgress": lambda: visitor.in_progress(self.in_progress),
        }
        return options[self.type]()


class ConcatenationTaskStatusVisitor(ABC):
    @abstractmethod
    def success(self, success: "ConcatenationTaskSuccess") -> Any:
        pass

    @abstractmethod
    def failure(self, failure: "ConcatenationTaskFailure") -> Any:
        pass

    @abstractmethod
    def queued(self, queued: "ConcatenationTaskQueued") -> Any:
        pass

    @abstractmethod
    def in_progress(self, in_progress: "ConcatenationTaskInProgress") -> Any:
        pass


class ConcatenationTaskSuccess(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {}

    __slots__: List[str] = []


class ConcatenationTaskFailure(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "concatenated_files_count": ConjureFieldDefinition(
                "concatenatedFilesCount", int
            ),
            "deleted_files_count": ConjureFieldDefinition("deletedFilesCount", int),
            "total_files_count": ConjureFieldDefinition("totalFilesCount", int),
            "error_message": ConjureFieldDefinition("errorMessage", str),
        }

    __slots__: List[str] = [
        "_concatenated_files_count",
        "_deleted_files_count",
        "_total_files_count",
        "_error_message",
    ]

    def __init__(
        self,
        concatenated_files_count: int,
        deleted_files_count: int,
        error_message: str,
        total_files_count: int,
    ):
        self._concatenated_files_count = concatenated_files_count
        self._deleted_files_count = deleted_files_count
        self._total_files_count = total_files_count
        self._error_message = error_message

    @property
    def concatenated_files_count(self) -> int:
        return self._concatenated_files_count

    @property
    def deleted_files_count(self) -> int:
        return self._deleted_files_count

    @property
    def total_files_count(self) -> int:
        return self._total_files_count

    @property
    def error_message(self) -> str:
        return self._error_message


class ConcatenationTaskQueued(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {}

    __slots__: List[str] = []


class ConcatenationTaskInProgress(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "concatenated_files_count": ConjureFieldDefinition(
                "concatenatedFilesCount", int
            ),
            "deleted_files_count": ConjureFieldDefinition("deletedFilesCount", int),
            "total_files_count": ConjureFieldDefinition("totalFilesCount", int),
        }

    __slots__: List[str] = [
        "_concatenated_files_count",
        "_deleted_files_count",
        "_total_files_count",
    ]

    def __init__(
        self,
        concatenated_files_count: int,
        deleted_files_count: int,
        total_files_count: int,
    ):
        self._concatenated_files_count = concatenated_files_count
        self._deleted_files_count = deleted_files_count
        self._total_files_count = total_files_count

    @property
    def concatenated_files_count(self) -> int:
        return self._concatenated_files_count

    @property
    def deleted_files_count(self) -> int:
        return self._deleted_files_count

    @property
    def total_files_count(self) -> int:
        return self._total_files_count


ConcatenationTaskId = str
