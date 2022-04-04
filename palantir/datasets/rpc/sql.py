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
from abc import abstractmethod
from typing import Dict, Any, Optional, List

from conjure_python_client import (
    Service,
    ConjureEncoder,
    ConjureDecoder,
    ConjureBeanType,
    ConjureFieldDefinition,
    OptionalType,
    ListType,
    ConjureEnumType,
    ConjureUnionType,
)


class SqlQueryService(Service):
    def execute(
        self, auth_header: str, request: "SqlExecuteRequest"
    ) -> "SqlExecuteResponse":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {}

        _json = ConjureEncoder().default(request)

        _path = "/queries/execute"
        _path = _path.format(**_path_params)

        _response = self._request(
            "POST", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), SqlExecuteResponse)

    def get_status(self, auth_header: str, query_id: str) -> "SqlGetStatusResponse":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "queryId": query_id,
        }

        _json: Any = None

        _path = "/queries/{queryId}/status"
        _path = _path.format(**_path_params)

        _response = self._request(  # type: ignore
            "GET", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), SqlGetStatusResponse)

    def get_results(self, auth_header: str, query_id: str) -> io.IOBase:
        _headers: Dict[str, Any] = {
            "Accept": "application/octet-stream",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {}

        _path_params: Dict[str, Any] = {
            "queryId": query_id,
        }

        _json: Any = None

        _path = "/queries/{queryId}/results"
        _path = _path.format(**_path_params)

        _response = self._request(  # type: ignore
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


class SqlExecuteRequest(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "query": ConjureFieldDefinition("query", SqlQuery),
            "dialect": ConjureFieldDefinition("dialect", SqlDialect),
            "serialization_protocol": ConjureFieldDefinition(
                "serializationProtocol", SerializationProtocol
            ),
            "fallback_branch_ids": ConjureFieldDefinition(
                "fallbackBranchIds", ListType(str)
            ),
            "timeout": ConjureFieldDefinition("timeout", OptionalType(TimeoutInMillis)),
        }

    __slots__: List[str] = [
        "_query",
        "_dialect",
        "_serialization_protocol",
        "_fallback_branch_ids",
        "_timeout",
    ]

    def __init__(
        self,
        dialect: "SqlDialect",
        fallback_branch_ids: List[str],
        query: str,
        serialization_protocol: "SerializationProtocol",
        timeout: Optional[int] = None,
    ) -> None:
        self._query = query
        self._dialect = dialect
        self._serialization_protocol = serialization_protocol
        self._fallback_branch_ids = fallback_branch_ids
        self._timeout = timeout

    @property
    def query(self) -> str:
        return self._query

    @property
    def dialect(self) -> "SqlDialect":
        return self._dialect

    @property
    def serialization_protocol(self) -> "SerializationProtocol":
        return self._serialization_protocol

    @property
    def fallback_branch_ids(self) -> List[str]:
        return self._fallback_branch_ids

    @property
    def timeout(self) -> Optional[int]:
        return self._timeout


class SqlExecuteResponse(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "query_id": ConjureFieldDefinition("queryId", QueryId),
            "status": ConjureFieldDefinition("status", QueryStatus),
        }

    __slots__: List[str] = ["_query_id", "_status"]

    def __init__(self, query_id: str, status: "QueryStatus") -> None:
        self._query_id = query_id
        self._status = status

    @property
    def query_id(self) -> str:
        return self._query_id

    @property
    def status(self) -> "QueryStatus":
        return self._status


class SqlGetStatusResponse(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {"status": ConjureFieldDefinition("status", QueryStatus)}

    __slots__: List[str] = ["_status"]

    def __init__(self, status: "QueryStatus") -> None:
        self._status = status

    @property
    def status(self) -> "QueryStatus":
        return self._status


class QueryStatus(ConjureUnionType):
    _canceled: Optional["CanceledQueryStatus"] = None
    _failed: Optional["FailedQueryStatus"] = None
    _ready: Optional["ReadyQueryStatus"] = None
    _running: Optional["RunningQueryStatus"] = None

    @classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "canceled": ConjureFieldDefinition("canceled", CanceledQueryStatus),
            "failed": ConjureFieldDefinition("failed", FailedQueryStatus),
            "ready": ConjureFieldDefinition("ready", ReadyQueryStatus),
            "running": ConjureFieldDefinition("running", RunningQueryStatus),
        }

    def __init__(
        self,
        canceled: Optional["CanceledQueryStatus"] = None,
        failed: Optional["FailedQueryStatus"] = None,
        ready: Optional["ReadyQueryStatus"] = None,
        running: Optional["RunningQueryStatus"] = None,
    ) -> None:
        if (canceled is not None) + (failed is not None) + (ready is not None) + (
            running is not None
        ) != 1:
            raise ValueError("a union must contain a single member")

        if canceled is not None:
            self._canceled = canceled
            self._type = "canceled"
        if failed is not None:
            self._failed = failed
            self._type = "failed"
        if ready is not None:
            self._ready = ready
            self._type = "ready"
        if running is not None:
            self._running = running
            self._type = "running"

    @property
    def canceled(self) -> "CanceledQueryStatus":
        return self._canceled  # type: ignore

    @property
    def failed(self) -> "FailedQueryStatus":
        return self._failed  # type: ignore

    @property
    def ready(self) -> "ReadyQueryStatus":
        return self._ready  # type: ignore

    @property
    def running(self) -> "RunningQueryStatus":
        return self._running  # type: ignore

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, QueryStatusVisitor):
            raise ValueError(
                f"{visitor.__class__.__name__} is not an instance of QueryStatusVisitor"
            )
        options = {
            "cancelled": lambda: visitor.canceled(self.canceled),
            "failed": lambda: visitor.failed(self.failed),
            "ready": lambda: visitor.ready(self.ready),
            "running": lambda: visitor.running(self.running),
        }
        return options[self.type]()


class QueryStatusVisitor:
    @abstractmethod
    def canceled(self, canceled: "CanceledQueryStatus") -> Any:
        pass

    @abstractmethod
    def failed(self, failed: "FailedQueryStatus") -> Any:
        pass

    @abstractmethod
    def ready(self, ready: "ReadyQueryStatus") -> Any:
        pass

    @abstractmethod
    def running(self, running: "RunningQueryStatus") -> Any:
        pass


class CanceledQueryStatus(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {}

    __slots__: List[str] = []


class FailedQueryStatus(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "error_message": ConjureFieldDefinition("errorMessage", OptionalType(str)),
            "failure_reason": ConjureFieldDefinition("failureReason", FailureReason),
        }

    __slots__: List[str] = ["_error_message", "_failure_reason"]

    def __init__(
        self, failure_reason: "FailureReason", error_message: Optional[str] = None
    ) -> None:
        self._error_message = error_message
        self._failure_reason = failure_reason

    @property
    def error_message(self) -> Optional[str]:
        return self._error_message

    @property
    def failure_reason(self) -> "FailureReason":
        return self._failure_reason


class ReadyQueryStatus(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {}

    __slots__: List[str] = []


class RunningQueryStatus(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {}

    __slots__: List[str] = []


class SqlDialect(ConjureEnumType):
    ANSI = "ANSI"
    """ANSI"""
    ODBC = "ODBC"
    """ODBC"""
    SPARK = "SPARK"
    """SPARK"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class SerializationProtocol(ConjureEnumType):
    JSON = "JSON"
    """JSON"""
    ROW_ITERATING_JSON = "ROW_ITERATING_JSON"
    """ROW_ITERATING_JSON"""
    ARROW = "ARROW"
    """ARROW"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class FailureReason(ConjureEnumType):
    FAILED_TO_START = "FAILED_TO_START"
    """FAILED_TO_START"""
    FAILED_TO_EXECUTE = "FAILED_TO_EXECUTE"
    """FAILED_TO_EXECUTE"""
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    """JOB_NOT_FOUND"""
    TIMED_OUT = "TIMED_OUT"
    """TIMED_OUT"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


QueryId = str
SqlQuery = str
TimeoutInMillis = int
