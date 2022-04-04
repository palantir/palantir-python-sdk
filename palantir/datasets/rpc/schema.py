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

from conjure_python_client import (
    Service,
    ConjureEncoder,
    ConjureDecoder,
    ListType,
    OptionalType,
    ConjureBeanType,
    ConjureFieldDefinition,
    DictType,
    ConjureEnumType,
)
from requests import Response
from typing import Optional, Mapping, Any, Dict, List


class SchemaService(Service):
    def put_schema(
        self,
        auth_header: str,
        dataset_rid: str,
        branch_id: str,
        schema: "FoundrySchema",
        end_transaction_rid: Optional[str],
    ) -> None:
        _headers: Mapping[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Mapping[str, Any] = {"endTransactionRid": end_transaction_rid}

        _json: Any = ConjureEncoder.do_encode(schema)

        _path = f"/schemas/datasets/{dataset_rid}/branches/{branch_id}"

        _response: Response = self._request(
            "POST",
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json,
        )

    def get_schema(
        self,
        auth_header: str,
        dataset_rid: str,
        branch_id: str,
        end_transaction_rid: str,
        version_rid: Optional[str] = None,
    ) -> "Optional[VersionedFoundrySchema]":
        _headers: Mapping[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Mapping[str, Any] = {
            "endTransactionRid": end_transaction_rid,
            "versionId": version_rid,
        }

        _path = f"/schemas/datasets/{dataset_rid}/branches/{branch_id}"

        _response: Response = self._request(
            "GET", self._uri + _path, params=_params, headers=_headers
        )

        _decoder = ConjureDecoder()
        return (
            None
            if _response.status_code == 204
            else VersionedFoundrySchema(
                branch_id=_response.json().get("branchId"),
                transaction_rid=_response.json().get("transactionRid"),
                version_id=_response.json().get("versionId"),
                schema=FoundrySchema(
                    field_schema_list=_decoder.decode(
                        _response.json().get("schema").get("fieldSchemaList"),
                        ListType(FoundryFieldSchema),
                    ),
                    primary_key=_decoder.decode(
                        _response.json().get("schema").get("primaryKey"),
                        OptionalType(PrimaryKey),
                    ),
                    data_frame_reader_class=_response.json()
                    .get("schema")
                    .get("dataFrameReaderClass"),
                    custom_metadata=_response.json()
                    .get("schema")
                    .get("customMetadata"),
                ),
                attribution=_decoder.decode(
                    _response.json().get("attribution"),
                    Attribution,
                ),
            )
        )


class VersionedFoundrySchema(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        """_fields is a mapping from constructor argument name to the field definition"""
        return {
            "branch_id": ConjureFieldDefinition(
                "branchId",
                str,
            ),
            "transaction_rid": ConjureFieldDefinition("transactionRid", str),
            "version_id": ConjureFieldDefinition("versionId", str),
            "schema": ConjureFieldDefinition("schema", FoundrySchema),
            "attribution": ConjureFieldDefinition(
                "attribution", OptionalType(Attribution)
            ),
        }

    def __init__(
        self,
        branch_id: str,
        transaction_rid: str,
        version_id: str,
        schema: "FoundrySchema",
        attribution: "Attribution" = None,
    ):
        self._branch_id = branch_id
        self._transaction_rid = transaction_rid
        self._version_id = version_id
        self._schema = schema
        self._attribution = attribution

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @property
    def transaction_rid(self) -> str:
        return self._transaction_rid

    @property
    def version_id(self) -> str:
        return self._version_id

    @property
    def schema(self) -> "FoundrySchema":
        return self._schema

    @property
    def attribution(self) -> "Optional[Attribution]":
        return self._attribution


class FoundrySchema(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        """_fields is a mapping from constructor argument name to the field definition"""
        return {
            "field_schema_list": ConjureFieldDefinition(
                "fieldSchemaList",
                ListType(FoundryFieldSchema),
            ),
            "primary_key": ConjureFieldDefinition(
                "primaryKey", OptionalType(PrimaryKey)
            ),
            "data_frame_reader_class": ConjureFieldDefinition(
                "dataFrameReaderClass", str
            ),
            "custom_metadata": ConjureFieldDefinition(
                "customMetadata", DictType(str, Any)  # type: ignore
            ),
        }

    def __init__(
        self,
        field_schema_list: "List[FoundryFieldSchema]",
        data_frame_reader_class: str,
        custom_metadata: Dict[str, Any] = None,
        primary_key: "Optional[PrimaryKey]" = None,
    ):
        self._field_schema_list = field_schema_list
        self._data_frame_reader_class = data_frame_reader_class
        self._custom_metadata = custom_metadata or {}
        self._primary_key = primary_key

    @property
    def field_schema_list(self) -> "List[FoundryFieldSchema]":
        return self._field_schema_list

    @property
    def data_frame_reader_class(self) -> str:
        return self._data_frame_reader_class

    @property
    def custom_metadata(self) -> Dict[str, Any]:
        return self._custom_metadata

    @property
    def primary_key(self) -> "Optional[PrimaryKey]":
        return self._primary_key


class PrimaryKey(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "columns": ConjureFieldDefinition("columns", ListType(str)),
        }

    __slots__ = ["_columns"]

    def __init__(self, columns: List[str]):
        self._columns = columns

    @property
    def columns(self) -> List[str]:
        return self._columns


class FoundryFieldSchema(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "type": ConjureFieldDefinition("type", FoundryFieldType),
            "name": ConjureFieldDefinition("name", OptionalType(str)),
            "nullable": ConjureFieldDefinition("nullable", OptionalType(bool)),
            "user_defined_type_class": ConjureFieldDefinition(
                "userDefinedTypeClass", OptionalType(str)
            ),
            "custom_metadata": ConjureFieldDefinition(
                "customMetadata", DictType(str, Any)  # type: ignore
            ),
            "array_subtype": ConjureFieldDefinition(
                "arraySubtype",
                OptionalType(FoundryFieldSchema),
            ),
            "precision": ConjureFieldDefinition("precision", OptionalType(int)),
            "scale": ConjureFieldDefinition("scale", OptionalType(int)),
            "map_key_type": ConjureFieldDefinition(
                "mapKeyType", OptionalType(FoundryFieldSchema)
            ),
            "map_value_type": ConjureFieldDefinition(
                "mapValueType",
                OptionalType(FoundryFieldSchema),
            ),
            "sub_schemas": ConjureFieldDefinition(
                "subSchemas",
                ListType(FoundryFieldSchema),
            ),
        }

    __slots__ = [
        "_type",
        "_name",
        "_nullable",
        "_user_defined_type_class",
        "_custom_metadata",
        "_array_subtype",
        "_precision",
        "_scale",
        "_map_key_type",
        "_map_value_type",
        "_sub_schemas",
    ]

    def __init__(
        self,
        custom_metadata: Dict[str, Any],
        field_type: "FoundryFieldType",
        array_subtype: "FoundryFieldSchema" = None,
        map_key_type: "FoundryFieldSchema" = None,
        map_value_type: "FoundryFieldSchema" = None,
        name: str = None,
        nullable: bool = None,
        precision: int = None,
        scale: int = None,
        sub_schemas: "List[FoundryFieldSchema]" = None,
        user_defined_type_class: str = None,
    ):
        self._type = field_type
        self._name = name
        self._nullable = nullable
        self._user_defined_type_class = user_defined_type_class
        self._custom_metadata = custom_metadata
        self._array_subtype = array_subtype
        self._precision = precision
        self._scale = scale
        self._map_key_type = map_key_type
        self._map_value_type = map_value_type
        self._sub_schemas = sub_schemas

    @property
    def type(self) -> "FoundryFieldType":
        return self._type

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def nullable(self) -> Optional[bool]:
        return self._nullable

    @property
    def user_defined_type_class(self) -> Optional[str]:
        return self._user_defined_type_class

    @property
    def custom_metadata(self) -> Dict[str, Any]:
        return self._custom_metadata

    @property
    def array_subtype(self) -> "Optional[FoundryFieldSchema]":
        return self._array_subtype

    @property
    def precision(self) -> Optional[int]:
        return self._precision

    @property
    def scale(self) -> Optional[int]:
        return self._scale

    @property
    def map_key_type(self) -> "Optional[FoundryFieldSchema]":
        return self._map_key_type

    @property
    def map_value_type(self) -> "Optional[FoundryFieldSchema]":
        return self._map_value_type

    @property
    def sub_schemas(self) -> "Optional[List[FoundryFieldSchema]]":
        return self._sub_schemas


class FoundryFieldType(ConjureEnumType):

    ARRAY = "ARRAY"
    """ARRAY"""
    DECIMAL = "DECIMAL"
    """DECIMAL"""
    MAP = "MAP"
    """MAP"""
    STRUCT = "STRUCT"
    """STRUCT"""
    LONG = "LONG"
    """LONG"""
    BINARY = "BINARY"
    """BINARY"""
    BOOLEAN = "BOOLEAN"
    """BOOLEAN"""
    BYTE = "BYTE"
    """BYTE"""
    DATE = "DATE"
    """DATE"""
    DOUBLE = "DOUBLE"
    """DOUBLE"""
    FLOAT = "FLOAT"
    """FLOAT"""
    INTEGER = "INTEGER"
    """INTEGER"""
    SHORT = "SHORT"
    """SHORT"""
    STRING = "STRING"
    """STRING"""
    TIMESTAMP = "TIMESTAMP"
    """TIMESTAMP"""
    UNKNOWN = "UNKNOWN"
    """UNKNOWN"""

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


class Attribution(ConjureBeanType):
    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "user_id": ConjureFieldDefinition("userId", str),
            "time": ConjureFieldDefinition("time", str),
        }

    __slots__ = ["_user_id", "_time"]

    def __init__(self, time: str, user_id: str):
        self._user_id = user_id
        self._time = time

    @property
    def user_id(self) -> str:
        return self._user_id

    @property
    def time(self) -> str:
        return self._time
