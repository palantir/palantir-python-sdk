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
    ConjureDecoder,
    OptionalType,
    ConjureBeanType,
    ConjureFieldDefinition,
)
from typing import Optional, Dict, Any


class PathService(Service):
    def get_resource_by_path(
        self, auth_header: str, path: Optional[str] = None
    ) -> "Optional[DecoratedResource]":
        _headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Authorization": auth_header,
        }

        _params: Dict[str, Any] = {
            "path": path,
            "decoration": [],
        }

        _path_params: Dict[str, Any] = {}

        _json = None

        _path = "/resources"
        _path = _path.format(**_path_params)

        _response = self._request(
            "GET", self._uri + _path, params=_params, headers=_headers, json=_json
        )

        _decoder = ConjureDecoder()
        return (
            None
            if _response.status_code == 204
            else _decoder.decode(_response.json(), OptionalType(DecoratedResource))
        )


class DecoratedResource(ConjureBeanType):
    """
    Decorations not included.
    """

    @classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            "rid": ConjureFieldDefinition("rid", str),
        }

    __slots__ = [
        "_rid",
    ]

    def __init__(
        self,
        rid: str,
    ):
        self._rid = rid

    @property
    def rid(self):
        return self._rid
