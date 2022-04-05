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

from conjure_python_client import RequestsClient, ServiceConfiguration, Service
from typing import TypeVar, Type, Any
from palantir._version import __version__

ServiceT = TypeVar("ServiceT", bound=Service)
USER_AGENT = [("palantir-python-sdk", __version__)]


def get_user_agent() -> str:
    return " ".join(f"{name}/{version}" for name, version in USER_AGENT)


class ConjureClient:
    def service(self, service: Type[ServiceT], uri: str) -> ServiceT:
        config = ServiceConfiguration()
        config.uris = [uri]
        return RequestsClient.create(service, get_user_agent(), config)


def _is_collection(arg: Any, item_type: Type = object) -> bool:
    return isinstance(arg, (list, set)) and (
        len(arg) == 0 or all(isinstance(i, item_type) for i in arg)
    )
