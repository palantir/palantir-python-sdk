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

import re
from dataclasses import dataclass
from re import Pattern
from typing import TYPE_CHECKING

from palantir.core.config import (
    AuthToken,
    HostnameProvider,
    TokenProvider,
)

if TYPE_CHECKING:
    from typing import Optional

_rid_pattern: Pattern = re.compile(
    "ri"
    "\\.(?P<service>[a-z][a-z0-9\\-]*)"
    "\\.(?P<instance>[a-z0-9][a-z0-9\\-]*)?"
    "\\.(?P<type>[a-z][a-z0-9\\-]*)"
    "\\.(?P<locator>[a-zA-Z0-9_\\-\\.]+)"
)


class PalantirContext:
    def __init__(
        self,
        hostname: HostnameProvider,
        auth: TokenProvider,
    ):
        self.hostname_provider = hostname
        self.token_provider = auth

    @property
    def hostname(self) -> str:
        return self.hostname_provider.get()

    @property
    def auth_token(self) -> AuthToken:
        return self.token_provider.get()

    def __eq__(self, other: object):
        return other is self or (
            isinstance(other, PalantirContext)
            and other.hostname_provider == self.hostname_provider
            and other.token_provider == self.token_provider
        )


@dataclass(frozen=True)
class ResourceIdentifier:
    service: str
    instance: str
    type: str
    locator: str

    def __str__(self):
        return f"ri.{self.service}.{self.instance}.{self.type}.{self.locator}"

    def __repr__(self):
        return str(self)

    @classmethod
    def try_parse(cls, value: str) -> "Optional[ResourceIdentifier]":
        """
        Returns a ResourceIdentifier object if the value can be parsed as a resource identifier.
        """
        try:
            return cls.from_string(value)
        except ValueError:
            return None

    @classmethod
    def from_string(cls, value: str) -> "ResourceIdentifier":
        """
        Returns a ResourceIdentifier object if the value can be parsed as a resource identifier
        or raises an error.
        """
        match = _rid_pattern.match(value)
        if match:
            return ResourceIdentifier(
                service=match.group("service"),
                instance=match.group("instance"),
                type=match.group("type"),
                locator=match.group("locator"),
            )
        raise ValueError("value could not be parsed as a ResourceIdentifier")
