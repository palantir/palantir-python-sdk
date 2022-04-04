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

from .config import (
    DefaultTokenProviderChain,
    DefaultHostnameProviderChain,
    StaticTokenProvider,
    StaticHostnameProvider,
    AuthToken,
)
from .types import PalantirContext


def context(hostname: str = None, token: str = None) -> PalantirContext:
    """
    Creates a new :class:`PalantirContext` object with hard coded values for `hostname` and `token`.

    If `hostname` is not specified then a default hostname provider chain will use, in order:
        1) The value of the `PALANTIR_HOSTNAME` environment variable
        2) The value of the `hostname` attribute in the default block of `~/.palantir/config`

    If `token` is not specified then a default token provider chain will use, in order:
        1) The value of the `PALANTIR_HOSTNAME` environment variable
        2) The value of the `token` attribute in the default block of `~/.palantir/config`

    Args:
        hostname: The hostname of the Palantir instance. E.g. `example.palantirfoundry.com`.
        token: A Palantir Bearer Token.

    Returns: A :class:`PalantirContext` object that can be passed to the `ctx` argument of many sdk functions.
    """
    return PalantirContext(
        hostname=(
            StaticHostnameProvider(hostname)
            if hostname
            else DefaultHostnameProviderChain()
        ),
        auth=(
            StaticTokenProvider(AuthToken(token))
            if token
            else DefaultTokenProviderChain()
        ),
    )
