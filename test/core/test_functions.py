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

from expects import equal, expect

from palantir.core import (
    context,
)
from palantir.core.config import (
    AuthToken,
    DefaultHostnameProviderChain,
    DefaultTokenProviderChain,
    StaticHostnameProvider,
    StaticTokenProvider,
)
from palantir.core.types import PalantirContext


class TestFunctions:
    def test_context(self):
        expected = PalantirContext(
            StaticHostnameProvider("hostname"),
            StaticTokenProvider(AuthToken("token")),
        )

        expect(context("hostname", "token")).to(equal(expected))

    def test_context_defaults(self):
        expected = PalantirContext(
            DefaultHostnameProviderChain(),
            DefaultTokenProviderChain(),
        )

        expect(context()).to(equal(expected))
