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

import os
from pathlib import Path
from unittest import mock

from expects import expect, equal, be_a, raise_error, be_none

from palantir.core.config import (
    Config,
    ConfigFileHostnameProvider,
    ConfigLoader,
    AuthToken,
    HostnameProviderChain,
    StaticHostnameProvider,
    EnvironmentHostnameProvider,
    EnvironmentTokenProvider,
    StaticTokenProvider,
    ConfigFileTokenProvider,
    TokenProviderChain,
)


class TestConfigLoader:
    def test_load_config(self):
        config_loader = ConfigLoader(
            path=Path(os.path.join(os.path.dirname(__file__)), "data/test_config")
        )

        expected = Config(
            hostname="sample.palantirfoundry.com",
            token=AuthToken("sample-token"),
        )
        actual = config_loader.load_config()
        expect(actual).to(be_a(Config))
        expect(actual).to(equal(expected))

    def test_load_config_missing_namespace(self):
        config_loader = ConfigLoader(
            namespace="missing",
            path=Path(os.path.join(os.path.dirname(__file__)), "data/test_config"),
        )

        expect(lambda: config_loader.load_config()).to(
            raise_error(
                ValueError,
                "did not find 'missing' namespace in palantir configuration file",
            )
        )

    def test_load_config_invalid_file(self):
        config_loader = ConfigLoader(
            path=Path(os.path.join(os.path.dirname(__file__)), "data/test_config_bad")
        )
        expect(lambda: config_loader.load_config()).to(
            raise_error(
                ValueError,
                "did not find 'default' namespace in palantir configuration file",
            )
        )


class TestStaticHostnameProvider:
    def test_get(self):
        provider = StaticHostnameProvider("token")
        expect(provider.get()).to(equal("token"))

    def test_equality(self):
        expect(StaticHostnameProvider("token")).to(
            equal(StaticHostnameProvider("token"))
        )
        expect(StaticHostnameProvider("token")).not_to(
            equal(StaticHostnameProvider("other-token"))
        )


class TestEnvironmentHostnameProvider:
    def test_get(self):
        provider = EnvironmentHostnameProvider()
        with mock.patch.dict(os.environ, {"PALANTIR_HOSTNAME": "hostname"}, clear=True):
            expect(provider.get()).to(equal("hostname"))

    def test_get_unset(self):
        provider = EnvironmentHostnameProvider()
        with mock.patch.dict(os.environ, {}, clear=True):
            expect(lambda: provider.get()).to(
                raise_error(ValueError, "$PALANTIR_HOSTNAME is unset or empty")
            )

    def test_try_get_unset(self):
        provider = EnvironmentHostnameProvider()
        with mock.patch.dict(os.environ, {}, clear=True):
            expect(provider.try_get()).to(be_none)

    def test_get_custom(self):
        provider = EnvironmentHostnameProvider("CUSTOM_HOSTNAME")
        with mock.patch.dict(os.environ, {"CUSTOM_HOSTNAME": "hostname"}, clear=True):
            expect(provider.get()).to(equal("hostname"))

    def test_equality(self):
        expect(EnvironmentHostnameProvider()).to(equal(EnvironmentHostnameProvider()))
        expect(EnvironmentHostnameProvider("custom")).to(
            equal(EnvironmentHostnameProvider("custom"))
        )
        expect(EnvironmentHostnameProvider("custom")).not_to(
            equal(EnvironmentHostnameProvider("different-custom"))
        )


class TestConfigFileHostnameProvider:
    def test_get(self):
        provider = ConfigFileHostnameProvider(
            ConfigLoader(
                path=Path(os.path.join(os.path.dirname(__file__)), "data/test_config")
            )
        )
        expect(provider.get()).to(equal("sample.palantirfoundry.com"))

    def test_try_get_unset(self):
        provider = ConfigFileHostnameProvider(
            ConfigLoader(
                path=Path(
                    os.path.join(os.path.dirname(__file__)), "data/test_config_empty"
                )
            )
        )
        expect(provider.try_get()).to(be_none)


class TestHostnameProviderChain:
    def test_get(self):
        config_loader = ConfigLoader(
            path=Path(
                os.path.join(os.path.dirname(__file__)), "data/missing_config_file"
            )
        )
        provider = HostnameProviderChain(
            ConfigFileHostnameProvider(config_loader),
            StaticHostnameProvider("fallback"),
        )
        expected = "fallback"
        actual = provider.get()
        expect(actual).to(equal(expected))

    def test_get_no_value(self):
        provider = HostnameProviderChain(StaticHostnameProvider(None))
        expect(lambda: provider.get()).to(
            raise_error(ValueError, "No configured hostname found.")
        )


class TestStaticTokenProvider:
    def test_get(self):
        provider = StaticTokenProvider(AuthToken("token"))
        expect(provider.get()).to(equal("token"))

    def test_equality(self):
        expect(StaticTokenProvider(AuthToken("token"))).to(
            equal(StaticTokenProvider(AuthToken("token")))
        )
        expect(StaticTokenProvider(AuthToken("token"))).not_to(
            equal(StaticTokenProvider(AuthToken("other-token")))
        )


class TestEnvironmentTokenProvider:
    def test_get(self):
        provider = EnvironmentTokenProvider()
        with mock.patch.dict(os.environ, {"PALANTIR_TOKEN": "token"}, clear=True):
            expect(provider.get()).to(equal("token"))

    def test_get_unset(self):
        provider = EnvironmentTokenProvider()
        with mock.patch.dict(os.environ, {}, clear=True):
            expect(lambda: provider.get()).to(
                raise_error(ValueError, "$PALANTIR_TOKEN is unset or empty")
            )

    def test_try_get_unset(self):
        provider = EnvironmentTokenProvider()
        with mock.patch.dict(os.environ, {}, clear=True):
            expect(provider.try_get()).to(be_none)

    def test_get_custom(self):
        provider = EnvironmentTokenProvider("CUSTOM_TOKEN")
        with mock.patch.dict(os.environ, {"CUSTOM_TOKEN": "token"}, clear=True):
            expect(provider.get()).to(equal("token"))

    def test_equality(self):
        expect(EnvironmentTokenProvider()).to(equal(EnvironmentTokenProvider()))
        expect(EnvironmentTokenProvider("custom")).to(
            equal(EnvironmentTokenProvider("custom"))
        )
        expect(EnvironmentTokenProvider("custom")).not_to(
            equal(EnvironmentTokenProvider("different-custom"))
        )


class TestConfigFileTokenProvider:
    def test_get(self):
        provider = ConfigFileTokenProvider(
            ConfigLoader(
                path=Path(os.path.join(os.path.dirname(__file__)), "data/test_config")
            )
        )
        expect(provider.get()).to(equal("sample-token"))

    def test_try_get_unset(self):
        provider = ConfigFileTokenProvider(
            ConfigLoader(
                path=Path(
                    os.path.join(os.path.dirname(__file__)), "data/test_config_empty"
                )
            )
        )
        expect(provider.try_get()).to(be_none)


class TestTokenProviderChain:
    def test_get(self):
        config_loader = ConfigLoader(
            path=Path(
                os.path.join(os.path.dirname(__file__)), "data/missing_config_file"
            )
        )
        provider = TokenProviderChain(
            ConfigFileTokenProvider(config_loader),
            StaticTokenProvider(AuthToken("fallback")),
        )
        expected = "fallback"
        actual = provider.get()
        expect(actual).to(equal(expected))

    def test_get_no_value(self):
        provider = HostnameProviderChain(StaticHostnameProvider(None))
        expect(lambda: provider.get()).to(
            raise_error(ValueError, "No configured hostname found.")
        )
