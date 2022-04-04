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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import NewType, Optional

import tomli

AuthToken = NewType("AuthToken", str)  # TODO(ahiggins): is a custom type worth it?


@dataclass(frozen=True)
class Config:
    hostname: Optional[str] = None
    token: "Optional[AuthToken]" = None


class ConfigLoader:
    def __init__(self, namespace: str = None, path: Path = None):
        self.namespace = namespace or "default"
        self.path = path or Path.home() / ".palantir" / "config"

    def load_config(self) -> Config:
        with open(self.path, "rb") as file:
            raw_config = tomli.load(file).get(self.namespace)
            if raw_config is None:
                raise ValueError(
                    f"did not find '{self.namespace}' namespace in palantir configuration file"
                )
            return Config(
                hostname=raw_config.get("hostname"), token=raw_config.get("token")
            )

    def __eq__(self, other):
        return other is self or (
            isinstance(other, ConfigLoader)
            and other.namespace == self.namespace
            and other.path == self.path
        )


class HostnameProvider(ABC):
    @abstractmethod
    def get(self) -> str:
        pass

    def try_get(self) -> Optional[str]:
        return self.get()


class StaticHostnameProvider(HostnameProvider):
    def __init__(self, hostname: str):
        self.hostname = hostname

    def get(self) -> str:
        return self.hostname

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, StaticHostnameProvider)
            and other.hostname == self.hostname
        )


class EnvironmentHostnameProvider(HostnameProvider):
    def __init__(self, env_var: str = None):
        self.env_var = env_var or "PALANTIR_HOSTNAME"

    def get(self) -> str:
        hostname = self.try_get()
        if hostname:
            return hostname
        raise ValueError(f"${self.env_var} is unset or empty")

    def try_get(self) -> Optional[str]:
        return os.environ.get(self.env_var)

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, EnvironmentHostnameProvider)
            and other.env_var == self.env_var
        )


class ConfigFileHostnameProvider(HostnameProvider):
    def __init__(self, config_loader: "ConfigLoader" = None):
        self.config_loader = config_loader or ConfigLoader()

    def get(self) -> str:
        hostname = self.try_get()
        if hostname:
            return hostname
        raise ValueError("hostname attribute not found in config file")

    def try_get(self) -> Optional[str]:
        try:
            return self.config_loader.load_config().hostname
        except FileNotFoundError:
            return None

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, ConfigFileHostnameProvider)
            and other.config_loader == self.config_loader
        )


class HostnameProviderChain(HostnameProvider):
    def __init__(self, *args: HostnameProvider):
        self.providers = args

    def get(self) -> str:
        try:
            return next(
                val
                for val in (provider.try_get() for provider in self.providers)
                if val is not None
            )
        except StopIteration as exc:
            raise ValueError("No configured hostname found.") from exc

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, HostnameProviderChain)
            and other.providers == self.providers
        )


class DefaultHostnameProviderChain(HostnameProviderChain):
    def __init__(self):
        super().__init__(EnvironmentHostnameProvider(), ConfigFileHostnameProvider())


class TokenProvider(ABC):
    @abstractmethod
    def get(self) -> AuthToken:
        pass

    def try_get(self) -> Optional[AuthToken]:
        return self.get()


class StaticTokenProvider(TokenProvider):
    def __init__(self, token: AuthToken):
        self.token = token

    def get(self) -> AuthToken:
        return self.token

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, StaticTokenProvider) and other.token == self.token
        )


class ConfigFileTokenProvider(TokenProvider):
    def __init__(self, config_loader: "ConfigLoader" = None):
        self.config_loader = config_loader or ConfigLoader()

    def get(self) -> AuthToken:
        token = self.try_get()
        if token:
            return token
        raise ValueError("token attribute not found in config file")

    def try_get(self):
        try:
            return self.config_loader.load_config().token
        except FileNotFoundError:
            return None

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, ConfigFileTokenProvider)
            and other.config_loader == self.config_loader
        )


class EnvironmentTokenProvider(TokenProvider):
    def __init__(self, env_var: str = None):
        self.env_var = env_var or "PALANTIR_TOKEN"

    def get(self) -> AuthToken:
        token = self.try_get()
        if token:
            return token
        raise ValueError(f"${self.env_var} is unset or empty")

    def try_get(self) -> Optional[AuthToken]:
        token = os.environ.get(self.env_var)
        return AuthToken(token) if token else None

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, EnvironmentTokenProvider)
            and other.env_var == self.env_var
        )


class TokenProviderChain(TokenProvider):
    def __init__(self, *args: TokenProvider):
        self.providers = args

    def get(self) -> AuthToken:
        try:
            return next(
                val
                for val in (provider.try_get() for provider in self.providers)
                if val is not None
            )
        except StopIteration as exc:
            raise ValueError("No configured token found.") from exc

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, TokenProviderChain) and other.providers == self.providers
        )


class DefaultTokenProviderChain(TokenProviderChain):
    def __init__(self):
        super().__init__(EnvironmentTokenProvider(), ConfigFileTokenProvider())
