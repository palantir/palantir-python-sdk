#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
import os
from typing import Tuple

import foundry
from foundry.core.api import PalantirSession
from foundry.errors import EnvironmentNotConfigured


class FoundryClient:
    """A client to perform operations on top of Foundry's Public APIs. It will first look for
    :hostname: and :token: in the provided arguments. If it cannot find both, the client will look for then look
    for those two value them in environment variables "FOUNDRY_HOSTNAME"
    and "FOUNDRY_TOKEN".

    :param hostname: Optional string for the hostname of the Foundry endpoints
    :param token: Optional credentials string with which to authenticate.
    """

    hostname_var = "FOUNDRY_HOSTNAME"
    token_var = "FOUNDRY_TOKEN"

    def __init__(self, hostname=None, token=None, preview=False) -> None:
        if hostname is not None and token is not None:
            self.hostname = hostname
            self.token = token
        else:
            self.hostname, self.token = self._init_from_env()

        self.session = PalantirSession(self.hostname, preview)
        headers = {
            "Authorization": "Bearer " + self.token,
            "User-Agent": (
                f"palantir-python-sdk-codegen/{foundry.__codegen_version__} foundry-api/{foundry.__version__}"
            ),
        }
        self.session.headers.update(headers)

    def _init_from_env(self) -> Tuple[str, str]:
        hostname = os.environ.get(self.hostname_var)
        token = os.environ.get(self.token_var)

        if hostname is None:
            raise EnvironmentNotConfigured(
                f"Unable to configure client: ${self.hostname_var} not found in environment."
            )

        if token is None:
            raise EnvironmentNotConfigured(f"Unable to configure client: ${self.token_var} not found in environment.")

        return hostname, token

    @property
    def datasets(self):
        from foundry.datasets import Datasets

        return Datasets(self.session)
