from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class InvalidPageToken(PalantirRPCException):
    """The provided page token could not be used to retrieve the next page of
    results."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
