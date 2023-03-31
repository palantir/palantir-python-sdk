from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class ResourceNameAlreadyExists(PalantirRPCException):
    """The provided resource name is already in use by another resource in the
    same folder."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
