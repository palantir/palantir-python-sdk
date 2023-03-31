from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class CreateDatasetPermissionDenied(PalantirRPCException):
    """The provided token does not have permission to create a dataset in this
    folder."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
