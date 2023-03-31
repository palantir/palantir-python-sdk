from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class UploadFilePermissionDenied(PalantirRPCException):
    """The provided token does not have permission to upload the given file to
    the given dataset and transaction."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
