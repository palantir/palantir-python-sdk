from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class FileAlreadyExists(PalantirRPCException):
    """The given file path already exists in the dataset and transaction."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
