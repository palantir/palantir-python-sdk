from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class ColumnTypesNotSupported(PalantirRPCException):
    """The dataset contains column types that are not supported."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
