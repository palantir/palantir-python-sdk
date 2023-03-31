from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class InvalidPageSize(PalantirRPCException):
    """The provided page size was zero or negative.

    Page sizes must be greater than zero.
    """

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
