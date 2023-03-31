from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class InvalidBranchId(PalantirRPCException):
    """The requested branch name cannot be used.

    Branch names cannot be empty and must not look like RIDs or UUIDs.
    """

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
