from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class BranchAlreadyExists(PalantirRPCException):
    """The branch cannot be created because a branch with that name already
    exists."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
