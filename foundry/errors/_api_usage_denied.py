from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class ApiUsageDenied(PalantirRPCException):
    """You are not allowed to use Palantir APIs."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
