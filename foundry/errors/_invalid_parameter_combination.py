from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class InvalidParameterCombination(PalantirRPCException):
    """The given parameters are individually valid but cannot be used in the
    given combination."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
