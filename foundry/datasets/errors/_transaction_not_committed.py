from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class TransactionNotCommitted(PalantirRPCException):
    """The given transaction has not been committed."""

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
