from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class InvalidTransactionType(PalantirRPCException):
    """The given transaction type is not valid.

    Valid transaction types are `SNAPSHOT`, `UPDATE`, `APPEND`, and
    `DELETE`.
    """

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
