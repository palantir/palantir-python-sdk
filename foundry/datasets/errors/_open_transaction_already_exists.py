from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class OpenTransactionAlreadyExists(PalantirRPCException):
    """A transaction is already open on this dataset and branch.

    A branch of a dataset can only have one open transaction at a time.
    """

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
