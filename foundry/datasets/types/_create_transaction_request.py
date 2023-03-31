from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping

    from foundry.datasets.types._transaction_type import TransactionType


class CreateTransactionRequest:
    def __init__(
        self, transaction_type: Optional["TransactionType"] = None, session: Optional[PalantirSession] = None
    ) -> None:
        self._session = session
        self._transaction_type = transaction_type

    @property
    def transaction_type(self) -> Optional["TransactionType"]:
        return self._transaction_type

    def __eq__(self, other: Any) -> bool:
        return self is other or (type(self) == type(other) and self._transaction_type == other._transaction_type)

    def __repr__(self) -> str:
        return f"CreateTransactionRequest(transaction_type={self.transaction_type})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"transactionType": self._transaction_type if self._transaction_type else None}
