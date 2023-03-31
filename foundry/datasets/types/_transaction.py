from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping

    from foundry.datasets.types._transaction_status import TransactionStatus
    from foundry.datasets.types._transaction_type import TransactionType


class Transaction:
    """An operation that modifies the files within a dataset."""

    def __init__(
        self,
        rid: str,
        transaction_type: "TransactionType",
        status: "TransactionStatus",
        created_time: datetime,
        closed_time: Optional[datetime] = None,
        session: Optional[PalantirSession] = None,
    ) -> None:
        self._session = session
        self._rid = rid
        self._transaction_type = transaction_type
        self._status = status
        self._created_time = created_time
        self._closed_time = closed_time

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def transaction_type(self) -> "TransactionType":
        return self._transaction_type

    @property
    def status(self) -> "TransactionStatus":
        return self._status

    @property
    def created_time(self) -> datetime:
        return self._created_time

    @property
    def closed_time(self) -> Optional[datetime]:
        return self._closed_time

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._rid == other._rid
            and (self._transaction_type == other._transaction_type)
            and (self._status == other._status)
            and (self._created_time == other._created_time)
            and (self._closed_time == other._closed_time)
        )

    def __repr__(self) -> str:
        return (
            f"Transaction(rid={self.rid}, transaction_type={self.transaction_type}, status={self.status},"
            f" created_time={self.created_time}, closed_time={self.closed_time})"
        )

    def _asdict(self) -> "Mapping[str, Any]":
        return {
            "rid": self._rid,
            "transactionType": self._transaction_type,
            "status": self._status,
            "createdTime": self._created_time,
            "closedTime": self._closed_time,
        }
