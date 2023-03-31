from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class File:
    def __init__(
        self,
        path: str,
        transaction_rid: str,
        updated_time: datetime,
        size_bytes: Optional[int] = None,
        session: Optional[PalantirSession] = None,
    ) -> None:
        self._session = session
        self._path = path
        self._transaction_rid = transaction_rid
        self._size_bytes = size_bytes
        self._updated_time = updated_time

    @property
    def path(self) -> str:
        return self._path

    @property
    def transaction_rid(self) -> str:
        return self._transaction_rid

    @property
    def size_bytes(self) -> Optional[int]:
        return self._size_bytes

    @property
    def updated_time(self) -> datetime:
        return self._updated_time

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._path == other._path
            and (self._transaction_rid == other._transaction_rid)
            and (self._size_bytes == other._size_bytes)
            and (self._updated_time == other._updated_time)
        )

    def __repr__(self) -> str:
        return (
            f"File(path={self.path}, transaction_rid={self.transaction_rid}, size_bytes={self.size_bytes},"
            f" updated_time={self.updated_time})"
        )

    def _asdict(self) -> "Mapping[str, Any]":
        return {
            "path": self._path,
            "transactionRid": self._transaction_rid,
            "sizeBytes": self._size_bytes,
            "updatedTime": self._updated_time,
        }
