from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class CreateBranchRequest:
    def __init__(
        self, branch_id: str, transaction_rid: Optional[str] = None, session: Optional[PalantirSession] = None
    ) -> None:
        self._session = session
        self._branch_id = branch_id
        self._transaction_rid = transaction_rid

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @property
    def transaction_rid(self) -> Optional[str]:
        return self._transaction_rid

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._branch_id == other._branch_id
            and (self._transaction_rid == other._transaction_rid)
        )

    def __repr__(self) -> str:
        return f"CreateBranchRequest(branch_id={self.branch_id}, transaction_rid={self.transaction_rid})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"branchId": self._branch_id, "transactionRid": self._transaction_rid if self._transaction_rid else None}
