from typing import TYPE_CHECKING, Any, Iterable, Optional, Union

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping

    from foundry.types._column_constraint import ColumnConstraint


class TabularType:
    def __init__(
        self, constraints: Iterable[Union["ColumnConstraint",]], session: Optional[PalantirSession] = None
    ) -> None:
        self._session = session
        self._constraints = constraints

    @property
    def constraints(self) -> Iterable[Union["ColumnConstraint",]]:
        return self._constraints

    def __eq__(self, other: Any) -> bool:
        return self is other or (type(self) == type(other) and self._constraints == other._constraints)

    def __repr__(self) -> str:
        return f"TabularType(constraints={self.constraints})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"constraints": [field._asdict() for field in self._constraints]}
