from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class DateType:
    def __init__(self, session: Optional[PalantirSession] = None) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        return self is other or (type(self) == type(other))

    def __repr__(self) -> str:
        return "DateType"

    def _asdict(self) -> "Mapping[str, Any]":
        return {}
