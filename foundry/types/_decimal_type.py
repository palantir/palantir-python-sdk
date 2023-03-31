from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class DecimalType:
    def __init__(
        self, precision: Optional[int] = None, scale: Optional[int] = None, session: Optional[PalantirSession] = None
    ) -> None:
        self._session = session
        self._precision = precision
        self._scale = scale

    @property
    def precision(self) -> Optional[int]:
        return self._precision

    @property
    def scale(self) -> Optional[int]:
        return self._scale

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other) and self._precision == other._precision and (self._scale == other._scale)
        )

    def __repr__(self) -> str:
        return f"DecimalType(precision={self.precision}, scale={self.scale})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"precision": self._precision, "scale": self._scale}
