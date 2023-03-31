from typing import TYPE_CHECKING, Any, Iterable, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping

    from foundry.types._struct_field import StructField


class StructType:
    def __init__(self, fields: Iterable["StructField"], session: Optional[PalantirSession] = None) -> None:
        self._session = session
        self._fields = fields

    @property
    def fields(self) -> Iterable["StructField"]:
        return self._fields

    def __eq__(self, other: Any) -> bool:
        return self is other or (type(self) == type(other) and self._fields == other._fields)

    def __repr__(self) -> str:
        return f"StructType(fields={self.fields})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"fields": [field._asdict() for field in self._fields]}
