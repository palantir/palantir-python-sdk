from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class Dataset:
    def __init__(self, rid: str, name: str, parent_folder_rid: str, session: Optional[PalantirSession] = None) -> None:
        self._session = session
        self._rid = rid
        self._name = name
        self._parent_folder_rid = parent_folder_rid

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent_folder_rid(self) -> str:
        return self._parent_folder_rid

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._rid == other._rid
            and (self._name == other._name)
            and (self._parent_folder_rid == other._parent_folder_rid)
        )

    def __repr__(self) -> str:
        return f"Dataset(rid={self.rid}, name={self.name}, parent_folder_rid={self.parent_folder_rid})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"rid": self._rid, "name": self._name, "parentFolderRid": self._parent_folder_rid}
