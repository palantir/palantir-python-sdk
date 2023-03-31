from typing import TYPE_CHECKING, Any, Optional

from foundry.core.api import PalantirSession

if TYPE_CHECKING:
    from typing import Mapping


class CreateDatasetRequest:
    def __init__(self, name: str, parent_folder_rid: str, session: Optional[PalantirSession] = None) -> None:
        self._session = session
        self._name = name
        self._parent_folder_rid = parent_folder_rid

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent_folder_rid(self) -> str:
        return self._parent_folder_rid

    def __eq__(self, other: Any) -> bool:
        return self is other or (
            type(self) == type(other)
            and self._name == other._name
            and (self._parent_folder_rid == other._parent_folder_rid)
        )

    def __repr__(self) -> str:
        return f"CreateDatasetRequest(name={self.name}, parent_folder_rid={self.parent_folder_rid})"

    def _asdict(self) -> "Mapping[str, Any]":
        return {"name": self._name, "parentFolderRid": self._parent_folder_rid}
