from foundry.core.api import PalantirSession
from foundry.datasets.branch import BranchService
from foundry.datasets.dataset import DatasetService
from foundry.datasets.file import FileService
from foundry.datasets.transaction import TransactionService


class Datasets:
    def __init__(self, session: PalantirSession) -> None:
        self.session = session

    @property
    def Dataset(self) -> DatasetService:
        return DatasetService(self.session)

    @property
    def Branch(self) -> BranchService:
        return BranchService(self.session)

    @property
    def Transaction(self) -> TransactionService:
        return TransactionService(self.session)

    @property
    def File(self) -> FileService:
        return FileService(self.session)
