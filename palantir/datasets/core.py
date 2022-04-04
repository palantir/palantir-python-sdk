#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import io
from datetime import datetime
from typing import Generator, Union, Tuple, TYPE_CHECKING, Optional

from palantir.core import context
from palantir.core.types import ResourceIdentifier
from palantir.datasets.client import DatasetsClient, DatasetServices
from palantir.datasets.errors import TransactionAbortedError
from palantir.datasets.schema import pandas_to_foundry_schema
from palantir.datasets.types import (
    FileLocator,
    TransactionType,
    TransactionStatus,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from palantir.datasets.types import DatasetLocator


class Dataset:
    """A reference to a Foundry Dataset, resolved to a branch and view (i.e. Transaction Range)."""

    def __init__(self, client: "DatasetsClient", locator: "DatasetLocator"):
        self.client = client
        self.locator = locator

    @property
    def rid(self):
        """Returns The globally unique Resource Identifier for the Dataset."""
        return self.locator.rid

    @property
    def branch(self):
        """Returns The Branch Id to be used for read/write operations."""
        return self.locator.branch_id

    @property
    def view(self) -> Tuple[Optional[ResourceIdentifier], Optional[ResourceIdentifier]]:
        """Returns The view (i.e. Transaction Range) that this Dataset object is bound to."""
        return (
            self.locator.start_transaction_rid,
            self.locator.end_transaction_rid,
        )

    def list_files(self, path: str = None) -> Generator["File", None, None]:
        """
        Lists the files in the Dataset for the :prop:`view`.

        Args:
            path: An optional path prefix to use to filter when listing files.

        Returns: A generator over pages of :class:`File` objects in the current view and branch.
        """
        return self.client.list_files(dataset=self, path=path)

    def file(self, file_ref: str) -> "File":
        """
        Creates a new :class:`File` object representing a File within a dataset.
        The file need not already exist, the returned object can be used to create a new File within the Dataset. If the
        file is written to then a new transaction will be created on the Dataset with the new file contents.

        Args:
            file_ref: The path to a File within the Dataset view.

        Returns:
            A :class:`File` object referencing a File within a Dataset.
        """
        return File(
            dataset=self,
            path=file_ref,
            client=self.client,
        )

    def read_arrow(self) -> "pa.Table":
        """
        Returns: The full content of the Dataset at the current view as an Apache Arrow :class:`pa.Table`. The dataset
        must have a schema and be tabular or this method will raise an Error.
        """
        return self.client.read_dataset(self.locator)

    def read_pandas(self) -> "pd.DataFrame":
        """
        Returns: The full content of the Dataset at the current view as a Pandas :class:`pd.DataFrame`. The dataset
        must have a schema and be tabular or this method will raise an Error.
        """
        return self.read_arrow().to_pandas()

    def write_pandas(self, df: "pd.DataFrame") -> None:
        """
        Writes the content of the provided DataFrame to a new Snapshot transaction in the Dataset. Uses parquet as a
        serialization format. Updates the schema of the Dataset based on the type information of the DataFrame.

        Args:
            df: a Pandas :class:`pd.DataFrame`
        """
        with io.BytesIO() as buf:
            df.to_parquet(buf)
            buf.seek(0)
            self.file("dataframe.parquet").write(buf.read(), TransactionType.SNAPSHOT)
        self.client.put_schema(self, pandas_to_foundry_schema(df))

    def start_transaction(
        self, txn_type: Union[str, TransactionType] = None
    ) -> "Transaction":
        """
        Starts a new Transaction on the Dataset. Transactions can be used as a context manager to automatically commit
        the transaction (or abort, in the case of a failure).

        Args:
            txn_type: The type of transation to open. Defaults to `TransactionType.UPDATE`.

        Returns:
            A :class:`Transaction` object that can be used to manage the lifecyle of the Transaction.

        Examples:
            >>> from palantir.datasets import dataset
            >>> ds = dataset("/path/to/dataset")
            ... with ds.start_transaction():
            ...     ds.file("file.txt").write(b"file content")


            >>> ds = dataset("/path/to/dataset")
            ... with ds.start_transaction('SNAPSHOT'):
            ...     ds.file("file.txt").write(b"file content")
        """
        _txn_type = (
            txn_type
            if isinstance(txn_type, TransactionType)
            else (
                TransactionType(txn_type)
                if txn_type is not None
                else TransactionType.UPDATE
            )
        )
        return self.client.start_transaction(self, _txn_type)

    def update_view(
        self,
        transaction_range: Tuple[str, str] = None,
    ) -> None:
        """
        Updates the current dataset view. Retrieves the latest view when parameters are not specified.

        Args:
            transaction_range: A tuple containing a start and end transaction rid.
        """
        (start_transaction_rid, end_transaction_rid,) = (
            transaction_range
            if transaction_range is not None
            else self.client.get_transaction_range(self.rid, self.branch)
        )
        self.locator = self.locator.with_updated(
            start_transaction_rid=ResourceIdentifier.from_string(start_transaction_rid)
            if start_transaction_rid
            else None,
            end_transaction_rid=ResourceIdentifier.from_string(end_transaction_rid)
            if end_transaction_rid
            else None,
        )

    def __repr__(self):
        return f'Dataset(rid="{self.locator.rid}", branch="{self.locator.branch_id}")'


class Transaction:
    def __init__(
        self,
        dataset: "Dataset",
        rid: ResourceIdentifier,
        status: TransactionStatus,
        txn_type: TransactionType,
        client: "DatasetsClient",
    ):
        self.dataset = dataset
        self.rid = rid
        self.status = status
        self.txn_type = txn_type
        self.client = client or DatasetsClient(DatasetServices(context()))

    def commit(self) -> None:
        """Commits the open transaction, updates the attached :class:`Dataset` object's view range."""
        self.client.commit_transaction(self)
        self.status = TransactionStatus.COMMITTED
        if self.txn_type == TransactionType.SNAPSHOT:
            self.dataset.locator = self.dataset.locator.with_updated(
                start_transaction_rid=self.rid,
                end_transaction_rid=self.rid,
            )
        else:
            self.dataset.locator = self.dataset.locator.with_updated(
                end_transaction_rid=self.rid
            )

    def abort(self) -> None:
        """Aborts the open transaction."""
        self.client.abort_transaction(self)
        self.status = TransactionStatus.ABORTED

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            self.abort()
            raise TransactionAbortedError(self) from exc_val
        self.commit()

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, Transaction)
            and other.dataset == self.dataset
            and other.rid == self.rid
            and other.status == self.status
            and other.txn_type == self.txn_type
        )

    def __repr__(self) -> str:
        return f"Transaction(rid='{self.rid}', dataset_rid='{self.dataset.locator.rid}', type={self.txn_type}, status={self.status})"


class File:
    def __init__(
        self,
        dataset: Dataset,
        path: str,
        modified: datetime = None,
        transaction_rid: ResourceIdentifier = None,
        length: int = None,
        client: "DatasetsClient" = None,
    ):
        self.dataset = dataset
        self.path = path
        self.modified = modified
        self.transaction_rid = transaction_rid
        self.length = length
        self.client = client or DatasetsClient(DatasetServices(context()))

    def locator(self):
        return FileLocator(
            dataset_rid=self.dataset.rid,
            end_ref=str(self.transaction_rid)
            if self.transaction_rid
            else self.dataset.branch,
            logical_path=self.path,
        )

    def read(self) -> io.IOBase:
        """Returns: A binary stream of the file content."""
        return self.client.read_file(self.locator()).read()

    def write(
        self,
        content: bytes,
        txn_type: Union[str, TransactionType] = TransactionType.UPDATE,
    ):
        """
        Writes the specified content to the File in a new transaction. Automatically commits the transaction, updating
        the view on the parent :class:`Dataset` object.

        Args:
            content: Binary content to upload.
            txn_type: Transaction Type, Defaults to `TransactionType.UPDATE`.
        """
        with self.client.start_transaction(
            self.dataset,
            txn_type
            if isinstance(txn_type, TransactionType)
            else TransactionType[txn_type.upper()],
        ) as txn:
            self.client.put_file(
                self.locator().with_updated(end_ref=str(txn.rid)), content
            )

    def __repr__(self):
        return f'File(dataset_rid="{self.dataset.rid}", path="{self.path}")'

    def __eq__(self, other: object) -> bool:
        return other is self or (
            isinstance(other, File)
            and other.dataset == self.dataset
            and other.path == self.path
            and other.modified == self.modified
            and other.transaction_rid == self.transaction_rid
            and other.client == self.client
        )
