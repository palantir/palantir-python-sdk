from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional
from urllib.parse import quote_plus

from foundry._response import convert_to_palantir_object
from foundry.core.api import PalantirSession
from foundry.datasets.types._file import File
from foundry.datasets.types._list_files_response import ListFilesResponse
from foundry.errors._helpers import check_for_errors
from foundry.types import PagedResponse

if TYPE_CHECKING:
    from foundry.datasets.types._transaction_type import TransactionType


class FileService:
    def __init__(self, session: PalantirSession) -> None:
        self.session = session

    def iterator(
        self,
        *,
        dataset_rid: str,
        branch_id: Optional[str] = None,
        start_transaction_rid: Optional[str] = None,
        end_transaction_rid: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None
    ) -> Iterable[File]:
        """Lists Files contained in a Dataset. By default files are listed on
        the latest view of the default.

        branch - master for most enrollments.
        Advanced Usage
        See Datasets Core Concepts for details on using branches and transactions.
        To list files on a specific Branch specify the Branch's identifier as branchId. This will include the most
        recent version of all files since the latest snapshot transaction, or the earliest ancestor transaction of the
        branch if there are no snapshot transactions.
        To list files on the resolved view of a transaction specify the Transaction's resource identifier
        as endTransactionRid. This will include the most recent version of all files since the latest snapshot
        transaction, or the earliest ancestor transaction if there are no snapshot transactions.
        To list files on the resolved view of a range of transactions specify the the start transaction's resource
        identifier as startTransactionRid and the end transaction's resource identifier as endTransactionRid. This
        will include the most recent version of all files since the startTransactionRid up to the endTransactionRid.
        Note that an intermediate snapshot transaction will remove all files from the view. Behavior is undefined when
        the start and end transactions do not belong to the same root-to-leaf path.
        To list files on a specific transaction specify the Transaction's resource identifier as both the
        startTransactionRid and endTransactionRid. This will include only files that were modified as part of that
        Transaction.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param start_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param end_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param page_size: The page size to use for the endpoint.
        :param page_token: The page token indicates where to start paging. This should be omitted from the first page's request.
        To fetch the next page, clients should take the value from the nextPageToken field of the previous response
        and populate the next request's pageToken field with it.
        :returns: Optional[File]
        """
        params: Dict[str, Any] = {
            "branchId": branch_id,
            "startTransactionRid": start_transaction_rid,
            "endTransactionRid": end_transaction_rid,
            "pageSize": page_size,
            "pageToken": page_token,
        }
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/files".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response)
        response_json = response.json()
        response_obj = ListFilesResponse(
            next_page_token=convert_to_palantir_object(
                Optional[str], response_json.get("nextPageToken"), session=self.session, paged_response=True
            ),
            data=convert_to_palantir_object(
                Iterable[File], response_json.get("data"), session=self.session, paged_response=True
            ),
        )
        return PagedResponse(
            paged_func=self.iterator,
            paged_type=File,
            response_obj=response_obj,
            dataset_rid=dataset_rid,
            branch_id=branch_id,
            start_transaction_rid=start_transaction_rid,
            end_transaction_rid=end_transaction_rid,
            page_size=page_size,
            page_token=page_token,
        )

    def upload(
        self,
        *,
        data: bytes,
        dataset_rid: str,
        file_path: str,
        branch_id: Optional[str] = None,
        transaction_type: Optional["TransactionType"] = None,
        transaction_rid: Optional[str] = None
    ) -> File:
        """Uploads a File to an existing Dataset. The body of the request must
        contain the binary content of the file and the Content-Type header must
        be application/octet-stream.

        By default the file is uploaded to a new transaction on the default branch - master for most enrollments.
        If the file already exists only the most recent version will be visible in the updated view.
        Advanced Usage
        See Datasets Core Concepts for details on using branches and transactions.
        To upload a file to a specific Branch specify the Branch's identifier as branchId. A new transaction will
        be created and committed on this branch. By default the TransactionType will be UPDATE, to override this
        default specify transactionType in addition to branchId.
        See createBranch to create a custom branch.
        To upload a file on a manually opened transaction specify the Transaction's resource identifier as
        transactionRid. This is useful for uploading multiple files in a single transaction.
        See createTransaction to open a transaction.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-write.

        :param data: Input bytes as part of the request body
        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param file_path: The path to a File within Foundry. Examples: my-file.txt, path/to/my-file.jpg, dataframe.snappy.parquet.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param transaction_type: The type of a Transaction.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: File
        """
        params: Dict[str, Any] = {
            "filePath": file_path,
            "branchId": branch_id,
            "transactionType": transaction_type,
            "transactionRid": transaction_rid,
        }
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        response = self.session.post(
            "https://{hostname}/api/v1/datasets/{datasetRid}/files:upload".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
            data=data,
        )
        check_for_errors(response)
        response_json = response.json()
        return File(
            path=convert_to_palantir_object(str, response_json.get("path"), session=self.session),
            transaction_rid=convert_to_palantir_object(str, response_json.get("transactionRid"), session=self.session),
            size_bytes=convert_to_palantir_object(Optional[int], response_json.get("sizeBytes"), session=self.session),
            updated_time=convert_to_palantir_object(datetime, response_json.get("updatedTime"), session=self.session),
        )

    def get_metadata(
        self,
        *,
        dataset_rid: str,
        file_path: str,
        branch_id: Optional[str] = None,
        start_transaction_rid: Optional[str] = None,
        end_transaction_rid: Optional[str] = None
    ) -> File:
        """Gets metadata about a File contained in a Dataset. By default this
        retrieves the file's metadata from the latest.

        view of the default branch - master for most enrollments.
        Advanced Usage
        See Datasets Core Concepts for details on using branches and transactions.
        To get a file's metadata from a specific Branch specify the Branch's identifier as branchId. This will
        retrieve metadata for the most recent version of the file since the latest snapshot transaction, or the earliest
        ancestor transaction of the branch if there are no snapshot transactions.
        To get a file's metadata from the resolved view of a transaction specify the Transaction's resource identifier
        as endTransactionRid. This will retrieve metadata for the most recent version of the file since the latest snapshot
        transaction, or the earliest ancestor transaction if there are no snapshot transactions.
        To get a file's metadata from the resolved view of a range of transactions specify the the start transaction's
        resource identifier as startTransactionRid and the end transaction's resource identifier as endTransactionRid.
        This will retrieve metadata for the most recent version of the file since the startTransactionRid up to the
        endTransactionRid. Behavior is undefined when the start and end transactions do not belong to the same root-to-leaf path.
        To get a file's metadata from a specific transaction specify the Transaction's resource identifier as both the
        startTransactionRid and endTransactionRid.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param file_path: The path to a File within Foundry. Examples: my-file.txt, path/to/my-file.jpg, dataframe.snappy.parquet.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param start_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param end_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: File
        """
        params: Dict[str, Any] = {
            "branchId": branch_id,
            "startTransactionRid": start_transaction_rid,
            "endTransactionRid": end_transaction_rid,
        }
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid), "filePath": quote_plus(file_path)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/files/{filePath}".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response)
        response_json = response.json()
        return File(
            path=convert_to_palantir_object(str, response_json.get("path"), session=self.session),
            transaction_rid=convert_to_palantir_object(str, response_json.get("transactionRid"), session=self.session),
            size_bytes=convert_to_palantir_object(Optional[int], response_json.get("sizeBytes"), session=self.session),
            updated_time=convert_to_palantir_object(datetime, response_json.get("updatedTime"), session=self.session),
        )

    def delete(
        self,
        *,
        dataset_rid: str,
        file_path: str,
        branch_id: Optional[str] = None,
        transaction_rid: Optional[str] = None
    ) -> None:
        """Deletes a File from a Dataset. By default the file is deleted in a
        new transaction on the default.

        branch - master for most enrollments. The file will still be visible on historical views.
        Advanced Usage
        See Datasets Core Concepts for details on using branches and transactions.
        To delete a File from a specific Branch specify the Branch's identifier as branchId. A new delete Transaction
        will be created and committed on this branch.
        To delete a File using a manually opened Transaction, specify the Transaction's resource identifier
        as transactionRid. The transaction must be of type DELETE. This is useful for deleting multiple files in a
        single transaction. See createTransaction to
        open a transaction.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-write.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param file_path: The path to a File within Foundry. Examples: my-file.txt, path/to/my-file.jpg, dataframe.snappy.parquet.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: None
        """
        params: Dict[str, Any] = {"branchId": branch_id, "transactionRid": transaction_rid}
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid), "filePath": quote_plus(file_path)}
        response = self.session.delete(
            "https://{hostname}/api/v1/datasets/{datasetRid}/files/{filePath}".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response, returns_json=False)

    def get_content(
        self,
        *,
        dataset_rid: str,
        file_path: str,
        branch_id: Optional[str] = None,
        start_transaction_rid: Optional[str] = None,
        end_transaction_rid: Optional[str] = None
    ) -> BytesIO:
        """Gets the content of a File contained in a Dataset. By default this
        retrieves the file's content from the latest.

        view of the default branch - master for most enrollments.
        Advanced Usage
        See Datasets Core Concepts for details on using branches and transactions.
        To get a file's content from a specific Branch specify the Branch's identifier as branchId. This will
        retrieve the content for the most recent version of the file since the latest snapshot transaction, or the
        earliest ancestor transaction of the branch if there are no snapshot transactions.
        To get a file's content from the resolved view of a transaction specify the Transaction's resource identifier
        as endTransactionRid. This will retrieve the content for the most recent version of the file since the latest
        snapshot transaction, or the earliest ancestor transaction if there are no snapshot transactions.
        To get a file's content from the resolved view of a range of transactions specify the the start transaction's
        resource identifier as startTransactionRid and the end transaction's resource identifier as endTransactionRid.
        This will retrieve the content for the most recent version of the file since the startTransactionRid up to the
        endTransactionRid. Note that an intermediate snapshot transaction will remove all files from the view. Behavior
        is undefined when the start and end transactions do not belong to the same root-to-leaf path.
        To get a file's content from a specific transaction specify the Transaction's resource identifier as both the
        startTransactionRid and endTransactionRid.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param file_path: The path to a File within Foundry. Examples: my-file.txt, path/to/my-file.jpg, dataframe.snappy.parquet.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param start_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param end_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: BytesIO
        """
        params: Dict[str, Any] = {
            "branchId": branch_id,
            "startTransactionRid": start_transaction_rid,
            "endTransactionRid": end_transaction_rid,
        }
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid), "filePath": quote_plus(file_path)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/files/{filePath}/content".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response, returns_json=False)
        return BytesIO(response.content)
