from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional
from urllib.parse import quote_plus

from foundry._response import convert_to_palantir_object
from foundry.core.api import PalantirSession
from foundry.datasets.types._create_dataset_request import CreateDatasetRequest
from foundry.datasets.types._dataset import Dataset
from foundry.errors._helpers import check_for_errors

if TYPE_CHECKING:
    from foundry.datasets.types._table_export_format import TableExportFormat


class DatasetService:
    def __init__(self, session: PalantirSession) -> None:
        self.session = session

    def create(self, *, name: str, parent_folder_rid: str) -> Dataset:
        """Creates a new Dataset. A default branch - master for most enrollments - will be created on the Dataset.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-write.

        :param name: No parameter documentation available.
        :param parent_folder_rid: No parameter documentation available.
        :returns: Dataset"""
        request_body = CreateDatasetRequest(name=name, parent_folder_rid=parent_folder_rid)._asdict()
        response = self.session.post(
            "https://{hostname}/api/v1/datasets".format(hostname=self.session.hostname), stream=True, json=request_body
        )
        check_for_errors(response)
        response_json = response.json()
        return Dataset(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            name=convert_to_palantir_object(str, response_json.get("name"), session=self.session),
            parent_folder_rid=convert_to_palantir_object(
                str, response_json.get("parentFolderRid"), session=self.session
            ),
        )

    def get(self, *, dataset_rid: str) -> Dataset:
        """Gets the Dataset with the given DatasetRid. Third-party applications
        using this endpoint via OAuth2 must request the following operation
        scope: api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :returns: Dataset
        """
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}".format(hostname=self.session.hostname, **path_params),
            stream=True,
        )
        check_for_errors(response)
        response_json = response.json()
        return Dataset(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            name=convert_to_palantir_object(str, response_json.get("name"), session=self.session),
            parent_folder_rid=convert_to_palantir_object(
                str, response_json.get("parentFolderRid"), session=self.session
            ),
        )

    def read_table(
        self,
        *,
        dataset_rid: str,
        format: "TableExportFormat",
        branch_id: Optional[str] = None,
        start_transaction_rid: Optional[str] = None,
        end_transaction_rid: Optional[str] = None,
        columns: Optional[Iterable[str]] = None,
        row_limit: Optional[int] = None,
        preview: Optional[bool] = False
    ) -> BytesIO:
        """This endpoint is in preview and may be modified or removed at any
        time. To use this endpoint, add preview=true to the request query
        parameters.

        Gets the content of a dataset as a table in the specified format.
        Third-party applications using this endpoint via OAuth2 must request the following operation scope: api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param start_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param end_transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param format: Format for tabular dataset export.
        :param columns: No parameter documentation available.
        :param row_limit: No parameter documentation available.
        :param preview: Enables the use of preview functionality.
        :returns: BytesIO
        """
        if columns is None:
            columns = []
        preview = preview or self.session.preview
        params: Dict[str, Any] = {
            "branchId": branch_id,
            "startTransactionRid": start_transaction_rid,
            "endTransactionRid": end_transaction_rid,
            "format": format,
            "columns": columns,
            "rowLimit": row_limit,
            "preview": preview,
        }
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/readTable".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response, returns_json=False)
        return BytesIO(response.content)
