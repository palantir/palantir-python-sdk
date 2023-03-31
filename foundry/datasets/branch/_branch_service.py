from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote_plus

from foundry._response import convert_to_palantir_object
from foundry.core.api import PalantirSession
from foundry.datasets.types._branch import Branch
from foundry.datasets.types._create_branch_request import CreateBranchRequest
from foundry.datasets.types._list_branches_response import ListBranchesResponse
from foundry.errors._helpers import check_for_errors
from foundry.types import PagedResponse


class BranchService:
    def __init__(self, session: PalantirSession) -> None:
        self.session = session

    def iterator(
        self, *, dataset_rid: str, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterable[Branch]:
        """Lists the Branches of a Dataset. Third-party applications using this
        endpoint via OAuth2 must request the following operation scope:
        api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param page_size: The page size to use for the endpoint.
        :param page_token: The page token indicates where to start paging. This should be omitted from the first page's request.
        To fetch the next page, clients should take the value from the nextPageToken field of the previous response
        and populate the next request's pageToken field with it.
        :returns: Optional[Branch]
        """
        params: Dict[str, Any] = {"pageSize": page_size, "pageToken": page_token}
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/branches".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
        )
        check_for_errors(response)
        response_json = response.json()
        response_obj = ListBranchesResponse(
            next_page_token=convert_to_palantir_object(
                Optional[str], response_json.get("nextPageToken"), session=self.session, paged_response=True
            ),
            data=convert_to_palantir_object(
                Iterable[Branch], response_json.get("data"), session=self.session, paged_response=True
            ),
        )
        return PagedResponse(
            paged_func=self.iterator,
            paged_type=Branch,
            response_obj=response_obj,
            dataset_rid=dataset_rid,
            page_size=page_size,
            page_token=page_token,
        )

    def create(self, *, branch_id: str, dataset_rid: str, transaction_rid: Optional[str] = None) -> Branch:
        """Creates a branch on an existing dataset. A branch may optionally
        point to a (committed) transaction. Third-party applications using this
        endpoint via OAuth2 must request the following operation scope:
        api:datasets-write.

        :param branch_id: The identifier (name) of a Branch. Example: master.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :returns: Branch
        """
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        request_body = CreateBranchRequest(branch_id=branch_id, transaction_rid=transaction_rid)._asdict()
        response = self.session.post(
            "https://{hostname}/api/v1/datasets/{datasetRid}/branches".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            json=request_body,
        )
        check_for_errors(response)
        response_json = response.json()
        return Branch(
            branch_id=convert_to_palantir_object(str, response_json.get("branchId"), session=self.session),
            transaction_rid=convert_to_palantir_object(
                Optional[str], response_json.get("transactionRid"), session=self.session
            ),
        )

    def get(self, *, dataset_rid: str, branch_id: str) -> Branch:
        """Get a Branch of a Dataset. Third-party applications using this
        endpoint via OAuth2 must request the following operation scope:
        api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :returns: Branch
        """
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid), "branchId": quote_plus(branch_id)}
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/branches/{branchId}".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
        )
        check_for_errors(response)
        response_json = response.json()
        return Branch(
            branch_id=convert_to_palantir_object(str, response_json.get("branchId"), session=self.session),
            transaction_rid=convert_to_palantir_object(
                Optional[str], response_json.get("transactionRid"), session=self.session
            ),
        )

    def delete(self, *, dataset_rid: str, branch_id: str) -> None:
        """Deletes the Branch with the given BranchId. Third-party applications
        using this endpoint via OAuth2 must request the following operation
        scope: api:datasets-write.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :returns: None
        """
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid), "branchId": quote_plus(branch_id)}
        response = self.session.delete(
            "https://{hostname}/api/v1/datasets/{datasetRid}/branches/{branchId}".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
        )
        check_for_errors(response, returns_json=False)
