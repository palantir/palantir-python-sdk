from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote_plus

from foundry._response import convert_to_palantir_object
from foundry.core.api import PalantirSession
from foundry.datasets.types._create_transaction_request import CreateTransactionRequest
from foundry.datasets.types._transaction import Transaction
from foundry.datasets.types._transaction_status import TransactionStatus
from foundry.datasets.types._transaction_type import TransactionType
from foundry.errors._helpers import check_for_errors


class TransactionService:
    def __init__(self, session: PalantirSession) -> None:
        self.session = session

    def create(
        self, *, dataset_rid: str, transaction_type: Optional["TransactionType"] = None, branch_id: Optional[str] = None
    ) -> Transaction:
        """Creates a Transaction on a Branch of a Dataset. Third-party
        applications using this endpoint via OAuth2 must request the following
        operation scope: api:datasets-write.

        :param transaction_type: The type of a Transaction.
        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param branch_id: The identifier (name) of a Branch. Example: master.
        :returns: Transaction
        """
        params: Dict[str, Any] = {"branchId": branch_id}
        path_params: Dict[str, str] = {"datasetRid": quote_plus(dataset_rid)}
        request_body = CreateTransactionRequest(transaction_type=transaction_type)._asdict()
        response = self.session.post(
            "https://{hostname}/api/v1/datasets/{datasetRid}/transactions".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
            params=params,
            json=request_body,
        )
        check_for_errors(response)
        response_json = response.json()
        return Transaction(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            transaction_type=convert_to_palantir_object(
                TransactionType, response_json.get("transactionType"), session=self.session
            ),
            status=convert_to_palantir_object(TransactionStatus, response_json.get("status"), session=self.session),
            created_time=convert_to_palantir_object(datetime, response_json.get("createdTime"), session=self.session),
            closed_time=convert_to_palantir_object(
                Optional[datetime], response_json.get("closedTime"), session=self.session
            ),
        )

    def get(self, *, dataset_rid: str, transaction_rid: str) -> Transaction:
        """Gets a Transaction of a Dataset. Third-party applications using this
        endpoint via OAuth2 must request the following operation scope:
        api:datasets-read.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: Transaction
        """
        path_params: Dict[str, str] = {
            "datasetRid": quote_plus(dataset_rid),
            "transactionRid": quote_plus(transaction_rid),
        }
        response = self.session.get(
            "https://{hostname}/api/v1/datasets/{datasetRid}/transactions/{transactionRid}".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
        )
        check_for_errors(response)
        response_json = response.json()
        return Transaction(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            transaction_type=convert_to_palantir_object(
                TransactionType, response_json.get("transactionType"), session=self.session
            ),
            status=convert_to_palantir_object(TransactionStatus, response_json.get("status"), session=self.session),
            created_time=convert_to_palantir_object(datetime, response_json.get("createdTime"), session=self.session),
            closed_time=convert_to_palantir_object(
                Optional[datetime], response_json.get("closedTime"), session=self.session
            ),
        )

    def commit(self, *, dataset_rid: str, transaction_rid: str) -> Transaction:
        """Commits an open Transaction. File modifications made on this
        Transaction are preserved and the Branch is updated to point to the
        Transaction. Third-party applications using this endpoint via OAuth2
        must request the following operation scope: api:datasets-write.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: Transaction
        """
        path_params: Dict[str, str] = {
            "datasetRid": quote_plus(dataset_rid),
            "transactionRid": quote_plus(transaction_rid),
        }
        response = self.session.post(
            "https://{hostname}/api/v1/datasets/{datasetRid}/transactions/{transactionRid}/commit".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
        )
        check_for_errors(response)
        response_json = response.json()
        return Transaction(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            transaction_type=convert_to_palantir_object(
                TransactionType, response_json.get("transactionType"), session=self.session
            ),
            status=convert_to_palantir_object(TransactionStatus, response_json.get("status"), session=self.session),
            created_time=convert_to_palantir_object(datetime, response_json.get("createdTime"), session=self.session),
            closed_time=convert_to_palantir_object(
                Optional[datetime], response_json.get("closedTime"), session=self.session
            ),
        )

    def abort(self, *, dataset_rid: str, transaction_rid: str) -> Transaction:
        """Aborts an open Transaction. File modifications made on this
        Transaction are not preserved and the Branch is not updated. Third-
        party applications using this endpoint via OAuth2 must request the
        following operation scope: api:datasets-write.

        :param dataset_rid: The Resource Identifier (RID) of a Dataset. Example: ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da.
        :param transaction_rid: The Resource Identifier (RID) of a Transaction. Example: ri.foundry.main.transaction.0a0207cb-26b7-415b-bc80-66a3aa3933f4.
        :returns: Transaction
        """
        path_params: Dict[str, str] = {
            "datasetRid": quote_plus(dataset_rid),
            "transactionRid": quote_plus(transaction_rid),
        }
        response = self.session.post(
            "https://{hostname}/api/v1/datasets/{datasetRid}/transactions/{transactionRid}/abort".format(
                hostname=self.session.hostname, **path_params
            ),
            stream=True,
        )
        check_for_errors(response)
        response_json = response.json()
        return Transaction(
            rid=convert_to_palantir_object(str, response_json.get("rid"), session=self.session),
            transaction_type=convert_to_palantir_object(
                TransactionType, response_json.get("transactionType"), session=self.session
            ),
            status=convert_to_palantir_object(TransactionStatus, response_json.get("status"), session=self.session),
            created_time=convert_to_palantir_object(datetime, response_json.get("createdTime"), session=self.session),
            closed_time=convert_to_palantir_object(
                Optional[datetime], response_json.get("closedTime"), session=self.session
            ),
        )
