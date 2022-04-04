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

import pytest
from expects import expect, be, raise_error
from mockito import mock, when

from palantir.core.types import ResourceIdentifier
from palantir.datasets.client import DatasetsClient
from palantir.datasets.core import Dataset, Transaction
from palantir.datasets.errors import TransactionAbortedError
from palantir.datasets.types import DatasetLocator, TransactionType, TransactionStatus


class TestTransaction:
    @pytest.fixture(autouse=True)
    def before(self):
        self.client = mock(DatasetsClient)
        self.locator = DatasetLocator(
            rid=ResourceIdentifier.from_string("ri.foundry.test.dataset.0"),
            branch_id="master",
            start_transaction_rid=ResourceIdentifier.from_string(
                "ri.foundry.test.transaction.0"
            ),
            end_transaction_rid=ResourceIdentifier.from_string(
                "ri.foundry.test.transaction.1"
            ),
        )
        self.dataset = Dataset(self.client, self.locator)

    def test_transaction_context_manager(self):
        txn = Transaction(
            self.dataset,
            rid="ri.foundry.test.transaction.2",
            txn_type=TransactionType.SNAPSHOT,
            status=TransactionStatus.OPEN,
            client=self.client,
        )
        when(self.client).start_transaction(
            self.dataset, TransactionType.SNAPSHOT
        ).thenReturn(txn)
        when(self.client).commit_transaction(txn).thenReturn(None)

        with self.dataset.start_transaction(TransactionType.SNAPSHOT) as txn_ctx:
            expect(txn.status).to(be(TransactionStatus.OPEN))
            expect(txn_ctx).to(be(txn))

        expect(txn.status).to(be(TransactionStatus.COMMITTED))

    def test_transaction_context_manager_abort(self):
        txn = Transaction(
            self.dataset,
            rid="ri.foundry.test.transaction.2",
            txn_type=TransactionType.SNAPSHOT,
            status=TransactionStatus.OPEN,
            client=self.client,
        )
        when(self.client).start_transaction(
            self.dataset, TransactionType.SNAPSHOT
        ).thenReturn(txn)
        when(self.client).abort_transaction(txn).thenReturn(None)

        error = ValueError()

        def _error_during_txn():
            with self.dataset.start_transaction(TransactionType.SNAPSHOT) as txn_ctx:
                expect(txn.status).to(be(TransactionStatus.OPEN))
                expect(txn_ctx).to(be(txn))
                raise error

        expect(_error_during_txn).to(
            raise_error(
                TransactionAbortedError,
                f"Transaction '{txn.rid}' on '{txn.dataset.rid}' aborted due to error",
            )
        )

        expect(txn.status).to(be(TransactionStatus.ABORTED))
