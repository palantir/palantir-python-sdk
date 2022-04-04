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

from typing import TYPE_CHECKING

from palantir.datasets.types import DatasetLocator

if TYPE_CHECKING:
    from palantir.datasets.core import Transaction


class SimultaneousOpenTransactionError(Exception):
    def __init__(self, locator: DatasetLocator):
        super().__init__(
            f"Cannot have simultaneous open transactions on '{locator.rid}'"
        )
        self.locator = locator


class TransactionAbortedError(Exception):
    def __init__(self, txn: "Transaction"):
        super().__init__(
            f"Transaction '{txn.rid}' on '{txn.dataset.rid}' aborted due to error"
        )
        self.txn = txn
