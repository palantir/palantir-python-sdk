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

from typing import Tuple

from palantir.core import context
from palantir.core.types import PalantirContext, ResourceIdentifier
from palantir.datasets.client import DatasetsClient, DatasetServices
from palantir.datasets.core import Dataset
from palantir.datasets.types import DatasetLocator


def dataset(
    dataset_ref: str,
    branch: str = None,
    transaction_range: Tuple[str, str] = None,
    create: bool = False,
    ctx: PalantirContext = None,
) -> "Dataset":
    """
    Constructs a new Dataset object from the provided reference.

    Args:
        dataset_ref: The path to a Dataset, or a Dataset Resource Identifier.
        branch: Defaults to "master"
        transaction_range: A tuple containing a start and end transaction rid.
        create: Whether to create the Dataset if it does not already exist.
        ctx: An optional :class:`PalantirContext` (see :func:`palantir.core.context`) to override environment defaults.

    Returns: A :class:`Dataset` object resolved to the view at the specified transaction range or at the latest
    transaction range at the time of initialization.

    Examples:
        >>> dataset("/MyOrg/MyProject/Path/To/Dataset")

        >>> dataset("ri.foundry.main.dataset.3bb94822-d16f-4094-9834-f79a61a29859")
    """
    client = DatasetsClient(DatasetServices(ctx or context()))
    rid = client.get_dataset(dataset_ref)
    branch_id = branch or "master"
    if not rid:
        if create:
            return client.create_dataset(dataset_ref, branch_id)
        raise ValueError(f"could not resolve dataset_ref '{dataset_ref}'")

    (start_transaction_rid, end_transaction_rid,) = (
        transaction_range
        if transaction_range is not None
        else client.get_transaction_range(rid, branch_id)
    )

    return Dataset(
        client=client,
        locator=DatasetLocator(
            rid=rid,
            branch_id=branch_id,
            start_transaction_rid=ResourceIdentifier.from_string(start_transaction_rid)
            if start_transaction_rid
            else None,
            end_transaction_rid=ResourceIdentifier.from_string(end_transaction_rid)
            if end_transaction_rid
            else None,
        ),
    )
