from typing import Any, Dict

from foundry.errors._palantir_rpc_exception import PalantirRPCException


class ApiFeaturePreviewUsageOnly(PalantirRPCException):
    """This feature is only supported in preview mode.

    Please use `preview=true` in the query parameters to call this
    endpoint.
    """

    def __init__(self, error_metadata: Dict[str, Any]) -> None:
        super().__init__(error_metadata)
