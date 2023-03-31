from typing import Any, Dict

from foundry.errors._helpers import format_error_message


class PalantirRPCException(Exception):
    def __init__(self, error_metadata: Dict[str, Any]):
        super().__init__(format_error_message(error_metadata))
        self.name = error_metadata.get("errorName")
        self.parameters = error_metadata.get("parameters")
        self.error_instance_id = error_metadata.get("errorInstanceId")
