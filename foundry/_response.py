import inspect
from datetime import datetime
from enum import EnumMeta
from typing import Any, Dict, Iterable, List, Optional, Union, get_args, get_origin

from foundry.core.api import PalantirSession
from foundry.helpers import to_snake_case


def convert_to_palantir_object(
    object_type: Any,
    obj: Optional[
        Union[
            str,
            Optional[str],
            List[str],
            Optional[List[str]],
            Dict[str, str],
            Optional[Dict[str, str]],
        ]
    ],
    session: Optional["PalantirSession"] = None,
    paged_response: bool = False,
) -> Any:
    if obj is None:
        return obj
    if get_origin(object_type) == Union:  # Get inner class from "Optional" types
        return convert_to_palantir_object(get_args(object_type)[0], obj, session, paged_response)
    if get_origin(object_type) == dict and isinstance(obj, dict):
        key_type, value_type = get_args(object_type)
        return {
            convert_to_palantir_object(key_type, k, session, paged_response): convert_to_palantir_object(
                value_type, v, session, paged_response
            )
            for k, v in obj.items()
        }
    if (get_origin(object_type) == list or getattr(object_type, "_name", None) == "Iterable") and isinstance(obj, list):
        return [convert_to_palantir_object(get_args(object_type)[0], x, session, paged_response) for x in obj]
    if object_type == datetime:
        assert isinstance(obj, str), "Cannot instantiate datetime from a non-string response"
        return datetime.strptime(obj, "%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(obj, dict):
        formatted_dict: dict[str, Any] = {to_snake_case(k): v for k, v in obj.items()}
        if session:
            formatted_dict["session"] = session
        return object_type(**formatted_dict)
    if inspect.isclass(object_type) and issubclass(object_type, EnumMeta):
        if not isinstance(obj, str):
            raise ValueError(f"Expected string to get enum value, got {obj}")
        return getattr(object_type, obj)
    return object_type(obj)
