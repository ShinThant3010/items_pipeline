from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel


def _to_plain(value: Any) -> Any:
    if isinstance(value, BaseModel):
        data: dict[str, Any] = {}
        for field_name in value.__class__.model_fields:
            field_value = getattr(value, field_name)
            if field_value is not None:
                data[field_name] = _to_plain(field_value)
        return data
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_to_plain(item) for item in value)
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


def apply_defaults(payload: BaseModel | Mapping[str, Any], defaults: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(defaults)
    payload_data = _to_plain(payload)
    merged.update({k: v for k, v in payload_data.items() if v is not None})
    return merged
