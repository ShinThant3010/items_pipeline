from functools import lru_cache
from typing import Any

from functions.utils.load_config import load_config


@lru_cache(maxsize=1)
def get_config() -> dict[str, Any]:
    return load_config()
