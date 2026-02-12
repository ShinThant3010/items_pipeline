from pathlib import Path
from typing import Any

import yaml


def load_config() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parents[1] / "parameters" / "config.yaml"
    with config_path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp) or {}
