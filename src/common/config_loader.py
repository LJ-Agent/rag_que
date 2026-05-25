"""YAML configuration loader with ${ENV:default} interpolation."""
import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_VAR_RE = re.compile(r"\$\{(\w+)(?::([^}]*))?}")

_CONFIG: dict[str, Any] | None = None
_CONFIG_DIR: Path = Path(__file__).parent.parent.parent / "config"


def _interpolate_env(value: Any) -> Any:
    if isinstance(value, str):
        def _replace(m: re.Match) -> str:
            var_name = m.group(1)
            default = m.group(2) if m.group(2) is not None else ""
            return os.environ.get(var_name, default)
        return _ENV_VAR_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v) for v in value]
    return value


def get_config() -> dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG
    settings_path = _CONFIG_DIR / "settings.yaml"
    if not settings_path.exists():
        raise FileNotFoundError(f"Config file not found: {settings_path}")
    with open(settings_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    _CONFIG = _interpolate_env(raw)
    return _CONFIG


def reload_config() -> dict[str, Any]:
    global _CONFIG
    _CONFIG = None
    return get_config()
