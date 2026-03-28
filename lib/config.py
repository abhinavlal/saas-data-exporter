"""Configuration loader — reads from .env file and environment variables.

Priority: CLI args > environment variables > .env file > defaults.
"""

import os
from pathlib import Path


def load_dotenv(path: str | Path | None = None) -> None:
    """Load a .env file into os.environ. Does not override existing env vars."""
    if path is None:
        path = Path.cwd() / ".env"
    else:
        path = Path(path)

    if not path.exists():
        return

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Don't override existing env vars
            if key not in os.environ:
                os.environ[key] = value


def env(key: str, default: str | None = None) -> str | None:
    """Get an environment variable, returning default if not set or empty."""
    val = os.environ.get(key, "")
    return val if val else default


def env_int(key: str, default: int) -> int:
    """Get an integer environment variable."""
    val = os.environ.get(key, "")
    if val:
        try:
            return int(val)
        except ValueError:
            pass
    return default


def env_bool(key: str, default: bool = False) -> bool:
    """Get a boolean environment variable. Truthy: 'true', '1', 'yes'."""
    val = os.environ.get(key, "").lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    return default


def env_list(key: str, default: list[str] | None = None) -> list[str]:
    """Get a comma-separated list from an environment variable."""
    val = os.environ.get(key, "")
    if val:
        return [item.strip() for item in val.split(",") if item.strip()]
    return default or []
