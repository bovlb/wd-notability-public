from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class MysqlCredentials:
    user: str | None
    password: str | None


def env_value(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        value = value.strip()
        if value:
            return value
    return default


def require_env_value(*names: str) -> str:
    value = env_value(*names)
    if value is None:
        joined = " or ".join(names)
        raise RuntimeError(f"Missing required environment variable: {joined}")
    return value


def has_any_env(*names: str) -> bool:
    return any(env_value(name) is not None for name in names)


def credentials_from_env(
    user_env: str,
    password_env: str,
) -> MysqlCredentials:
    return MysqlCredentials(
        user=require_env_value(user_env),
        password=require_env_value(password_env),
    )
