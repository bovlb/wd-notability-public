from __future__ import annotations

import pytest

from wd_notability.db_env import credentials_from_env, env_value, require_env_value


def test_env_value_prefers_first_non_empty(monkeypatch):
    monkeypatch.delenv("FIRST", raising=False)
    monkeypatch.setenv("SECOND", "  hello  ")

    assert env_value("FIRST", "SECOND", default="fallback") == "hello"


def test_require_env_value_raises_when_missing(monkeypatch):
    monkeypatch.delenv("REPLICADB_USER", raising=False)

    with pytest.raises(RuntimeError, match="REPLICADB_USER"):
        require_env_value("REPLICADB_USER")


def test_credentials_from_env_reads_env(monkeypatch):
    monkeypatch.setenv("REPLICADB_USER", "env-user")
    monkeypatch.setenv("REPLICADB_PASSWORD", "env-password")

    credentials = credentials_from_env("REPLICADB_USER", "REPLICADB_PASSWORD")

    assert credentials.user == "env-user"
    assert credentials.password == "env-password"
