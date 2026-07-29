from __future__ import annotations

import importlib
from pathlib import Path


def test_toolforge_mode_uses_tmp_cache_root(monkeypatch):
    monkeypatch.setenv("WD_NOTABILITY_TOOLFORGE", "1")

    import wd_notability.toolforge_defaults as toolforge_defaults

    importlib.reload(toolforge_defaults)

    assert toolforge_defaults.toolforge_mode_enabled() is True
    assert toolforge_defaults.toolforge_cache_root() == Path("/tmp/wd-notability")


def test_toolforge_mode_defaults_to_false(monkeypatch):
    monkeypatch.delenv("WD_NOTABILITY_TOOLFORGE", raising=False)

    import wd_notability.toolforge_defaults as toolforge_defaults

    importlib.reload(toolforge_defaults)

    assert toolforge_defaults.toolforge_mode_enabled() is False


def test_toolforge_database_name_defaults_from_credential_file(monkeypatch, tmp_path):
    defaults_file = tmp_path / "replica.my.cnf"
    defaults_file.write_text(
        """
        [client]
        user = tool-wd-notability
        password = secret
        """.strip(),
        encoding="utf-8",
    )

    from wd_notability.toolforge_defaults import toolforge_database_name

    assert toolforge_database_name(defaults_file=defaults_file) == "tool-wd-notability"
