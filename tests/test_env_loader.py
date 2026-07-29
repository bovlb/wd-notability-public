from __future__ import annotations

import importlib
import os

import wd_notability

from wd_notability.env_loader import load_env_file


def test_load_env_file_parses_basic_assignments(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text(
        """
        # comment
        export WD_NOTABILITY_SAMPLE='hello world'
        WD_NOTABILITY_NUMBER=42
        """.strip(),
        encoding="utf-8",
    )

    monkeypatch.delenv("WD_NOTABILITY_SAMPLE", raising=False)
    monkeypatch.delenv("WD_NOTABILITY_NUMBER", raising=False)

    assert load_env_file(env_path) is True
    assert os.environ["WD_NOTABILITY_SAMPLE"] == "hello world"
    assert os.environ["WD_NOTABILITY_NUMBER"] == "42"


def test_importing_package_loads_dotenv_from_cwd(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text("WD_NOTABILITY_AUTOLOADED=1\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("WD_NOTABILITY_AUTOLOADED", raising=False)

    importlib.reload(wd_notability)

    assert os.environ["WD_NOTABILITY_AUTOLOADED"] == "1"
