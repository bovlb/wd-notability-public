from __future__ import annotations

import io
import threading
import sys
from pathlib import Path

import pytest

from wd_notability.launcher import (
    DEFAULT_LAUNCH_MEMORY_LIMIT_MB,
    DEFAULT_RUNTIME_MANIFEST_PATH,
    _local_command,
    _memory_limit_preexec,
    _prefix_print,
    _spawn_unit,
    deploy_units,
    load_runtime_manifest,
    select_units,
)


def test_runtime_manifest_loads_and_contains_expected_units():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")

    assert manifest.version == 1
    assert manifest.defaults_env["PYTHONUNBUFFERED"] == "1"
    assert manifest.defaults_env["WD_NOTABILITY_TOOLFORGE"] == "1"
    assert "WD_NOTABILITY_LOOKUP_BACKEND" not in manifest.defaults_env

    names = [unit.name for unit in manifest.units]
    assert "webserver" in names
    assert "content" in names
    assert "wikisub-worker" in names
    assert "build-namespace-cache" in names

    webserver = next(unit for unit in manifest.units if unit.name == "webserver")
    assert webserver.command == (
        "main.py",
        "serve",
        "--host",
        "0.0.0.0",
        "--log-level",
        "debug",
    )

    prod_manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="prod")
    prod_webserver = next(unit for unit in prod_manifest.units if unit.name == "webserver")
    assert prod_webserver.command == (
        "/bin/sh",
        "/workspace/scripts/toolforge-entrypoint.sh",
        "main.py",
        "serve",
    )
    prod_content = next(unit for unit in prod_manifest.units if unit.name == "content")
    assert prod_content.command == (
        "/bin/sh",
        "/workspace/scripts/toolforge-entrypoint.sh",
        "main.py",
        "worker",
        "--workers",
        "10",
    )
    dev_content = next(unit for unit in manifest.units if unit.name == "content")
    assert dev_content.command == ("main.py", "worker", "--workers", "10")


def test_select_units_by_default_group():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")

    units = select_units(manifest, default_groups=("dev",))
    names = [unit.name for unit in units]

    assert "webserver" in names
    assert "content" in names
    assert "build-namespace-cache" not in names


def test_runtime_manifest_accepts_comments_and_trailing_commas(tmp_path: Path):
    manifest_path = tmp_path / "runtime_units.json"
    manifest_path.write_text(
        """{
          // comment at the top
          "version": 1,
          "defaults": {
            "env": {
              "PYTHONUNBUFFERED": "1",
            },
          },
          "units": [
            {
              "name": "demo",
              "groups": ["dev",],
              "mode": "continuous",
              "dev-command": ["main.py", "demo",],
              "prod-command": [
                "/bin/sh",
                "/workspace/scripts/toolforge-entrypoint.sh",
                "main.py",
                "demo",
              ],
            },
          ],
        }""",
        encoding="utf-8",
    )

    manifest = load_runtime_manifest(manifest_path, command_variant="dev")

    assert manifest.version == 1
    assert manifest.defaults_env["PYTHONUNBUFFERED"] == "1"
    assert [unit.name for unit in manifest.units] == ["demo"]
    assert manifest.units[0].command == ("main.py", "demo")


def test_select_units_by_name_and_group():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")

    wikisub = select_units(manifest, names=["wikisub-worker"])
    assert [unit.name for unit in wikisub] == ["wikisub-worker"]

    by_group = select_units(manifest, groups=["bootstrap"])
    assert {"build-namespace-cache", "build-property-cache", "build-osm-cache", "build-sdc-cache", "build-wikisub-cache"} <= {
        unit.name for unit in by_group
    }

    toolforge = select_units(manifest, groups=["toolforge"])
    assert "webserver" in {unit.name for unit in toolforge}
    assert "reset-main-cache" not in {unit.name for unit in toolforge}


def test_select_units_rejects_missing_selection():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")

    with pytest.raises(ValueError, match="No runtime units matched"):
        select_units(manifest, names=["does-not-exist"])


def test_deploy_units_defaults_to_unmounted_storage(capsys):
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="prod")
    units = select_units(manifest, names=["webserver"])

    deploy_units(
        units,
        defaults_env=manifest.defaults_env,
        image="example/image:latest",
        dry_run=True,
    )

    captured = capsys.readouterr().out
    assert "--mount none" in captured
    assert "REPLICADB_HOST=" not in captured
    assert "TOOLSDB_HOST=" not in captured


def test_local_command_rewrites_toolforge_entrypoint():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")
    webserver = next(unit for unit in manifest.units if unit.name == "webserver")

    assert _local_command(webserver) == (
        sys.executable,
        "main.py",
        "serve",
        "--host",
        "0.0.0.0",
        "--log-level",
        "debug",
    )


def test_prefix_print_ignores_broken_pipe(monkeypatch):
    def raise_broken_pipe(*args, **kwargs):  # noqa: ANN001, ARG001
        raise BrokenPipeError

    monkeypatch.setattr("builtins.print", raise_broken_pipe)

    _prefix_print("content", "hello\n", lock=threading.Lock())


def test_memory_limit_preexec_sets_rlimit_as(monkeypatch):
    recorded: list[tuple[int, tuple[int, int]]] = []

    fake_resource = type(
        "FakeResource",
        (),
        {
            "RLIMIT_AS": 9,
            "setrlimit": staticmethod(lambda which, limits: recorded.append((which, limits))),
        },
    )()

    monkeypatch.setattr("wd_notability.launcher._resource", fake_resource)

    preexec = _memory_limit_preexec(DEFAULT_LAUNCH_MEMORY_LIMIT_MB)
    assert callable(preexec)

    preexec()

    assert recorded == [
        (fake_resource.RLIMIT_AS, (DEFAULT_LAUNCH_MEMORY_LIMIT_MB * 1024 * 1024,) * 2)
    ]


def test_spawn_unit_applies_memory_limit(monkeypatch):
    captured_kwargs = {}

    class FakeProcess:
        pid = 1234
        stdout = io.StringIO("")

        def poll(self):
            return 0

    def fake_popen(*args, **kwargs):  # noqa: ANN001, ARG001
        captured_kwargs.update(kwargs)
        return FakeProcess()

    monkeypatch.setattr("wd_notability.launcher.subprocess.Popen", fake_popen)
    monkeypatch.setattr("wd_notability.launcher._memory_limit_preexec", lambda mb: ("limit", mb))

    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH, command_variant="dev")
    webserver = next(unit for unit in manifest.units if unit.name == "webserver")

    process, reader = _spawn_unit(
        webserver,
        defaults_env={},
        print_lock=threading.Lock(),
        memory_limit_mb=DEFAULT_LAUNCH_MEMORY_LIMIT_MB,
    )

    reader.join(timeout=1)

    assert process.pid == 1234
    assert captured_kwargs["preexec_fn"] == ("limit", DEFAULT_LAUNCH_MEMORY_LIMIT_MB)
