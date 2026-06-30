from __future__ import annotations

import pytest

from wd_notability.launcher import DEFAULT_RUNTIME_MANIFEST_PATH, deploy_units, load_runtime_manifest, select_units


def test_runtime_manifest_loads_and_contains_expected_units():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH)

    assert manifest.version == 1
    assert manifest.defaults_env["PYTHONUNBUFFERED"] == "1"
    assert manifest.defaults_env["WD_NOTABILITY_LOOKUP_BACKEND"] == "mariadb"

    names = [unit.name for unit in manifest.units]
    assert "webserver" in names
    assert "content" in names
    assert "external-usage" in names
    assert "wikisub-worker" in names
    assert "build-namespace-cache" in names

    webserver = next(unit for unit in manifest.units if unit.name == "webserver")
    assert webserver.command == ("/bin/sh", "/workspace/scripts/toolforge-entrypoint.sh", "main.py", "serve")


def test_select_units_by_default_group():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH)

    units = select_units(manifest, default_groups=("dev",))
    names = [unit.name for unit in units]

    assert "webserver" in names
    assert "content" in names
    assert "build-namespace-cache" not in names


def test_select_units_by_name_and_group():
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH)

    by_name = select_units(manifest, names=["external-usage"])
    assert [unit.name for unit in by_name] == ["external-usage"]

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
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH)

    with pytest.raises(ValueError, match="No runtime units matched"):
        select_units(manifest, names=["does-not-exist"])


def test_deploy_units_defaults_to_mounted_storage(capsys):
    manifest = load_runtime_manifest(DEFAULT_RUNTIME_MANIFEST_PATH)
    units = select_units(manifest, names=["webserver"])

    deploy_units(
        units,
        defaults_env=manifest.defaults_env,
        image="example/image:latest",
        dry_run=True,
    )

    captured = capsys.readouterr().out
    assert "--mount all" in captured
