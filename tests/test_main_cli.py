from __future__ import annotations

from argparse import Namespace
from types import SimpleNamespace

import main


def test_parse_args_accepts_redeploy_alias(monkeypatch):
    monkeypatch.setattr(main.sys, "argv", ["main.py", "stop+deploy"])

    args = main.parse_args()

    assert args.command == "stop+deploy"


def test_redeploy_stops_before_deploying(monkeypatch):
    calls: list[tuple[str, object]] = []
    manifest = SimpleNamespace(defaults_env={"WD_NOTABILITY_TOOLFORGE": "1"})
    units = [SimpleNamespace(name="webserver", groups=("toolforge",))]

    monkeypatch.setattr(main, "parse_args", lambda: Namespace(
        command="redeploy",
        manifest=None,
        names=None,
        groups=None,
        image="example/image:latest",
        mount="none",
        dry_run=True,
        once=False,
        log_level="INFO",
        third_party_log_level="WARNING",
        log_format="%(message)s",
        log_date_format="%Y-%m-%d %H:%M:%S",
        profile=False,
        profile_sort="cumulative",
        profile_limit=50,
        profile_output="",
    ))
    monkeypatch.setattr(main, "configure_logging", lambda args: None)

    def fake_load_runtime_manifest(path, command_variant):
        calls.append(("load", command_variant))
        return manifest

    def fake_select_units(manifest_arg, *, names, groups, default_groups):
        calls.append(("select", tuple(default_groups)))
        return units

    def fake_stop_units(selected_units, dry_run):
        calls.append(("stop", dry_run, tuple(unit.name for unit in selected_units)))

    def fake_deploy_units(selected_units, **kwargs):
        calls.append(("deploy", tuple(unit.name for unit in selected_units), kwargs["dry_run"]))

    monkeypatch.setattr(main, "load_runtime_manifest", fake_load_runtime_manifest)
    monkeypatch.setattr(main, "select_units", fake_select_units)
    monkeypatch.setattr(main, "stop_units", fake_stop_units)
    monkeypatch.setattr(main, "deploy_units", fake_deploy_units)

    main.main()

    assert calls == [
        ("load", "prod"),
        ("select", ("toolforge",)),
        ("stop", True, ("webserver",)),
        ("deploy", ("webserver",), True),
    ]
