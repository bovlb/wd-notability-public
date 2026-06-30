from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_RUNTIME_MANIFEST_PATH = Path(__file__).resolve().parent / "data" / "runtime_units.json"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
VALID_MODES = {"continuous", "scheduled", "oneshot"}


@dataclass(frozen=True)
class RuntimeUnit:
    name: str
    groups: tuple[str, ...]
    mode: str
    command: tuple[str, ...]
    env: dict[str, str]
    schedule: str | None = None


@dataclass(frozen=True)
class RuntimeManifest:
    version: int
    defaults_env: dict[str, str]
    units: tuple[RuntimeUnit, ...]


def _normalize_str_list(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name} entries must be non-empty strings")
        items.append(item)
    return tuple(items)


def _normalize_env_map(value: object, *, field_name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")

    env: dict[str, str] = {}
    for key, raw_value in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field_name} keys must be non-empty strings")
        if raw_value is None:
            env[key] = ""
        elif isinstance(raw_value, str):
            env[key] = raw_value
        else:
            env[key] = str(raw_value)
    return env


def load_runtime_manifest(path: Path | None = None) -> RuntimeManifest:
    manifest_path = path or DEFAULT_RUNTIME_MANIFEST_PATH
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest root must be a JSON object")

    version = payload.get("version", 1)
    if not isinstance(version, int) or version < 1:
        raise ValueError("Manifest version must be a positive integer")

    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        raise ValueError("Manifest defaults must be an object")
    defaults_env = _normalize_env_map(defaults.get("env", {}), field_name="defaults.env")

    raw_units = payload.get("units")
    if not isinstance(raw_units, list) or not raw_units:
        raise ValueError("Manifest units must be a non-empty list")

    units: list[RuntimeUnit] = []
    seen_names: set[str] = set()
    for raw_unit in raw_units:
        if not isinstance(raw_unit, dict):
            raise ValueError("Each manifest unit must be an object")

        name = raw_unit.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Each manifest unit must have a non-empty name")
        if name in seen_names:
            raise ValueError(f"Duplicate manifest unit name: {name}")
        seen_names.add(name)

        groups = _normalize_str_list(raw_unit.get("groups", []), field_name=f"{name}.groups")
        mode = raw_unit.get("mode", "continuous")
        if not isinstance(mode, str) or mode not in VALID_MODES:
            raise ValueError(f"{name}.mode must be one of {sorted(VALID_MODES)}")

        command = _normalize_str_list(raw_unit.get("command"), field_name=f"{name}.command")
        env = _normalize_env_map(raw_unit.get("env", {}), field_name=f"{name}.env")

        schedule = raw_unit.get("schedule")
        if schedule is not None and (not isinstance(schedule, str) or not schedule.strip()):
            raise ValueError(f"{name}.schedule must be a non-empty string when provided")
        if mode == "scheduled" and not schedule:
            raise ValueError(f"{name}.schedule is required for scheduled units")
        if mode != "scheduled" and schedule is not None:
            raise ValueError(f"{name}.schedule is only valid for scheduled units")

        units.append(
            RuntimeUnit(
                name=name,
                groups=groups,
                mode=mode,
                command=command,
                env=env,
                schedule=schedule,
            )
        )

    return RuntimeManifest(version=version, defaults_env=defaults_env, units=tuple(units))


def select_units(
    manifest: RuntimeManifest,
    *,
    names: list[str] | None = None,
    groups: list[str] | None = None,
    default_groups: tuple[str, ...] = (),
) -> list[RuntimeUnit]:
    selected_names = {name for name in (names or []) if name}
    selected_groups = {group for group in (groups or []) if group}
    if not selected_names and not selected_groups:
        selected_groups = set(default_groups)

    selected: list[RuntimeUnit] = []
    for unit in manifest.units:
        if selected_names and unit.name in selected_names:
            selected.append(unit)
            continue
        if selected_groups and any(group in selected_groups for group in unit.groups):
            selected.append(unit)

    seen: set[str] = set()
    deduped: list[RuntimeUnit] = []
    for unit in selected:
        if unit.name in seen:
            continue
        seen.add(unit.name)
        deduped.append(unit)

    if not deduped:
        raise ValueError("No runtime units matched the requested names or groups")
    return deduped


def _merged_env(unit: RuntimeUnit, defaults_env: dict[str, str]) -> dict[str, str]:
    env = dict(defaults_env)
    env.update(unit.env)
    return env


def _prefix_print(name: str, line: str, *, lock: threading.Lock) -> None:
    with lock:
        print(f"[{name}] {line}", end="" if line.endswith("\n") else "\n")
        sys.stdout.flush()


def _stream_process_output(name: str, stream, *, lock: threading.Lock) -> None:
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            _prefix_print(name, line, lock=lock)
    finally:
        try:
            stream.close()
        except Exception:  # noqa: BLE001
            pass


def _python_files_mtime(root: Path) -> float:
    newest = 0.0
    for path in [root / "main.py", *(root / "wd_notability").rglob("*.py"), *(root / "server").rglob("*.py")]:
        try:
            newest = max(newest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return newest


def _spawn_unit(
    unit: RuntimeUnit,
    *,
    defaults_env: dict[str, str],
    print_lock: threading.Lock,
) -> tuple[subprocess.Popen[str], threading.Thread]:
    process = subprocess.Popen(
        list(unit.command),
        cwd=PROJECT_ROOT,
        env={**os.environ, **_merged_env(unit, defaults_env)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    reader = threading.Thread(
        target=_stream_process_output,
        args=(unit.name, process.stdout),
        kwargs={"lock": print_lock},
        daemon=True,
    )
    reader.start()
    return process, reader


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _wait_for_processes(processes: dict[str, subprocess.Popen[str]], readers: list[threading.Thread]) -> None:
    for process in processes.values():
        _terminate_process(process)
    for reader in readers:
        reader.join(timeout=1)


def launch_units(
    units: list[RuntimeUnit],
    *,
    defaults_env: dict[str, str],
    reload: bool = False,
    reload_seconds: float = 1.0,
) -> None:
    if not units:
        return
    non_continuous = [unit.name for unit in units if unit.mode != "continuous"]
    if non_continuous:
        raise ValueError(
            "launch is only for continuous units; use run for one-shot units: "
            + ", ".join(non_continuous)
        )

    stop_event = threading.Event()
    readers: list[threading.Thread] = []
    processes: dict[str, subprocess.Popen[str]] = {}
    root = PROJECT_ROOT
    last_mtime = _python_files_mtime(root)
    print_lock = threading.Lock()

    def handle_signal(signum, frame):  # noqa: ANN001, ARG001
        stop_event.set()

    previous_sigint = signal.signal(signal.SIGINT, handle_signal)
    previous_sigterm = signal.signal(signal.SIGTERM, handle_signal)

    def start_all() -> None:
        processes.clear()
        readers.clear()
        for unit in units:
            process, reader = _spawn_unit(unit, defaults_env=defaults_env, print_lock=print_lock)
            processes[unit.name] = process
            readers.append(reader)
            print(f"Started {unit.name} (pid {process.pid})")
            sys.stdout.flush()

    try:
        start_all()
        while not stop_event.is_set():
            for unit in units:
                process = processes[unit.name]
                returncode = process.poll()
                if returncode is not None:
                    stop_event.set()
                    raise RuntimeError(f"{unit.name} exited with status {returncode}")

            if reload:
                current_mtime = _python_files_mtime(root)
                if current_mtime > last_mtime:
                    print("Python file change detected; restarting selected units")
                    _wait_for_processes(processes, readers)
                    start_all()
                    last_mtime = current_mtime

            time.sleep(max(0.1, reload_seconds))
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        _wait_for_processes(processes, readers)
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)


def run_units(units: list[RuntimeUnit], *, defaults_env: dict[str, str]) -> None:
    for unit in units:
        process = subprocess.Popen(
            list(unit.command),
            cwd=PROJECT_ROOT,
            env={**os.environ, **_merged_env(unit, defaults_env)},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        print_lock = threading.Lock()
        reader = threading.Thread(
            target=_stream_process_output,
            args=(unit.name, process.stdout),
            kwargs={"lock": print_lock},
            daemon=True,
        )
        reader.start()
        returncode = process.wait()
        reader.join(timeout=1)
        if returncode != 0:
            raise RuntimeError(f"{unit.name} exited with status {returncode}")


def _render_shell_export(env: dict[str, str]) -> str:
    parts = [f"{key}={shlex.quote(value)}" for key, value in env.items()]
    return "export " + " ".join(parts) + ";" if parts else ""


def _render_toolforge_command(unit: RuntimeUnit, *, defaults_env: dict[str, str]) -> str:
    exports = _render_shell_export(_merged_env(unit, defaults_env))
    command = shlex.join(unit.command)
    if exports:
        return f"{exports} exec {command}"
    return f"exec {command}"


def deploy_units(
    units: list[RuntimeUnit],
    *,
    defaults_env: dict[str, str],
    image: str,
    mount: str = "all",
    once: bool = False,
    dry_run: bool = False,
) -> None:
    for unit in units:
        command = _render_toolforge_command(unit, defaults_env=defaults_env)
        toolforge_args = [
            "toolforge",
            "jobs",
            "run",
            unit.name,
            "--image",
            image,
            "--mount",
            mount,
        ]
        if unit.mode == "continuous" and not once:
            toolforge_args.append("--continuous")
        elif unit.mode == "scheduled" and not once:
            toolforge_args.extend(["--schedule", unit.schedule or ""])
        toolforge_args.extend(["--command", command])

        if dry_run:
            print(shlex.join(toolforge_args))
            continue

        subprocess.run(toolforge_args, check=True)


def stop_units(units: list[RuntimeUnit], *, dry_run: bool = False) -> None:
    for unit in units:
        toolforge_args = ["toolforge", "jobs", "delete", unit.name]
        if dry_run:
            print(shlex.join(toolforge_args))
            continue
        subprocess.run(toolforge_args, check=False)
