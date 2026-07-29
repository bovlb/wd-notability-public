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
from typing import Any

try:
    import resource as _resource
except ImportError:  # pragma: no cover - non-Unix platforms
    _resource = None

DEFAULT_RUNTIME_MANIFEST_PATH = Path(__file__).resolve().parent / "data" / "runtime_units.json"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
VALID_MODES = {"continuous", "scheduled", "oneshot"}
VALID_COMMAND_VARIANTS = {"dev", "prod"}
TOOLFORGE_ENTRYPOINT = "/workspace/scripts/toolforge-entrypoint.sh"
DEFAULT_LAUNCH_MEMORY_LIMIT_MB = 3840


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


def _strip_json_comments_and_trailing_commas(payload: str) -> str:
    """
    Accept a small JSON5-like subset for the runtime manifest.

    We only support line comments, block comments, and trailing commas.
    Everything else still goes through the standard JSON parser.
    """

    cleaned: list[str] = []
    index = 0
    length = len(payload)
    in_string = False
    string_quote = ""

    while index < length:
        char = payload[index]
        next_char = payload[index + 1] if index + 1 < length else ""

        if in_string:
            cleaned.append(char)
            if char == "\\":
                if index + 1 < length:
                    cleaned.append(payload[index + 1])
                    index += 2
                    continue
            elif char == string_quote:
                in_string = False
            index += 1
            continue

        if char in {'"', "'"}:
            in_string = True
            string_quote = char
            cleaned.append(char)
            index += 1
            continue

        if char == "/" and next_char == "/":
            index += 2
            while index < length and payload[index] not in "\r\n":
                index += 1
            continue

        if char == "/" and next_char == "*":
            index += 2
            while index < length - 1:
                if payload[index] == "*" and payload[index + 1] == "/":
                    index += 2
                    break
                if payload[index] in "\r\n":
                    cleaned.append(payload[index])
                index += 1
            continue

        cleaned.append(char)
        index += 1

    stripped: list[str] = []
    index = 0
    length = len(cleaned)
    in_string = False
    string_quote = ""
    while index < length:
        char = cleaned[index]

        if in_string:
            stripped.append(char)
            if char == "\\":
                if index + 1 < length:
                    stripped.append(cleaned[index + 1])
                    index += 2
                    continue
            elif char == string_quote:
                in_string = False
            index += 1
            continue

        if char in {'"', "'"}:
            in_string = True
            string_quote = char
            stripped.append(char)
            index += 1
            continue

        if char == ",":
            lookahead = index + 1
            while lookahead < length and cleaned[lookahead] in " \t\r\n":
                lookahead += 1
            if lookahead < length and cleaned[lookahead] in "}]":
                index += 1
                continue

        stripped.append(char)
        index += 1

    return "".join(stripped)


def _load_command(raw_unit: dict[str, object], *, name: str, command_variant: str) -> tuple[str, ...]:
    if command_variant not in VALID_COMMAND_VARIANTS:
        raise ValueError(f"command_variant must be one of {sorted(VALID_COMMAND_VARIANTS)}")

    variant_field = f"{command_variant}-command"
    command_value = raw_unit.get(variant_field)
    if command_value is None:
        command_value = raw_unit.get("command")
        if command_value is None:
            raise ValueError(f"{name} must define {variant_field}")

    return _normalize_str_list(command_value, field_name=f"{name}.{variant_field}")


def load_runtime_manifest(path: Path | None = None, *, command_variant: str = "dev") -> RuntimeManifest:
    manifest_path = path or DEFAULT_RUNTIME_MANIFEST_PATH
    raw_text = manifest_path.read_text(encoding="utf-8")
    payload = json.loads(_strip_json_comments_and_trailing_commas(raw_text))
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

        command = _load_command(raw_unit, name=name, command_variant=command_variant)
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


def _local_defaults_env(defaults_env: dict[str, str]) -> dict[str, str]:
    env = dict(defaults_env)
    env.pop("WD_NOTABILITY_TOOLFORGE", None)
    return env


def _local_command(unit: RuntimeUnit) -> tuple[str, ...]:
    command = unit.command
    if len(command) >= 3 and command[0] == "/bin/sh" and command[1] == TOOLFORGE_ENTRYPOINT:
        return (sys.executable, *command[2:])
    if command and command[0] == "main.py":
        return (sys.executable, *command)
    return command


def _safe_print(*args, **kwargs) -> bool:
    try:
        print(*args, **kwargs)
        return True
    except BrokenPipeError:
        return False


def _prefix_print(name: str, line: str, *, lock: threading.Lock) -> None:
    with lock:
        if not _safe_print(f"[{name}] {line}", end="" if line.endswith("\n") else "\n"):
            return
        try:
            sys.stdout.flush()
        except BrokenPipeError:
            return


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


def _memory_limit_preexec(memory_limit_mb: int | None) -> Any:
    if memory_limit_mb is None or memory_limit_mb <= 0:
        return None
    if _resource is None:
        raise RuntimeError("Memory limits are not supported on this platform")

    memory_limit_bytes = memory_limit_mb * 1024 * 1024

    def apply_memory_limit() -> None:
        _resource.setrlimit(_resource.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))

    return apply_memory_limit


def _spawn_unit(
    unit: RuntimeUnit,
    *,
    defaults_env: dict[str, str],
    print_lock: threading.Lock,
    memory_limit_mb: int | None,
) -> tuple[subprocess.Popen[str], threading.Thread]:
    process = subprocess.Popen(
        list(_local_command(unit)),
        cwd=PROJECT_ROOT,
        env={**os.environ, **_merged_env(unit, defaults_env)},
        preexec_fn=_memory_limit_preexec(memory_limit_mb),
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
    memory_limit_mb: int | None = DEFAULT_LAUNCH_MEMORY_LIMIT_MB,
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
            process, reader = _spawn_unit(
                unit,
                defaults_env=_local_defaults_env(defaults_env),
                print_lock=print_lock,
                memory_limit_mb=memory_limit_mb,
            )
            processes[unit.name] = process
            readers.append(reader)
            if not _safe_print(f"Started {unit.name} (pid {process.pid})"):
                stop_event.set()
                return
            try:
                sys.stdout.flush()
            except BrokenPipeError:
                stop_event.set()
                return

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
                    if not _safe_print("Python file change detected; restarting selected units"):
                        stop_event.set()
                        break
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
            list(_local_command(unit)),
            cwd=PROJECT_ROOT,
            env={**os.environ, **_merged_env(unit, _local_defaults_env(defaults_env))},
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


def _render_toolforge_command(unit: RuntimeUnit) -> str:
    return shlex.join(unit.command)


def deploy_units(
    units: list[RuntimeUnit],
    *,
    defaults_env: dict[str, str],
    image: str,
    mount: str = "none",
    once: bool = False,
    dry_run: bool = False,
) -> None:
    for unit in units:
        command = _render_toolforge_command(unit)
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
            _safe_print(shlex.join(toolforge_args))
            continue

        subprocess.run(toolforge_args, check=True)


def stop_units(units: list[RuntimeUnit], *, dry_run: bool = False) -> None:
    for unit in units:
        toolforge_args = ["toolforge", "jobs", "delete", unit.name]
        if dry_run:
            _safe_print(shlex.join(toolforge_args))
            continue
        subprocess.run(toolforge_args, check=False)
