from __future__ import annotations

import argparse
import cProfile
import asyncio
import io
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
import pstats

from wd_notability.file_lock import acquire_file_lock
from wd_notability.launcher import (
    DEFAULT_RUNTIME_MANIFEST_PATH,
    deploy_units,
    launch_units,
    load_runtime_manifest,
    run_units,
    select_units,
    stop_units,
)
from wd_notability.localdb_paths import EVALUATION_CACHE_PATH, LOOKUP_CACHE_PATH
from wd_notability.toolforge_defaults import toolforge_defaults_file_exists

DEFAULT_LOOKUP_CACHE_PATH = LOOKUP_CACHE_PATH
DEFAULT_EVALUATION_CACHE_PATH = EVALUATION_CACHE_PATH
WIKISUB_LOOKUP_CACHE_PATH = LOOKUP_CACHE_PATH
WIKISUB_MAIN_CACHE_PATH = EVALUATION_CACHE_PATH
INLINKS_WORKER_LOCK_TARGET = Path(__file__).resolve().parent / "data" / "inlinks_worker"
CACHE_SYNC_WORKER_LOCK_TARGET = Path(__file__).resolve().parent / "data" / "cache_sync_worker"

INLINKS_VISIBLE_LIMIT = 100
INLINKS_WORKER_RUN_INTERVAL_SECONDS = 5.0
RECENT_CHANGES_WORKER_POLL_SECONDS = 60.0
RECENT_CHANGES_WORKER_REWIND_SECONDS = 300.0
ENTITYDATA_DELETION_LOG_BATCH_SIZE = 200
CACHE_SYNC_WORKER_BATCH_SIZE = 100
CACHE_SYNC_WORKER_RUN_INTERVAL_SECONDS = 60.0
CACHE_OBSERVABILITY_WORKER_RUN_INTERVAL_SECONDS = 60.0
WIKISUB_BLOCK_SIZE = 100_000
WIKISUB_SLEEP_SECONDS = 1.0
WIKISUB_WORKER_POLL_SECONDS = 60.0
WIKISUB_DATABASE = "wikidatawiki_p"
WIKISUB_DEFAULTS_FILE = Path.home() / "replica.my.cnf"
WIKISUB_HOST = os.getenv("WD_NOTABILITY_REPLICA_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def _web_default_host() -> str:
    if os.getenv("PORT") is not None:
        return "0.0.0.0"
    return os.getenv("WD_NOTABILITY_WEB_HOST", "127.0.0.1")


def _web_default_port() -> int:
    port = os.getenv("PORT")
    if port is not None:
        return int(port)
    return int(os.getenv("WD_NOTABILITY_WEB_PORT", "8000"))


def _coalesce(value, default):
    return default if value is None else value


def _namespace_with_defaults(args: argparse.Namespace, **defaults) -> argparse.Namespace:
    values = vars(args).copy()
    for key, default in defaults.items():
        if values.get(key) is None:
            values[key] = default
    return argparse.Namespace(**values)


def parse_args() -> argparse.Namespace:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--log-level",
        default=os.getenv("WD_NOTABILITY_LOG_LEVEL", "INFO"),
        help="Root logging level (for example DEBUG, INFO, WARNING, ERROR)",
    )
    common_parser.add_argument(
        "--third-party-log-level",
        default=os.getenv("WD_NOTABILITY_THIRD_PARTY_LOG_LEVEL", "WARNING"),
        help="Logging level for noisy third-party libraries such as httpx",
    )
    common_parser.add_argument(
        "--log-format",
        default=os.getenv(
            "WD_NOTABILITY_LOG_FORMAT",
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
        ),
        help="Logging format string",
    )
    common_parser.add_argument(
        "--log-date-format",
        default=os.getenv("WD_NOTABILITY_LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S"),
        help="Logging date format string",
    )
    common_parser.add_argument(
        "--profile",
        action="store_true",
        help="Collect a cProfile run and print the hottest functions on exit",
    )
    common_parser.add_argument(
        "--profile-sort",
        default=os.getenv("WD_NOTABILITY_PROFILE_SORT", "cumulative"),
        help="Sort order for profile output (for example cumulative, time, calls)",
    )
    common_parser.add_argument(
        "--profile-limit",
        type=int,
        default=int(os.getenv("WD_NOTABILITY_PROFILE_LIMIT", "50")),
        help="Maximum number of rows to print from the profile report",
    )
    common_parser.add_argument(
        "--profile-output",
        default=os.getenv("WD_NOTABILITY_PROFILE_OUTPUT", ""),
        help="Write the profile report to this file instead of stdout",
    )
    parser = argparse.ArgumentParser(description="wd_notability utilities", parents=[common_parser])
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Process entitydata evaluation batches", parents=[common_parser])
    worker_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of items to process before exiting (0 = run continuously)",
    )
    worker_parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker coroutines to run",
    )
    worker_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Delay between empty-batch polls in continuous mode",
    )
    worker_parser.add_argument(
        "--allow-entitydata-without-interest",
        action="store_true",
        default=None,
        help="Allow EntityData to process cache rows even when they have no pubsub interest",
    )
    worker_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    worker_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    dev_parser = subparsers.add_parser("dev-workers", help="Run the worker pool for development", parents=[common_parser])
    dev_parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker coroutines to run",
    )
    dev_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Delay between empty-batch polls in each worker process",
    )
    dev_parser.add_argument(
        "--allow-entitydata-without-interest",
        action="store_true",
        default=None,
        help="Allow EntityData to process cache rows even when they have no pubsub interest",
    )
    dev_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart all worker processes when Python files change",
    )
    dev_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    inlinks_parser = subparsers.add_parser("inlinks-worker", help="Process unknown inlinks and queue missing N12 work", parents=[common_parser])
    inlinks_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of candidate qids to process before exiting (0 = run continuously)",
    )
    inlinks_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of candidate qids to process per batch",
    )
    inlinks_parser.add_argument(
        "--run-interval-seconds",
        type=float,
        default=None,
        help="Minimum delay between the start of successive inlinks batches",
    )
    inlinks_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    inlinks_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    recent_changes_parser = subparsers.add_parser("recent-changes-worker", help="Monitor recent changes and refresh cached revision metadata", parents=[common_parser])
    recent_changes_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Delay between successive recent changes polls",
    )
    recent_changes_parser.add_argument(
        "--rewind-seconds",
        type=float,
        default=None,
        help="How far back to start on the first poll",
    )
    recent_changes_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    recent_changes_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    deletion_monitor_parser = subparsers.add_parser(
        "entitydata-deletion-worker",
        help="Monitor deletion logs and refresh cached entitydata metadata",
        parents=[common_parser],
    )
    deletion_monitor_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Delay between successive deletion-log polls",
    )
    deletion_monitor_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of deletion-log qids to process per batch",
    )
    deletion_monitor_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    deletion_monitor_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    cache_sync_parser = subparsers.add_parser(
        "cache-sync-worker",
        help="Sync cache rows from the side caches, prioritizing interest",
        parents=[common_parser],
    )
    cache_sync_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of candidate qids to process before exiting (0 = run continuously)",
    )
    cache_sync_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of candidate qids to process per batch",
    )
    cache_sync_parser.add_argument(
        "--run-interval-seconds",
        type=float,
        default=None,
        help="Minimum delay between the start of successive sync runs",
    )
    cache_sync_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    cache_sync_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    cache_observability_parser = subparsers.add_parser(
        "cache-observability-worker",
        help="Log cache breakdown snapshots for observability",
        parents=[common_parser],
    )
    cache_observability_parser.add_argument(
        "--run-interval-seconds",
        type=float,
        default=None,
        help="Minimum delay between successive cache observability snapshots",
    )

    wikisub_worker_parser = subparsers.add_parser(
        "wikisub-worker",
        help="Poll wiki subscriber changes and advance the ratchet cache",
        parents=[common_parser],
    )
    wikisub_worker_parser.add_argument(
        "--lookup-cache",
        default=None,
        help="Lookup cache database path",
    )
    wikisub_worker_parser.add_argument(
        "--main-cache",
        default=None,
        help="Main evaluation cache path",
    )
    wikisub_worker_parser.add_argument(
        "--block-size",
        type=int,
        default=None,
        help="Number of wb_changes_subscription rows to scan per query block",
    )
    wikisub_worker_parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=None,
        help="Pause between query blocks",
    )
    wikisub_worker_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Delay between successive replica polls",
    )
    wikisub_worker_parser.add_argument(
        "--defaults-file",
        default=None,
        help="Toolforge replica defaults file",
    )
    wikisub_worker_parser.add_argument(
        "--database",
        default=None,
        help="Replica database name",
    )
    wikisub_worker_parser.add_argument(
        "--host",
        default=None,
        help="Replica host",
    )
    wikisub_worker_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart the worker process when Python files change",
    )
    wikisub_worker_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    launch_parser = subparsers.add_parser("launch", help="Launch manifest-selected runtime units locally", parents=[common_parser])
    launch_parser.add_argument(
        "--manifest",
        default=None,
        help="Runtime manifest JSON file",
    )
    launch_parser.add_argument(
        "--name",
        action="append",
        dest="names",
        help="Launch only the named runtime unit (repeatable)",
    )
    launch_parser.add_argument(
        "--group",
        action="append",
        dest="groups",
        help="Launch only runtime units in the named group (repeatable)",
    )
    launch_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart launched units when Python files change",
    )
    launch_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=None,
        help="Delay between reload file scans",
    )

    run_parser = subparsers.add_parser("run", help="Run manifest-selected one-shot units locally", parents=[common_parser])
    run_parser.add_argument(
        "--manifest",
        default=None,
        help="Runtime manifest JSON file",
    )
    run_parser.add_argument(
        "--name",
        action="append",
        dest="names",
        help="Run only the named runtime unit (repeatable)",
    )
    run_parser.add_argument(
        "--group",
        action="append",
        dest="groups",
        help="Run only runtime units in the named group (repeatable)",
    )

    deploy_parser = subparsers.add_parser("deploy", help="Deploy manifest-selected jobs to ToolForge", parents=[common_parser])
    deploy_parser.add_argument(
        "--manifest",
        default=None,
        help="Runtime manifest JSON file",
    )
    deploy_parser.add_argument(
        "--name",
        action="append",
        dest="names",
        help="Deploy only the named runtime unit (repeatable)",
    )
    deploy_parser.add_argument(
        "--group",
        action="append",
        dest="groups",
        help="Deploy only runtime units in the named group (repeatable)",
    )
    deploy_parser.add_argument(
        "--image",
        default=os.getenv("WD_NOTABILITY_TOOLFORGE_IMAGE", "tools-harbor.wmcloud.org/tool-wd-notability/tool-wd-notability:latest"),
        help="ToolForge image to deploy",
    )
    deploy_parser.add_argument(
        "--mount",
        default="all",
        choices=("all", "none"),
        help="ToolForge storage mount mode for deployed jobs",
    )
    deploy_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print ToolForge commands instead of running them",
    )
    deploy_parser.add_argument(
        "--once",
        action="store_true",
        help="Deploy selected jobs as one-shot runs instead of scheduled or continuous jobs",
    )

    stop_parser = subparsers.add_parser("stop", help="Stop manifest-selected ToolForge jobs", parents=[common_parser])
    stop_parser.add_argument(
        "--manifest",
        default=None,
        help="Runtime manifest JSON file",
    )
    stop_parser.add_argument(
        "--name",
        action="append",
        dest="names",
        help="Stop only the named runtime unit (repeatable)",
    )
    stop_parser.add_argument(
        "--group",
        action="append",
        dest="groups",
        help="Stop only runtime units in the named group (repeatable)",
    )
    stop_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print ToolForge commands instead of running them",
    )

    osm_build_parser = subparsers.add_parser("build-osm-cache", help="Build the OSM lookup cache", parents=[common_parser])
    osm_build_parser.add_argument(
        "--output",
        default=None,
        help="Output lookup cache database path",
    )
    osm_build_parser.add_argument(
        "--page-size",
        type=int,
        default=None,
        help="Taginfo rows per page",
    )
    osm_build_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of accepted QID rows to process (0 = all)",
    )
    osm_build_parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        help="Skip Taginfo fetching and only resync N3_osm from the existing lookup cache",
    )

    sdc_build_parser = subparsers.add_parser("build-sdc-cache", help="Build the SDC lookup cache", parents=[common_parser])
    sdc_build_parser.add_argument(
        "--output",
        default=None,
        help="Output lookup cache database path",
    )
    sdc_build_parser.add_argument(
        "--dump-url",
        default=None,
        help="Commons mediainfo TTL dump URL",
    )
    sdc_build_parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even if the remote dump timestamp has not changed",
    )
    sdc_build_parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        help="Skip dump fetching and only resync N3_sdc from the existing lookup cache",
    )
    sdc_build_parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show a tqdm progress bar while downloading the SDC dump",
    )

    wikisub_build_parser = subparsers.add_parser("build-wikisub-cache", help="Build the wiki-subscriber lookup cache", parents=[common_parser])
    wikisub_build_parser.add_argument(
        "--output",
        default=None,
        help="Output lookup cache database path",
    )
    wikisub_build_parser.add_argument(
        "--block-size",
        type=int,
        default=None,
        help="Number of wb_changes_subscription rows to scan per query block",
    )
    wikisub_build_parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=None,
        help="Pause between query blocks",
    )
    wikisub_build_parser.add_argument(
        "--sync-main-cache",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Synchronize N3_wikisub in the main evaluation cache after the lookup cache is rebuilt",
    )
    wikisub_build_parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        default=None,
        help="Skip the subscription scan and only resync N3_wikisub from the existing lookup cache",
    )
    wikisub_build_parser.add_argument(
        "--main-cache",
        default=None,
        help="Main evaluation cache path",
    )
    wikisub_build_parser.add_argument(
        "--defaults-file",
        default=None,
        help="Toolforge replica defaults file",
    )
    wikisub_build_parser.add_argument(
        "--database",
        default=None,
        help="Replica database name",
    )
    wikisub_build_parser.add_argument(
        "--host",
        default=None,
        help="Replica host",
    )
    wikisub_build_parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show a tqdm progress bar while scanning blocks",
    )

    namespace_build_parser = subparsers.add_parser(
        "build-namespace-cache",
        help="Build the namespace and site API lookup cache",
        parents=[common_parser],
    )
    namespace_build_parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory where the lookup cache database is written",
    )
    namespace_build_parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Maximum concurrent namespace requests",
    )
    namespace_build_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of sites to fetch (0 = all)",
    )
    namespace_build_parser.add_argument(
        "--from-json",
        action="store_true",
        help="Refresh the cache from the checked-in JSON snapshot instead of live siteinfo requests",
    )
    namespace_build_parser.add_argument(
        "--namespaces-json",
        default=None,
        help="Path to the old namespaces JSON cache",
    )
    namespace_build_parser.add_argument(
        "--site-api-urls-json",
        default=None,
        help="Path to the old site API URLs JSON cache",
    )

    property_build_parser = subparsers.add_parser(
        "build-property-cache",
        help="Build the property-instance lookup cache",
        parents=[common_parser],
    )
    property_build_parser.add_argument(
        "--output",
        default=None,
        help="Output lookup cache database path",
    )
    property_build_parser.add_argument(
        "--qid",
        action="append",
        dest="qids",
        help="QID to include (repeatable). Defaults are used if omitted.",
    )
    property_build_parser.add_argument(
        "--delay-seconds",
        type=float,
        default=None,
        help="Delay between SPARQL requests to avoid rate limits",
    )
    property_build_parser.add_argument(
        "--from-json",
        action="store_true",
        help="Refresh the cache from the checked-in JSON snapshot instead of live SPARQL requests",
    )
    property_build_parser.add_argument(
        "--properties-json",
        default=None,
        help="Path to the old property-instance JSON cache",
    )

    reset_main_cache_parser = subparsers.add_parser(
        "reset-main-cache",
        help="Reset the main evaluation cache and flush queued work",
        parents=[common_parser],
    )
    reset_main_cache_parser.add_argument(
        "--main-cache",
        default=None,
        help="Main evaluation cache path",
    )

    serve_parser = subparsers.add_parser("serve", help="Run the web server", parents=[common_parser])
    serve_parser.add_argument(
        "--host",
        default=_web_default_host(),
        help="Host interface to bind",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=_web_default_port(),
        help="Port to bind",
    )
    serve_parser.add_argument(
        "--reload",
        action="store_true",
        help="Reload the server when Python files change",
    )
    return parser.parse_args()


def configure_logging(args: argparse.Namespace) -> None:
    level_name = str(getattr(args, "log_level", "INFO")).upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(f"Unknown log level: {args.log_level}")

    third_party_level_name = str(getattr(args, "third_party_log_level", "WARNING")).upper()
    third_party_level = getattr(logging, third_party_level_name, None)
    if not isinstance(third_party_level, int):
        raise ValueError(f"Unknown third-party log level: {args.third_party_log_level}")

    logging.basicConfig(
        level=level,
        format=getattr(args, "log_format", None) or "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt=getattr(args, "log_date_format", None) or "%Y-%m-%d %H:%M:%S",
    )

    for logger_name in ("httpx", "httpcore", "urllib3", "aiosqlite", "pymysql"):
        logging.getLogger(logger_name).setLevel(third_party_level)


def _print_profile_report(profile: cProfile.Profile, *, sort_by: str, limit: int, output_path: str | None) -> None:
    if output_path:
        profile.dump_stats(output_path)
        print(f"Profile data written to {output_path}")
        print(f"Run: snakeviz {output_path}")
        return

    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream).strip_dirs()
    stats.sort_stats(sort_by).print_stats(limit)
    print(stream.getvalue(), end="")


def _run_profiled(func, *, sort_by: str, limit: int, output_path: str | None) -> None:
    profile = cProfile.Profile()
    try:
        profile.enable()
        func()
    finally:
        profile.disable()
        _print_profile_report(profile, sort_by=sort_by, limit=limit, output_path=output_path)


def _python_files_mtime(root: Path) -> float:
    newest = 0.0
    for path in [root / "main.py", *(root / "wd_notability").rglob("*.py"), *(root / "server").rglob("*.py")]:
        try:
            newest = max(newest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return newest


def _start_dev_worker_processes(
    worker_count: int,
    poll_seconds: float,
    *,
    allow_uninterested: bool = False,
) -> list[subprocess.Popen]:
    if worker_count < 1:
        return []
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "worker",
        "--workers",
        str(worker_count),
        "--poll-seconds",
        str(poll_seconds),
    ]
    if allow_uninterested:
        cmd.append("--allow-entitydata-without-interest")
    print(f"Starting worker process with {worker_count} coroutine(s)")
    return [subprocess.Popen(cmd)]


def _reloadable_argv() -> list[str]:
    argv = [sys.executable, str(Path(__file__).resolve())]
    skip_next = False
    for index, arg in enumerate(sys.argv[1:]):
        if skip_next:
            skip_next = False
            continue
        if arg == "--reload":
            continue
        if arg == "--reload-seconds":
            skip_next = True
            continue
        if arg.startswith("--reload-seconds="):
            continue
        argv.append(arg)
    return argv


def _run_with_reload(*, reload_seconds: float) -> None:
    root = Path(__file__).resolve().parent
    argv = _reloadable_argv()
    process = subprocess.Popen(argv)
    last_mtime = _python_files_mtime(root)

    try:
        while True:
            time.sleep(max(0.1, reload_seconds))
            returncode = process.poll()
            if returncode is not None:
                print(f"Worker process {process.pid} exited with {returncode}; restarting")
                process = subprocess.Popen(argv)
                last_mtime = _python_files_mtime(root)
                continue

            current_mtime = _python_files_mtime(root)
            if current_mtime > last_mtime:
                print("Python file change detected; restarting worker")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                process = subprocess.Popen(argv)
                last_mtime = current_mtime
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def _stop_dev_worker_processes(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.time() + 5
    for process in processes:
        remaining = max(0, deadline - time.time())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
    for process in processes:
        if process.poll() is None:
            process.wait()


def run_dev_workers(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parent
    worker_count = _coalesce(args.workers, 1)
    poll_seconds = _coalesce(args.poll_seconds, 5.0)
    reload_seconds = _coalesce(args.reload_seconds, 1.0)
    allow_uninterested = bool(args.allow_entitydata_without_interest)
    processes = _start_dev_worker_processes(
        worker_count,
        poll_seconds,
        allow_uninterested=allow_uninterested,
    )
    last_mtime = _python_files_mtime(root)

    def stop_all(signum=None, frame=None):
        _stop_dev_worker_processes(processes)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGINT, stop_all)

    while True:
        time.sleep(max(0.1, reload_seconds))
        for process in processes:
            if process.poll() is not None:
                print(f"Worker process {process.pid} exited with {process.returncode}; restarting all workers")
                _stop_dev_worker_processes(processes)
                processes[:] = _start_dev_worker_processes(
                    worker_count,
                    poll_seconds,
                    allow_uninterested=allow_uninterested,
                )
                last_mtime = _python_files_mtime(root)
                break

        if args.reload:
            current_mtime = _python_files_mtime(root)
            if current_mtime > last_mtime:
                print("Python file change detected; restarting all workers")
                _stop_dev_worker_processes(processes)
                processes[:] = _start_dev_worker_processes(
                    worker_count,
                    poll_seconds,
                    allow_uninterested=allow_uninterested,
                )
                last_mtime = current_mtime


def main():
    args = parse_args()
    configure_logging(args)
    def dispatch() -> None:
        if args.command in {"launch", "run", "deploy", "stop"}:
            try:
                manifest = load_runtime_manifest(Path(_coalesce(args.manifest, DEFAULT_RUNTIME_MANIFEST_PATH)))
                if args.command == "launch":
                    units = select_units(manifest, names=args.names, groups=args.groups, default_groups=("dev",))
                    launch_units(
                        units,
                        defaults_env=manifest.defaults_env,
                        reload=bool(args.reload),
                        reload_seconds=_coalesce(args.reload_seconds, 1.0),
                    )
                elif args.command == "run":
                    units = select_units(manifest, names=args.names, groups=args.groups, default_groups=("maintenance",))
                    run_units(units, defaults_env=manifest.defaults_env)
                elif args.command == "deploy":
                    units = select_units(manifest, names=args.names, groups=args.groups, default_groups=("toolforge",))
                    deploy_units(
                        units,
                        defaults_env=manifest.defaults_env,
                        image=args.image,
                        mount=args.mount,
                        once=bool(args.once),
                        dry_run=bool(args.dry_run),
                    )
                elif args.command == "stop":
                    units = select_units(manifest, names=args.names, groups=args.groups, default_groups=("toolforge",))
                    stop_units(units, dry_run=bool(args.dry_run))
            except ValueError as exc:
                raise SystemExit(str(exc)) from exc
        elif args.command == "worker":
            from wd_notability.content.worker import run_worker_pool, work_queued_items

            if _coalesce(args.limit, 0) > 0:
                processed = asyncio.run(
                    work_queued_items(
                        limit=_coalesce(args.limit, 0),
                        allow_uninterested=bool(args.allow_entitydata_without_interest),
                    )
                )
                print(f"Processed {processed} entitydata item(s)")
            else:
                if args.reload:
                    _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
                else:
                    try:
                        worker_kwargs = {
                            "allow_uninterested": bool(args.allow_entitydata_without_interest),
                        }
                        if args.workers is not None:
                            worker_kwargs["worker_count"] = args.workers
                        if args.poll_seconds is not None:
                            worker_kwargs["poll_seconds"] = args.poll_seconds
                        asyncio.run(run_worker_pool(**worker_kwargs))
                    except KeyboardInterrupt:
                        print("Worker pool stopped")
        elif args.command == "dev-workers":
            run_dev_workers(args)
        elif args.command == "inlinks-worker":
            from wd_notability.inlinks.worker import (
                INLINKS_WORKER_LOCK_TARGET,
                inlinks_worker_loop,
                work_inlinks_pass,
            )

            if _coalesce(args.limit, 0) > 0:
                with acquire_file_lock(INLINKS_WORKER_LOCK_TARGET):
                    inlinks_batch_size = _coalesce(args.batch_size, INLINKS_VISIBLE_LIMIT)
                    processed = asyncio.run(work_inlinks_pass(batch_size=inlinks_batch_size, limit=_coalesce(args.limit, 0)))
                print(f"Processed {processed} inlinks candidate qid(s)")
            else:
                if args.reload:
                    _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
                else:
                    try:
                        inlinks_kwargs = {}
                        if args.batch_size is not None:
                            inlinks_kwargs["batch_size"] = args.batch_size
                        if args.run_interval_seconds is not None:
                            inlinks_kwargs["run_interval_seconds"] = args.run_interval_seconds
                        asyncio.run(
                            inlinks_worker_loop(**inlinks_kwargs)
                        )
                    except KeyboardInterrupt:
                        print("Inlinks worker stopped")
        elif args.command == "recent-changes-worker":
            from wd_notability.content.recent_changes import recent_changes_worker_loop

            if args.reload:
                _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
            else:
                try:
                    recent_changes_kwargs = {}
                    if args.poll_seconds is not None:
                        recent_changes_kwargs["poll_seconds"] = args.poll_seconds
                    if args.rewind_seconds is not None:
                        recent_changes_kwargs["rewind_seconds"] = args.rewind_seconds
                    asyncio.run(
                        recent_changes_worker_loop(**recent_changes_kwargs)
                    )
                except KeyboardInterrupt:
                    print("Recent changes worker stopped")
        elif args.command == "entitydata-deletion-worker":
            from wd_notability.content.deletion import deletion_monitor_loop

            if args.reload:
                _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
            else:
                try:
                    deletion_kwargs = {}
                    if args.poll_seconds is not None:
                        deletion_kwargs["poll_seconds"] = args.poll_seconds
                    if args.batch_size is not None:
                        deletion_kwargs["batch_size"] = args.batch_size
                    asyncio.run(
                        deletion_monitor_loop(**deletion_kwargs)
                    )
                except KeyboardInterrupt:
                    print("EntityData deletion monitor stopped")
        elif args.command == "cache-sync-worker":
            from wd_notability.external_usage.worker import (
                CACHE_SYNC_WORKER_BATCH_SIZE,
                CACHE_SYNC_WORKER_LOCK_TARGET,
                cache_sync_worker_loop,
                work_cache_sync_pass,
            )

            if _coalesce(args.limit, 0) > 0:
                with acquire_file_lock(CACHE_SYNC_WORKER_LOCK_TARGET):
                    cache_sync_batch_size = _coalesce(args.batch_size, CACHE_SYNC_WORKER_BATCH_SIZE)
                    processed = asyncio.run(work_cache_sync_pass(batch_size=cache_sync_batch_size, limit=_coalesce(args.limit, 0)))
                print(f"Processed {processed} cache sync candidate qid(s)")
            else:
                if args.reload:
                    _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
                else:
                    try:
                        cache_sync_kwargs = {}
                        if args.batch_size is not None:
                            cache_sync_kwargs["batch_size"] = args.batch_size
                        if args.run_interval_seconds is not None:
                            cache_sync_kwargs["run_interval_seconds"] = args.run_interval_seconds
                        asyncio.run(
                            cache_sync_worker_loop(**cache_sync_kwargs)
                        )
                    except KeyboardInterrupt:
                        print("Cache sync worker stopped")
        elif args.command == "cache-observability-worker":
            from wd_notability.cache_observability import cache_observability_worker_loop

            try:
                cache_observability_kwargs = {}
                if args.run_interval_seconds is not None:
                    cache_observability_kwargs["run_interval_seconds"] = args.run_interval_seconds
                asyncio.run(cache_observability_worker_loop(**cache_observability_kwargs))
            except KeyboardInterrupt:
                print("Cache observability worker stopped")
        elif args.command == "wikisub-worker":
            from wd_notability.external_usage.wiki_subscribers.worker import wikisub_worker_loop

            if args.reload:
                _run_with_reload(reload_seconds=_coalesce(args.reload_seconds, 1.0))
            else:
                try:
                    wikisub_args = _namespace_with_defaults(
                        args,
                        lookup_cache=str(WIKISUB_LOOKUP_CACHE_PATH),
                        main_cache=str(WIKISUB_MAIN_CACHE_PATH),
                        block_size=WIKISUB_BLOCK_SIZE,
                        sleep_seconds=WIKISUB_SLEEP_SECONDS,
                        poll_seconds=WIKISUB_WORKER_POLL_SECONDS,
                        defaults_file=str(WIKISUB_DEFAULTS_FILE),
                        database=WIKISUB_DATABASE,
                        host=WIKISUB_HOST,
                    )
                    asyncio.run(
                        wikisub_worker_loop(
                            lookup_cache_path=Path(wikisub_args.lookup_cache),
                            main_cache_path=Path(wikisub_args.main_cache),
                            block_size=max(1, wikisub_args.block_size),
                            sleep_seconds=max(0.0, wikisub_args.sleep_seconds),
                            poll_seconds=max(0.0, wikisub_args.poll_seconds),
                            args=wikisub_args,
                        )
                    )
                except KeyboardInterrupt:
                    print("Wikisub worker stopped")
        elif args.command == "build-osm-cache":
            from wd_notability.external_usage.osm.builder import OSM_BUILDER

            asyncio.run(
                OSM_BUILDER(
                    Path(_coalesce(args.output, DEFAULT_LOOKUP_CACHE_PATH)),
                    _coalesce(args.page_size, 999),
                    limit=_coalesce(args.limit, 0),
                    sync_main_cache_only=bool(args.sync_main_cache_only),
                )
            )
        elif args.command == "build-sdc-cache":
            from wd_notability.external_usage.sdc.builder import DUMP_URL, SDC_BUILDER

            asyncio.run(
                SDC_BUILDER(
                    Path(_coalesce(args.output, DEFAULT_LOOKUP_CACHE_PATH)),
                    _coalesce(args.dump_url, DUMP_URL),
                    force=bool(args.force),
                    sync_main_cache_only=bool(args.sync_main_cache_only),
                    progress=bool(_coalesce(args.progress, True)),
                )
            )
        elif args.command == "build-wikisub-cache":
            from wd_notability.external_usage.wiki_subscribers.builder import WIKI_SUBSCRIBERS_BUILDER

            wikisub_args = _namespace_with_defaults(
                args,
                output=str(LOOKUP_CACHE_PATH),
                main_cache=str(EVALUATION_CACHE_PATH),
                block_size=WIKISUB_BLOCK_SIZE,
                sleep_seconds=WIKISUB_SLEEP_SECONDS,
                sync_main_cache=True,
                sync_main_cache_only=False,
                defaults_file=str(WIKISUB_DEFAULTS_FILE),
                database=WIKISUB_DATABASE,
                host=WIKISUB_HOST,
                progress=False,
            )
            asyncio.run(
                WIKI_SUBSCRIBERS_BUILDER(
                    Path(wikisub_args.output),
                    max(1, wikisub_args.block_size),
                    max(0.0, wikisub_args.sleep_seconds),
                    bool(wikisub_args.sync_main_cache),
                    bool(wikisub_args.sync_main_cache_only),
                    Path(wikisub_args.main_cache),
                    wikisub_args,
                    progress=bool(wikisub_args.progress),
                )
            )
        elif args.command == "build-namespace-cache":
            from wd_notability.external_usage.namespace.builder import NAMESPACE_BUILDER

            namespace_output_dir = _coalesce(
                args.output_dir,
                str(Path("/tmp/wd-notability") if toolforge_defaults_file_exists() else Path.home() / "localdbs" / "wd-notability"),
            )
            asyncio.run(
                NAMESPACE_BUILDER(
                    Path(namespace_output_dir),
                    max(1, _coalesce(args.concurrency, 5)),
                    max(0, _coalesce(args.limit, 0)),
                    from_json=bool(args.from_json),
                    namespaces_json=Path(_coalesce(args.namespaces_json, str(Path(__file__).resolve().parent / "wd_notability" / "data" / "namespaces_by_site.json"))),
                    site_api_urls_json=Path(_coalesce(args.site_api_urls_json, str(Path(__file__).resolve().parent / "wd_notability" / "data" / "site_api_urls.json"))),
                )
            )
        elif args.command == "build-property-cache":
            from wd_notability.external_usage.property.builder import PROPERTY_BUILDER

            qids = args.qids if args.qids else None
            asyncio.run(
                PROPERTY_BUILDER(
                    Path(_coalesce(args.output, DEFAULT_LOOKUP_CACHE_PATH)),
                    sorted(set(qids)) if qids else [],
                    _coalesce(args.delay_seconds, 1.0),
                    from_json=bool(args.from_json),
                    properties_json=Path(_coalesce(args.properties_json, str(Path(__file__).resolve().parent / "wd_notability" / "data" / "property_instances_by_qid.json"))),
                )
            )
        elif args.command == "reset-main-cache":
            from wd_notability.evaluation_cache import reset_main_cache

            asyncio.run(reset_main_cache(Path(_coalesce(args.main_cache, DEFAULT_EVALUATION_CACHE_PATH))))
        elif args.command == "serve":
            import uvicorn

            uvicorn.run(
                "server.app:app",
                host=args.host,
                port=args.port,
                reload=bool(args.reload),
            )

    if args.profile:
        _run_profiled(
            dispatch,
            sort_by=args.profile_sort,
            limit=args.profile_limit,
            output_path=args.profile_output.strip() or None,
        )
    else:
        dispatch()


if __name__ == "__main__":
    main()
