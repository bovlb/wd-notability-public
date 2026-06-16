from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from wd_notability.file_lock import acquire_file_lock
from wd_notability.workers.cache_sync import CACHE_SYNC_WORKER_LOCK_TARGET, cache_sync_worker_loop, work_cache_sync_pass
from wd_notability.workers.entitydata import run_worker_pool, work_queued_items
from wd_notability.workers.inlinks import INLINKS_WORKER_LOCK_TARGET, inlinks_worker_loop, work_inlinks_pass
from wd_notability.workers.recent_changes import recent_changes_worker_loop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="wd_notability utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Process entitydata evaluation batches")
    worker_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of items to process before exiting (0 = run continuously)",
    )
    worker_parser.add_argument("--workers", type=int, default=1, help="Number of worker coroutines to run")
    worker_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=5.0,
        help="Delay between empty-batch polls in continuous mode",
    )

    dev_parser = subparsers.add_parser("dev-workers", help="Run the worker pool for development")
    dev_parser.add_argument("--workers", type=int, default=1, help="Number of worker coroutines to run")
    dev_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=1.0,
        help="Delay between empty-batch polls in each worker process",
    )
    dev_parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart all worker processes when Python files change",
    )
    dev_parser.add_argument(
        "--reload-seconds",
        type=float,
        default=1.0,
        help="Delay between reload file scans",
    )

    inlinks_parser = subparsers.add_parser("inlinks-worker", help="Process unknown inlinks and queue missing N12 work")
    inlinks_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of candidate qids to process before exiting (0 = run continuously)",
    )
    inlinks_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of candidate qids to process per batch",
    )
    inlinks_parser.add_argument(
        "--run-interval-seconds",
        type=float,
        default=600.0,
        help="Minimum delay between the start of successive inlinks runs",
    )

    recent_changes_parser = subparsers.add_parser("recent-changes-worker", help="Monitor recent changes and refresh cached revision metadata")
    recent_changes_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=60.0,
        help="Delay between successive recent changes polls",
    )
    recent_changes_parser.add_argument(
        "--rewind-seconds",
        type=float,
        default=300.0,
        help="How far back to start on the first poll",
    )

    cache_sync_parser = subparsers.add_parser("cache-sync-worker", help="Sync interested cache rows from the side caches")
    cache_sync_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of candidate qids to process before exiting (0 = run continuously)",
    )
    cache_sync_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of candidate qids to process per batch",
    )
    cache_sync_parser.add_argument(
        "--run-interval-seconds",
        type=float,
        default=60.0,
        help="Minimum delay between the start of successive sync runs",
    )
    return parser.parse_args()


def _python_files_mtime(root: Path) -> float:
    newest = 0.0
    for path in [root / "main.py", *(root / "wd_notability").rglob("*.py"), *(root / "server").rglob("*.py")]:
        try:
            newest = max(newest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return newest


def _start_dev_worker_processes(worker_count: int, poll_seconds: float) -> list[subprocess.Popen]:
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
    print(f"Starting worker process with {worker_count} coroutine(s)")
    return [subprocess.Popen(cmd)]


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
    processes = _start_dev_worker_processes(args.workers, args.poll_seconds)
    last_mtime = _python_files_mtime(root)

    def stop_all(signum=None, frame=None):
        _stop_dev_worker_processes(processes)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGINT, stop_all)

    while True:
        time.sleep(max(0.1, args.reload_seconds))
        for process in processes:
            if process.poll() is not None:
                print(f"Worker process {process.pid} exited with {process.returncode}; restarting all workers")
                _stop_dev_worker_processes(processes)
                processes[:] = _start_dev_worker_processes(args.workers, args.poll_seconds)
                last_mtime = _python_files_mtime(root)
                break

        if args.reload:
            current_mtime = _python_files_mtime(root)
            if current_mtime > last_mtime:
                print("Python file change detected; restarting all workers")
                _stop_dev_worker_processes(processes)
                processes[:] = _start_dev_worker_processes(args.workers, args.poll_seconds)
                last_mtime = current_mtime


def main():
    args = parse_args()
    if args.command == "worker":
        if args.limit > 0:
            processed = asyncio.run(work_queued_items(limit=args.limit))
            print(f"Processed {processed} entitydata item(s)")
        else:
            try:
                asyncio.run(
                    run_worker_pool(
                        worker_count=args.workers,
                        poll_seconds=args.poll_seconds,
                    )
                )
            except KeyboardInterrupt:
                print("Worker pool stopped")
    elif args.command == "dev-workers":
        run_dev_workers(args)
    elif args.command == "inlinks-worker":
        if args.limit > 0:
            with acquire_file_lock(INLINKS_WORKER_LOCK_TARGET):
                processed = asyncio.run(work_inlinks_pass(batch_size=args.batch_size, limit=args.limit))
            print(f"Processed {processed} inlinks candidate qid(s)")
        else:
            try:
                asyncio.run(
                    inlinks_worker_loop(
                        batch_size=args.batch_size,
                        run_interval_seconds=args.run_interval_seconds,
                    )
                    )
            except KeyboardInterrupt:
                print("Inlinks worker stopped")
    elif args.command == "recent-changes-worker":
        try:
            asyncio.run(
                recent_changes_worker_loop(
                    poll_seconds=args.poll_seconds,
                    rewind_seconds=args.rewind_seconds,
                )
            )
        except KeyboardInterrupt:
            print("Recent changes worker stopped")
    elif args.command == "cache-sync-worker":
        if args.limit > 0:
            with acquire_file_lock(CACHE_SYNC_WORKER_LOCK_TARGET):
                processed = asyncio.run(work_cache_sync_pass(batch_size=args.batch_size, limit=args.limit))
            print(f"Processed {processed} cache sync candidate qid(s)")
        else:
            try:
                asyncio.run(
                    cache_sync_worker_loop(
                        batch_size=args.batch_size,
                        run_interval_seconds=args.run_interval_seconds,
                    )
                )
            except KeyboardInterrupt:
                print("Cache sync worker stopped")


if __name__ == "__main__":
    main()
