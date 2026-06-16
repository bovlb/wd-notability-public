#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from wd_notability.env_loader import load_default_env
from wd_notability.file_lock import acquire_file_lock
from wd_notability.evaluation_cache import EvaluationCache

load_default_env()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset the main evaluation cache from lookup-backed sources.")
    parser.add_argument(
        "--main-cache",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "evaluation_cache.sqlite3"),
        help="Main evaluation cache path",
    )
    return parser.parse_args()


async def reset_main_cache(main_cache: Path) -> None:
    with acquire_file_lock(main_cache, "main"):
        cache = EvaluationCache(main_cache)
        await cache.initialize()

        await cache.clear()
        print("Reset main cache and flushed work queue")


def main() -> None:
    args = parse_args()
    asyncio.run(reset_main_cache(Path(args.main_cache)))


if __name__ == "__main__":
    main()
