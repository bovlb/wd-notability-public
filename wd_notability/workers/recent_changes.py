from __future__ import annotations

import asyncio
import calendar
import time
from pathlib import Path

from wd_notability.evaluation_cache import CACHE
from wd_notability.async_limiters import WIKIDATA_ACTION_API_LIMITER
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.wikidata_api import WIKIDATA_API_URL, WikidataBackoffActiveError, wikidata_session

RECENT_CHANGES_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "recent_changes_worker"
RECENT_CHANGES_WORKER_POLL_SECONDS = 60.0
RECENT_CHANGES_WORKER_REWIND_SECONDS = 300.0
RECENT_CHANGES_WORKER_OVERLAP_SECONDS = 5.0
RECENT_CHANGES_API_BATCH_SIZE = 500


def _format_rc_timestamp(epoch_seconds: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch_seconds))


def _parse_rc_timestamp(timestamp: object) -> float | None:
    if not isinstance(timestamp, str):
        return None
    try:
        return float(calendar.timegm(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")))
    except (ValueError, OverflowError):
        return None


async def _fetch_recent_changes_batch(start_epoch: float, *, continue_token: str | None = None) -> tuple[list[dict[str, object]], str | None]:
    params: dict[str, object] = {
        "action": "query",
        "list": "recentchanges",
        "rcnamespace": "0",
        "rcprop": "title|ids|timestamp",
        "rcdir": "newer",
        "rcstart": _format_rc_timestamp(start_epoch),
        "rclimit": "max",
        "format": "json",
    }
    if continue_token:
        params["rccontinue"] = continue_token

    response, timings = await wikidata_session.get_with_timings(
        WIKIDATA_API_URL,
        limiter=WIKIDATA_ACTION_API_LIMITER,
        params=params,
    )
    print(f"Fetched recent changes: {timings}")
    response.raise_for_status()
    payload = response.json()
    changes = payload.get("query", {}).get("recentchanges", [])
    if not isinstance(changes, list):
        changes = []
    next_continue = payload.get("continue", {}).get("rccontinue")
    return changes, next_continue if isinstance(next_continue, str) and next_continue else None


async def _work_recent_changes_pass(start_epoch: float) -> tuple[int, float]:
    latest_seen = start_epoch
    qid_to_revid: dict[str, int] = {}
    continue_token: str | None = None

    while True:
        changes, continue_token = await _fetch_recent_changes_batch(start_epoch, continue_token=continue_token)
        for change in changes:
            if not isinstance(change, dict):
                continue
            title = change.get("title")
            qid = title if isinstance(title, str) and title.startswith("Q") and title[1:].isdigit() else None
            revid = change.get("revid")
            timestamp = _parse_rc_timestamp(change.get("timestamp"))
            if qid is None or not isinstance(revid, int):
                continue
            if timestamp is not None:
                latest_seen = max(latest_seen, timestamp)
            previous = qid_to_revid.get(qid)
            if previous is None or revid > previous:
                qid_to_revid[qid] = revid

        if continue_token is None:
            break

    updated = 0
    if qid_to_revid:
        # Deduped by QID here: each target is written once, with the highest revid seen.
        updated = await CACHE.update_recent_changes_last_revids(qid_to_revid)
    return updated, latest_seen


async def recent_changes_worker_loop(
    *,
    poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS,
    rewind_seconds: float = RECENT_CHANGES_WORKER_REWIND_SECONDS,
) -> None:
    with acquire_file_lock(RECENT_CHANGES_WORKER_LOCK_TARGET):
        start_epoch = max(0.0, time.time() - max(0.0, rewind_seconds))
        while True:
            run_started = time.monotonic()
            try:
                await wait_for_foreground_evaluations()
                updated, latest_seen = await _work_recent_changes_pass(start_epoch)
                print(f"Recent changes worker updated {updated} cached item(s)")
                next_start = latest_seen - RECENT_CHANGES_WORKER_OVERLAP_SECONDS
                start_epoch = max(next_start, time.time() - RECENT_CHANGES_WORKER_OVERLAP_SECONDS)
            except WikidataBackoffActiveError as exc:
                print(f"Recent changes worker backing off for {exc.remaining_seconds:.1f} seconds")
                await asyncio.sleep(max(0.1, min(poll_seconds, exc.remaining_seconds)))
            except Exception as exc:  # noqa: BLE001
                print(f"Recent changes worker failed: {exc}")

            sleep_for = max(0.0, poll_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)
