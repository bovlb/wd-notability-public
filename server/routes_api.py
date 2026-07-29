from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import uuid4
from urllib.parse import parse_qs

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse

import server.app as app_module
from wd_notability.content.worker import find_content_qids
from server.schemas import (
    CreatorHistoryRequest,
    PubSubAddRequest,
    PubSubCreateRequest,
    PubSubRefreshRequest,
    SubscribeRequest,
)
from server.page_renderers import (
    _render_observability_dashboard_html,
    _render_item_trace_html,
    _render_pubsub_debugger_html,
    _render_static_markdown_page,
)
from server.badge_examples import list_badge_examples, render_badge_example_svg
from wd_notability.creations_status import creator_needs_attention, creator_status_bucket
from wd_notability.web.creations import (
    resolve_creation_metadata as web_resolve_creation_metadata,
    render_creations_dashboard_html as web_render_creations_dashboard_html,
)

router = APIRouter()
STATIC_DIR = Path(__file__).resolve().parent / "static"


def _creator_history_signature(history) -> tuple[object, ...] | None:
    if history is None:
        return None
    return (
        history.window_start,
        history.window_end,
        history.requested_at,
        history.started_at,
        history.finished_at,
        history.last_refresh_at,
        history.error_text,
        history.row_count,
    )


@router.get("/api/observability")
async def api_observability(
    period: str | None = Query(default="24h"),
    workers: list[str] = Query(default_factory=list),
):
    period_seconds = app_module._parse_observability_period(period)
    until = int(time.time())
    since = until - period_seconds
    worker_filters = [worker.strip() for worker in workers if isinstance(worker, str) and worker.strip()]
    series, workers = await app_module.CACHE.observability.snapshot_views(
        since=since,
        until=until,
        worker_names=worker_filters or None,
    )
    chartable_fields = {field: points for field, points in series.items() if field in app_module.OBSERVABILITY_FIELD_METADATA}
    chartable_workers = {
        worker_name: {field: points for field, points in fields.items() if field in app_module.OBSERVABILITY_FIELD_METADATA}
        for worker_name, fields in workers.items()
    }
    return {
        "period": period if period is not None else "24h",
        "period_seconds": period_seconds,
        "period_label": app_module._format_observability_title(period_seconds),
        "since": since,
        "until": until,
        "fields": chartable_fields,
        "workers": chartable_workers,
        "metrics": app_module._observability_metrics_payload(),
    }


@router.get("/api/item-trace")
async def api_item_trace(
    qid: str | None = Query(default=None),
    since: int | None = Query(default=None),
    until: int | None = Query(default=None),
    event_types: list[str] = Query(default_factory=list),
    workers: list[str] = Query(default_factory=list),
    limit: int | None = Query(default=200),
):
    if not app_module.ITEM_TRACE_ENABLED:
        raise HTTPException(status_code=404, detail="Item trace is disabled")
    trace = await app_module.CACHE.item_trace.list_events(
        qid=qid,
        since=since,
        until=until,
        event_types=event_types or None,
        worker_names=workers or None,
        limit=limit,
    )
    return {
        "qid": qid,
        "since": since,
        "until": until,
        "event_types": event_types,
        "workers": workers,
        "limit": limit,
        "count": len(trace),
        "items": trace,
    }


@router.get("/api/content-candidates")
async def api_content_candidates(
    limit: int = 1000,
):
    qids = await find_content_qids(limit)
    content_reasons = await app_module.CACHE.interest.list_interest_content_candidate_reasons(qids)
    return {
        "limit": limit,
        "count": len(qids),
        "items": [
            {
                "row_number": index,
                "qid": qid,
                "content_reason": content_reasons.get(qid),
            }
            for index, qid in enumerate(qids, start=1)
        ],
    }


@router.get("/api/inlinks-candidates")
async def api_inlinks_candidates(
    limit: int = 1000,
):
    rows = await app_module.CACHE.list_inlinks_work_candidates(limit=limit)
    return {
        "limit": limit,
        "count": len(rows),
        "items": [
            {
                "row_number": index,
                "qid": qid,
                "inlinks_last_evaluated": inlinks_last_evaluated,
                "active_priority": active_priority,
                "is_unknown": is_unknown,
            }
            for index, (qid, inlinks_last_evaluated, active_priority, is_unknown) in enumerate(rows, start=1)
        ],
    }


@router.get("/api/badge-examples")
async def api_badge_examples():
    items = list_badge_examples()
    return {
        "count": len(items),
        "items": items,
    }


@router.get("/api/badge-examples/{example_id}.svg")
async def api_badge_example_svg(example_id: str):
    try:
        svg = render_badge_example_svg(example_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Badge example not found") from exc
    return Response(content=svg, media_type="image/svg+xml")


@router.get("/api/creations")
async def api_creations(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    creators: list[str] = Query(default_factory=list),
):
    if isinstance(start, str) and not start.strip():
        start = None
    if isinstance(end, str) and not end.strip():
        end = None
    if start is None or end is None:
        default_start, default_end = app_module.CREATIONS.default_window()
        start = default_start if start is None else start
        end = default_end if end is None else end

    try:
        rows = await asyncio.to_thread(
            app_module.CREATIONS.fetch_creations,
            start=start,
            end=end,
            creators=creators,
        )
        print(
            f"Queried replica creations for start={start}, end={end}, creators={creators}: got {len(rows)} rows"
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "start": start,
        "end": end,
        "creators": creators,
        "items": [
            {
                "qid": row.qid,
                "creator": row.creator,
                "creation_time": row.creation_time,
            }
            for row in rows
        ],
    }


@router.get("/api/creations/users/{username}/history")
async def api_creator_history(username: str):
    normalized_username = app_module._normalize_creator_username(username)
    history = await app_module.CACHE.get_user_history(normalized_username)
    return {
        "username": normalized_username,
        "history": app_module._creator_history_payload(history),
        "exists": history is not None,
    }


@router.post("/api/creations/users/{username}/history")
async def api_creator_history_request(username: str, request: CreatorHistoryRequest):
    normalized_username = app_module._normalize_creator_username(username)
    try:
        history, queued = await app_module.CACHE.request_user_history(
            username=normalized_username,
            window_start=request.window_start,
            window_end=request.window_end,
            force=bool(request.force),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "username": normalized_username,
        "queued": queued,
        "history": app_module._creator_history_payload(history),
    }


@router.get("/api/creations/users/{username}/items")
async def api_creator_items(
    username: str,
    ensure: bool = Query(default=False),
    window_start: str | None = Query(default=None),
    window_end: str | None = Query(default=None),
    status: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
):
    normalized_username = app_module._normalize_creator_username(username)

    if ensure:
        try:
            await app_module.CACHE.request_user_history(
                username=normalized_username,
                window_start=window_start,
                window_end=window_end,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    history = await app_module.CACHE.get_user_history(normalized_username)
    if history is None:
        return {
            "username": normalized_username,
            "history": None,
            "queued": False,
            "status": status,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "counts": {
                "all": 0,
                "strong": 0,
                "partial_weak": 0,
                "partial_strong": 0,
                "weak": 0,
                "unknown": 0,
                "none": 0,
                "redirect": 0,
                "deleted": 0,
                "needs_attention": 0,
            },
            "items": [],
        }

    status_key = status.strip().lower()
    allowed_statuses = {
        "all",
        "strong",
        "partial_weak",
        "partial_strong",
        "weak",
        "unknown",
        "none",
        "redirect",
        "deleted",
        "needs_attention",
    }
    if status_key not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail=(
                "status must be one of all, strong, partial_weak, partial_strong, weak, "
                "unknown, none, redirect, deleted, or needs_attention"
            ),
        )

    creator_actor_map = await asyncio.to_thread(app_module.CREATIONS.lookup_actor_ids, [normalized_username])
    creator_actor_id = creator_actor_map.get(normalized_username)
    if creator_actor_id is None:
        return {
            "username": normalized_username,
            "history": app_module._creator_history_payload(history),
            "queued": False,
            "status": status_key,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "counts": {
                "all": 0,
                "strong": 0,
                "partial_weak": 0,
                "partial_strong": 0,
                "weak": 0,
                "unknown": 0,
                "none": 0,
                "redirect": 0,
                "deleted": 0,
                "needs_attention": 0,
            },
            "items": [],
        }

    try:
        creation_rows = await app_module.CACHE.list_creation_metadata(
            start=history.window_start,
            end=history.window_end,
            creator_actor_ids=[creator_actor_id],
        )
        summaries = await app_module.CACHE.get_many([row.qid for row in creation_rows])
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    counts = {
        "all": 0,
        "strong": 0,
        "partial_weak": 0,
        "partial_strong": 0,
        "weak": 0,
        "unknown": 0,
        "none": 0,
        "redirect": 0,
        "deleted": 0,
        "needs_attention": 0,
    }
    annotated_items: list[dict[str, object]] = []
    for row in creation_rows:
        cached_result = summaries.get(row.qid)
        if cached_result is None:
            continue
        bucket = creator_status_bucket(cached_result)
        counts["all"] += 1
        counts[bucket] += 1
        if creator_needs_attention(bucket):
            counts["needs_attention"] += 1
        annotated_items.append(
            {
                "qid": row.qid,
                "creation_time": row.creation_time,
                "creator": normalized_username,
                "status": bucket,
                "needs_attention": creator_needs_attention(bucket),
                "is_redirect": cached_result.is_redirect,
                "is_deleted": cached_result.is_deleted,
                "levels": cached_result.levels_str,
                "content_last_revid": cached_result.content_last_revid,
                "recent_changes_last_revid": cached_result.recent_changes_last_revid,
            }
        )

    if status_key == "needs_attention":
        filtered_items = [item for item in annotated_items if bool(item["needs_attention"])]
    elif status_key == "all":
        filtered_items = annotated_items
    else:
        filtered_items = [item for item in annotated_items if item["status"] == status_key]

    total = len(filtered_items)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    page_items = filtered_items[start_index:end_index]

    return {
        "username": normalized_username,
        "history": app_module._creator_history_payload(history),
        "queued": False,
        "status": status_key,
        "page": page,
        "page_size": page_size,
        "total": total,
        "counts": counts,
        "items": page_items,
    }


@router.get("/api/creations/users/{username}/events")
async def api_creator_events(
    username: str,
    request: Request,
    poll_seconds: float = Query(default=2.0, gt=0.0),
):
    normalized_username = app_module._normalize_creator_username(username)
    return StreamingResponse(
        _creator_dashboard_event_stream(
            normalized_username,
            request,
            poll_seconds=poll_seconds,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


async def _creator_dashboard_event_stream(
    username: str,
    request: Request,
    *,
    poll_seconds: float = 2.0,
) -> AsyncGenerator[str, None]:
    stream_task = await app_module._register_stream_task()
    deadline = time.monotonic() + app_module.SSE_STREAM_MAX_SECONDS
    normalized_username = app_module._normalize_creator_username(username)
    creator_actor_id: int | None = None
    last_history_signature: tuple[object, ...] | None = None
    last_seen: dict[str, tuple[int, int | None, int | None, bool | None]] = {}
    try:
        while time.monotonic() < deadline:
            if app_module.SHUTDOWN_EVENT is not None and app_module.SHUTDOWN_EVENT.is_set():
                break
            if await request.is_disconnected():
                break

            sent_anything = False
            history = await app_module.CACHE.get_user_history(normalized_username)
            history_signature = _creator_history_signature(history)
            if history_signature != last_history_signature:
                payload = {
                    "event": "history_update",
                    "username": normalized_username,
                    "history": app_module._creator_history_payload(history),
                }
                yield app_module._sse_message(payload)
                sent_anything = True
                if history is not None and history.finished_at is not None and (
                    last_history_signature is None or len(last_history_signature) < 5 or last_history_signature[4] is None
                ):
                    yield app_module._sse_message(
                        {
                            "event": "history_complete",
                            "username": normalized_username,
                            "history": app_module._creator_history_payload(history),
                        }
                    )
                    sent_anything = True
                if history is not None and history.error_text is not None:
                    yield app_module._sse_message(
                        {
                            "event": "history_failed",
                            "username": normalized_username,
                            "history": app_module._creator_history_payload(history),
                        }
                    )
                    sent_anything = True
                last_history_signature = history_signature

            if history is not None:
                if creator_actor_id is None:
                    creator_actor_map = await asyncio.to_thread(app_module.CREATIONS.lookup_actor_ids, [normalized_username])
                    creator_actor_id = creator_actor_map.get(normalized_username)

                if creator_actor_id is not None:
                    creation_rows = await app_module.CACHE.list_creation_metadata(
                        start=history.window_start,
                        end=history.window_end,
                        creator_actor_ids=[creator_actor_id],
                    )
                    creation_by_qid = {row.qid: row for row in creation_rows}
                    qids = list(creation_by_qid)
                    if qids:
                        cached_rows = await app_module.CACHE.get_many(qids)
                        content_staleness_fetcher = getattr(
                            app_module.CACHE, "get_content_staleness_for_qids", None
                        )
                        if content_staleness_fetcher is None:
                            content_staleness = {}
                        else:
                            content_staleness = await content_staleness_fetcher(qids)
                        emitted = set()
                        for qid in qids:
                            if await request.is_disconnected():
                                return
                            row = cached_rows.get(qid)
                            if row is None:
                                continue
                            current_seen = (
                                row.n1,
                                row.n2a,
                                row.n2b,
                                row.n3_inlinks,
                                row.n3_osm,
                                row.n3_wikisub,
                                row.n3_sdc,
                                row.inlinks_count,
                                row.content_last_revid,
                                row.recent_changes_last_revid,
                                content_staleness.get(qid),
                            )
                            if last_seen.get(qid) == current_seen:
                                continue
                            last_seen[qid] = current_seen
                            emitted.add(qid)
                            yield app_module._sse_message(
                                app_module._badge_payload(
                                    qid,
                                    row,
                                    content_stale=content_staleness.get(qid),
                                    creator=normalized_username,
                                    creation_time=creation_by_qid[qid].creation_time,
                                )
                            )
                            sent_anything = True
                        if emitted:
                            print(
                                f"Emitted updates for {len(emitted)} creator QID(s) for {normalized_username}"
                            )

            if not sent_anything:
                yield app_module._sse_message({"event": "keepalive"})

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if await app_module._sleep_or_shutdown(min(poll_seconds, remaining)):
                break

        if (
            time.monotonic() >= deadline
            and (app_module.SHUTDOWN_EVENT is None or not app_module.SHUTDOWN_EVENT.is_set())
            and not await request.is_disconnected()
        ):
            yield app_module._sse_message({"event": "stream_end"})
    finally:
        await app_module._unregister_stream_task(stream_task)


@router.get("/api/items/{qid}/signals")
async def api_item_signals(qid: str):
    return await app_module._evaluate_or_404(qid)


@router.get("/api/evaluate/{qid}")
async def api_evaluate_compat(qid: str):
    return await app_module._evaluate_or_404(qid)


@router.get("/api/cache/stats")
async def api_cache_stats():
    return {
        **await app_module.CACHE.stats(),
        "lookup_cache": app_module.lookup_cache.stats(),
    }


@router.get("/api/cache/breakdown")
async def api_cache_breakdown():
    return await app_module.CACHE.breakdown()


@router.get("/api/cache/pubsub-stats")
async def api_cache_pubsub_stats():
    return await app_module.CACHE.interest.interest_stats()


@router.get("/api/pubsub/debug")
async def api_pubsub_debug(limit: int | None = Query(default=None)):
    items = await app_module.CACHE.interest.list_interest_items(limit=limit)
    stats = await app_module.CACHE.interest.interest_stats()
    return {
        "generated_at": int(time.time()),
        "stats": stats,
        "items": items,
    }


async def _parse_subscribe_request(request: Request) -> SubscribeRequest:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type == "application/json":
        payload = await request.json()
        return SubscribeRequest.model_validate(payload)

    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(status_code=400, detail="subscribe request body must not be empty")

    form = parse_qs(raw_body.decode("utf-8"), keep_blank_values=True)
    session_id = form.get("session_id", [""])[0] or None

    items_raw = form.get("items", [""])[0].strip()
    qids_raw = form.get("qids", [""])[0].strip()
    items: list[dict[str, object]] = []
    qids: list[str] = []

    if items_raw:
        parsed_items = json.loads(items_raw)
        if isinstance(parsed_items, list):
            items = [item for item in parsed_items if isinstance(item, dict)]
    elif qids_raw:
        parsed_qids = json.loads(qids_raw)
        if isinstance(parsed_qids, list):
            qids = [str(qid) for qid in parsed_qids]

    return SubscribeRequest(qids=qids, items=items, session_id=session_id)


@router.get("/api/worker-queue-stats")
async def api_worker_queue_stats():
    content, deletion, inlinks, recent_changes = await asyncio.gather(
        app_module.content_queue_stats(),
        app_module.deletion_queue_stats(),
        app_module.inlinks_queue_stats(),
        app_module.recent_changes_queue_stats(),
    )
    return {
        "content": content,
        "deletion": deletion,
        "inlinks": inlinks,
        "recent_changes": recent_changes,
    }


@router.get("/observability", response_class=HTMLResponse)
async def observability_page():
    return _render_observability_dashboard_html(app_module.OBSERVABILITY_JS_VERSION)


@router.get("/item-trace", response_class=HTMLResponse)
async def item_trace_page():
    if not app_module.ITEM_TRACE_ENABLED:
        raise HTTPException(status_code=404, detail="Item trace is disabled")
    return _render_item_trace_html(app_module.ITEM_TRACE_JS_VERSION)


@router.get("/pubsub", response_class=HTMLResponse)
async def pubsub_debug_page():
    return _render_pubsub_debugger_html()


@router.post("/subscribe")
async def api_subscribe(request: Request):
    request = await _parse_subscribe_request(request)
    items = app_module._normalize_subscription_items(request)
    if not items:
        raise HTTPException(
            status_code=400, detail="qids must include at least one valid QID")

    qids = list(items)
    print(f"Subscription request for {len(qids)} QIDs")

    subscription_id = request.session_id.strip() if isinstance(
        request.session_id, str) else ""
    if not subscription_id:
        subscription_id = str(app_module.uuid4())

    await app_module._store_pubsub_subscription_qids(subscription_id, qids)

    return {
        "subscription_id": subscription_id,
        "reevaluate": app_module.REVALUATE_ON_SUBSCRIBE,
    }


@router.post("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_create_session(owner_id: str, session_id: str, request: PubSubCreateRequest):
    owner = app_module._normalize_owner_id(owner_id)
    qids = app_module._normalize_qids(request.qids)
    created = await app_module.CACHE.interest.create_interest_lease(
        owner_id=owner,
        lease_id=session_id,
        ttl_seconds=request.ttl_seconds,
        priority=request.priority,
        wants_creation=request.wants_creation,
        wants_content=request.wants_content,
        wants_inlinks=request.wants_inlinks,
        qids=qids,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "ttl_seconds": request.ttl_seconds,
        "qids": qids,
        "created_rows": created,
    }


@router.post("/api/pubsub/sessions/{owner_id}/{session_id}/qids")
async def api_pubsub_add_session_qids(owner_id: str, session_id: str, request: PubSubAddRequest):
    owner = app_module._normalize_owner_id(owner_id)
    qids = app_module._normalize_qids(request.qids)
    added = await app_module.CACHE.interest.add_interest_lease_qids(
        owner_id=owner,
        lease_id=session_id,
        qids=qids,
        priority=request.priority,
        wants_creation=request.wants_creation,
        wants_content=request.wants_content,
        wants_inlinks=request.wants_inlinks,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "qids": qids,
        "added_rows": added,
    }


@router.patch("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_refresh_session(owner_id: str, session_id: str, request: PubSubRefreshRequest):
    owner = app_module._normalize_owner_id(owner_id)
    refreshed = await app_module.CACHE.interest.refresh_interest_lease(
        owner_id=owner,
        lease_id=session_id,
        ttl_seconds=request.ttl_seconds,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "ttl_seconds": request.ttl_seconds,
        "refreshed_rows": refreshed,
    }


@router.delete("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_delete_session(owner_id: str, session_id: str):
    owner = app_module._normalize_owner_id(owner_id)
    deleted = await app_module.CACHE.interest.delete_interest_lease(owner_id=owner, lease_id=session_id)
    return {
        "owner_id": owner,
        "session_id": session_id,
        "deleted_rows": deleted,
    }


async def _pubsub_event_stream(
    owner_id: str,
    session_id: str,
    request: Request,
    *,
    after_event_id: int = 0,
    poll_seconds: float = 0.5,
) -> AsyncGenerator[str, None]:
    stream_task = await app_module._register_stream_task()
    deadline = time.monotonic() + app_module.SSE_STREAM_MAX_SECONDS
    qid_list = await app_module._get_pubsub_subscription_qids(session_id)
    last_seen: dict[str, tuple[int, int | None, int | None, bool | None]] = {}
    manager = None
    session = None
    try:
        if not qid_list:
            yield app_module._sse_message({
                "event": "primed",
                "owner_id": owner_id,
                "session_id": session_id,
                "qid_count": 0,
            })
            yield app_module._sse_message({
                "event": "stream_end",
                "owner_id": owner_id,
                "session_id": session_id,
            })
            return

        interest_store = getattr(app_module.CACHE, "interest", None) or getattr(app_module.CACHE, "pubsub", None)
        if owner_id == "gadget" and interest_store is not None:
            await interest_store.create_interest_lease(
                owner_id=owner_id,
                lease_id=session_id,
                ttl_seconds=app_module.PUBSUB_GADGET_SESSION_TTL_SECONDS,
                priority=10,
                wants_creation=True,
                wants_content=True,
                wants_inlinks=True,
                qids=qid_list,
            )
            print(
                "Gadget published creation interest: "
                f"owner_id={owner_id}, "
                f"session_id={session_id}, "
                f"qid_count={len(qid_list)}, "
                "wants_creation=True, wants_content=True, wants_inlinks=True"
            )
        else:
            manager, session, interest_store = await app_module._start_interest_stream(
                owner_id=owner_id,
                session_id=session_id,
                qids=qid_list,
                worker_id=f"{owner_id}:{session_id}",
                priority=10,
                wants_creation=True,
                wants_content=True,
                wants_inlinks=True,
            )
            print(
                "Gadget published creation interest: "
                f"owner_id={owner_id}, "
                f"session_id={session_id}, "
                f"qid_count={len(qid_list)}, "
                "wants_creation=True, wants_content=True, wants_inlinks=True"
            )
        yield app_module._sse_message({
            "event": "primed",
            "owner_id": owner_id,
            "session_id": session_id,
            "qid_count": len(qid_list),
        })
        yield app_module._sse_message({
            "event": "primed_count",
            "owner_id": owner_id,
            "session_id": session_id,
            "qid_count": len(qid_list),
        })
        while time.monotonic() < deadline:
            if app_module.SHUTDOWN_EVENT is not None and app_module.SHUTDOWN_EVENT.is_set():
                break
            if await request.is_disconnected():
                break
            trace_batch_id = str(uuid4())
            if session is None and interest_store is not None:
                refresh_interest_lease = getattr(interest_store, "refresh_interest_lease", None)
                if callable(refresh_interest_lease):
                    await refresh_interest_lease(
                        owner_id=owner_id,
                        lease_id=session_id,
                        ttl_seconds=app_module.PUBSUB_GADGET_SESSION_TTL_SECONDS,
                    )

            try:
                cached_rows = await app_module.CACHE.get_many(qid_list)
            except Exception as exc:
                print(
                    f"Notability stream cache lookup failed for subscription {session_id}: {exc}"
                )
                cached_rows = {}

            try:
                creation_metadata = await web_resolve_creation_metadata(qid_list)
            except Exception as exc:
                print(
                    f"Notability stream creation metadata lookup failed for subscription {session_id}: {exc}"
                )
                creation_metadata = {}

            try:
                content_staleness_fetcher = getattr(
                    app_module.CACHE, "get_content_staleness_for_qids", None
                )
                if content_staleness_fetcher is None:
                    content_staleness = {}
                else:
                    content_staleness = await content_staleness_fetcher(qid_list)
            except Exception as exc:
                print(
                    f"Notability stream content staleness lookup failed for subscription {session_id}: {exc}"
                )
                content_staleness = {}

            emitted = False
            for qid in qid_list:
                cached_result = cached_rows.get(qid)
                if cached_result is None:
                    continue
                current_seen = (
                    cached_result.n1,
                    cached_result.n2a,
                    cached_result.n2b,
                    cached_result.n3_inlinks,
                    cached_result.n3_osm,
                    cached_result.n3_wikisub,
                    cached_result.n3_sdc,
                    cached_result.inlinks_count,
                    cached_result.content_last_revid,
                    cached_result.recent_changes_last_revid,
                    content_staleness.get(qid),
                )
                if last_seen.get(qid) == current_seen:
                    continue
                last_seen[qid] = current_seen
                metadata = creation_metadata.get(qid, {})
                payload = {
                    "event": "summary_change",
                    "qid": qid,
                    "event_type": "summary_change",
                    "levels": cached_result.levels_str,
                    "content_last_revid": cached_result.content_last_revid,
                    "is_redirect": cached_result.is_redirect,
                    "has_sitelinks_count": cached_result.has_sitelinks_count,
                    "has_claims_count": cached_result.has_claims_count,
                    "is_deleted": cached_result.is_deleted,
                }
                payload.update(
                    app_module._badge_payload(
                        qid,
                        cached_result,
                        content_stale=content_staleness.get(qid),
                        creator=metadata.get("creator"),
                        creation_time=metadata.get("creation_time"),
                    )
                )
                payload["event"] = "summary_change"
                payload["event_id"] = app_module._payload_signature(payload)
                if app_module._badge_payload_has_meaningful_state(payload):
                    emitted = True
                    try:
                        await app_module._record_badge_served(
                            qid=qid,
                            payload=payload,
                            stream_name="pubsub_session",
                            batch_id=trace_batch_id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        print(f"PubSub badge SSE trace emit failed for {qid}: {exc}")
                    yield app_module._sse_message(payload)

            if not emitted:
                yield app_module._sse_message({"event": "keepalive"})

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if await app_module._sleep_or_shutdown(min(poll_seconds, remaining)):
                break

        if (
            time.monotonic() >= deadline
            and (app_module.SHUTDOWN_EVENT is None or not app_module.SHUTDOWN_EVENT.is_set())
            and not await request.is_disconnected()
        ):
            yield app_module._sse_message({"event": "stream_end"})
    finally:
        if session is not None:
            await session.close()
        await app_module._unregister_stream_task(stream_task)


@router.get("/api/pubsub/sessions/{owner_id}/{session_id}/events")
async def api_pubsub_session_events(
    owner_id: str,
    session_id: str,
    request: Request,
    after_event_id: int = Query(default=0, ge=0),
):
    owner = app_module._normalize_owner_id(owner_id)
    return StreamingResponse(
        _pubsub_event_stream(owner, session_id, request, after_event_id=after_event_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.get("/creations", response_class=HTMLResponse)
async def ui_creations():
    return web_render_creations_dashboard_html()


@router.get("/{filename}.md", include_in_schema=False)
async def static_markdown_page(filename: str):
    return _render_static_markdown_page(STATIC_DIR, f"{filename}.md")
