from __future__ import annotations

import time

from wd_notability.evaluate_common import with_unfinished_criteria_unknown
from wd_notability.evaluation_cache import CACHE
from wd_notability.models import EvaluationResult, NotabilityLevel, QID, Source
from wd_notability.sources import SOURCES
from wd_notability.wikidata import EntityDeletedError


def _is_valid_qid(qid: str) -> bool:
    return len(qid) >= 2 and qid[0] == "Q" and qid[1:].isdigit()


def _normalize_qids(qids: list[QID] | tuple[QID, ...] | set[QID] | list[str]) -> list[QID]:
    deduped: list[QID] = []
    seen: set[str] = set()
    for qid in qids:
        if not isinstance(qid, str):
            continue
        candidate = qid.strip().upper()
        if not _is_valid_qid(candidate) or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


async def evaluate_many(
    qids: list[QID] | tuple[QID, ...] | set[QID] | list[str],
    sources: list[Source] | None = None,
    *,
    stop_on_strong: bool = True,
    update_cache: bool = False,
    possible_sources: list[Source] | None = None,
) -> dict[QID, EvaluationResult]:
    print(
        f"Starting evaluation for {len(qids)} entities with "
        f"stop_on_strong={stop_on_strong}, update_cache={update_cache}"
    )
    qid_list = _normalize_qids(list(qids))
    source_list = SOURCES if sources is None else sources
    possible_source_list = source_list if possible_sources is None else possible_sources
    source_names = {source.name for source in source_list}
    omitted_possible_sources = [
        source for source in possible_source_list
        if source.name not in source_names
    ]

    states: dict[QID, dict[str, object]] = {
        qid: {
            "source_results": [],
            "completed_sources": set(),
            "stopped": False,
            "deleted": False,
            "skipped_sources": [],
        }
        for qid in qid_list
    }

    for source in source_list:
        active_qids = [qid for qid in qid_list if not states[qid]["stopped"]]
        if not active_qids:
            break

        print(f"Running source {source.name} for {len(active_qids)} active entities...")
        fetch_started = time.perf_counter()
        try:
            contexts = await source.get_contexts(active_qids)
        except Exception as exc:  # noqa: BLE001
            contexts = {qid: exc for qid in active_qids}
        fetch_elapsed = time.perf_counter() - fetch_started
        print(
            f"Fetched contexts for {len(active_qids)} entities from source "
            f"{source.name} in {fetch_elapsed:.2f} seconds"
        )

        source_results_by_qid: dict[QID, EvaluationResult] = {}
        contexts_by_qid: dict[QID, object] = {}
        progress_results: list[EvaluationResult] = []

        for qid in active_qids:
            state = states[qid]
            if state["stopped"]:
                continue

            context = contexts.get(qid)
            contexts_by_qid[qid] = context
            if context is None:
                context = KeyError(f"Source {source.name} did not return context for {qid}")

            source_result: EvaluationResult
            if isinstance(context, EntityDeletedError):
                source_result = EvaluationResult(qid=qid, is_deleted=True)
                state["deleted"] = True
                state["stopped"] = True
            elif isinstance(context, Exception):
                source_result = EvaluationResult(qid=qid)
                for detector in source.detectors:
                    source_result.add_error(detector, context)
            else:
                try:
                    context_to_run = context
                    if isinstance(context_to_run, dict):
                        context_to_run = dict(context_to_run)
                        timings = context_to_run.get("_timings")
                        if not isinstance(timings, dict):
                            timings = {}
                        context_to_run["_timings"] = {
                            **timings,
                            "get_context": timings.get("get_context", fetch_elapsed),
                        }
                    source_result = await source._run_context_core(qid, context_to_run)
                except Exception as exc:  # noqa: BLE001
                    source_result = EvaluationResult(qid=qid)
                    for detector in source.detectors:
                        source_result.add_error(detector, exc)

            state["source_results"].append(source_result)
            source_results_by_qid[qid] = source_result
            state["completed_sources"].add(source.name)

        extra_timings = await source.extra_many(active_qids, contexts_by_qid, source_results_by_qid)
        for qid, elapsed in extra_timings.items():
            source_results_by_qid[qid].source_timings["extra"] = elapsed

        get_context_elapsed = fetch_elapsed
        detectors_elapsed = sum(
            source_result.source_timings.get("detectors", 0.0)
            for source_result in source_results_by_qid.values()
        )
        post_process_elapsed = sum(
            source_result.source_timings.get("post_process", source_result.source_timings.get("update_result", 0.0))
            for source_result in source_results_by_qid.values()
        )
        extra_elapsed = sum(
            source_result.source_timings.get("extra", 0.0)
            for source_result in source_results_by_qid.values()
        )

        write_elapsed = 0.0
        for qid in active_qids:
            state = states[qid]
            if state["stopped"]:
                continue

            combined = EvaluationResult.combine(qid, state["source_results"])
            remaining_sources = [
                source_item
                for source_item in source_list
                if source_item.name not in state["completed_sources"]
            ] + omitted_possible_sources

            if update_cache:
                progress_result = combined if state["deleted"] else with_unfinished_criteria_unknown(
                    EvaluationResult.model_validate(combined.model_dump()),
                    remaining_sources,
                )
                progress_results.append(progress_result)

            if state["deleted"] or (stop_on_strong and combined.n == NotabilityLevel.STRONG):
                state["stopped"] = True
                state["skipped_sources"] = remaining_sources

        if update_cache and progress_results:
            write_started = time.perf_counter()
            payload = [
                (
                    result.qid,
                    result.summary,
                    result.entitydata_last_revid,
                    result.recent_changes_last_revid,
                )
                for result in progress_results
            ]
            if len(payload) == 1 and payload[0][2] is None and payload[0][3] is None:
                qid, summary, _, _ = payload[0]
                await CACHE.upsert(qid, summary)
            else:
                await CACHE.upsert_many(payload)
            write_elapsed = time.perf_counter() - write_started
            for progress_result in progress_results:
                source_result = source_results_by_qid.get(progress_result.qid)
                if source_result is not None:
                    source_result.source_timings["write"] = write_elapsed
            progress_results.clear()

        print(
            f"Batch timings for source {source.name}: "
            f"get_context={get_context_elapsed:.2f}s, "
            f"detectors={detectors_elapsed:.2f}s, "
            f"post_process={post_process_elapsed:.2f}s, "
            f"extra={extra_elapsed:.2f}s, "
            f"write={write_elapsed if update_cache and source_results_by_qid else 0.0:.2f}s"
        )

    results: dict[QID, EvaluationResult] = {}
    for qid in qid_list:
        state = states[qid]
        result = EvaluationResult.combine(qid, state["source_results"])
        remaining_at_end = state["skipped_sources"] if state["skipped_sources"] else omitted_possible_sources
        if not result.is_deleted:
            result = with_unfinished_criteria_unknown(
                EvaluationResult.model_validate(result.model_dump()),
                remaining_at_end,
            )
        results[qid] = result

    print(
        f"Completed evaluation for {len(qid_list)} entities with "
        f"stop_on_strong={stop_on_strong}, update_cache={update_cache}"
    )

    return results
