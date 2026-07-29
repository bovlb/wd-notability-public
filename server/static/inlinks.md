# Inlinks Worker

This page describes the interest-driven inlinks pipeline.

It focuses on how the worker keeps a shared blackboard moving, in what order it refreshes counts and graphs, and how it evaluates linked items without backfill.

## Purpose

The inlinks worker owns the active inlinks working set.

Its job is to:

1. watch pubsub lease interest for inlinks targets
2. refresh inlink counts from the Wikidata replica
3. fetch inlink graphs for the active set
4. evaluate targets from the shared blackboard
5. republish short-lived interest for the linked QIDs

This worker is not a general background queue. It is a targeted, interest-driven resolver for items whose inlinks may affect notability.

## Queue sources

The worker pulls work from one source only:

1. PubSub leases

That interest is mirrored into the in-memory blackboard. From there the coroutines decide what needs a count refresh, what needs graph refresh, and what needs evaluation.

## Stateful evaluation

The current implementation keeps the active state in a shared blackboard keyed by QID.

Each entry carries the current count, the current inlink list, and the timestamps needed to decide whether counts or graphs are stale.

The worker does not backfill missing targets. It only advances the interest-driven working set that already exists in pubsub.

## Finalization rules

A target can be finalized in four common ways:

- `N3_inlinks = NONE` when the count is zero or the graph is empty
- `N3_inlinks = STRONG` when any visible inlink is already strong enough
- `N3_inlinks = UNKNOWN` when visible inlinks remain unresolved
- `N3_inlinks = the best known level` when all visible inlinks are resolved and none is strong

If the target stops being interesting, the worker drops the blackboard entry.

## What counts as work

The worker treats these as real work:

- a new subscribed target
- a target whose count has never been fetched
- a target whose graph is stale
- a target that still has unresolved inlinks after a prior pass

It does not treat already dropped items as new queue items.

## Ordering guarantees

The queue order is intentionally conservative:

- active pubsub lease interest comes first
- missing counts are processed before stale counts
- missing graphs are processed before stale graphs
- small graphs are batched, large graphs are fetched one-by-one
- within a batch, QIDs are processed in the order returned by the source query

The worker therefore behaves like a batch-first resolver with explicit priority tiers rather than a single global FIFO queue.

## Implementation notes

The queue selection lives in the pipeline and cache layer, not in a separate scheduler.

The main entry point is `run_inlinks_pipeline()`, which runs the interest fetcher, counts fetcher, graph fetcher, evaluator, and interest publisher together.

If you want the exact SQL, see the cache and pubsub helpers in `wd_notability/evaluation_cache.py`, `wd_notability/inlinks/cache.py`, and `wd_notability/pubsub.py`.
