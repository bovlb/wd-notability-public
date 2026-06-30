# Inlinks Worker

This page describes the intended queue behavior of the Inlinks worker.

It focuses on how the worker chooses targets, in what order it looks at them, and what it does with items that are not fully resolved yet.

## Purpose

The Inlinks worker owns `N3_inlinks`.

Its job is to:

1. find items that need inlinks evaluation
2. fetch the inlinks for those items from the Wikidata replica
3. decide whether `N3_inlinks` can be written directly
4. enqueue unresolved linking items for `N12` evaluation
5. keep long-lived unresolved targets moving until they can be finalized

This worker is not a general background queue. It is a targeted resolver for items whose inlinks may affect notability.

## Queue sources

The worker pulls work from three sources, in this order:

1. PubSub interest
2. No-interest unknowns
3. low-priority refresh candidates

### 1. PubSub interest

This is the main queue.

The worker asks the cache for QIDs that have active PubSub interest in inlinks work and are still unknown for `N3_inlinks`.

Those targets are:

- grouped by QID
- ordered by summed subscriber priority, highest first
- filtered so the worker does not compete with another owner for the same QID

If a target already has an in-flight state in the current process, the worker keeps that state instead of creating a duplicate.

### 2. No-interest unknowns

If there are no active PubSub interest targets and no active inlinks states, the worker falls back to no-interest unknowns.

These are QIDs in the main cache whose `N3_inlinks` is still unknown and which do not already have PubSub interest from another owner.

This fallback is intentionally limited and rate-limited:

- it only runs when the worker is otherwise idle
- it uses a cooldown so the same QID is not repeatedly chosen in a tight loop
- it only initializes a bounded batch each pass

This is the bootstrap path for items that still need a first inlinks check even without explicit subscription.

### 3. Low-priority refresh candidates

After the main and cache-only paths, the worker looks for stale but known items that should be refreshed.

These are items that:

- already have a known `N3_inlinks` result
- are not deleted
- have a creation time and a previous `inlinks_last_evaluated` value
- are not already owned by another inlinks session

The worker scores those candidates by age and last evaluation time, then takes the highest-scoring items up to the configured in-flight cap.

This is the worker’s maintenance path: it keeps old resolutions from staying stale forever, but it never crowds out active interest work.

## Stateful evaluation

When the worker picks a target, it does not just evaluate it once and forget it.

Instead, it creates a session for the target and watches unresolved inlinks in a rolling window.

The state machine is:

1. fetch the target’s visible inlinks from the replica
2. resolve whatever inlinks are already cached
3. if the item is fully resolved, write `N3_inlinks`
4. if some inlinks are still unknown, create or refresh a session
5. queue the unresolved inlinks for `N12` evaluation
6. revisit the target when those inlinks become available

The worker keeps a bounded watch window so it can make forward progress without holding an unbounded number of inlinks in flight.

## Finalization rules

A target can be finalized in three common ways:

- `N3_inlinks = NONE` when there are no visible inlinks and the result is not truncated
- `N3_inlinks = STRONG` when any visible inlink is already strong enough
- `N3_inlinks = UNKNOWN` when visible inlinks remain unresolved and no strong evidence is found yet
- `N3_inlinks = the best known level` when all visible inlinks are resolved and none is strong

If the target is truncated and unresolved inlinks remain, the worker keeps the state alive until it can learn more.

If the target stops being interesting, the worker drops the state and deletes the associated session.

## What counts as work

The worker treats these as real work:

- a new subscribed target
- a cache-only unknown item that has never been checked in practice
- a stale known item eligible for refresh
- a target that still has unresolved inlinks after a prior pass

It does not treat already owned or already resolved items as new queue items.

## Ordering guarantees

The queue order is intentionally conservative:

- active PubSub interest comes first
- cache-only fallback only runs when there is no active interest and no current state
- low-priority refreshes only run after the higher-priority paths
- within a batch, QIDs are processed in the order returned by the source query

The worker therefore behaves like a batch-first resolver with explicit priority tiers rather than a single global FIFO queue.

## Implementation notes

The queue selection lives in the worker and cache layer, not in a separate scheduler.

The main entry point is `work_inlinks_pass()`, which:

1. loads active PubSub targets
2. reconciles current state against that target set
3. initializes new targets in batches
4. polls active sessions for newly resolved inlinks
5. falls back to cache-only unknowns when idle
6. appends low-priority refresh work

The underlying queries are:

- `list_pubsub_inlinks_targets()` for subscribed work
- `list_unknown_inlinks_qids()` for cache-only unknowns
- `list_known_inlinks_refresh_candidates()` for stale refresh work

If you want the exact SQL, see the cache and pubsub helpers in `wd_notability/inlinks/cache.py` and `wd_notability/pubsub.py`.
