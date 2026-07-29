# Recent Changes Worker

This page describes what the recent changes worker does and how it decides what work to perform.

## Purpose

The worker keeps the main cache fresh from Wikidata recent changes, and it also opportunistically fills in missing creation metadata for cached items and queued creator requests.

It is not a generic catch-all queue. It has three related jobs:

1. Update `recent_changes_last_revid` in `recent_changes_cache` for QIDs seen in `recentchanges`, and seed creation metadata from creation events when the source row says the item is new.
2. Backfill `creation_time` and `creator_actor_id` in `recent_changes_cache` for cached rows that are still missing them.
3. Backfill creation data for queued user requests in `user_history`.

## State

The worker keeps one saved cursor in `lookup_state` under `recent_changes_worker_cursor`.

That cursor stores:

- `rc_timestamp`
- `rc_id`

If there is no saved cursor, the worker (by default) starts from 24 hours ago.

If the cache is reset, the cursor is reset too, so the worker starts from the same 24 hour bootstrap window again.

## Main loop

The worker runs three coroutines in parallel:

1. Recent changes scan: load the saved cursor, clamp the start point, read recent changes from the Wikidata replica in batches, update the newest revid per QID, seed creation metadata from `mw.new` rows, save the newest cursor position, and sleep.
2. Creation-interest backfill: read interested QIDs from pubsub rows with `wants_creation = 1`, backfill their creation metadata, and sleep.
3. User-creation backfill: read queued creator requests from `user_history`, backfill their creation metadata, and sleep.

The worker uses a file lock so only one copy of the recent changes worker runs at a time.

## Recent changes source

The worker reads from the `recentchanges` table on the Wikidata replica.

The query currently:

- restricts to namespace 0
- skips deletion-log rows by filtering out `rc_log_type = 'delete'`
- orders by `rc_timestamp ASC, rc_id ASC`
- pages through the result set using the saved timestamp and `rc_id`

For each row, the worker records:

- `title` as a normalized QID
- `creator_actor_id` from `rc_actor`
- `revid` from `rc_this_oldid`
- `old_revid` from `rc_last_oldid`
- `timestamp` as an ISO UTC string

The worker treats the row as a creation event when:

- `rc_source == 'mw.new'`

The worker still stores `rc_this_oldid` and `rc_last_oldid` in case they are useful for debugging, but `rc_source` is the creation signal.

That creation signal is used to seed creation metadata in `recent_changes_cache`.

## Cache update behavior

For recent changes rows, the worker builds a per-QID map and writes each QID once per pass using the highest revid seen for that QID.

For creation events, the worker stores:

- `creation_time`
- `creator_actor_id`

These creation rows may come from:

- the recent changes pass itself, or
- the separate creation backfill pass

## Creation backfill

The worker also looks for cached rows missing creation metadata.

It asks the main cache for up to `RECENT_CHANGES_CREATION_BACKFILL_LIMIT` QIDs that are missing `creation_time` or `creator_actor_id`, then resolves them through the creation metadata source.

This backfill is intentionally limited per cycle.

Important: the backfill only works on rows that are already in the main cache. It does not invent new cached QIDs.

## Queue reporting

The worker reports three different kinds of work:

- `lag`: how far the recent changes cursor is behind real time, measured by the scan coroutine
- `scan_range`: the effective recent-changes timerange scanned in that pass, from the chosen start cursor to the newest row reached
- `live_creation`: how many creation rows were discovered in the recent-changes pass
- `backfill_creation`: how many creation-interest rows were repaired by the creation-interest backfill pass
- `backfill_range`: the creation metadata date range processed by the creation-interest backfill pass
- `user_creation_backfill`: how many queued user requests were serviced by the user-creation backfill pass

The backlog estimate still combines:

- recent changes backlog on the replica, based on the saved cursor
- creation-interest rows still pending in pubsub
- queued user creation requests still waiting in `user_history`

This is only an estimate. It is useful for seeing whether the worker still has meaningful work to do.

## Retry and overlap

The worker keeps a small 5 second overlap when advancing its cursor.

That overlap is there to reduce the risk of missing rows around restart boundaries.

The worker does not call the Wikidata API during its normal recent-changes pass, so it does not share the content API backoff path.

## What it does not do

The worker does not:

- scan the full historical deletion log
- rebuild the main cache from scratch
- populate the creations report directly
- treat deletion-log entries as normal recent changes rows

The creations report reads the main cache. The recent changes worker only makes that cache more complete.
