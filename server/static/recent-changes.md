# Recent Changes Worker

This page describes what the recent changes worker does and how it decides what work to perform.

## Purpose

The worker keeps the main cache fresh from Wikidata recent changes, and it also opportunistically fills in missing creation metadata for cached items.

It is not a generic catch-all queue. It has two specific jobs:

1. Update `recent_changes_last_revid`, `creation_time` and `creator_actor_id` for QIDs seen in `recentchanges`.
2. Backfill `creation_time` and `creator_actor_id` for cached rows that are still missing them.

## State

The worker keeps one saved cursor in `lookup_state` under `recent_changes_worker_cursor`.

That cursor stores:

- `rc_timestamp`
- `rc_id`

If there is no saved cursor, the worker (by default) starts from 24 hours ago.

If the cache is reset, the cursor is reset too, so the worker starts from the same 24 hour bootstrap window again.

## Main loop

The worker runs in a loop:

1. Load the saved cursor.
2. Clamp the start point so it never goes earlier than 24 hours ago on a fresh bootstrap.
3. Read recent changes from the Wikidata replica in batches.
4. Update the main cache with the newest revid per QID.
5. Backfill creation metadata for a limited number of cached rows.
6. Save the newest cursor position.
7. Sleep until the next cycle.

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

That creation signal is used to seed creation metadata in the main cache.

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

The worker reports two different kinds of work:

- `lag`: how far the recent changes cursor is behind real time, measured immediately after the recent-changes pass and before creation backfill runs
- `scan_range`: the effective recent-changes timerange scanned in that pass, from the chosen start cursor to the newest row reached
- `live_creation`: how many creation rows were discovered in the recent-changes pass
- `backfill_creation`: how many missing creation rows were repaired by the backfill pass
- `backfill_range`: the creation metadata date range processed by the backfill pass

The backlog estimate still combines:

- recent changes backlog on the replica, based on the saved cursor
- creation metadata rows still missing in the cache

This is only an estimate. It is useful for seeing whether the worker still has meaningful work to do.

## Retry and overlap

The worker keeps a small 5 second overlap when advancing its cursor.

That overlap is there to reduce the risk of missing rows around restart boundaries.

The worker does not call the Wikidata API during its normal recent-changes pass, so it does not share the EntityData API backoff path.

## What it does not do

The worker does not:

- scan the full historical deletion log
- rebuild the main cache from scratch
- populate the creations report directly
- treat deletion-log entries as normal recent changes rows

The creations report reads the main cache. The recent changes worker only makes that cache more complete.
