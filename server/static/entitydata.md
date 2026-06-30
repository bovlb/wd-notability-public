# EntityData Worker

This page specifies the behaviour of the EntityData worker.

## Work selection

The short version:

1. It gives priority to deletion-log entries when replica access is enabled.
2. It then processes subscribed QIDs that need EntityData work.
3. It stops selecting new work when the shared Wikidata API backoff is active.
4. It skips non-deleted items that cannot be assigned an EntityData `lastrevid`.

### Work categories

EntityData work comes from three practical categories:

#### Deletion-log items

These are items seen in the Wikidata deletion log.

They are handled first, because deletion status needs to be reflected even if the item is not currently subscribed.

#### Subscribed stale items

These are QIDs that have active pubsub interest and need EntityData refresh.

They are selected when:

- the item is subscribed for EntityData
- the item is not deleted in the cache
- the item has never been evaluated for EntityData, or its EntityData revision is older than the recent-changes revision

#### Skipped items

These are items that were considered but not written back.

Examples include:

- items blocked by shared Wikidata backoff
- items missing a usable `entitydata_lastrevid`
- items whose API or replica lookup failed

Deleted items are handled separately: they are converted into a deleted result and may still be upserted even when no EntityData revision is available.

### Work sources

EntityData pulls work from two places.

#### 1. Deletion log

The worker scans the `logging` table on the Wikidata replica for namespace 0 deletion log entries:

- `log_namespace = 0`
- `log_type = 'delete'`
- `log_id > last_saved_cursor`
- ordered by `log_id ASC`

Note that these will include both deletion and undeletion events.

The worker:

- deduplicates QIDs seen in the log batch
- claims only QIDs that are not already in flight in the current process
- processes this work before normal queue items
- starts from about one day back if it has no saved cursor yet
- treats a saved zero cursor as "start one day back"

The saved cursor advances only after a batch is processed, and it is tracked as a timestamp plus a log id tie-breaker.

####2. PubSub entity-data candidates

After the deletion-log pass, the worker asks the main cache for pubsub candidates.

The query selects QIDs from `pubsub_sessions` that:

- are not `Q0` (a placeholder for sessions)
- have `wants_entitydata = 1`
- are not marked deleted in the main cache, because deleted items cannot advance their revision
- either have never been evaluated for EntityData, or have a stale EntityData revision compared with `recent_changes_last_revid`

The result is ordered by:

- summed subscriber priority, descending
- never-evaluated first
- QID, ascending

The worker then claims up to the requested batch size, skipping anything already in flight in this process.

### What counts as stale

A pubsub candidate is eligible when:

- `entitydata_last_revid` is `NULL`, or
- `entitydata_last_revid < recent_changes_last_revid`

This is the main rule that decides whether an item needs another EntityData run.

### Shared backoff

EntityData checks the shared Wikidata API backoff before queue selection and again before each chunk.

If the API is under backoff, the worker does not pick new EntityData work.

### Chunking

Selected QIDs are evaluated in chunks of `ENTITYDATA_EVALUATION_CHUNK_SIZE` items.

That means one worker batch may contain multiple chunks, but each chunk is independently evaluated and upserted.

### Redirect handling

Redirects are special.

When EntityData sees a redirect and replica access is enabled, it verifies the redirect against the replica `page` and `redirect` tables.

For redirects, the worker records:

- the source page revision from the replica `revision` table
- the redirect target QID
- the original entity revision returned by the API

If the replica state does not still match the redirect target, the item is treated as an error for that run.

## Processing

The N1/2 detectors are run:
* N1: Sitelinks
* N2a: Identifiers
* N2b: Sources

We also detect:
* Is this item a redirect? If so, we perform N1/2 analysis of the target, but record the source revid.
* Is this page deleted? See also deletiion scanning above.
* Does this page have claims? This is used to label the page as empty.
* Does this page have sitelinks? Not really used.
* Revision id at which we made the evaluation. (Not for deletion.)
