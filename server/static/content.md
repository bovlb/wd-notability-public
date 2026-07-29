# Content Worker

This page specifies the behaviour of the content worker.

## Work selection

The short version:

1. It processes subscribed QIDs that need content work.
2. It considers a subscribed item stale for one of four reasons: never evaluated, newer recent changes, newer deletion or undeletion history, or a redirect target change.
3. It skips non-deleted items that cannot be assigned a content `lastrevid`.

### Work categories

Content work comes from two practical categories:

#### Subscribed stale items

These are QIDs that have active pubsub lease interest and need content refresh.

They are selected when any of these are true:

1. `content_last_revid` is `NULL`
1. `content_last_revid` is older than `recent_changes_cache.recent_changes_last_revid`
1. the row is stale because of a newer deletion or undeletion event
1. the row is a redirect and the redirect target has changed since the source evaluation

#### Skipped items

These are items that were considered but not written back.

Examples include:

- items blocked by shared Wikidata backoff
- items missing a usable `content_lastrevid`
- items whose API or replica lookup failed

Deleted items are handled separately: they are converted into a deleted result and may still be upserted even when no content revision is available.

### Work sources

Content pulls work from active pubsub leases only.

The query selects QIDs from `pubsub_sessions` that:

- have `wants_content = 1`
- are not marked deleted in the main cache unless a deletion event makes that row stale again
- have never been evaluated for content, or have a stale content revision compared with `recent_changes_cache.recent_changes_last_revid`
- have a stale redirect target evaluation when the row is a redirect

The result is ordered by:

- summed subscriber priority, descending
- never-evaluated first
- QID, ascending

The worker then claims up to the requested batch size, skipping anything already in flight in this process.

### Content staleness criteria

A pubsub candidate is eligible when any of these are true:

1. `content_last_revid` is `NULL`
1. `content_last_revid < recent_changes_cache.recent_changes_last_revid`
1. `last_updated` is older than the newest deletion event for that QID
1. `last_updated` is older than the current content-policy cutoff set by the reset command
1. the row is a redirect and its target has changed since the source evaluation

That is the complete set of selector reasons.

### Request retries

Content requests retry individually on transient failures and 429 responses.

There is no shared limiter or queue-selection backoff layer.

### Chunking

Selected QIDs are evaluated in chunks of `CONTENT_EVALUATION_CHUNK_SIZE` items.

That means one worker batch may contain multiple chunks, but each chunk is independently evaluated and upserted.

### Redirect handling

Redirects are special.

When content sees a redirect and replica access is enabled, it verifies the redirect against the replica `page` and `redirect` tables.

For redirects, the worker records:

- the source page revision from the replica `revision` table
- the redirect target QID
- the original entity revision returned by the API
- the redirect target in the main cache

Redirect rows are treated as potentially stale even when the redirect source itself has not changed. If the source evaluation is still fresh, the worker checks whether the target's content evaluation timestamp is newer than the source evaluation timestamp and refreshes the redirect when it has.

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
