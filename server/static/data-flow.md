# Data Flow and Caches

This document describes where each detector gets its data from and how the caches are updated.

It is intentionally separate from [detectors.md](detectors.md), which is editor-facing and describes how evidence is judged rather than how the system is implemented.

## Main cache

The main evaluation cache stores only the detected criteria:

- `N1`
- `N2a`
- `N2b`
- `N3_inlinks`
- `N3_osm`
- `N3_wikisub`
- `N3_sdc`

Derived values such as `N2`, `N3`, `N12`, and `N` are computed when results are assembled for the UI or for downstream evaluation.

## Lookup cache

The lookup cache is a separate database used for data that is expensive to query repeatedly.

It currently holds:

- namespace metadata
- property metadata
- OSM usage
- SDC usage
- wiki subscriber membership

The lookup cache can be backed by SQLite for local development or MariaDB in production.

## Sources

### EntityData

EntityData fetches item data from the Wikidata API with `wbgetentities`.

It provides:

- the entity payload for detectors
- redirect information
- claims and sitelink presence
- delete information
- source URLs for the UI

It also has an `extra` step that inspects outlinks found in claims, qualifiers, and reference snaks. Those outlinks are used to update `N3_inlinks` on other items.

### Inlinks

Inlinks fetches backlink data from the Wikidata replica.

The source itself only supplies the inlink list for each item. A separate worker processes items with unknown `N3_inlinks` and decides whether to:

- set `N3_inlinks` directly
- enqueue missing inlinks for `N12` evaluation
- leave `N3_inlinks` unknown while linked items are still unresolved

For the queue order and refresh behavior, see [Inlinks worker](inlinks.md).

### OSM

OSM reads prebuilt usage data from the lookup cache.

When a QID is present in the OSM usage cache, the source sets `N3_osm` to `WEAK`.

### SDC

SDC reads prebuilt Commons structured-data usage from the lookup cache.

The cache is built by downloading a TTL dump of Commons SDC and extracting Wikidata ids.

When a QID is present in the SDC usage cache, the source sets `N3_sdc` to `STRONG`.

### Wiki subscribers

Wiki subscribers reads the cached set of Wikimedia items used by non-Wikidata wikis.

The cache is rebuilt from `wb_changes_subscription` in a ratchet-style process:

- a full rebuild creates a fresh cache from the current table contents
- a follow-up updater polls for new rows and records new QIDs as they are added, but does not detect deletion.

When a QID is present in the wiki-subscriber cache, the source sets `N3_wikisub` to `STRONG`.

## Worker behavior

### Foreground requests

Foreground requests run the configured sources in parallel where possible, but still preserve the foreground priority model.

### N12 evaluation

The foreground evaluator handles only `N12` work.

It takes queued items, evaluates the configured foreground sources for those items, and writes the resulting N1, N2a, N2b, and derived N12 state back to the main cache.

The foreground evaluator does not own the N3 sources. Those are handled by separate workers:

- the EntityData worker owns entity data, sitelinks, claims, and the direct N1/N2 criteria
- the Inlinks worker owns `N3_inlinks`
- the cache sync worker owns `N3_osm` and `N3_sdc`
- the wiki-subscriber updater owns `N3_wikisub`

### Inlinks worker

The inlinks worker scans items with unknown `N3_inlinks`, fetches their inlinks, and then follows the queue behavior described in [Inlinks worker](inlinks.md).
