# Help

Use these pages as the starting point for the UI and the implementation details behind it.

High level: the dashboard and gadget express interest in items, the backend serves badge data from cache with an update stream, and the content, inlinks, and recent-changes workers respond to that interest. The layers are intentionally decoupled so the UI can paint the item you are looking at without owning the worker pipeline.

## Help pages

- [Badge](badge.md) - explains what each color and region of the notability badge means.
- [Detectors](detectors.md) - describes the evidence sources and how each criterion is judged.
- [Content worker](content.md) - explains how content picks deletion-log work, subscribed work, and stale items.
- [Data flow and caches](data-flow.md) - describes the worker pipeline, cache layout, and update paths.
- [Inlinks worker](inlinks.md) - explains how inlinks keeps its interest-driven working set moving.
- [Creations classifications](creations.md) - explains the buckets used in the creations dashboard.
- [Recent changes worker](recent-changes.md) - explains how recent changes, creation-interest backfill, and user-request backfill share the same monitor.

## Tools

- [Creations dashboard](../creations?start=1h&group_by=user&bucket_sort=strong_rate_asc&min_user_items=5) - fixed population report with local aggregation and live evaluation updates, preset to 1 hour, user buckets, quality sort, and a minimum of 5 items per user.
- [Observability](../observability) - baby Grafana view for aggregated worker snapshots and time-series inspection.
