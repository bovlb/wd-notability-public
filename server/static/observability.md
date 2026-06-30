# Observability

This page explains the worker snapshot trail used by the baby Grafana view.

- Each worker emits one JSON snapshot periodically, about once every 60 seconds.
- Snapshots are stored in the tool database as append-only log rows.
- The API reads those rows back, aggregates numeric values across all workers, and returns `field -> series`.
- The API also publishes static field descriptions so the UI can show hover help without storing extra text in the log table.
- Throughput cards prefer the worker's rolling rate when one is reported, and fall back to a derived rate only when needed.
- The UI groups tiles by worker, hides each worker in a collapsible section, and lets you click a square to open a zoom chart.
- Nested objects are flattened with dotted field names, so a payload like `{ "queue": { "total": 10 } }` becomes `queue.total`.
- The UI focuses on time-series inspection, not dashboard configuration.

For now, the EntityData worker is the first source wired into the log table.
