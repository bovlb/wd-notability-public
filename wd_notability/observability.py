from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


class ObservabilityStore:
    def __init__(self, cache: EvaluationCache):
        self.cache = cache

    @staticmethod
    def _worker_group_name(worker_name: str) -> str:
        raw = worker_name.strip()
        if not raw:
            raise ValueError("worker_name must not be empty")
        base = raw.split("/", 1)[0]
        return {
            "entitydata": "content",
            "recent_changes": "recent changes",
            "cache_sync": "external usage",
            "deletion": "content deletion",
        }.get(base, base.replace("_", " "))

    @staticmethod
    def _normalize_worker_name(worker_name: str) -> str:
        worker = worker_name.strip()
        if not worker:
            raise ValueError("worker_name must not be empty")
        return worker

    @staticmethod
    def _normalize_timestamp(timestamp: int | float | None) -> int:
        if timestamp is None:
            return int(time.time())
        return int(timestamp)

    @staticmethod
    def _flatten_data(data: Mapping[str, Any], *, prefix: str = "") -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        for key, value in data.items():
            field = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, Mapping):
                flattened.update(ObservabilityStore._flatten_data(value, prefix=field))
            else:
                flattened[field] = value
        return flattened

    async def record_worker_snapshots(
        self,
        snapshots: Sequence[tuple[str, Mapping[str, Any], int | float | None]],
    ) -> int:
        await self.cache.initialize()

        normalized: list[tuple[int, str, str]] = []
        for worker_name, data, timestamp in snapshots:
            worker = self._normalize_worker_name(worker_name)
            normalized.append(
                (
                    self._normalize_timestamp(timestamp),
                    worker,
                    json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                )
            )

        if not normalized:
            return 0

        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                if self.cache._backend_name == "mariadb":
                    rows = [(timestamp, worker_name, data) for timestamp, worker_name, data in normalized]
                    await db.executemany(
                        """
                        INSERT INTO worker_observability_log (`timestamp`, worker_name, data)
                        VALUES (?, ?, ?)
                        """,
                        rows,
                    )
                else:
                    rows = [(timestamp, worker_name, data) for timestamp, worker_name, data in normalized]
                    await db.executemany(
                        """
                        INSERT INTO worker_observability_log (`timestamp`, worker_name, data)
                        VALUES (?, ?, ?)
                        """,
                        rows,
                    )
                await db.commit()
        self.cache._warn_slow_write("record_worker_snapshots", started, row_count=len(normalized))
        return len(normalized)

    async def record_worker_snapshot(
        self,
        *,
        worker_name: str,
        data: Mapping[str, Any],
        timestamp: int | float | None = None,
    ) -> None:
        await self.record_worker_snapshots([(worker_name, data, timestamp)])

    async def list_worker_snapshots(
        self,
        *,
        since: int,
        until: int | None = None,
        worker_names: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        await self.cache.initialize()

        clauses = ["`timestamp` >= ?"]
        params: list[Any] = [int(since)]
        if until is not None:
            clauses.append("`timestamp` <= ?")
            params.append(int(until))
        if worker_names:
            workers = [self._normalize_worker_name(worker_name) for worker_name in worker_names if str(worker_name).strip()]
            if workers:
                placeholders = ", ".join("?" for _ in workers)
                clauses.append(f"worker_name IN ({placeholders})")
                params.extend(workers)
        sql = (
            "SELECT `timestamp`, worker_name, data "
            "FROM worker_observability_log "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY timestamp ASC, worker_name ASC"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        async with self.cache._connect() as db:
            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()

        results: list[dict[str, Any]] = []
        for timestamp, worker_name, data in rows:
            payload: Any
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            try:
                payload = json.loads(data)
            except (TypeError, ValueError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            results.append(
                {
                    "timestamp": int(timestamp),
                    "worker_name": str(worker_name),
                    "data": payload,
                }
            )
        return results

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def _derive_rate_from_cumulative_series(series: list[tuple[int, Any]], *, window_size: int = 4) -> list[tuple[int, float]]:
        if not series:
            return []
        if len(series) == 1:
            timestamp, _value = series[0]
            return [(int(timestamp), 0.0)]

        raw_deltas: list[tuple[int, float, float]] = []
        previous_timestamp = int(series[0][0])
        previous_total = float(series[0][1])
        for timestamp, total in series[1:]:
            current_timestamp = int(timestamp)
            current_total = float(total)
            delta_seconds = current_timestamp - previous_timestamp
            if delta_seconds <= 0:
                previous_timestamp = current_timestamp
                previous_total = current_total
                continue
            delta_total = current_total - previous_total
            if delta_total < 0:
                delta_total = 0.0
            raw_deltas.append((current_timestamp, max(0.0, delta_total), float(delta_seconds)))
            previous_timestamp = current_timestamp
            previous_total = current_total

        if not raw_deltas:
            timestamp, _value = series[-1]
            return [(int(timestamp), 0.0)]

        smoothed: list[tuple[int, float]] = [(int(series[0][0]), 0.0)]
        for index, (timestamp, _delta_total, _delta_seconds) in enumerate(raw_deltas):
            window = raw_deltas[max(0, index - window_size + 1) : index + 1]
            total_processed = sum(delta_total for _ts, delta_total, _elapsed in window)
            total_elapsed = sum(delta_seconds for _ts, _delta_total, delta_seconds in window)
            smoothed.append(
                (
                    int(timestamp),
                    total_processed / total_elapsed if total_elapsed > 0 else 0.0,
                )
            )
        return smoothed

    @classmethod
    def _series_from_points(cls, points_by_timestamp: Mapping[int, float]) -> list[tuple[int, float]]:
        series: list[tuple[int, float]] = []
        for timestamp in sorted(points_by_timestamp):
            value = float(points_by_timestamp[timestamp])
            series.append((int(timestamp), int(value) if value.is_integer() else value))
        return series

    @classmethod
    def _aggregate_family_series(cls, points_by_family: Mapping[str, Mapping[int, float]]) -> list[tuple[int, float]]:
        if not points_by_family:
            return []

        timestamps = sorted({int(timestamp) for points in points_by_family.values() for timestamp in points})
        if not timestamps:
            return []

        current_values: dict[str, float] = {}
        series: list[tuple[int, float]] = []
        for timestamp in timestamps:
            for family_name, points in points_by_family.items():
                current_value = points.get(timestamp)
                if current_value is not None:
                    current_values[family_name] = float(current_value)
            if not current_values:
                continue
            total = sum(current_values.values())
            series.append((timestamp, int(total) if float(total).is_integer() else total))
        return series

    async def snapshot_views(
        self,
        *,
        since: int,
        until: int | None = None,
        worker_names: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> tuple[dict[str, list[tuple[int, Any]]], dict[str, dict[str, list[tuple[int, Any]]]]]:
        rows = await self.list_worker_snapshots(
            since=since,
            until=until,
            worker_names=worker_names,
            limit=limit,
        )

        all_timestamps = sorted({int(row["timestamp"]) for row in rows})
        field_family_points: dict[str, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        for row in rows:
            family_name = self._worker_group_name(str(row["worker_name"]))
            timestamp = int(row["timestamp"])
            data = row["data"]
            if not isinstance(data, dict):
                continue
            flattened = self._flatten_data(data)
            for field, value in flattened.items():
                if not self._is_numeric(value):
                    continue
                field_family_points[field][family_name][timestamp] += float(value)

        aggregated: dict[str, list[tuple[int, Any]]] = {}
        family_series: dict[str, dict[str, list[tuple[int, Any]]]] = {}

        for field, points_by_family in field_family_points.items():
            per_family_index = {
                family_name: 0
                for family_name in points_by_family
            }
            per_family_current: dict[str, float] = {}
            series_points: list[tuple[int, Any]] = []
            per_family_output: dict[str, list[tuple[int, Any]]] = {family_name: [] for family_name in points_by_family}

            for timestamp in all_timestamps:
                for family_name, points in points_by_family.items():
                    current_value = points.get(timestamp)
                    if current_value is not None:
                        per_family_current[family_name] = current_value
                    if family_name not in per_family_current:
                        continue
                    per_family_output[family_name].append(
                        (
                            timestamp,
                            int(per_family_current[family_name])
                            if float(per_family_current[family_name]).is_integer()
                            else per_family_current[family_name],
                        )
                    )

                if not per_family_current:
                    continue

                total = sum(per_family_current.values())
                series_points.append(
                    (
                        timestamp,
                        int(total) if float(total).is_integer() else total,
                    )
                )

            if series_points:
                aggregated[field] = series_points
            if per_family_output:
                for family_name, series in per_family_output.items():
                    if series:
                        family_series.setdefault(family_name, {})[field] = series

        throughput_points = field_family_points.get("throughput.total_processed")
        throughput_rate_points = field_family_points.get("throughput.rate_per_second")
        if throughput_points:
            aggregated_throughput = self._aggregate_family_series(throughput_points)
            if aggregated_throughput:
                aggregated["throughput.total_processed"] = aggregated_throughput
                if not throughput_rate_points:
                    aggregated["throughput.rate_per_second"] = self._derive_rate_from_cumulative_series(aggregated_throughput)
            for family_name, points in throughput_points.items():
                family_throughput = self._series_from_points(points)
                if not family_throughput:
                    continue
                family_fields = family_series.setdefault(family_name, {})
                family_fields["throughput.total_processed"] = family_throughput
                family_rate_points = throughput_rate_points.get(family_name) if throughput_rate_points else None
                if not family_rate_points:
                    family_fields["throughput.rate_per_second"] = self._derive_rate_from_cumulative_series(family_throughput)

        return aggregated, family_series

    async def aggregate_worker_snapshots(
        self,
        *,
        since: int,
        until: int | None = None,
        worker_names: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, list[tuple[int, Any]]]:
        aggregated, _transposed = await self.snapshot_views(
            since=since,
            until=until,
            worker_names=worker_names,
            limit=limit,
        )
        return aggregated

    async def transposed_worker_snapshots(
        self,
        *,
        since: int,
        until: int | None = None,
        worker_names: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, dict[str, list[tuple[int, Any]]]]:
        _aggregated, transposed = await self.snapshot_views(
            since=since,
            until=until,
            worker_names=worker_names,
            limit=limit,
        )
        return transposed
